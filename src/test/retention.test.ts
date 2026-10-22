import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { startRetention, type RetentionOptions } from "../retention.js";
import { PathMatcher } from "../path-matcher.js";

type RetentionClient = Parameters<typeof startRetention>[0];

interface Script {
  /** Expired partitions per table, as table_partitions() returns them. */
  partitions?: Record<string, [string, string][]>;
  /** Own-vessel paths recorded in a signalk partition. */
  paths?: string[];
  /** count() answer for a statement; defaults to 10. */
  count?: (sql: string) => number;
  /** Throw for a statement matching this. */
  failOn?: RegExp;
  suspended?: boolean;
}

const DAY_ONE: [string, string] = ["2026-08-01", "2026-08-01T23:59:58.000000Z"];
const DAY_TWO: [string, string] = ["2026-08-02", "2026-08-02T23:59:58.000000Z"];

function scripted(script: Script) {
  const statements: string[] = [];
  const client: RetentionClient = {
    exec: async (sql: string) => {
      statements.push(sql);
      if (script.failOn?.test(sql)) throw new Error(`refused: ${sql}`);
      const answer = (dataset: unknown[][]) => ({
        columns: [],
        dataset,
        count: dataset.length,
        timestamp: 0,
      });
      const partitions = /table_partitions\('(\w+)'\)/.exec(sql);
      if (partitions) return answer(script.partitions?.[partitions[1]] ?? []);
      if (sql.startsWith("SELECT DISTINCT path")) {
        return answer((script.paths ?? []).map((p) => [p]));
      }
      if (sql.startsWith("SELECT count()")) {
        return answer([[script.count ? script.count(sql) : 10]]);
      }
      if (sql.startsWith("SELECT suspended")) {
        return answer([[script.suspended === true, 7, 7]]);
      }
      return answer([]);
    },
  };
  return { client, statements };
}

const OPTIONS: RetentionOptions = {
  retentionDays: 30,
  archiveOwnTrack: true,
  archivePaths: null,
  archiveResolutionSeconds: 10,
};

/** Run retention once and return what it sent, stopped before the next day. */
async function runOnce(
  script: Script,
  options: Partial<RetentionOptions> = {},
): Promise<{ statements: string[]; logs: string[]; archived: number }> {
  const { client, statements } = scripted(script);
  const logs: string[] = [];
  let archived = 0;
  let done!: () => void;
  const finished = new Promise<void>((resolve) => (done = resolve));
  const retention = startRetention(
    client,
    { ...OPTIONS, ...options },
    (msg) => {
      logs.push(msg);
      if (msg.startsWith("Retention check complete")) done();
    },
    async () => {
      archived++;
    },
  );
  assert.ok(retention);
  await finished;
  retention.stop();
  return { statements, logs, archived };
}

const index = (statements: string[], pattern: RegExp) =>
  statements.findIndex((s) => pattern.test(s));

describe("retention", () => {
  it("does nothing with retention off", () => {
    assert.equal(
      startRetention(
        scripted({}).client,
        { ...OPTIONS, retentionDays: 0 },
        () => {},
      ),
      null,
    );
  });

  it("copies the own track into the archive, waits for it, then drops the day", async () => {
    const { statements, archived } = await runOnce({
      partitions: { signalk_position: [DAY_ONE] },
    });

    const copy = index(statements, /^INSERT INTO signalk_position_archive /);
    const applied = index(
      statements,
      /^SELECT suspended, writerTxn, sequencerTxn FROM wal_tables\(\) WHERE name = 'signalk_position_archive'/,
    );
    const drop = index(
      statements,
      /^ALTER TABLE signalk_position DROP PARTITION LIST '2026-08-01'$/,
    );
    assert.ok(copy >= 0, "expected a copy into the archive");
    assert.ok(copy < applied && applied < drop, statements.join("\n"));
    assert.equal(archived, 1);

    const sql = statements[copy];
    assert.match(sql, /context = 'self'/);
    // From midnight, not from the partition's first row: the first bucket
    // of a thinned copy starts there.
    assert.match(
      sql,
      /ts >= '2026-08-01T00:00:00\.000Z' AND ts <= '2026-08-01T23:59:58\.000Z'/,
    );
    assert.match(sql, /first\(lat\) lat, first\(lon\) lon/);
    assert.match(sql, /SAMPLE BY 10s ALIGN TO CALENDAR$/);
  });

  it("copies every row when the archive resolution is 0", async () => {
    const { statements } = await runOnce(
      { partitions: { signalk_position: [DAY_ONE] } },
      { archiveResolutionSeconds: 0 },
    );
    const copy = statements.find((s) => s.startsWith("INSERT INTO"));
    assert.ok(copy);
    assert.doesNotMatch(copy, /SAMPLE BY/);
    assert.match(
      copy,
      /SELECT ts, context, source, lat, lon FROM signalk_position/,
    );
  });

  it("drops without copying when the own track is not kept", async () => {
    const { statements, archived } = await runOnce(
      { partitions: { signalk_position: [DAY_ONE] } },
      { archiveOwnTrack: false },
    );
    assert.equal(index(statements, /^INSERT/), -1);
    assert.ok(index(statements, /DROP PARTITION LIST '2026-08-01'/) >= 0);
    assert.equal(archived, 0);
  });

  it("never copies string values", async () => {
    const { statements } = await runOnce({
      partitions: { signalk_str: [DAY_ONE] },
    });
    assert.equal(index(statements, /^INSERT/), -1);
    assert.ok(
      index(
        statements,
        /^ALTER TABLE signalk_str DROP PARTITION LIST '2026-08-01'$/,
      ) >= 0,
    );
  });

  it("copies only the numeric paths the patterns match", async () => {
    const { statements } = await runOnce(
      {
        partitions: { signalk: [DAY_ONE] },
        paths: [
          "environment.depth.belowTransducer",
          "navigation.speedOverGround",
          "environment.depth.bad name",
        ],
      },
      { archivePaths: new PathMatcher(["environment.depth.*"]) },
    );
    const copy = statements.find((s) =>
      s.startsWith("INSERT INTO signalk_archive"),
    );
    assert.ok(copy, statements.join("\n"));
    assert.match(copy, /path IN \('environment\.depth\.belowTransducer'\)/);
    assert.match(copy, /first\(value\) value/);
    assert.ok(
      index(statements, /^ALTER TABLE signalk DROP PARTITION/) >
        index(statements, /^INSERT/),
    );
  });

  it("copies no numeric path when no pattern is set", async () => {
    const { statements } = await runOnce(
      {
        partitions: { signalk: [DAY_ONE] },
        paths: ["environment.depth.belowTransducer"],
      },
      { archivePaths: new PathMatcher([]) },
    );
    assert.equal(index(statements, /^INSERT/), -1);
    assert.equal(index(statements, /SELECT DISTINCT path/), -1);
    assert.ok(index(statements, /^ALTER TABLE signalk DROP PARTITION/) >= 0);
  });

  it("skips the copy of a day without own-vessel rows", async () => {
    const { statements, archived } = await runOnce({
      partitions: { signalk_position: [DAY_ONE] },
      count: () => 0,
    });
    assert.equal(index(statements, /^INSERT/), -1);
    assert.ok(index(statements, /DROP PARTITION LIST '2026-08-01'/) >= 0);
    assert.equal(archived, 0);
  });

  it("keeps the partition when the copy fails, and carries on with other tables", async () => {
    const { statements, logs } = await runOnce({
      partitions: {
        signalk_position: [DAY_ONE, DAY_TWO],
        signalk_str: [DAY_ONE],
      },
      failOn: /^INSERT INTO signalk_position_archive/,
    });
    assert.equal(index(statements, /^ALTER TABLE signalk_position DROP/), -1);
    assert.ok(index(statements, /^ALTER TABLE signalk_str DROP/) >= 0);
    assert.ok(
      logs.some((l) => /Retention for signalk_position failed/.test(l)),
    );
  });

  it("refreshes what reads know of the archive before each drop", async () => {
    const order: string[] = [];
    const { client, statements } = scripted({
      partitions: { signalk_position: [DAY_ONE, DAY_TWO] },
    });
    let done!: () => void;
    const finished = new Promise<void>((resolve) => (done = resolve));
    const retention = startRetention(
      client,
      OPTIONS,
      (msg) => {
        if (msg.startsWith("Retention check complete")) done();
      },
      async () => {
        order.push(`refresh after ${statements.length} statements`);
      },
    );
    await finished;
    retention?.stop();
    const drops = statements
      .map((s, i) => [s, i] as const)
      .filter(([s]) => s.includes("DROP PARTITION"))
      .map(([, i]) => i);
    assert.equal(order.length, 2);
    // Each refresh came after its copy and before its drop.
    assert.ok(Number(order[0]!.split(" ")[2]) <= drops[0]!);
    assert.ok(Number(order[1]!.split(" ")[2]) <= drops[1]!);
  });

  it("keeps the partition when the refresh fails", async () => {
    const { client, statements } = scripted({
      partitions: { signalk_position: [DAY_ONE] },
    });
    let done!: () => void;
    const finished = new Promise<void>((resolve) => (done = resolve));
    const logs: string[] = [];
    const retention = startRetention(
      client,
      OPTIONS,
      (msg) => {
        logs.push(msg);
        if (msg.startsWith("Retention check complete")) done();
      },
      async () => {
        throw new Error("QuestDB query failed (503)");
      },
    );
    await finished;
    retention?.stop();
    assert.equal(index(statements, /DROP PARTITION/), -1);
    assert.ok(logs.some((l) => /signalk_position failed: .*503/.test(l)));
  });

  it("keeps the partition when the archive is suspended", async () => {
    const { statements } = await runOnce({
      partitions: { signalk_position: [DAY_ONE] },
      suspended: true,
    });
    assert.ok(index(statements, /^INSERT/) >= 0);
    assert.equal(index(statements, /DROP PARTITION/), -1);
  });

  it("keeps the partition when the copy cannot be found afterwards", async () => {
    const { statements } = await runOnce({
      partitions: { signalk_position: [DAY_ONE] },
      count: (sql) => (sql.includes("_archive") ? 0 : 10),
    });
    assert.equal(index(statements, /DROP PARTITION/), -1);
  });

  it("asks only for partitions wholly before the cutoff and ignores odd names", async () => {
    const { statements } = await runOnce({
      partitions: {
        signalk_position: [["2026-08", "2026-08-31T23:59:58.000000Z"], DAY_ONE],
      },
    });
    const listing = statements.find((s) =>
      s.includes("table_partitions('signalk_position')"),
    );
    assert.match(
      listing ?? "",
      /WHERE maxTimestamp < '\d{4}-\d{2}-\d{2}T[\d:.]+Z'/,
    );
    assert.equal(index(statements, /DROP PARTITION LIST '2026-08'$/), -1);
    assert.ok(index(statements, /DROP PARTITION LIST '2026-08-01'$/) >= 0);
  });
});
