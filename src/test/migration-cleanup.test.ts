import { test, describe } from "node:test";
import assert from "node:assert";
import {
  LEGACY_IMPORT_ROWS_WHERE,
  countLegacyImportRows,
  removeLegacyImportRows,
} from "../migration-cleanup.js";
import type { QuestDBResult } from "../query-client.js";

/** Records every statement and answers the few the cleanup reads. */
class FakeQuery {
  statements: string[] = [];
  legacyRows = 3;
  retiredExists = false;
  markerVisible = true;
  /** Visibility per marker asked for, in order; falls back to markerVisible. */
  markerAnswers: boolean[] = [];
  failOn: RegExp | null = null;
  async exec(sql: string): Promise<QuestDBResult> {
    this.statements.push(sql);
    if (this.failOn?.test(sql)) throw new Error(`refused: ${sql.slice(0, 40)}`);
    const row = (values: unknown[]): QuestDBResult => ({
      columns: [],
      dataset: [values],
      count: 1,
      timestamp: 0,
    });
    if (/FROM tables\(\)/.test(sql)) return row([this.retiredExists ? 1 : 0]);
    if (/cleanup\.marker/.test(sql)) {
      const answer = this.markerAnswers.shift() ?? this.markerVisible;
      return row([answer ? 1 : 0]);
    }
    if (/^SELECT count\(\) FROM signalk_str WHERE/.test(sql))
      return row([this.legacyRows]);
    return { columns: [], dataset: [], count: 0, timestamp: 0 };
  }
}

class FakeWriter {
  events: string[] = [];
  droppedLineCount = 0;
  /** What the watch reports: the oldest timestamp written during the copy. */
  oldestWritten: bigint | null = null;
  startTimestampWatch() {
    this.events.push("watch");
  }
  endTimestampWatch() {
    this.events.push("unwatch");
    return this.oldestWritten;
  }
  hold() {
    this.events.push("hold");
  }
  release() {
    this.events.push("release");
  }
  writeStringAtNanos(path: string, _c: string, value: string) {
    this.events.push(`write ${path}=${value}`);
  }
}

const noSleep = async () => {};
const heads = (q: FakeQuery) =>
  q.statements.map((s) => s.split(" ").slice(0, 3).join(" "));

describe("legacy import rows", () => {
  test("the rows are the imported ones under a suffixed path, no others", () => {
    // A row without a source is never one of them, whatever the database
    // makes of comparing null.
    assert.match(
      LEGACY_IMPORT_ROWS_WHERE,
      /source IS NOT NULL AND source = 'influxdb-import'/,
    );
    for (const suffix of [".stringValue", ".boolValue", ".jsonValue"]) {
      assert.ok(LEGACY_IMPORT_ROWS_WHERE.includes(`path LIKE '%${suffix}'`));
    }
  });

  test("counting checks for a retired table, then counts", async () => {
    const query = new FakeQuery();
    query.legacyRows = 42;
    assert.strictEqual(await countLegacyImportRows(query), 42);
    assert.strictEqual(query.statements.length, 2);
    assert.match(query.statements[0], /FROM tables\(\)/);
    assert.match(query.statements[1], /FROM signalk_str WHERE/);
  });

  test("the table is rebuilt without them, swapped, and the stragglers carried over", async () => {
    const query = new FakeQuery();
    const writer = new FakeWriter();
    writer.oldestWritten =
      BigInt(Date.parse("2026-09-18T11:59:30Z")) * 1_000_000n;
    const result = await removeLegacyImportRows(query, writer, {
      now: () => Date.parse("2026-09-18T12:00:00Z"),
      sleep: noSleep,
    });
    assert.deepStrictEqual(result, { removed: 3, dropped: 0 });
    assert.deepStrictEqual(heads(query), [
      "SELECT count() FROM", // a retired table left behind?
      "SELECT count() FROM", // the legacy rows
      "DROP TABLE IF",
      "SELECT count() FROM", // the marker before the copy
      "CREATE TABLE signalk_str_clean",
      "ALTER TABLE signalk_str_clean",
      "SELECT count() FROM", // the marker before the swap
      "RENAME TABLE signalk_str",
      "RENAME TABLE signalk_str_clean",
      "INSERT INTO signalk_str",
      "DROP TABLE signalk_str_old",
    ]);
    // The rebuilt table keeps what the original had, minus the legacy rows
    // and the markers.
    assert.match(query.statements[4], /TIMESTAMP\(ts\) PARTITION BY DAY WAL$/);
    assert.match(
      query.statements[4],
      /WHERE NOT \(source IS NOT NULL AND source = 'influxdb-import'/,
    );
    assert.match(
      query.statements[4],
      /\(source IS NULL OR source != 'signalk-questdb-cleanup'\)/,
    );
    assert.match(
      query.statements[5],
      /DEDUP ENABLE UPSERT KEYS\(ts, path, context, source\)/,
    );
    // Stragglers: from the oldest row the writer wrote meanwhile, never the
    // legacy rows, never the markers.
    assert.match(
      query.statements[9],
      /WHERE ts >= '2026-09-18T11:59:30.000Z' AND NOT \(source IS NOT NULL AND source = 'influxdb-import'/,
    );
    assert.match(
      query.statements[9],
      /\(source IS NULL OR source != 'signalk-questdb-cleanup'\)/,
    );
    // The watch spans both markers; the second is the last thing written
    // before the hold; the writer is held only across the swap.
    assert.strictEqual(writer.events.length, 6);
    assert.strictEqual(writer.events[0], "watch");
    assert.match(writer.events[1], /^write cleanup\.marker=/);
    assert.match(writer.events[2], /^write cleanup\.marker=/);
    assert.deepStrictEqual(writer.events.slice(3), [
      "hold",
      "unwatch",
      "release",
    ]);
    // Each marker is asked for by its own value, in order.
    const first = writer.events[1].slice("write cleanup.marker=".length);
    const last = writer.events[2].slice("write cleanup.marker=".length);
    assert.ok(query.statements[3].includes(`value_str = '${first}'`));
    assert.ok(query.statements[6].includes(`value_str = '${last}'`));
  });

  // A source with a wrong clock records the present under an old date. The
  // rows it wrote during the copy must still be carried over.
  test("the carry-over reaches back to the oldest row written during the copy", async () => {
    const query = new FakeQuery();
    const writer = new FakeWriter();
    writer.oldestWritten =
      BigInt(Date.parse("2020-01-01T00:00:00Z")) * 1_000_000n;
    await removeLegacyImportRows(query, writer, {
      now: () => Date.parse("2026-09-18T12:00:00Z"),
      sleep: noSleep,
    });
    assert.match(query.statements[9], /WHERE ts >= '2020-01-01T00:00:00.000Z'/);
  });

  test("nothing written during the copy means nothing to carry over", async () => {
    const query = new FakeQuery();
    const writer = new FakeWriter();
    await removeLegacyImportRows(query, writer, { sleep: noSleep });
    assert.ok(!query.statements.some((s) => s.startsWith("INSERT INTO")));
    assert.ok(query.statements.some((s) => s === "DROP TABLE signalk_str_old"));
  });

  test("nothing to remove means nothing is touched", async () => {
    const query = new FakeQuery();
    query.legacyRows = 0;
    const writer = new FakeWriter();
    assert.deepStrictEqual(
      await removeLegacyImportRows(query, writer, { sleep: noSleep }),
      { removed: 0, dropped: 0 },
    );
    assert.strictEqual(query.statements.length, 2);
    assert.deepStrictEqual(writer.events, []);
  });

  // The old table must never be left renamed away with nothing in its place.
  test("a failed swap puts the old table back and releases the writer", async () => {
    const query = new FakeQuery();
    query.failOn = /RENAME TABLE signalk_str_clean TO signalk_str/;
    const writer = new FakeWriter();
    await assert.rejects(
      removeLegacyImportRows(query, writer, { sleep: noSleep }),
      /refused/,
    );
    const renames = query.statements.filter((s) => s.startsWith("RENAME"));
    assert.deepStrictEqual(renames, [
      "RENAME TABLE signalk_str TO signalk_str_old",
      "RENAME TABLE signalk_str_clean TO signalk_str",
      "RENAME TABLE signalk_str_old TO signalk_str",
    ]);
    assert.ok(
      !query.statements.some((s) => s.startsWith("DROP TABLE signalk_str_old")),
    );
    assert.deepStrictEqual(writer.events.slice(3), [
      "hold",
      "unwatch",
      "release",
    ]);
  });

  // Once the rebuilt table is in place it has accepted writes, so the old one
  // must not come back; the rows it still holds are carried over next time.
  test("a failure after the swap keeps the rebuilt table, and the next run finishes the job", async () => {
    const query = new FakeQuery();
    query.failOn = /^INSERT INTO/;
    const writer = new FakeWriter();
    writer.oldestWritten = 1_700_000_000_000_000_000n;
    await assert.rejects(
      removeLegacyImportRows(query, writer, { sleep: noSleep }),
      /refused/,
    );
    assert.strictEqual(
      query.statements.filter((s) => s.startsWith("RENAME")).length,
      2,
      "the old table was put back",
    );
    assert.deepStrictEqual(writer.events.slice(3), [
      "hold",
      "unwatch",
      "release",
    ]);

    // Even a mere count finishes it: the panel counts when it opens, and a
    // zero with a retired table still full would leave nobody to press
    // anything.
    const next = new FakeQuery();
    next.retiredExists = true;
    next.legacyRows = 0;
    assert.strictEqual(await countLegacyImportRows(next), 0);
    assert.deepStrictEqual(heads(next), [
      "SELECT count() FROM",
      "INSERT INTO signalk_str",
      "DROP TABLE signalk_str_old",
      "SELECT count() FROM",
    ]);
    // Everything the retired table held, not just a window.
    assert.match(
      next.statements[1],
      /WHERE NOT \(source IS NOT NULL AND source = 'influxdb-import'/,
    );
    assert.ok(!/ts >=/.test(next.statements[1]));
  });

  // The marker is the last line sent before the hold. Until QuestDB shows
  // it, lines sent before it may still be on their way — into a table about
  // to be renamed away.
  // The first marker guards the snapshot: until QuestDB shows it, lines sent
  // before it may still be on their way into the table about to be copied.
  test("the copy waits for the first marker, and gives up with a reason", async () => {
    const query = new FakeQuery();
    query.markerVisible = false;
    const writer = new FakeWriter();
    let slept = 0;
    await assert.rejects(
      removeLegacyImportRows(query, writer, {
        sleep: async () => {
          slept++;
        },
        barrierTimeoutMs: 1000,
      }),
      /not taken the last writes/,
    );
    assert.ok(slept > 1);
    assert.ok(!query.statements.some((s) => s.startsWith("CREATE TABLE")));
    // Nothing was held: the writer was never in the way.
    assert.deepStrictEqual(writer.events.slice(0, 1), ["watch"]);
    assert.ok(!writer.events.includes("hold"));
  });

  // The second guards the swap: until QuestDB shows it, lines sent before
  // the hold may still be on their way — into a table about to be renamed.
  test("the swap waits for the second marker, and gives up with a reason", async () => {
    const query = new FakeQuery();
    query.markerAnswers = [true, false];
    query.markerVisible = false;
    const writer = new FakeWriter();
    await assert.rejects(
      removeLegacyImportRows(query, writer, {
        sleep: noSleep,
        barrierTimeoutMs: 1000,
      }),
      /not taken the last writes/,
    );
    assert.ok(query.statements.some((s) => s.startsWith("CREATE TABLE")));
    assert.ok(!query.statements.some((s) => s.startsWith("RENAME")));
    assert.deepStrictEqual(writer.events.slice(3), [
      "hold",
      "unwatch",
      "release",
    ]);
  });

  test("lines the writer's cap discarded during the hold are reported", async () => {
    const query = new FakeQuery();
    const writer = new FakeWriter();
    writer.droppedLineCount = 10;
    writer.hold = () => {
      writer.events.push("hold");
      writer.droppedLineCount = 17;
    };
    const result = await removeLegacyImportRows(query, writer, {
      sleep: noSleep,
    });
    assert.deepStrictEqual(result, { removed: 3, dropped: 7 });
  });
});
