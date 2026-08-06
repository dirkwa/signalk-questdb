import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { restoreFromHistory } from "../restore.js";
import type { RestoreDeps, RestoreOptions } from "../restore.js";

const SELF = "vessels.urn:mrn:imo:mmsi:123456789";
const AIS = "vessels.urn:mrn:imo:mmsi:987654321";

// Fixed clock so the staleness window is deterministic.
const NOW = Date.parse("2024-06-01T12:00:00.000Z");
const ago = (ms: number) => new Date(NOW - ms).toISOString();

type Row = [string, string, string, string | null, string | null];

interface Captured {
  sql: string;
}

function mockClient(captured: Captured[], dataset: Row[]) {
  return {
    exec: async (sql: string) => {
      captured.push({ sql });
      return { columns: [], dataset, count: dataset.length, timestamp: 0 };
    },
    toObjects: (result: { dataset: unknown[][] }) =>
      result.dataset.map((row) => ({
        ts: row[0],
        path: row[1],
        context: row[2],
        valuetext: row[3],
        kind: row[4],
      })),
  } as unknown as RestoreDeps["queryClient"];
}

// The delta shape the restore emits — enough to assert on without `any`.
interface RestoredValue {
  path: string;
  value: unknown;
}
interface RestoredDelta {
  context: string;
  updates: {
    $source: string;
    timestamp: string;
    values: RestoredValue[];
  }[];
}

function run(
  dataset: Row[],
  overrides: Partial<RestoreOptions> = {},
  captured: Captured[] = [],
  hasLiveData?: (context: string) => boolean,
) {
  const deltas: RestoredDelta[] = [];
  const promise = restoreFromHistory(
    {
      queryClient: mockClient(captured, dataset),
      handleMessage: (d) => deltas.push(d as RestoredDelta),
      selfContext: SELF,
      debug: () => {},
      hasLiveData,
    },
    {
      maxAgeMs: 9 * 60_000,
      restoreSelf: true,
      restoreOthers: true,
      now: () => NOW,
      ...overrides,
    },
  );
  return { promise, deltas };
}

const position = (ctx: string, ageMs: number, lat = "-36.8", lon = "174.7") =>
  [ago(ageMs), "navigation.position", ctx, `${lat},${lon}`, "position"] as Row;

// Values are split across one update per distinct recorded timestamp, so
// assertions about "what was restored for this vessel" span all of them.
const allValues = (delta: RestoredDelta): RestoredValue[] =>
  delta.updates.flatMap((u) => u.values);

const valueAt = (delta: RestoredDelta, path: string) =>
  allValues(delta).find((v) => v.path === path);

describe("restore: what reaches the data model", () => {
  it("replays an AIS target under its own context, not self", async () => {
    const { promise, deltas } = run([position(AIS, 60_000)]);
    const result = await promise;

    assert.equal(result.contexts, 1);
    assert.equal(deltas.length, 1);
    assert.equal(deltas[0].context, AIS);
    assert.deepEqual(deltas[0].updates[0].values[0], {
      path: "navigation.position",
      value: { latitude: -36.8, longitude: 174.7 },
    });
  });

  it("maps the stored 'self' placeholder onto this server's self context", async () => {
    // Contexts are stored with self normalized to the literal "self"; a
    // replay that leaked that string would create a bogus vessel named
    // "self" instead of updating the server's own vessel.
    const { promise, deltas } = run([position("self", 60_000)]);
    await promise;

    assert.equal(deltas[0].context, SELF);
  });

  it("carries the original recorded timestamp, never now", async () => {
    // This is what lets a consumer age a restored target out normally.
    // Stamping `now` would present a 8-minute-old fix as a fresh contact.
    const recorded = ago(8 * 60_000);
    const { promise, deltas } = run([position(AIS, 8 * 60_000)]);
    await promise;

    assert.equal(deltas[0].updates[0].timestamp, recorded);
    assert.notEqual(
      deltas[0].updates[0].timestamp,
      new Date(NOW).toISOString(),
    );
  });

  it("tags the source so replayed values are distinguishable from live ones", async () => {
    const { promise, deltas } = run([position(AIS, 60_000)]);
    await promise;

    assert.equal(deltas[0].updates[0].$source, "signalk-questdb.restore");
  });
});

describe("restore: per-timestamp grouping", () => {
  it("keeps each path under its own recorded time", async () => {
    // An AIS name repeats every ~6 minutes while position arrives every few
    // seconds. Putting both under the newest timestamp would present the
    // stale name as being as fresh as the fix.
    const rows: Row[] = [
      position(AIS, 30_000),
      [ago(6 * 60_000), "name", AIS, "Black Pearl", "identity"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    assert.equal(deltas[0].updates.length, 2);

    const posUpdate = deltas[0].updates.find((u) =>
      u.values.some((v) => v.path === "navigation.position"),
    );
    const nameUpdate = deltas[0].updates.find((u) =>
      u.values.some((v) => v.path === ""),
    );

    assert.ok(posUpdate && nameUpdate);
    assert.equal(posUpdate.timestamp, ago(30_000));
    assert.equal(nameUpdate.timestamp, ago(6 * 60_000));
  });

  it("emits a single update when everything shares a timestamp", async () => {
    const ts = ago(60_000);
    const rows: Row[] = [
      position(AIS, 60_000),
      [ts, "navigation.speedOverGround", AIS, "5.4", "number"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    assert.equal(deltas[0].updates.length, 1);
    assert.equal(deltas[0].updates[0].timestamp, ts);
  });

  it("counts every restored value across updates", async () => {
    const rows: Row[] = [
      position(AIS, 30_000),
      [ago(6 * 60_000), "name", AIS, "Black Pearl", "identity"],
    ];
    const { promise } = run(rows);
    const result = await promise;

    assert.equal(result.values, 2);
  });
});

describe("restore: staleness boundary", () => {
  it("drops a row older than the window even if SQL returned it", async () => {
    // The window is applied in SQL, but the client re-checks: a clock skew
    // or a wider query must not put an expired ghost on the chart.
    const { promise, deltas } = run([position(AIS, 20 * 60_000)]);
    const result = await promise;

    assert.equal(deltas.length, 0);
    assert.equal(result.contexts, 0);
    assert.equal(result.skippedStale, 1);
  });

  it("keeps a row just inside the window", async () => {
    const { promise, deltas } = run([position(AIS, 9 * 60_000 - 1000)]);
    await promise;

    assert.equal(deltas.length, 1);
  });

  it("skips a row whose timestamp cannot be parsed", async () => {
    // An unparseable timestamp cannot be aged out by any consumer, so it
    // would linger on the chart forever.
    const bad: Row = [
      "not-a-timestamp",
      "navigation.position",
      AIS,
      "-36.8,174.7",
      "position",
    ];
    const { promise, deltas } = run([bad]);
    const result = await promise;

    assert.equal(deltas.length, 0);
    assert.equal(result.skippedStale, 1);
  });

  it("honours a custom window", async () => {
    const rows = [position(AIS, 20 * 60_000)];
    const { promise, deltas } = run(rows, { maxAgeMs: 30 * 60_000 });
    await promise;

    assert.equal(deltas.length, 1);
  });
});

describe("restore: positionless contexts", () => {
  it("does not replay a context that has no position", async () => {
    // A name with no fix would create an undrawable target that never ages
    // out, because nothing about it is renderable or expirable.
    const nameOnly: Row = [ago(60_000), "name", AIS, "Black Pearl", "identity"];
    const { promise, deltas } = run([nameOnly]);
    const result = await promise;

    assert.equal(deltas.length, 0);
    assert.equal(result.contexts, 0);
  });

  it("replays identity alongside a position when both are present", async () => {
    const rows: Row[] = [
      position(AIS, 60_000),
      [ago(120_000), "name", AIS, "Black Pearl", "identity"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    assert.equal(deltas.length, 1);
    // Names were received as empty-path object deltas and must replay in
    // that shape — the only one Freeboard reads names from.
    const nameValue = valueAt(deltas[0], "");
    assert.deepEqual(nameValue, { path: "", value: { name: "Black Pearl" } });
  });

  it("leaves a genuine string path named 'name' as a plain string", async () => {
    // Only rows tagged 'identity' are vessel names; an untagged one is data.
    const rows: Row[] = [
      position(AIS, 60_000),
      [ago(120_000), "name", AIS, "not-identity", null],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    const entry = valueAt(deltas[0], "name");
    assert.deepEqual(entry, { path: "name", value: "not-identity" });
  });
});

describe("restore: recording toggles", () => {
  it("skips other vessels when restoreOthers is off", async () => {
    const rows = [position("self", 60_000), position(AIS, 60_000)];
    const { promise, deltas } = run(rows, { restoreOthers: false });
    await promise;

    assert.equal(deltas.length, 1);
    assert.equal(deltas[0].context, SELF);
  });

  it("skips self when restoreSelf is off", async () => {
    const rows = [position("self", 60_000), position(AIS, 60_000)];
    const { promise, deltas } = run(rows, { restoreSelf: false });
    await promise;

    assert.equal(deltas.length, 1);
    assert.equal(deltas[0].context, AIS);
  });
});

describe("restore: vessels already live", () => {
  it("does not replay a context that transmitted during startup", async () => {
    // The query is slow enough that vessels report while it runs. Replaying
    // a stored fix over a live one would move the target BACKWARDS.
    const rows = [position(AIS, 60_000)];
    const { promise, deltas } = run(rows, {}, [], (ctx) => ctx === AIS);
    const result = await promise;

    assert.equal(deltas.length, 0);
    assert.equal(result.skippedLive, 1);
  });

  it("still replays contexts that have not been seen live", async () => {
    const other = "vessels.urn:mrn:imo:mmsi:111111111";
    const rows = [position(AIS, 60_000), position(other, 60_000)];
    const { promise, deltas } = run(rows, {}, [], (ctx) => ctx === AIS);
    const result = await promise;

    assert.equal(deltas.length, 1);
    assert.equal(deltas[0].context, other);
    assert.equal(result.skippedLive, 1);
  });

  it("keys the live check on the stored context, not the resolved one", async () => {
    // Own vessel is stored as the literal "self"; checking the resolved
    // self context would never match what the recorder recorded.
    const seen: string[] = [];
    const { promise } = run([position("self", 60_000)], {}, [], (ctx) => {
      seen.push(ctx);
      return false;
    });
    await promise;

    assert.deepEqual(seen, ["self"]);
  });
});

describe("restore: duplicate rows across tables", () => {
  it("keeps the newer row when a path appears in two tables", async () => {
    // LATEST ON runs PER TABLE, so a path whose type changed over time
    // returns one row from each. Union order is not timestamp order.
    const rows: Row[] = [
      position(AIS, 60_000),
      [ago(30_000), "navigation.state", AIS, "sailing", null],
      [ago(300_000), "navigation.state", AIS, "42", "number"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    const state = valueAt(deltas[0], "navigation.state");
    assert.deepEqual(state, { path: "navigation.state", value: "sailing" });
  });

  it("emits a path only once even when duplicated", async () => {
    const rows: Row[] = [
      position(AIS, 60_000),
      [ago(30_000), "navigation.state", AIS, "sailing", null],
      [ago(300_000), "navigation.state", AIS, "42", "number"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    const states = allValues(deltas[0]).filter(
      (v) => v.path === "navigation.state",
    );
    assert.equal(states.length, 1);
  });
});

describe("restore: value decoding", () => {
  it("decodes numbers as numbers, not strings", async () => {
    // Consumers do arithmetic on these; a string COG breaks course maths.
    const rows: Row[] = [
      position(AIS, 60_000),
      [ago(60_000), "navigation.speedOverGround", AIS, "5.4", "number"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    const sog = valueAt(deltas[0], "navigation.speedOverGround");
    assert.ok(sog, "expected speedOverGround to be restored");
    assert.equal(sog.value, 5.4);
    assert.equal(typeof sog.value, "number");
  });

  it("drops a position whose coordinates are not finite", async () => {
    const rows: Row[] = [
      [ago(60_000), "navigation.position", AIS, "not,coords", "position"],
    ];
    const { promise, deltas } = run(rows);
    await promise;

    assert.equal(deltas.length, 0);
  });
});

describe("restore: query shape", () => {
  it("applies LATEST ON per table rather than over a union", async () => {
    // Running LATEST ON over a materialized union makes QuestDB scan instead
    // of using each table's index — it timed out on large installs.
    const captured: Captured[] = [];
    const { promise } = run([], {}, captured);
    await promise;

    const sql = captured[0].sql;
    const latestCount = (sql.match(/LATEST ON/g) ?? []).length;
    assert.equal(latestCount, 3);
    for (const table of ["signalk", "signalk_str", "signalk_position"]) {
      assert.ok(sql.includes(table), `expected ${table} in query`);
    }
  });

  it("retries without value_kind when the column is missing", async () => {
    // A read racing an unmigrated table must not fail the whole restore.
    const captured: Captured[] = [];
    let calls = 0;
    const deltas: unknown[] = [];

    await restoreFromHistory(
      {
        queryClient: {
          exec: async (sql: string) => {
            captured.push({ sql });
            if (++calls === 1) {
              throw new Error("Invalid column: value_kind");
            }
            return { columns: [], dataset: [], count: 0, timestamp: 0 };
          },
          toObjects: () => [],
        } as unknown as RestoreDeps["queryClient"],
        handleMessage: (d) => deltas.push(d),
        selfContext: SELF,
        debug: () => {},
      },
      {
        maxAgeMs: 9 * 60_000,
        restoreSelf: true,
        restoreOthers: true,
        now: () => NOW,
      },
    );

    assert.equal(calls, 2);
    assert.ok(captured[0].sql.includes("value_kind"));
    assert.ok(!captured[1].sql.includes("CAST(value_kind AS STRING)"));
  });

  it("propagates a non-schema query error instead of masking it", async () => {
    await assert.rejects(
      restoreFromHistory(
        {
          queryClient: {
            exec: async () => {
              throw new Error("connection refused");
            },
            toObjects: () => [],
          } as unknown as RestoreDeps["queryClient"],
          handleMessage: () => {},
          selfContext: SELF,
          debug: () => {},
        },
        {
          maxAgeMs: 9 * 60_000,
          restoreSelf: true,
          restoreOthers: true,
          now: () => NOW,
        },
      ),
      /connection refused/,
    );
  });
});
