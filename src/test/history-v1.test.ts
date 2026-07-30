import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHistoryProviderV1 } from "../history-v1";

interface Captured {
  sql: string;
}

// Rows as QuestDB returns them for the unified value query: the reader keys
// off `kind` to turn `valuetext` back into a real delta value.
function mockClient(captured: Captured[], dataset: unknown[][]) {
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
  } as any;
}

const SELF = "vessels.urn:mrn:imo:mmsi:123456789";
const noop = () => {};

describe("history-v1 hasAnyData", () => {
  it("counts every value table, not just the numeric one", async () => {
    // A vessel recording only string/boolean channels still has history;
    // answering false here disables playback entirely.
    const captured: Captured[] = [];
    const provider = createHistoryProviderV1(
      mockClient(captured, [[7]]),
      SELF,
      noop,
    );

    const result = await new Promise<boolean>((resolve) =>
      provider.hasAnyData(
        { startTime: new Date("2024-01-01T00:00:00Z"), playbackRate: 1 },
        resolve,
      ),
    );

    assert.equal(result, true);
    for (const table of ["signalk", "signalk_str", "signalk_position"]) {
      assert.ok(
        captured[0].sql.includes(`FROM ${table} `),
        `expected ${table} in the count, got: ${captured[0].sql}`,
      );
    }
  });

  it("reports no data when every table is empty", async () => {
    const provider = createHistoryProviderV1(mockClient([], [[0]]), SELF, noop);

    const result = await new Promise<boolean>((resolve) =>
      provider.hasAnyData(
        { startTime: new Date("2024-01-01T00:00:00Z"), playbackRate: 1 },
        resolve,
      ),
    );

    assert.equal(result, false);
  });
});

describe("history-v1 value decoding", () => {
  // One row per stored kind, as the union query returns them.
  const rows: unknown[][] = [
    [
      "2024-01-01T00:00:00.000000Z",
      "navigation.speedOverGround",
      "self",
      "4.2",
      "number",
    ],
    [
      "2024-01-01T00:00:00.000000Z",
      "navigation.state",
      "self",
      "anchored",
      "string",
    ],
    [
      "2024-01-01T00:00:00.000000Z",
      "electrical.switches.bilgePump.state",
      "self",
      "true",
      "boolean",
    ],
    // A path whose text value genuinely is the word "true": untagged, so it
    // must stay a string.
    ["2024-01-01T00:00:00.000000Z", "some.text.path", "self", "true", null],
    [
      "2024-01-01T00:00:00.000000Z",
      "navigation.position",
      "self",
      "-17.77,177.38",
      "position",
    ],
  ];

  function replay(dataset: unknown[][]): Promise<Record<string, unknown>> {
    const provider = createHistoryProviderV1(
      mockClient([], dataset),
      SELF,
      noop,
    );
    return new Promise((resolve) => {
      provider.getHistory(new Date("2024-01-01T01:00:00Z"), "", (deltas) => {
        const values: Record<string, unknown> = {};
        for (const delta of deltas) {
          for (const update of delta.updates) {
            for (const { path, value } of update.values) values[path] = value;
          }
        }
        resolve(values);
      });
    });
  }

  it("restores numbers as numbers, not strings", async () => {
    const values = await replay(rows);
    assert.equal(values["navigation.speedOverGround"], 4.2);
    assert.equal(typeof values["navigation.speedOverGround"], "number");
  });

  it("passes strings through verbatim", async () => {
    const values = await replay(rows);
    assert.equal(values["navigation.state"], "anchored");
  });

  it("replays a tagged boolean as a real boolean", async () => {
    const values = await replay(rows);
    assert.equal(values["electrical.switches.bilgePump.state"], true);
    assert.equal(
      typeof values["electrical.switches.bilgePump.state"],
      "boolean",
    );
  });

  it('leaves an untagged "true" as a string', async () => {
    // Parsing every "true"/"false" text would corrupt legitimate strings —
    // and rows written before value_kind existed carry no tag at all.
    const values = await replay(rows);
    assert.equal(values["some.text.path"], "true");
    assert.equal(typeof values["some.text.path"], "string");
  });

  it("replays a tagged false", async () => {
    const values = await replay([
      ["2024-01-01T00:00:00.000000Z", "x.off", "self", "false", "boolean"],
    ]);
    assert.equal(values["x.off"], false);
  });

  it("reassembles a position into a lat/lon object", async () => {
    const values = await replay(rows);
    assert.deepEqual(values["navigation.position"], {
      latitude: -17.77,
      longitude: 177.38,
    });
  });

  it("degrades a malformed value to null rather than emitting NaN", async () => {
    const values = await replay([
      [
        "2024-01-01T00:00:00.000000Z",
        "x.broken",
        "self",
        "not-a-number",
        "number",
      ],
      [
        "2024-01-01T00:00:00.000000Z",
        "navigation.position",
        "self",
        "bad",
        "position",
      ],
    ]);
    assert.equal(values["x.broken"], null);
    assert.equal(values["navigation.position"], null);
  });
});

describe("history-v1 getHistory query shape", () => {
  it("applies LATEST ON per table instead of over a union", async () => {
    // LATEST ON over a materialized union makes QuestDB scan rather than use
    // each table's index — that timed out (>30s) on a real install.
    const captured: Captured[] = [];
    const provider = createHistoryProviderV1(
      mockClient(captured, []),
      SELF,
      noop,
    );

    await new Promise<void>((resolve) =>
      provider.getHistory(new Date("2024-01-01T00:00:00Z"), "", () =>
        resolve(),
      ),
    );

    const sql = captured[0].sql;
    assert.equal(
      (sql.match(/LATEST ON/g) || []).length,
      3,
      `expected one LATEST ON per table, got: ${sql}`,
    );
    // The track table has no path column, so it partitions by context only.
    assert.ok(sql.includes("FROM signalk_position"));
    assert.ok(sql.includes("PARTITION BY context)"));
    // getHistory builds its own SQL rather than reusing the streaming
    // helper, so it must select the stored type tag too — hardcoding a kind
    // here silently replayed booleans as strings on this path only.
    assert.ok(
      sql.includes("value_kind"),
      `getHistory must select value_kind, got: ${sql}`,
    );
    assert.ok(
      !/'string' kind/.test(sql),
      "getHistory must not hardcode the value kind",
    );
  });
});

describe("history-v1 streamHistory chunking", () => {
  // A window holding more rows than one read returns must be drained, not
  // skipped: advancing past a truncated read silently loses the remainder,
  // and querying three tables makes truncation far likelier.
  function streamOnce(datasets: unknown[][][], playbackRate = 1) {
    const queries: string[] = [];
    let call = 0;
    const client = {
      exec: async (sql: string) => {
        queries.push(sql);
        const dataset = datasets[Math.min(call++, datasets.length - 1)];
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
    } as any;
    const written: any[] = [];
    const provider = createHistoryProviderV1(client, SELF, noop);
    const stop = provider.streamHistory(
      { write: (d: unknown) => written.push(d), on: () => {} },
      { startTime: new Date("2024-01-01T00:00:00Z"), playbackRate },
      () => {},
    );
    return { queries, written, stop };
  }

  const row = (ts: string, path: string) => [ts, path, "self", "1", "number"];

  it("resumes after the last sent row instead of skipping the remainder", async () => {
    // First read returns a full page (10000 rows) ending mid-window; the
    // next read must start from that row's timestamp, not the window end.
    const full = Array.from({ length: 10000 }, (_, i) =>
      row(
        i === 9999
          ? "2024-01-01T00:00:30.000000Z"
          : "2024-01-01T00:00:00.000000Z",
        `p${i}`,
      ),
    );
    const { queries, stop } = streamOnce([full, []]);
    await new Promise((r) => setTimeout(r, 60));
    stop();

    assert.ok(queries.length >= 2, "expected a follow-up read");
    assert.ok(
      queries[1].includes("2024-01-01T00:00:30.000"),
      `second read should resume AT the last row, got: ${queries[1]}`,
    );
  });

  it("re-reads the final timestamp so tied rows are not dropped", async () => {
    // QuestDB stamps at microsecond precision but JS Date truncates to ms,
    // and one instrument update commonly stamps several paths in the same
    // millisecond. If the page boundary falls inside such a group, stepping
    // past that millisecond would silently lose the siblings.
    const tied = Array.from({ length: 10000 }, (_, i) =>
      row("2024-01-01T00:00:10.000000Z", `p${i}`),
    );
    // Not every row shares currentTime's own millisecond, so the cursor
    // parks on the tie rather than stepping over it.
    tied[0] = row("2024-01-01T00:00:05.000000Z", "earlier");
    const { queries, stop } = streamOnce([tied, []]);
    await new Promise((r) => setTimeout(r, 60));
    stop();

    assert.ok(
      queries[1].includes("2024-01-01T00:00:10.000"),
      `expected a re-read of the tied millisecond, got: ${queries[1]}`,
    );
  });

  it("still advances when an entire page shares one millisecond", async () => {
    // Degenerate case: resuming at the same timestamp would repeat the
    // identical read forever, so the cursor must step forward.
    const sameMs = Array.from({ length: 10000 }, (_, i) =>
      row("2024-01-01T00:00:00.000000Z", `p${i}`),
    );
    const { queries, stop } = streamOnce([sameMs, []]);
    await new Promise((r) => setTimeout(r, 60));
    stop();

    assert.ok(
      queries[1].includes("2024-01-01T00:00:00.001"),
      `expected forward progress, got: ${queries[1]}`,
    );
  });

  it("advances to the next window when a read is not truncated", async () => {
    // A non-truncated read waits CHUNK_SECONDS/playbackRate before the next
    // one, so drive playback fast enough that the follow-up lands promptly.
    const { queries, stop } = streamOnce(
      [[row("2024-01-01T00:00:05.000000Z", "p")], []],
      6000,
    );
    await new Promise((r) => setTimeout(r, 80));
    stop();

    assert.ok(queries.length >= 2, "expected a follow-up read");
    assert.ok(
      queries[1].includes("2024-01-01T00:01:00.000"),
      `expected the next 60s window, got: ${queries[1]}`,
    );
  });
});
