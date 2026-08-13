import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHistoryProviderV1 } from "../history-v1.js";

interface Captured {
  sql: string;
}

// The shape the provider actually depends on. Narrowing to this (instead of
// `as any`) means a change to exec/toObjects fails at compile time here
// rather than at runtime.
type HistoryClient = Parameters<typeof createHistoryProviderV1>[0];

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
  } as unknown as HistoryClient;
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

  it("replays a stored vessel-name row in the empty-path shape", async () => {
    // Names are stored under the synthetic path "name" (kind "identity")
    // but were received as empty-path object deltas — the only shape
    // consumers (Freeboard) read names from, so replay must reconstruct it.
    const values = await replay([
      [
        "2024-01-01T00:00:00.000000Z",
        "name",
        "vessels.urn:mrn:imo:mmsi:244813000",
        "Sea Breeze",
        "identity",
      ],
    ]);
    assert.deepEqual(values[""], { name: "Sea Breeze" });
  });

  it('leaves a DATA path literally named "name" alone', async () => {
    // Only rows tagged kind "identity" are synthetic vessel names; a source
    // emitting a real path called "name" must round-trip as the plain
    // string it is, not become a vessel identity update.
    const values = await replay([
      [
        "2024-01-01T00:00:00.000000Z",
        "name",
        "self",
        "not an identity",
        "string",
      ],
    ]);
    assert.equal(values["name"], "not an identity");
    assert.equal(values[""], undefined);
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

describe("history-v1 unmigrated value_kind column", () => {
  // ensureTables() adds value_kind at startup, but a read racing that (or an
  // external QuestDB the plugin does not own) must not lose ALL history to
  // "Invalid column: value_kind" — verified against a live pre-migration
  // database, which returned 0 paths before this fallback.
  function failingKindClient(captured: Captured[]) {
    return {
      exec: async (sql: string) => {
        captured.push({ sql });
        if (sql.includes("value_kind")) {
          throw new Error(
            "QuestDB query failed (400): Invalid column: value_kind",
          );
        }
        return {
          columns: [],
          dataset: [
            [
              "2024-01-01T00:00:00.000000Z",
              "navigation.state",
              "self",
              "on",
              null,
            ],
          ],
          count: 1,
          timestamp: 0,
        };
      },
      toObjects: (result: { dataset: unknown[][] }) =>
        result.dataset.map((row) => ({
          ts: row[0],
          path: row[1],
          context: row[2],
          valuetext: row[3],
          kind: row[4],
        })),
    } as unknown as HistoryClient;
  }

  it("retries getHistory without the tag and still returns history", async () => {
    const captured: Captured[] = [];
    const provider = createHistoryProviderV1(
      failingKindClient(captured),
      SELF,
      noop,
    );

    const values = await new Promise<{ path: string; value: unknown }[]>(
      (resolve) =>
        provider.getHistory(new Date("2024-01-01T01:00:00Z"), "", (deltas) =>
          resolve(deltas.flatMap((d) => d.updates.flatMap((u) => u.values))),
        ),
    );

    assert.equal(values.length, 1);
    assert.equal(values[0].value, "on");
    assert.equal(captured.length, 2, "expected a tagged attempt then a retry");
    assert.ok(!captured[1].sql.includes("value_kind"));
  });

  it("does not swallow unrelated query failures", async () => {
    // Only the missing-column case degrades; anything else must surface.
    const client = {
      exec: async () => {
        throw new Error("QuestDB query failed (500): something else");
      },
      toObjects: () => [],
    } as unknown as HistoryClient;
    const errors: string[] = [];
    const provider = createHistoryProviderV1(client, SELF, (m) =>
      errors.push(m),
    );

    const deltas = await new Promise<unknown[]>((resolve) =>
      provider.getHistory(new Date("2024-01-01T01:00:00Z"), "", resolve),
    );

    assert.deepEqual(deltas, []);
    assert.ok(errors.some((e) => e.includes("something else")));
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
    // `queries` holds WINDOW reads only: the one-shot last-known-names
    // lookup (LATEST ON) would otherwise shift the indexes every assertion
    // below relies on, and the sequential datasets feed window reads.
    const queries: string[] = [];
    let call = 0;
    const client = {
      exec: async (sql: string) => {
        if (sql.includes("LATEST ON")) {
          return { columns: [], dataset: [], count: 0, timestamp: 0 };
        }
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
    } as unknown as HistoryClient;
    const written: unknown[] = [];
    const provider = createHistoryProviderV1(client, SELF, noop);
    const stop = provider.streamHistory(
      { write: (d: unknown) => written.push(d), on: () => {} },
      { startTime: new Date("2024-01-01T00:00:00Z"), playbackRate },
      () => {},
    );
    return { queries, written, stop };
  }

  const row = (ts: string, path: string) => [ts, path, "self", "1", "number"];

  // Poll rather than sleep a fixed interval: on a loaded runner a fixed wait
  // can miss the follow-up read and fail as a TypeError on undefined instead
  // of a readable assertion.
  async function waitForQueries(
    queries: string[],
    n: number,
    timeoutMs = 2000,
  ) {
    const deadline = Date.now() + timeoutMs;
    while (queries.length < n && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.ok(
      queries.length >= n,
      `expected at least ${n} reads, got ${queries.length}`,
    );
  }

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
    await waitForQueries(queries, 2);
    stop();
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
    await waitForQueries(queries, 2);
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
    await waitForQueries(queries, 2);
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
    await waitForQueries(queries, 2);
    stop();
    assert.ok(
      queries[1].includes("2024-01-01T00:01:00.000"),
      `expected the next 60s window, got: ${queries[1]}`,
    );
  });
});

describe("history-v1 playback vessel-name injection", () => {
  // A playback window almost never contains a name row (names are written
  // on change only, typically when the vessel was first seen), so playback
  // fetches the last-known name per context once and injects it ahead of a
  // context's first delta — otherwise restored targets stay anonymous.
  const VESSEL = "vessels.urn:mrn:imo:mmsi:244813000";

  interface WrittenDelta {
    context: string;
    updates: { values: { path: string; value: unknown }[] }[];
  }

  function streamWithNames(
    windowDatasets: unknown[][][],
    names: unknown[][] | Error,
  ) {
    let call = 0;
    const nameQueries: string[] = [];
    const client = {
      exec: async (sql: string) => {
        if (sql.includes("LATEST ON")) {
          nameQueries.push(sql);
          if (names instanceof Error) throw names;
          return {
            __names: true,
            columns: [],
            dataset: names,
            count: names.length,
            timestamp: 0,
          };
        }
        const dataset =
          windowDatasets[Math.min(call++, windowDatasets.length - 1)];
        return { columns: [], dataset, count: dataset.length, timestamp: 0 };
      },
      toObjects: (result: { dataset: unknown[][] } & { __names?: boolean }) =>
        result.__names
          ? result.dataset.map((row) => ({
              context: row[0],
              value_str: row[1],
            }))
          : result.dataset.map((row) => ({
              ts: row[0],
              path: row[1],
              context: row[2],
              valuetext: row[3],
              kind: row[4],
            })),
    } as unknown as HistoryClient;
    const written: WrittenDelta[] = [];
    const provider = createHistoryProviderV1(client, SELF, noop);
    const stop = provider.streamHistory(
      { write: (d: unknown) => written.push(d as WrittenDelta), on: () => {} },
      { startTime: new Date("2024-01-01T00:00:00Z"), playbackRate: 1 },
      () => {},
    );
    return { written, stop, nameQueries };
  }

  async function waitForWritten(
    written: WrittenDelta[],
    n: number,
    timeoutMs = 2000,
  ) {
    const deadline = Date.now() + timeoutMs;
    while (written.length < n && Date.now() < deadline) {
      await new Promise((r) => setTimeout(r, 5));
    }
    assert.ok(
      written.length >= n,
      `expected at least ${n} written deltas, got ${written.length}`,
    );
  }

  const dataRow = (ts: string, context: string) => [
    ts,
    "navigation.speedOverGround",
    context,
    "4.2",
    "number",
  ];

  it("injects the last-known name once, ahead of the first delta", async () => {
    const { written, stop, nameQueries } = streamWithNames(
      [
        [
          dataRow("2024-01-01T00:00:01.000000Z", VESSEL),
          dataRow("2024-01-01T00:00:02.000000Z", VESSEL),
        ],
        [],
      ],
      [[VESSEL, "Sea Breeze"]],
    );
    try {
      await waitForWritten(written, 3);

      assert.deepEqual(written[0].updates[0].values, [
        { path: "", value: { name: "Sea Breeze" } },
      ]);
      assert.equal(written[0].context, VESSEL);
      const nameDeltas = written.filter((d) =>
        d.updates.some((u) => u.values.some((v) => v.path === "")),
      );
      assert.equal(
        nameDeltas.length,
        1,
        "one name delta per context, not per row",
      );
      // The lookup is bound to the playback start (a later rename must not
      // leak into a historical replay) and to identity-tagged rows (a data
      // path literally named "name" is not a vessel name).
      assert.equal(nameQueries.length, 1);
      assert.ok(
        nameQueries[0].includes("ts <= '2024-01-01T00:00:00"),
        `expected a start-time bound, got: ${nameQueries[0]}`,
      );
      assert.ok(
        nameQueries[0].includes("value_kind = 'identity'"),
        `expected the identity filter, got: ${nameQueries[0]}`,
      );
    } finally {
      stop();
    }
  });

  it("resolves the self context on the injected delta", async () => {
    const { written, stop } = streamWithNames(
      [[dataRow("2024-01-01T00:00:01.000000Z", "self")], []],
      [["self", "Vessel Aurora"]],
    );
    try {
      await waitForWritten(written, 2);

      assert.equal(written[0].context, SELF);
      assert.deepEqual(written[0].updates[0].values[0], {
        path: "",
        value: { name: "Vessel Aurora" },
      });
    } finally {
      stop();
    }
  });

  it("streams unlabeled when the names lookup fails", async () => {
    const { written, stop } = streamWithNames(
      [[dataRow("2024-01-01T00:00:01.000000Z", VESSEL)], []],
      new Error("QuestDB query failed (500): boom"),
    );
    try {
      await waitForWritten(written, 1);

      assert.equal(written[0].context, VESSEL);
      assert.ok(
        written.every((d) =>
          d.updates.every((u) => u.values.every((v) => v.path !== "")),
        ),
        "no name delta may be fabricated on lookup failure",
      );
    } finally {
      stop();
    }
  });
});

describe("history-v1 streamHistory does not outlive its process", () => {
  // The regression this pins down is not a wrong VALUE, it is a process that
  // will not exit. Stopping a playback set a `stopped` flag but left the
  // already-scheduled inter-chunk timer armed for CHUNK_SECONDS/playbackRate
  // (60s at rate 1), so node stayed alive until it fired. That made this file
  // take 60s to run assertions completing in milliseconds, and the plugin
  // registry — which runs `npm test` under `timeout 60s` — scored the whole
  // suite as failing.
  //
  // Asserting on timer handles does not work: node does not expose Timeouts
  // via process._getActiveHandles(). So assert the observable property
  // instead — spawn a real process that starts a playback, and require it to
  // exit on its own well inside the 60s the orphaned timer would have cost.
  it("lets node exit promptly after a playback is started", async () => {
    const { spawn } = await import("node:child_process");
    const script = `
      const { createHistoryProviderV1 } = require("./dist/history-v1.js");
      const dataset = [["2024-01-01T00:00:01.000000Z","navigation.speedOverGround","self","1","number"]];
      const client = {
        exec: async (sql) => sql.includes("LATEST ON")
          ? { columns: [], dataset: [], count: 0, timestamp: 0 }
          : { columns: [], dataset, count: dataset.length, timestamp: 0 },
        toObjects: (r) => r.dataset.map((row) => ({
          ts: row[0], path: row[1], context: row[2], valuetext: row[3], kind: row[4],
        })),
      };
      const provider = createHistoryProviderV1(client, "vessels.self", () => {});
      provider.streamHistory(
        { write: () => {}, on: () => {} },
        { startTime: new Date("2024-01-01T00:00:00Z"), playbackRate: 1 },
        () => {},
      );
      // Deliberately do NOT call stop(): an unref'd timer must not be enough
      // to keep the process alive on its own.
    `;

    const started = Date.now();
    const exitCode = await new Promise<number | null>((resolve) => {
      const child = spawn(process.execPath, ["-e", script], {
        stdio: "ignore",
      });
      const kill = setTimeout(() => {
        child.kill("SIGKILL");
        resolve(null);
      }, 15000);
      child.on("exit", (code) => {
        clearTimeout(kill);
        resolve(code);
      });
    });
    const elapsed = Date.now() - started;

    assert.equal(exitCode, 0, "the playback process must exit on its own");
    assert.ok(
      elapsed < 10000,
      `expected a prompt exit, took ${elapsed}ms (an orphaned 60s timer would hang here)`,
    );
  });
});

describe("history-v1 source attribution", () => {
  // Rows as the unified query returns them since the source migration:
  // ts, path, context, source, valuetext, kind.
  function sourceMockClient(
    captured: Captured[],
    responder: (sql: string) => unknown[][],
  ) {
    return {
      exec: async (sql: string) => {
        captured.push({ sql });
        const dataset = responder(sql);
        return { columns: [], dataset, count: dataset.length, timestamp: 0 };
      },
      toObjects: (result: { dataset: unknown[][] }) =>
        result.dataset.map((row) => ({
          ts: row[0],
          path: row[1],
          context: row[2],
          source: row[3],
          valuetext: row[4],
          kind: row[5],
        })),
    } as unknown as HistoryClient;
  }

  const getHistoryDeltas = (
    responder: (sql: string) => unknown[][],
    captured: Captured[] = [],
  ) => {
    const provider = createHistoryProviderV1(
      sourceMockClient(captured, responder),
      SELF,
      noop,
    );
    return new Promise<any[]>((resolve) =>
      provider.getHistory(new Date("2024-01-01T00:00:00Z"), "", resolve),
    );
  };

  it("replays each source as its own $source-labelled update", async () => {
    // Two receivers' rows sharing a timestamp must not merge into one
    // update — a single $source would misattribute one receiver's fix to
    // the other, exactly the ambiguity the source column exists to remove.
    const ts = "2024-01-01T00:00:00.000000Z";
    const deltas = await getHistoryDeltas(() => [
      [ts, "navigation.position", "self", "gps.main", "60.1,24.9", "position"],
      [
        ts,
        "navigation.position",
        "self",
        "gps.backup",
        "60.2,24.8",
        "position",
      ],
      [ts, "environment.depth.belowKeel", "self", null, "3.2", "number"],
    ]);

    assert.equal(deltas.length, 3);
    const bySource = new Map(
      deltas.map((d: any) => [d.updates[0].$source, d.updates[0].values]),
    );
    assert.deepEqual(bySource.get("gps.main"), [
      {
        path: "navigation.position",
        value: { latitude: 60.1, longitude: 24.9 },
      },
    ]);
    assert.deepEqual(bySource.get("gps.backup"), [
      {
        path: "navigation.position",
        value: { latitude: 60.2, longitude: 24.8 },
      },
    ]);
    // The pre-migration row (source null) replays without a $source at all.
    assert.deepEqual(bySource.get(undefined), [
      { path: "environment.depth.belowKeel", value: 3.2 },
    ]);
    const unlabelled = deltas.find((d: any) => !d.updates[0].$source);
    assert.ok(!("$source" in unlabelled.updates[0]));
  });

  it("selects the source column from every table", async () => {
    const captured: Captured[] = [];
    await getHistoryDeltas(() => [], captured);
    const branches = captured[0].sql.split("UNION ALL");
    assert.equal(branches.length, 3);
    for (const branch of branches) {
      assert.ok(
        branch.includes("CAST(source AS STRING) source"),
        `every branch must carry source, got: ${branch}`,
      );
    }
  });

  it("retries without source when the column is not migrated yet", async () => {
    const captured: Captured[] = [];
    const deltas = await getHistoryDeltas((sql) => {
      if (sql.includes("CAST(source AS STRING)")) {
        throw new Error("Invalid column: source");
      }
      return [
        [
          "2024-01-01T00:00:00.000000Z",
          "environment.depth.belowKeel",
          "self",
          null,
          "3.2",
          "number",
        ],
      ];
    }, captured);

    assert.equal(captured.length, 2);
    assert.ok(
      captured[1].sql.includes("CAST(NULL AS STRING) source"),
      `retry must degrade source to NULL, got: ${captured[1].sql}`,
    );
    assert.equal(deltas.length, 1);
    assert.ok(!("$source" in deltas[0].updates[0]));
  });

  it("drops value_kind and source independently when both are missing", async () => {
    // An external QuestDB the plugin does not own can predate BOTH
    // migrations; each retry must shed only the column actually complained
    // about, or the fallback ladder never reaches a query that runs.
    const captured: Captured[] = [];
    const deltas = await getHistoryDeltas((sql) => {
      if (sql.includes("CAST(value_kind AS STRING)")) {
        throw new Error("Invalid column: value_kind");
      }
      if (sql.includes("CAST(source AS STRING)")) {
        throw new Error("Invalid column: source");
      }
      return [
        [
          "2024-01-01T00:00:00.000000Z",
          "environment.depth.belowKeel",
          "self",
          null,
          "3.2",
          "number",
        ],
      ];
    }, captured);

    assert.equal(captured.length, 3);
    assert.ok(
      captured[2].sql.includes("CAST(NULL AS STRING) source") &&
        !captured[2].sql.includes("CAST(value_kind"),
      `final query must run with both degraded, got: ${captured[2].sql}`,
    );
    assert.equal(deltas.length, 1);
  });
});
