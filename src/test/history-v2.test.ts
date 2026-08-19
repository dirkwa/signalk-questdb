import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHistoryProviderV2 } from "../history-v2.js";

interface CapturedQuery {
  sql: string;
}

function makeMockClient(captured: CapturedQuery[]) {
  return {
    exec: async (sql: string) => {
      captured.push({ sql });
      return { columns: [], dataset: [], count: 0, timestamp: 0 };
    },
  } as any;
}

const SELF_CONTEXT = "vessels.urn:mrn:imo:mmsi:123456789";

describe("history-v2 context normalization", () => {
  it("translates vessels.self to stored 'self'", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      context: "vessels.self",
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    assert.ok(
      captured[0].sql.includes("context = 'self'"),
      `Expected query to use stored context 'self', got: ${captured[0].sql}`,
    );
  });

  it("translates fully-qualified self context to stored 'self'", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      context: SELF_CONTEXT,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    assert.ok(
      captured[0].sql.includes("context = 'self'"),
      `Expected query to use stored context 'self', got: ${captured[0].sql}`,
    );
  });

  it("passes through 'self' context unchanged", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      context: "self",
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    assert.ok(captured[0].sql.includes("context = 'self'"));
  });

  it("passes through other vessel contexts unchanged", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    const otherVessel = "vessels.urn:mrn:imo:mmsi:987654321";
    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      context: otherVessel,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    assert.ok(
      captured[0].sql.includes(`context = '${otherVessel}'`),
      `Expected query to use other vessel context, got: ${captured[0].sql}`,
    );
  });

  it("defaults to vessels.self in response when context omitted", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    const result = await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    assert.equal(result.context, "vessels.self");
    assert.ok(captured[0].sql.includes("context = 'self'"));
  });
});

describe("history-v2 navigation.position aggregate", () => {
  async function capturePositionSql(aggregate: string): Promise<string> {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      resolution: 60,
      pathSpecs: [{ path: "navigation.position", aggregate, parameter: [] }],
    } as any);

    return captured[0].sql;
  }

  it("uses first(lat)/first(lon) for aggregate 'first'", async () => {
    const sql = await capturePositionSql("first");
    assert.ok(
      sql.includes("first(lat)") && sql.includes("first(lon)"),
      `Expected first(lat)/first(lon), got: ${sql}`,
    );
  });

  it("honors aggregate 'last' with last(lat)/last(lon)", async () => {
    const sql = await capturePositionSql("last");
    assert.ok(
      sql.includes("last(lat)") && sql.includes("last(lon)"),
      `Expected last(lat)/last(lon), got: ${sql}`,
    );
  });

  it("falls back to first for non-pair-preserving aggregates", async () => {
    for (const aggregate of ["average", "min", "max", "mid", "middle_index"]) {
      const sql = await capturePositionSql(aggregate);
      assert.ok(
        sql.includes("first(lat)") && sql.includes("first(lon)"),
        `Expected first(lat)/first(lon) for '${aggregate}', got: ${sql}`,
      );
    }
  });
});

describe("history-v2 sample bucket guard", () => {
  it("rejects a resolution that would fabricate millions of FILL(NULL) rows", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    // 60 days at 1s resolution = 5.18M buckets — over the 1M cap.
    await assert.rejects(
      provider.getValues({
        from: { toString: () => "2024-01-01T00:00:00Z" },
        to: { toString: () => "2024-03-01T00:00:00Z" },
        resolution: 1,
        pathSpecs: [
          { path: "navigation.position", aggregate: "first", parameter: [] },
        ],
      } as any),
      /sample buckets/,
    );
    // The guard must fire before any SQL reaches QuestDB.
    assert.equal(captured.length, 0);
  });

  it("allows the same range at a sane resolution", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-03-01T00:00:00Z" },
      resolution: 2600, // ~2000 buckets, the well-behaved-client budget
      pathSpecs: [
        { path: "navigation.position", aggregate: "first", parameter: [] },
      ],
    } as any);

    assert.equal(captured.length, 1);
    assert.ok(captured[0].sql.includes("SAMPLE BY 2600s"));
  });

  it("clamps fractional resolutions to 1s instead of SAMPLE BY 0s", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      resolution: 0.5,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    // The mock returns no rows, so the string-table fallback also runs; both
    // queries must carry the clamped period, never `SAMPLE BY 0s`. Assert the
    // fallback actually fired, so this cannot pass by silently skipping it.
    const stringQuery = captured.find(({ sql }) => sql.includes("signalk_str"));
    assert.ok(stringQuery, "expected a string-table fallback query");
    for (const { sql } of captured) {
      assert.ok(
        sql.includes("SAMPLE BY 1s"),
        `expected SAMPLE BY 1s, got: ${sql}`,
      );
    }
  });

  it("caps the fabricated total across multiple sampled paths", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    // 7 days at 1s = ~605K buckets per series: one series passes the 1M cap,
    // two series must not slip past it (1.21M fabricated rows total).
    await assert.rejects(
      provider.getValues({
        from: { toString: () => "2024-01-01T00:00:00Z" },
        to: { toString: () => "2024-01-08T00:00:00Z" },
        resolution: 1,
        pathSpecs: [
          { path: "navigation.position", aggregate: "first", parameter: [] },
          {
            path: "navigation.speedOverGround",
            aggregate: "average",
            parameter: [],
          },
        ],
      } as any),
      /sample buckets/,
    );
    assert.equal(captured.length, 0);
  });

  it("budgets the string-table fallback's second SAMPLE BY", async () => {
    // A non-numeric path costs two SAMPLE BY queries (numeric miss, then the
    // signalk_str fallback). Counting only the first would let a request of
    // boolean/string paths run at ~2x the cap this guard exists to enforce.
    // 7 days at 1s = ~605k buckets: one query fits under 1M, two do not.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await assert.rejects(
      provider.getValues({
        from: { toString: () => "2024-01-01T00:00:00Z" },
        to: { toString: () => "2024-01-08T00:00:00Z" },
        resolution: 1,
        pathSpecs: [
          {
            path: "electrical.switches.bilgePump.state",
            aggregate: "first",
            parameter: [],
          },
        ],
      } as any),
      /sample buckets/,
    );
    assert.equal(captured.length, 0, "must reject before querying QuestDB");
  });

  it("does not double-count navigation.position, which never falls back", async () => {
    // Position is served by its own table, so it costs exactly one query —
    // budgeting it as two would reject requests that are actually fine.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeMockClient(captured),
      SELF_CONTEXT,
    );

    await provider.getValues({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-08T00:00:00Z" },
      resolution: 1,
      pathSpecs: [
        { path: "navigation.position", aggregate: "first", parameter: [] },
      ],
    } as any);

    assert.ok(captured.length >= 1, "expected the position query to run");
  });

  it("does not reject client-side aggregates, which never SAMPLE BY", async () => {
    // 60 days at 1s would be 5.18M buckets — but every client-side aggregate
    // reads raw rows under a LIMIT, so the cap must not apply to any of them.
    const cases: [string, string[]][] = [
      ["sma", ["5"]],
      ["ema", ["0.2"]],
      ["middle_index", []],
    ];
    for (const [aggregate, parameter] of cases) {
      const captured: CapturedQuery[] = [];
      const provider = createHistoryProviderV2(
        makeMockClient(captured),
        SELF_CONTEXT,
      );

      await provider.getValues({
        from: { toString: () => "2024-01-01T00:00:00Z" },
        to: { toString: () => "2024-03-01T00:00:00Z" },
        resolution: 1,
        pathSpecs: [
          { path: "navigation.speedOverGround", aggregate, parameter },
        ],
      } as any);

      assert.equal(captured.length, 1, `no query captured for '${aggregate}'`);
      assert.ok(
        captured[0].sql.includes("LIMIT 50000") &&
          !captured[0].sql.includes("SAMPLE BY"),
        `expected raw-row LIMIT query for '${aggregate}', got: ${captured[0].sql}`,
      );
    }
  });
});

describe("history-v2 string-table fallback", () => {
  // Values live in `signalk` (numeric) or `signalk_str` (strings, and
  // booleans stored as "true"/"false" since #79). getValues only ever queried
  // the numeric table, so every non-numeric path was listed by getPaths but
  // returned no data at all.
  function mockClient(
    captured: CapturedQuery[],
    responder: (sql: string) => unknown[][],
  ) {
    return {
      exec: async (sql: string) => {
        captured.push({ sql });
        const dataset = responder(sql);
        return { columns: [], dataset, count: dataset.length, timestamp: 0 };
      },
    } as any;
  }

  const query = (path: string, extra: Record<string, unknown> = {}): any => {
    const { aggregate = "first", ...rest } = extra;
    return {
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      context: "self",
      pathSpecs: [{ path, aggregate, parameter: [] }],
      ...rest,
    };
  };

  it("serves a boolean path from signalk_str", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:01.000000Z", "true"]]
          : [],
      ),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(
      query("watermaker.brineomatic.high_pressure_pump_on"),
    );

    assert.deepEqual(result.data, [["2024-01-01T00:00:01.000000Z", "true"]]);
    assert.ok(captured.some((c) => c.sql.includes("signalk_str")));
  });

  it("does not query the string table when numeric data exists", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, () => [["2024-01-01T00:00:01.000000Z", 4.2]]),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(
      query("environment.depth.belowKeel"),
    );

    assert.deepEqual(result.data, [["2024-01-01T00:00:01.000000Z", 4.2]]);
    assert.ok(
      !captured.some((c) => c.sql.includes("signalk_str")),
      "numeric paths must not pay for a second query",
    );
  });

  it("falls back when SAMPLE BY fabricated only FILL(NULL) rows", async () => {
    // A downsampled numeric query returns a row per bucket even with no data,
    // so emptiness has to be judged on values, not row count.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "false"]]
          : [
              ["2024-01-01T00:00:00.000000Z", null],
              ["2024-01-01T00:10:00.000000Z", null],
            ],
      ),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(
      query("electrical.switches.bilgePump.state", { resolution: 600 }),
    );

    assert.deepEqual(result.data, [["2024-01-01T00:00:00.000000Z", "false"]]);
  });

  it("reports last() as the method actually applied when downsampling", async () => {
    // The response's `method` labels the series for consumers like Grafana;
    // echoing the requested "average" would name an aggregate that never ran.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "on"]]
          : [],
      ),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(
      query("navigation.state", { resolution: 600, aggregate: "average" }),
    );

    assert.equal(result.values[0].method, "last");
  });

  it("keeps the requested method for raw (non-downsampled) string reads", async () => {
    // Without a resolution no aggregate is applied at all, so nothing is
    // being misreported and the caller's choice stands.
    const provider = createHistoryProviderV2(
      mockClient([], (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "on"]]
          : [],
      ),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(query("navigation.state"));
    assert.equal(result.values[0].method, "first");
  });

  it("replays a tagged boolean as a boolean, matching v1", async () => {
    // The same path must not read `true` through v1 and `"true"` through v2.
    const provider = createHistoryProviderV2(
      mockClient([], (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "true", "boolean"]]
          : [],
      ),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(
      query("electrical.switches.bilgePump.state"),
    );
    assert.equal(result.data[0][1], true);
  });

  it('leaves an untagged "true" as text', async () => {
    // A path whose value genuinely is the word "true" carries no tag, as do
    // all rows written before value_kind existed.
    const provider = createHistoryProviderV2(
      mockClient([], (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "true", null]]
          : [],
      ),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(query("navigation.state"));
    assert.equal(result.data[0][1], "true");
  });

  it("degrades to text when value_kind is not migrated yet", async () => {
    // A read racing ensureTables(), or an external QuestDB, must not lose the
    // whole response to "Invalid column: value_kind".
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, (sql) => {
        if (sql.includes("value_kind") && !sql.includes("NULL as value_kind")) {
          throw new Error(
            "QuestDB query failed (400): Invalid column: value_kind",
          );
        }
        return sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "true", null]]
          : [];
      }),
      SELF_CONTEXT,
    );

    const result = await provider.getValues(query("some.path"));
    assert.equal(result.data[0][1], "true");
  });

  it("aggregates a downsampled string path with last(), not avg()", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:00.000000Z", "on"]]
          : [],
      ),
      SELF_CONTEXT,
    );

    await provider.getValues(query("navigation.state", { resolution: 600 }));

    const strSql = captured.find((c) => c.sql.includes("signalk_str"))!.sql;
    assert.ok(
      strSql.includes("last(value_str)"),
      `expected last(value_str), got: ${strSql}`,
    );
    assert.ok(!strSql.includes("avg("));
  });
});

describe("history-v2 path and context discovery", () => {
  type QueryClientArg = Parameters<typeof createHistoryProviderV2>[0];
  type RangeArg = Parameters<
    ReturnType<typeof createHistoryProviderV2>["getPaths"]
  >[0];

  function captureClient(captured: CapturedQuery[]): QueryClientArg {
    return {
      exec: async (sql: string) => {
        captured.push({ sql });
        return { columns: [], dataset: [], count: 0, timestamp: 0 };
      },
    } as unknown as QueryClientArg;
  }

  const range: RangeArg = {
    from: { toString: () => "2024-01-01T00:00:00Z", add: () => undefined },
    to: { toString: () => "2024-01-01T01:00:00Z" },
  };

  it("advertises navigation.position in getPaths", async () => {
    // The track table has no `path` column, so it was omitted entirely:
    // getValues served the position series while getPaths never listed it,
    // leaving clients unable to discover the one series they most want.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      captureClient(captured),
      SELF_CONTEXT,
    );

    await provider.getPaths(range);

    assert.ok(
      captured[0].sql.includes("signalk_position"),
      `getPaths must consult the track table, got: ${captured[0].sql}`,
    );
    assert.ok(captured[0].sql.includes("'navigation.position'"));
  });

  it("includes the track table in getContexts", async () => {
    // A vessel can be position-only (an AIS target whose other paths are
    // filtered out), and would otherwise be invisible.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      captureClient(captured),
      SELF_CONTEXT,
    );

    await provider.getContexts(range);

    assert.ok(
      captured[0].sql.includes("signalk_position"),
      `getContexts must consult the track table, got: ${captured[0].sql}`,
    );
  });
});

describe("history-v2 sourceRef filtering", () => {
  // The server delivers `paths=<path>|<sourceRef>` (signalk-server #2737) as
  // PathSpec.sourceRef. Filtering happens in SQL — the rows carry a `source`
  // column since the same-named migration — and the spec's sourceRef is
  // echoed in the response metadata so a caller can tell the columns apart.
  function mockClient(
    captured: CapturedQuery[],
    responder: (sql: string) => unknown[][],
  ) {
    return {
      exec: async (sql: string) => {
        captured.push({ sql });
        const dataset = responder(sql);
        return { columns: [], dataset, count: dataset.length, timestamp: 0 };
      },
    } as any;
  }

  const range = {
    from: { toString: () => "2024-01-01T00:00:00Z" },
    to: { toString: () => "2024-01-01T01:00:00Z" },
    context: "self",
  };

  it("adds a source clause when the spec carries a sourceRef", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, () => [["2024-01-01T00:00:01.000000Z", 4.2]]),
      SELF_CONTEXT,
    );

    await provider.getValues({
      ...range,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
          sourceRef: "n2k-on-ve.can0.115",
        },
      ],
    } as any);

    assert.ok(
      captured[0].sql.includes("AND source = 'n2k-on-ve.can0.115'"),
      `expected a source filter, got: ${captured[0].sql}`,
    );
  });

  it("omits the source clause when no sourceRef is given", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, () => [["2024-01-01T00:00:01.000000Z", 4.2]]),
      SELF_CONTEXT,
    );

    await provider.getValues({
      ...range,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    assert.ok(
      !captured[0].sql.includes("source ="),
      `expected no source filter, got: ${captured[0].sql}`,
    );
  });

  it("filters the position table by source too", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, () => [["2024-01-01T00:00:01.000000Z", 60.1, 24.9]]),
      SELF_CONTEXT,
    );

    await provider.getValues({
      ...range,
      pathSpecs: [
        {
          path: "navigation.position",
          aggregate: "first",
          parameter: [],
          sourceRef: "gps.main",
        },
      ],
    } as any);

    assert.ok(
      captured[0].sql.includes("signalk_position") &&
        captured[0].sql.includes("AND source = 'gps.main'"),
      `expected a source-filtered position query, got: ${captured[0].sql}`,
    );
  });

  it("keeps the source clause on the string-table fallback", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      mockClient(captured, (sql) =>
        sql.includes("signalk_str")
          ? [["2024-01-01T00:00:01.000000Z", "true"]]
          : [],
      ),
      SELF_CONTEXT,
    );

    await provider.getValues({
      ...range,
      pathSpecs: [
        {
          path: "switches.bilge.state",
          aggregate: "first",
          parameter: [],
          sourceRef: "n2k-on-ve.can0.42",
        },
      ],
    } as any);

    const fallback = captured.find((c) => c.sql.includes("signalk_str"));
    assert.ok(fallback, "expected the string-table fallback to run");
    assert.ok(
      fallback.sql.includes("AND source = 'n2k-on-ve.can0.42'"),
      `fallback must keep the source filter, got: ${fallback.sql}`,
    );
  });

  it("echoes the sourceRef in the values metadata", async () => {
    const provider = createHistoryProviderV2(
      mockClient([], () => [["2024-01-01T00:00:01.000000Z", 4.2]]),
      SELF_CONTEXT,
    );

    const result = await provider.getValues({
      ...range,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
          sourceRef: "gps.main",
        },
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
        },
      ],
    } as any);

    // signalk-server #2817 renamed the RESPONSE-side field to `$source`
    // while the request-side PathSpec kept `sourceRef`. Emitting the old key
    // left consumers unable to tell the columns apart.
    assert.equal(result.values[0].$source, "gps.main");
    assert.ok(
      !("$source" in result.values[1]),
      "an unfiltered spec must not grow a $source",
    );
    assert.ok(
      !("sourceRef" in result.values[0]),
      "the pre-#2817 key must not linger alongside $source",
    );
  });

  it("serves the same path from two sources as two distinct columns", async () => {
    // sourceRef filtering is what makes the same path twice in one request
    // meaningful; keyed by path the second spec's rows would overwrite the
    // first's and both columns would show the same series.
    const provider = createHistoryProviderV2(
      mockClient([], (sql) => {
        if (sql.includes("source = 'gps.main'"))
          return [["2024-01-01T00:00:01.000000Z", 1.1]];
        if (sql.includes("source = 'gps.backup'"))
          return [["2024-01-01T00:00:01.000000Z", 2.2]];
        return [];
      }),
      SELF_CONTEXT,
    );

    const result = await provider.getValues({
      ...range,
      pathSpecs: [
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
          sourceRef: "gps.main",
        },
        {
          path: "navigation.speedOverGround",
          aggregate: "average",
          parameter: [],
          sourceRef: "gps.backup",
        },
      ],
    } as any);

    assert.deepEqual(result.data, [["2024-01-01T00:00:01.000000Z", 1.1, 2.2]]);
  });

  it("rejects a sourceRef that is not a safe identifier", async () => {
    const provider = createHistoryProviderV2(
      mockClient([], () => []),
      SELF_CONTEXT,
    );

    await assert.rejects(() =>
      provider.getValues({
        ...range,
        pathSpecs: [
          {
            path: "navigation.speedOverGround",
            aggregate: "average",
            parameter: [],
            sourceRef: "x'; DROP TABLE signalk",
          },
        ],
      } as any),
    );
  });
});

/** Mock whose responses depend on the SQL, so DISTINCT can be answered. */
function makeSourceAwareClient(
  captured: CapturedQuery[],
  sourcesByTable: Record<string, (string | null)[]>,
  dataRows: [string, unknown][] = [],
) {
  return {
    exec: async (sql: string) => {
      captured.push({ sql });
      if (/SELECT DISTINCT source FROM (\w+)/.test(sql)) {
        const table = /FROM (\w+)/.exec(sql)![1];
        const list = sourcesByTable[table];
        if (list === undefined)
          throw new Error(`table does not exist: ${table}`);
        return {
          columns: [],
          dataset: list.map((s) => [s]),
          count: list.length,
          timestamp: 0,
        };
      }
      return {
        columns: [],
        dataset: dataRows.map(([ts, v]) => [ts, v]),
        count: dataRows.length,
        timestamp: 0,
      };
    },
  } as any;
}

describe("history-v2 sourcePolicy=all", () => {
  const RANGE = {
    from: { toString: () => "2024-01-01T00:00:00Z" },
    to: { toString: () => "2024-01-01T01:00:00Z" },
  };
  const SPEC = {
    path: "navigation.speedOverGround",
    aggregate: "average",
    parameter: [],
  };

  it("is inert until the operator enables it", async () => {
    const captured: CapturedQuery[] = [];
    // Third argument omitted = disabled, which is the shipped default.
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: ["gps.main", "gps.aux"] }),
      SELF_CONTEXT,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1, "must not expand while disabled");
    assert.ok(!("$source" in result.values[0]));
    assert.ok(
      !captured.some((c) => /SELECT DISTINCT source/.test(c.sql)),
      "a disabled provider must not even probe for sources",
    );
  });

  it("expands one path into one column per source when enabled", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk: ["gps.aux", "gps.main"],
        signalk_str: [],
      }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.deepEqual(
      result.values.map((v) => v.$source),
      ["gps.aux", "gps.main"],
      "one column per source, named sources sorted for stable order",
    );
    // Each column must be FILTERED to its own source, or every column would
    // carry identical merged data under different labels.
    assert.ok(captured.some((c) => /source = 'gps.main'/.test(c.sql)));
    assert.ok(captured.some((c) => /source = 'gps.aux'/.test(c.sql)));
  });

  // The data array must be as wide as `values`. Assembling rows against the
  // REQUESTED spec count instead of the expanded column count leaves every
  // expanded column without a slot — the metadata promises N series and the
  // rows carry one.
  it("gives every expanded column its own slot in each data row", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(
        captured,
        { signalk: ["gps.aux", "gps.main"], signalk_str: [] },
        [
          ["2024-01-01T00:00:00.000Z", 1.5],
          ["2024-01-01T00:00:01.000Z", 2.5],
        ],
      ),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 2);
    assert.ok(result.data.length > 0, "expected data rows");
    for (const row of result.data) {
      assert.equal(
        row.length - 1,
        result.values.length,
        `row has ${row.length - 1} value slots for ${result.values.length} columns`,
      );
    }
  });

  it("leaves an explicit sourceRef as a filter, not an expansion", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: ["gps.main", "gps.aux"] }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [{ ...SPEC, sourceRef: "gps.main" }],
    } as any);

    assert.equal(result.values.length, 1);
    assert.equal(result.values[0].$source, "gps.main");
    assert.ok(
      !captured.some((c) => /SELECT DISTINCT source/.test(c.sql)),
      "an explicit source needs no discovery",
    );
  });

  it("does nothing without the policy, even when enabled", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: ["gps.main", "gps.aux"] }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1);
    assert.ok(!("$source" in result.values[0]));
  });

  it("treats an unrecognised policy as absent", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: ["gps.main", "gps.aux"] }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "none",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1, "only 'all' expands");
    assert.ok(!("$source" in result.values[0]));
    assert.ok(
      !captured.some((c) => /SELECT DISTINCT source/.test(c.sql)),
      "an unknown policy must not probe for sources",
    );
  });

  // The per-path cap alone is not enough: many paths at many sources each is
  // hundreds of queries from one request.
  it("refuses a request whose total expansion is too wide", async () => {
    const captured: CapturedQuery[] = [];
    const eight = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: eight, signalk_str: [] }),
      SELF_CONTEXT,
      true,
    );

    // 9 paths x 8 sources = 72 columns, past the 64 request ceiling.
    const specs = Array.from({ length: 9 }, (_, i) => ({
      path: `nav.p${i}`,
      aggregate: "average",
      parameter: [],
    }));

    await assert.rejects(
      () =>
        provider.getValues({
          ...RANGE,
          sourcePolicy: "all",
          pathSpecs: specs,
        } as any),
      /expands these paths into 72 columns/,
    );
  });

  // Discovery is a query per path per table, so a request far past the
  // ceiling must be rejected BEFORE issuing hundreds of DISTINCT probes.
  it("stops probing once the column budget is blown", async () => {
    const captured: CapturedQuery[] = [];
    const eight = ["s1", "s2", "s3", "s4", "s5", "s6", "s7", "s8"];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: eight, signalk_str: [] }),
      SELF_CONTEXT,
      true,
    );
    const specs = Array.from({ length: 100 }, (_, i) => ({
      path: `nav.p${i}`,
      aggregate: "average",
      parameter: [],
    }));

    await assert.rejects(
      () =>
        provider.getValues({
          ...RANGE,
          sourcePolicy: "all",
          pathSpecs: specs,
        } as any),
      /more than 64 columns/,
    );

    const probes = captured.filter((c) =>
      /SELECT DISTINCT source/.test(c.sql),
    ).length;
    // 100 paths x 2 tables would be 200 if discovery ran to completion.
    assert.ok(
      probes < 40,
      `issued ${probes} discovery probes before giving up`,
    );
  });

  // The two value tables migrate independently, so `signalk` can have a
  // `source` column while `signalk_str` does not. Expansion is driven by
  // whichever table answered and then applied to both, so the string fallback
  // carried `AND source = '...'` into a table with no such column and failed
  // the WHOLE request — verified against a live QuestDB.
  it("degrades the string fallback when only signalk_str lacks source", async () => {
    const warnings: string[] = [];
    const captured: CapturedQuery[] = [];
    const client = {
      exec: async (sql: string) => {
        captured.push({ sql });
        if (/DISTINCT source FROM signalk_str/.test(sql)) {
          throw new Error("Invalid column: source");
        }
        if (/DISTINCT source FROM signalk/.test(sql)) {
          return {
            columns: [],
            dataset: [["gps.main"]],
            count: 1,
            timestamp: 0,
          };
        }
        // The numeric table holds nothing, forcing the string fallback.
        if (/FROM signalk WHERE/.test(sql)) {
          return { columns: [], dataset: [], count: 0, timestamp: 0 };
        }
        // The string table rejects any query carrying a source predicate.
        if (/FROM signalk_str/.test(sql) && /source/.test(sql)) {
          throw new Error("Invalid column: source");
        }
        return {
          columns: [],
          dataset: [["2024-01-01T00:00:00.000Z", "docked", null]],
          count: 1,
          timestamp: 0,
        };
      },
    } as any;

    const provider = createHistoryProviderV2(client, SELF_CONTEXT, true, (m) =>
      warnings.push(m),
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [
        { path: "navigation.state", aggregate: "last", parameter: [] },
      ],
    } as any);

    // The request succeeds rather than failing outright...
    assert.equal(result.values.length, 1);
    assert.deepEqual(result.data, [["2024-01-01T00:00:00.000Z", "docked"]]);
    // ...and the column withdraws its $source claim, because the rows it
    // returned were never filtered by source.
    assert.ok(
      !("$source" in result.values[0]),
      "unfiltered rows must not be labelled as one source's",
    );
    assert.ok(
      warnings.some((w) => /signalk_str has no 'source'/.test(w)),
      "the degradation must be reported",
    );
  });

  it("gives unattributed rows their own column, ordered last", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk: [null, "gps.main"],
        signalk_str: [],
      }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 2);
    assert.equal(result.values[0].$source, "gps.main");
    assert.ok(
      !("$source" in result.values[1]),
      "the unattributed column carries no source claim",
    );
    // The unattributed column must select `source IS NULL`, NOT run unfiltered.
    // An unfiltered query returns EVERY source's rows, so the "no source"
    // column silently duplicates all the others — verified against a live
    // QuestDB, where it returned 60 rows instead of its own 20.
    const dataQueries = captured.filter(
      (c) => !/DISTINCT source/.test(c.sql) && /FROM signalk\b/.test(c.sql),
    );
    assert.ok(
      dataQueries.some((c) => /AND source IS NULL/.test(c.sql)),
      "the unattributed column must filter on source IS NULL",
    );
    assert.equal(
      dataQueries.filter(
        (c) =>
          !/AND source = /.test(c.sql) && !/AND source IS NULL/.test(c.sql),
      ).length,
      0,
      "no expanded column may run without a source predicate",
    );
    assert.ok(
      !dataQueries.some((c) => /source = 'null'/.test(c.sql)),
      "a null source must never become the literal string 'null'",
    );
  });

  it("falls back to a single column when nothing was recorded in range", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: [], signalk_str: [] }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1, "the path must not vanish");
    assert.ok(!("$source" in result.values[0]));
  });

  // A legacy table with no `source` column must not make expansion quietly
  // do nothing: the request still succeeds with one merged column, but the
  // operator is told why, naming the column and the table.
  it("warns, rather than silently degrading, on a missing source column", async () => {
    const warnings: string[] = [];
    const client = {
      exec: async (sql: string) => {
        if (/DISTINCT source/.test(sql)) {
          throw new Error("Invalid column: source");
        }
        return { columns: [], dataset: [], count: 0, timestamp: 0 };
      },
    } as any;
    const provider = createHistoryProviderV2(client, SELF_CONTEXT, true, (m) =>
      warnings.push(m),
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1, "still answers, unexpanded");
    assert.ok(warnings.length > 0, "the degradation must not be silent");
    assert.match(warnings[0], /source/);
    assert.match(warnings[0], /signalk/, "the warning names the table");
  });

  // A timeout or 5xx is NOT "no sources". Reporting it as an empty result
  // would hand back a plausible single column built on a failure nobody saw.
  it("propagates a real query failure instead of reporting no sources", async () => {
    const client = {
      exec: async (sql: string) => {
        if (/DISTINCT source/.test(sql)) {
          throw new Error("connection reset by peer");
        }
        return { columns: [], dataset: [], count: 0, timestamp: 0 };
      },
    } as any;
    const provider = createHistoryProviderV2(client, SELF_CONTEXT, true);

    await assert.rejects(
      () =>
        provider.getValues({
          ...RANGE,
          sourcePolicy: "all",
          pathSpecs: [SPEC],
        } as any),
      /connection reset/,
    );
  });

  // A table that does not exist yet (no string values recorded) is normal,
  // not a schema fault, and must not produce a warning.
  it("stays quiet when a value table simply does not exist", async () => {
    const warnings: string[] = [];
    const client = {
      exec: async (sql: string) => {
        if (/DISTINCT source FROM signalk_str/.test(sql)) {
          throw new Error("table does not exist [table=signalk_str]");
        }
        if (/DISTINCT source/.test(sql)) {
          return {
            columns: [],
            dataset: [["gps.main"]],
            count: 1,
            timestamp: 0,
          };
        }
        return { columns: [], dataset: [], count: 0, timestamp: 0 };
      },
    } as any;
    const provider = createHistoryProviderV2(client, SELF_CONTEXT, true, (m) =>
      warnings.push(m),
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values[0].$source, "gps.main");
    assert.equal(warnings.length, 0, "an absent table is not a fault");
  });

  // A STORED sourceRef need not satisfy the identifier guard — "tcp://gw:2000"
  // is an ordinary Signal K source containing characters the guard rejects.
  // Letting it reach the query builder throws and takes the WHOLE request
  // down, so expansion would break a query that works today.
  it("skips a source whose name cannot be used as a filter", async () => {
    const warnings: string[] = [];
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk: ["gps.main", "tcp://gw:2000"],
        signalk_str: [],
      }),
      SELF_CONTEXT,
      true,
      (m) => warnings.push(m),
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.deepEqual(
      result.values.map((v) => v.$source),
      ["gps.main"],
      "the usable source still expands",
    );
    assert.ok(
      warnings.some((w) => w.includes("tcp://gw:2000")),
      "the skipped source must be reported",
    );
  });

  it("keeps the path as one column when every source is unusable", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk: ["tcp://a", "tcp://b"],
        signalk_str: [],
      }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1, "the path must not vanish");
    assert.ok(!("$source" in result.values[0]));
  });

  // The bucket cap only bites when a resolution was named. An unresolved
  // request runs one raw query per source with nothing else bounding it.
  it("caps how many columns one path may expand into", async () => {
    const warnings: string[] = [];
    const captured: CapturedQuery[] = [];
    const many = Array.from(
      { length: 40 },
      (_, i) => `src${String(i).padStart(2, "0")}`,
    );
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, { signalk: many, signalk_str: [] }),
      SELF_CONTEXT,
      true,
      (m) => warnings.push(m),
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 16, `got ${result.values.length}`);
    assert.ok(
      warnings.some((w) => /40 sources/.test(w)),
      "truncation must be reported, not silent",
    );
    // Deterministic: the FIRST 16 sorted, not an arbitrary 16.
    assert.equal(result.values[0].$source, "src00");
    assert.equal(result.values[15].$source, "src15");
  });

  it("survives a database where no value table exists yet", async () => {
    const captured: CapturedQuery[] = [];
    // No table answers DISTINCT: the mock throws for both.
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {}),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [SPEC],
    } as any);

    assert.equal(result.values.length, 1);
    assert.ok(!("$source" in result.values[0]));
  });

  it("consults the string table so boolean paths expand too", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk: [],
        signalk_str: ["switch.a"],
      }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [
        {
          path: "electrical.switches.nav.state",
          aggregate: "last",
          parameter: [],
        },
      ],
    } as any);

    assert.equal(result.values[0].$source, "switch.a");
  });

  it("counts EXPANDED columns against the sample-bucket cap", async () => {
    // Four sources over a range that is just under the cap for ONE column.
    // Counting requested paths instead of columns would run at 4x the ceiling.
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk: ["a", "b", "c", "d"],
        signalk_str: [],
      }),
      SELF_CONTEXT,
      true,
    );

    await assert.rejects(
      () =>
        provider.getValues({
          from: { toString: () => "2024-01-01T00:00:00Z" },
          // 400_000s at 1s = 400k buckets per series; 4 sources fall back too,
          // so 8 queries x 400k = 3.2M, past the 1M cap.
          to: { toString: () => "2024-01-05T15:06:40Z" },
          resolution: 1,
          sourcePolicy: "all",
          pathSpecs: [SPEC],
        } as any),
      /sample buckets/,
    );
  });

  it("position expands from its own table", async () => {
    const captured: CapturedQuery[] = [];
    const provider = createHistoryProviderV2(
      makeSourceAwareClient(captured, {
        signalk_position: ["gps.main", "gps.aux"],
      }),
      SELF_CONTEXT,
      true,
    );

    const result = await provider.getValues({
      ...RANGE,
      sourcePolicy: "all",
      pathSpecs: [
        { path: "navigation.position", aggregate: "first", parameter: [] },
      ],
    } as any);

    assert.deepEqual(
      result.values.map((v) => v.$source),
      ["gps.aux", "gps.main"],
    );
    assert.ok(
      captured.some((c) => /DISTINCT source FROM signalk_position/.test(c.sql)),
      "position must be probed in its own table, not signalk",
    );
  });
});
