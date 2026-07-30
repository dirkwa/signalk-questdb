import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { createHistoryProviderV2 } from "../history-v2";

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
    // queries must carry the clamped period, never `SAMPLE BY 0s`.
    assert.ok(captured.length >= 1);
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

  const query = (path: string, extra: Record<string, unknown> = {}) =>
    ({
      from: { toString: () => "2024-01-01T00:00:00Z" },
      to: { toString: () => "2024-01-01T01:00:00Z" },
      context: "self",
      pathSpecs: [{ path, aggregate: "first", parameter: [] }],
      ...extra,
    }) as any;

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
