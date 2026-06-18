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
