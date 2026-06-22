import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  validateIdentifier,
  validateTimestamp,
  isReadOnlySQL,
  QueryClient,
} from "../query-client";
import type { QuestDBResult } from "../query-client";

// Build a QueryClient whose exec() is stubbed: every call records the SQL and
// returns the next canned result (a single-column `column` dataset for the
// designated-timestamp introspection, or an empty dataset for DDL). Lets us
// test the schema-heal logic without a live QuestDB.
function stubClient(responder: (sql: string) => QuestDBResult): {
  client: QueryClient;
  sql: string[];
} {
  const client = new QueryClient("127.0.0.1", 9000);
  const sql: string[] = [];
  (client as unknown as { exec: (q: string) => Promise<QuestDBResult> }).exec =
    (q: string) => {
      sql.push(q);
      return Promise.resolve(responder(q));
    };
  return { client, sql };
}

const tsRow = (column: string): QuestDBResult => ({
  columns: [{ name: "column", type: "STRING" }],
  dataset: [[column]],
  count: 1,
  timestamp: -1,
});
const emptyResult: QuestDBResult = {
  columns: [{ name: "column", type: "STRING" }],
  dataset: [],
  count: 0,
  timestamp: -1,
};

describe("validateIdentifier", () => {
  it("accepts valid Signal K paths", () => {
    assert.equal(
      validateIdentifier("navigation.speedOverGround"),
      "navigation.speedOverGround",
    );
    assert.equal(validateIdentifier("self"), "self");
    assert.equal(
      validateIdentifier("vessels.urn:mrn:imo:mmsi:123456789"),
      "vessels.urn:mrn:imo:mmsi:123456789",
    );
  });

  it("accepts paths containing underscores", () => {
    assert.equal(
      validateIdentifier("electrical.batteries.house_bank.voltage"),
      "electrical.batteries.house_bank.voltage",
    );
    assert.equal(
      validateIdentifier("tanks.fuel.starboard_main.currentLevel"),
      "tanks.fuel.starboard_main.currentLevel",
    );
  });

  it("rejects SQL injection attempts", () => {
    assert.throws(() => validateIdentifier("'; DROP TABLE signalk;--"));
    assert.throws(() => validateIdentifier("path OR 1=1"));
    assert.throws(() => validateIdentifier("path\nSELECT"));
  });
});

describe("validateTimestamp", () => {
  it("accepts valid ISO timestamps", () => {
    const result = validateTimestamp("2024-06-15T12:00:00.000Z");
    assert.equal(result, "2024-06-15T12:00:00.000Z");
  });

  it("normalizes various timestamp formats to ISO", () => {
    const result = validateTimestamp("2024-06-15");
    assert.ok(result.startsWith("2024-06-15"), `Got: ${result}`);
  });

  it("rejects invalid timestamps", () => {
    assert.throws(() => validateTimestamp("not-a-date"));
    assert.throws(() => validateTimestamp(""));
  });
});

describe("isReadOnlySQL", () => {
  it("allows SELECT queries", () => {
    assert.ok(isReadOnlySQL("SELECT count() FROM signalk"));
    assert.ok(
      isReadOnlySQL("  SELECT ts, value FROM signalk WHERE path = 'nav'"),
    );
    assert.ok(isReadOnlySQL("WITH cte AS (SELECT 1) SELECT * FROM cte"));
  });

  it("allows SHOW queries", () => {
    assert.ok(isReadOnlySQL("SHOW TABLES"));
    assert.ok(isReadOnlySQL("SHOW COLUMNS FROM signalk"));
  });

  it("blocks DDL and DML", () => {
    assert.ok(!isReadOnlySQL("DROP TABLE signalk"));
    assert.ok(!isReadOnlySQL("ALTER TABLE signalk ADD COLUMN x INT"));
    assert.ok(!isReadOnlySQL("INSERT INTO signalk VALUES (1)"));
    assert.ok(!isReadOnlySQL("DELETE FROM signalk WHERE ts < now()"));
    assert.ok(!isReadOnlySQL("CREATE TABLE evil (x INT)"));
    assert.ok(!isReadOnlySQL("TRUNCATE TABLE signalk"));
  });

  it("blocks DDL hidden in SELECT", () => {
    assert.ok(
      !isReadOnlySQL("SELECT 1; DROP TABLE signalk"),
      "Should block DROP even after SELECT",
    );
  });
});

describe("QueryClient schema heal", () => {
  it("reports the designated timestamp column", async () => {
    const { client } = stubClient(() => tsRow("ts"));
    assert.equal(await client.designatedTimestamp("signalk"), "ts");
  });

  it("returns null when the table has no designated timestamp / is missing", async () => {
    const { client } = stubClient(() => emptyResult);
    assert.equal(await client.designatedTimestamp("signalk"), null);
  });

  it("treats a `ts` designated timestamp as no mismatch", async () => {
    const { client } = stubClient(() => tsRow("ts"));
    assert.equal(await client.hasSchemaMismatch("signalk"), false);
  });

  it("flags a `timestamp` (ILP auto-created) designated timestamp as a mismatch", async () => {
    const { client } = stubClient(() => tsRow("timestamp"));
    assert.equal(await client.hasSchemaMismatch("signalk"), true);
  });

  it("treats a missing table as no mismatch", async () => {
    const { client } = stubClient(() => emptyResult);
    assert.equal(await client.hasSchemaMismatch("signalk"), false);
  });

  it("treats introspection failure as no mismatch (swallowed)", async () => {
    const { client } = stubClient(() => {
      throw new Error("table does not exist");
    });
    assert.equal(await client.hasSchemaMismatch("signalk"), false);
  });

  it("healSchema is a no-op on a correct table", async () => {
    const { client, sql } = stubClient(() => tsRow("ts"));
    assert.equal(await client.healSchema("signalk"), false);
    // Only the introspection query ran — no DROP, no CREATE.
    assert.ok(sql.every((q) => !/DROP TABLE|CREATE TABLE/i.test(q)));
  });

  it("healSchema drops and recreates a wrong-schema table", async () => {
    // First introspection says `timestamp` (mismatch); after the rebuild the
    // CREATE TABLE statements just return empty.
    let firstIntrospection = true;
    const { client, sql } = stubClient((q) => {
      if (/table_columns/.test(q)) {
        if (firstIntrospection) {
          firstIntrospection = false;
          return tsRow("timestamp");
        }
        return tsRow("ts");
      }
      return emptyResult;
    });
    assert.equal(await client.healSchema("signalk"), true);
    assert.ok(
      sql.some((q) => /DROP TABLE IF EXISTS signalk/i.test(q)),
      "should drop the wrong-schema table",
    );
    assert.ok(
      sql.some((q) => /CREATE TABLE IF NOT EXISTS signalk\b/i.test(q)),
      "should recreate via ensureTables",
    );
  });

  it("rejects an invalid table identifier", async () => {
    const { client } = stubClient(() => emptyResult);
    await assert.rejects(() =>
      client.designatedTimestamp("signalk; DROP TABLE x"),
    );
  });

  it("hasSchemaMismatch rejects an invalid identifier instead of swallowing it", async () => {
    const { client } = stubClient(() => emptyResult);
    await assert.rejects(() =>
      client.hasSchemaMismatch("signalk; DROP TABLE x"),
    );
  });
});
