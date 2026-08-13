import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  validateIdentifier,
  validateTimestamp,
  isReadOnlySQL,
  QueryClient,
} from "../query-client.js";
import type { QuestDBResult } from "../query-client.js";

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

  it("double-quotes the `column` keyword in the introspection query", async () => {
    // `column` is a SQL keyword in QuestDB; an unquoted `SELECT column` is
    // rejected at parse time. The stub matches blind, so guard the exact SQL.
    const { client, sql } = stubClient(() => tsRow("ts"));
    await client.designatedTimestamp("signalk");
    assert.match(sql[0], /SELECT "column" FROM table_columns\(/);
    assert.doesNotMatch(sql[0], /SELECT column\b/);
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

describe("QueryClient.toObjects", () => {
  const client = new QueryClient("127.0.0.1", 9000);

  it("maps a populated dataset to named objects", () => {
    const objs = client.toObjects({
      columns: [
        { name: "name", type: "STRING" },
        { name: "suspended", type: "BOOLEAN" },
        { name: "errorMessage", type: "STRING" },
      ],
      dataset: [["signalk", true, "OUT_OF_MEMORY: mmap failed"]],
      count: 1,
      timestamp: -1,
    });
    assert.deepEqual(objs, [
      {
        name: "signalk",
        suspended: true,
        errorMessage: "OUT_OF_MEMORY: mmap failed",
      },
    ]);
  });

  it("returns [] when the result carries rows but no column metadata", () => {
    // QuestDB omits `columns` when queried with nm=true. A non-empty dataset
    // in that shape previously threw; it must degrade to [] instead.
    const result = {
      dataset: [["signalk", true]],
      count: 1,
    } as unknown as QuestDBResult;
    assert.deepEqual(client.toObjects(result), []);
  });
});

describe("QueryClient statement timeout header", () => {
  // A real (loopback) HTTP server instead of a fetch stub: the point is to
  // assert what actually goes on the wire, since Statement-Timeout is only
  // honored when QuestDB receives it as a request header.
  async function captureHeaders(
    run: (client: QueryClient) => Promise<unknown>,
  ): Promise<Record<string, string | string[] | undefined>> {
    const http = await import("node:http");
    let headers: Record<string, string | string[] | undefined> = {};
    const server = http.createServer((req, res) => {
      headers = req.headers;
      res.setHeader("content-type", "application/json");
      res.end(
        JSON.stringify({ columns: [], dataset: [], count: 0, timestamp: -1 }),
      );
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as { port: number }).port;
    try {
      await run(new QueryClient("127.0.0.1", port));
    } finally {
      server.close();
    }
    return headers;
  }

  it("mirrors the exec() deadline into Statement-Timeout (ms)", async () => {
    const headers = await captureHeaders((client) =>
      client.exec("SELECT 1", 12345),
    );
    assert.equal(headers["statement-timeout"], "12345");
  });

  it("sends the default deadline when none is given", async () => {
    const headers = await captureHeaders((client) => client.exec("SELECT 1"));
    assert.equal(headers["statement-timeout"], "30000");
  });

  it("bounds CSV exports the same way", async () => {
    const headers = await captureHeaders((client) =>
      client.execCsv("SELECT 1"),
    );
    assert.equal(headers["statement-timeout"], "60000");
  });

  it("mirrors an explicit CSV deadline into Statement-Timeout", async () => {
    const headers = await captureHeaders((client) =>
      client.execCsv("SELECT 1", 12345),
    );
    assert.equal(headers["statement-timeout"], "12345");
  });
});

describe("QueryClient.ensureTables schema migration", () => {
  it("creates signalk_str with value_kind and migrates existing tables", async () => {
    // CREATE TABLE IF NOT EXISTS leaves a pre-existing table untouched, so
    // installs that predate value_kind need the explicit ALTER. It must run
    // unconditionally (it is idempotent) or those installs never gain the
    // column and every replayed boolean silently degrades to text.
    const { client, sql } = stubClient(() => emptyResult);
    await client.ensureTables();

    const created = sql.find((q) =>
      q.includes("CREATE TABLE IF NOT EXISTS signalk_str"),
    );
    assert.ok(created, "expected signalk_str to be created");
    assert.ok(
      created.includes("value_kind"),
      `new tables must include value_kind, got: ${created}`,
    );

    const altered = sql.find((q) => q.includes("ALTER TABLE signalk_str"));
    assert.ok(altered, "expected the migration ALTER for existing tables");
    assert.ok(
      altered.includes("ADD COLUMN IF NOT EXISTS value_kind"),
      `migration must be idempotent, got: ${altered}`,
    );
  });

  it("adds the source column and grows the dedup keys on every table", async () => {
    // Same shape as the value_kind migration: CREATE TABLE IF NOT EXISTS
    // leaves pre-existing tables untouched, so each table needs the explicit
    // idempotent ALTER. The dedup keys must grow with the column too — a
    // replayed ILP batch keeps its original stamps, so two sources stamped
    // identically would otherwise upsert over each other and replay would
    // silently drop one source's row.
    const { client, sql } = stubClient(() => emptyResult);
    await client.ensureTables();

    for (const table of ["signalk", "signalk_str", "signalk_position"]) {
      const created = sql.find((q) =>
        q.includes(`CREATE TABLE IF NOT EXISTS ${table} (`),
      );
      assert.ok(created, `expected ${table} to be created`);
      assert.match(
        created,
        /source\s+SYMBOL/,
        `new ${table} must include the source column`,
      );
      assert.match(
        created,
        /DEDUP UPSERT KEYS\([^)]*\bsource\)/,
        `new ${table} must dedup on source, got: ${created}`,
      );

      assert.ok(
        sql.some((q) =>
          q.includes(`ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS source`),
        ),
        `expected the source ADD COLUMN migration for ${table}`,
      );
      const dedup = sql.find((q) =>
        q.includes(`ALTER TABLE ${table} DEDUP ENABLE UPSERT KEYS(`),
      );
      assert.ok(dedup, `expected the dedup-key migration for ${table}`);
      assert.ok(
        dedup.includes("source"),
        `migrated dedup keys must include source, got: ${dedup}`,
      );
    }
  });
});
