const SAFE_IDENTIFIER = /^[a-zA-Z0-9_.:-]+$/;
const READ_ONLY_SQL = /^\s*(SELECT|SHOW|WITH)\b/i;
const DANGEROUS_SQL =
  /\b(DROP|ALTER|INSERT|UPDATE|DELETE|CREATE|TRUNCATE|GRANT|REVOKE)\b/i;

export interface QuestDBColumn {
  name: string;
  type: string;
}

export interface QuestDBResult {
  columns: QuestDBColumn[];
  dataset: unknown[][];
  count: number;
  timestamp: number;
}

export function validateIdentifier(value: string): string {
  if (!SAFE_IDENTIFIER.test(value)) {
    throw new Error(`Invalid identifier: ${value}`);
  }
  return value;
}

export function validateTimestamp(value: string): string {
  const d = new Date(value);
  if (isNaN(d.getTime())) {
    throw new Error(`Invalid timestamp: ${value}`);
  }
  return d.toISOString();
}

export function isReadOnlySQL(sql: string): boolean {
  return READ_ONLY_SQL.test(sql) && !DANGEROUS_SQL.test(sql);
}

export class QueryClient {
  private baseUrl: string;

  constructor(host: string, port: number) {
    this.baseUrl = `http://${host}:${port}`;
  }

  async exec(sql: string, timeoutMs = 30000): Promise<QuestDBResult> {
    const url = new URL("/exec", this.baseUrl);
    url.searchParams.set("query", sql);
    // Do NOT pass nm=true: it tells QuestDB to omit the `columns` metadata,
    // which `toObjects` needs to map each row to a named object. Without the
    // names, toObjects throws on any non-empty result — silently breaking
    // every consumer that reads rows by name (history playback, /api/query,
    // the suspended-WAL status probe). The metadata is small; correctness
    // wins over the few bytes saved.
    //
    // `timeoutMs` matters for statements QuestDB's HTTP endpoint can PARK:
    // an ALTER against a busy table is retried server-side once per second
    // with the response held open ("JsonQueryProcessor resource busy, will
    // retry"), so without a deadline the call would wait the full default.
    //
    // The deadline is enforced on BOTH sides. The AbortSignal frees us, but
    // an aborted fetch leaves QuestDB executing the statement until its own
    // default timeout (60s) — and the managed container runs a single shared
    // worker (low-power tuning), so one abandoned heavy query convoys every
    // query behind it. `Statement-Timeout` (milliseconds) makes QuestDB stop
    // the work itself at the same deadline.

    const res = await fetch(url.toString(), {
      signal: AbortSignal.timeout(timeoutMs),
      headers: { "Statement-Timeout": String(timeoutMs) },
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`QuestDB query failed (${res.status}): ${body}`);
    }

    return res.json() as Promise<QuestDBResult>;
  }

  async execSafe(sql: string): Promise<QuestDBResult> {
    if (!isReadOnlySQL(sql)) {
      throw new Error("Only read-only SQL queries are allowed");
    }
    return this.exec(sql);
  }

  async execCsv(sql: string, timeoutMs = 60000): Promise<string> {
    const url = new URL("/exp", this.baseUrl);
    url.searchParams.set("query", sql);

    // Same double-sided deadline as exec(): the abort frees us, the header
    // stops QuestDB from burning its worker on an export nobody is reading.
    const res = await fetch(url.toString(), {
      signal: AbortSignal.timeout(timeoutMs),
      headers: { "Statement-Timeout": String(timeoutMs) },
    });

    if (!res.ok) {
      const body = await res.text().catch(() => "");
      throw new Error(`QuestDB CSV export failed (${res.status}): ${body}`);
    }

    return res.text();
  }

  async isHealthy(): Promise<boolean> {
    try {
      const res = await fetch(`${this.baseUrl}/exec?query=SELECT+1`, {
        signal: AbortSignal.timeout(5000),
      });
      return res.ok;
    } catch {
      return false;
    }
  }

  async ensureTables(): Promise<void> {
    await this.exec(`
      CREATE TABLE IF NOT EXISTS signalk (
        ts        TIMESTAMP,
        path      SYMBOL CAPACITY 512 CACHE,
        context   SYMBOL CAPACITY 128 CACHE,
        source    SYMBOL CAPACITY 256 CACHE,
        value     DOUBLE
      ) TIMESTAMP(ts)
        PARTITION BY DAY
        WAL
        DEDUP UPSERT KEYS(ts, path, context, source)
    `);

    await this.exec(`
      CREATE TABLE IF NOT EXISTS signalk_str (
        ts         TIMESTAMP,
        path       SYMBOL CAPACITY 256 CACHE,
        context    SYMBOL CAPACITY 128 CACHE,
        source     SYMBOL CAPACITY 256 CACHE,
        value_str  VARCHAR,
        value_kind SYMBOL CAPACITY 8 CACHE
      ) TIMESTAMP(ts)
        PARTITION BY DAY
        WAL
        DEDUP UPSERT KEYS(ts, path, context, source)
    `);

    // Tables created before value_kind existed keep working: CREATE TABLE IF
    // NOT EXISTS leaves them untouched, so add the column separately. Rows
    // written before this migration have value_kind = null, which readers
    // treat as "text" — the original type of those rows is not recoverable,
    // and guessing from the text would turn a legitimate "true" string into a
    // boolean. Idempotent, so it can run on every start.
    await this.exec(
      `ALTER TABLE signalk_str ADD COLUMN IF NOT EXISTS value_kind SYMBOL CAPACITY 8 CACHE`,
    );

    await this.exec(`
      CREATE TABLE IF NOT EXISTS signalk_position (
        ts        TIMESTAMP,
        context   SYMBOL CAPACITY 128 CACHE,
        source    SYMBOL CAPACITY 256 CACHE,
        lat       DOUBLE,
        lon       DOUBLE
      ) TIMESTAMP(ts)
        PARTITION BY DAY
        WAL
        DEDUP UPSERT KEYS(ts, context, source)
    `);

    // Same migration pattern for `source` (the delta's sourceRef): rows from
    // before the column have source = null, which reads back as "unknown" —
    // there is nothing to backfill them from. The dedup keys must grow with
    // the column, or two sources stamped identically (only possible on ILP
    // batch replay, where original stamps are kept) would upsert over each
    // other and replay-idempotency would silently drop one source's row.
    for (const [table, keys] of [
      ["signalk", "ts, path, context, source"],
      ["signalk_str", "ts, path, context, source"],
      ["signalk_position", "ts, context, source"],
    ] as const) {
      await this.exec(
        `ALTER TABLE ${table} ADD COLUMN IF NOT EXISTS source SYMBOL CAPACITY 256 CACHE`,
      );
      await this.exec(`ALTER TABLE ${table} DEDUP ENABLE UPSERT KEYS(${keys})`);
    }
  }

  // QuestDB's ILP ingestion auto-creates a missing table, but names the
  // designated timestamp `timestamp`, whereas our schema (and every query)
  // uses `ts`. If the `signalk` table is dropped while the plugin runs, the
  // next ILP flush can recreate it with the wrong shape — rows then ingest but
  // `WHERE ts > ...` and the history providers read nothing. This returns the
  // name of the designated-timestamp column, or null if the table is missing
  // (or has no designated timestamp).
  async designatedTimestamp(table: string): Promise<string | null> {
    // `column` is a SQL keyword in QuestDB, so the column reference must be
    // double-quoted — `SELECT column` is rejected at parse time. Without the
    // quotes this query always threw, which `hasSchemaMismatch` swallowed as
    // "no mismatch", silently disabling the self-heal and flooding the log
    // once per heartbeat.
    const result = await this.exec(
      `SELECT "column" FROM table_columns('${validateIdentifier(table)}') WHERE designated = true`,
    );
    return result.dataset.length > 0 ? (result.dataset[0][0] as string) : null;
  }

  // True when `table` exists but its designated timestamp is not our expected
  // `ts` (i.e. ILP auto-created it). A missing table is NOT a mismatch —
  // ensureTables creates it correctly.
  async hasSchemaMismatch(table: string): Promise<boolean> {
    // Validate up front so an invalid identifier rejects loudly instead of
    // being swallowed below as "no mismatch".
    validateIdentifier(table);
    try {
      const ts = await this.designatedTimestamp(table);
      return ts !== null && ts !== "ts";
    } catch {
      // table doesn't exist / introspection unavailable — not a mismatch.
      return false;
    }
  }

  // Self-heal a table ILP auto-created with the wrong schema: drop it and let
  // ensureTables recreate it correctly. Returns true if a rebuild happened.
  // The rows ILP wrote into the wrong-schema table are lost, but they are
  // unreadable anyway (history API and status query filter on `ts`, which the
  // auto-created table lacks).
  async healSchema(table: string): Promise<boolean> {
    if (!(await this.hasSchemaMismatch(table))) return false;
    await this.exec(`DROP TABLE IF EXISTS ${validateIdentifier(table)}`);
    await this.ensureTables();
    return true;
  }

  toObjects(result: QuestDBResult): Record<string, unknown>[] {
    // Defend against a result without column metadata (e.g. a QuestDB response
    // that omits `columns`): without names there is nothing to key by, so
    // degrade to an empty list rather than throwing inside a caller's catch.
    const columns = result.columns;
    if (!columns) return [];
    return result.dataset.map((row) => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < columns.length; i++) {
        obj[columns[i].name] = row[i];
      }
      return obj;
    });
  }
}
