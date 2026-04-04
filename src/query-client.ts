const SAFE_IDENTIFIER = /^[a-zA-Z0-9._:-]+$/;
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

  async exec(sql: string): Promise<QuestDBResult> {
    const url = new URL("/exec", this.baseUrl);
    url.searchParams.set("query", sql);
    url.searchParams.set("nm", "true");

    const res = await fetch(url.toString(), {
      signal: AbortSignal.timeout(30000),
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

  async execCsv(sql: string): Promise<string> {
    const url = new URL("/exp", this.baseUrl);
    url.searchParams.set("query", sql);

    const res = await fetch(url.toString(), {
      signal: AbortSignal.timeout(60000),
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
        value     DOUBLE
      ) TIMESTAMP(ts)
        PARTITION BY DAY
        WAL
        DEDUP UPSERT KEYS(ts, path, context)
    `);

    await this.exec(`
      CREATE TABLE IF NOT EXISTS signalk_str (
        ts        TIMESTAMP,
        path      SYMBOL CAPACITY 256 CACHE,
        context   SYMBOL CAPACITY 128 CACHE,
        value_str VARCHAR
      ) TIMESTAMP(ts)
        PARTITION BY DAY
        WAL
        DEDUP UPSERT KEYS(ts, path, context)
    `);

    await this.exec(`
      CREATE TABLE IF NOT EXISTS signalk_position (
        ts        TIMESTAMP,
        context   SYMBOL CAPACITY 128 CACHE,
        lat       DOUBLE,
        lon       DOUBLE
      ) TIMESTAMP(ts)
        PARTITION BY DAY
        WAL
        DEDUP UPSERT KEYS(ts, context)
    `);
  }

  toObjects(result: QuestDBResult): Record<string, unknown>[] {
    return result.dataset.map((row) => {
      const obj: Record<string, unknown> = {};
      for (let i = 0; i < result.columns.length; i++) {
        obj[result.columns[i].name] = row[i];
      }
      return obj;
    });
  }
}
