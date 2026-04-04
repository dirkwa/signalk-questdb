import {
  QueryClient,
  validateIdentifier,
  validateTimestamp,
} from "./query-client";
import { resolveTimeRange, ResolvedRange } from "./time-range";

interface PathSpec {
  path: string;
  aggregate: string;
  parameter: string[];
}

interface ValuesRequest {
  from?: {
    toString(): string;
    add(d: unknown): unknown;
    subtract?(d: unknown): unknown;
  };
  to?: { toString(): string; subtract?(d: unknown): unknown };
  duration?: unknown;
  context?: string;
  resolution?: number;
  pathSpecs: PathSpec[];
}

interface ValuesResponse {
  context: string;
  range: { from: string; to: string };
  values: { path: string; method: string }[];
  data: [string, ...unknown[]][];
}

type PathsRequest = {
  from?: { toString(): string; add(d: unknown): unknown };
  to?: { toString(): string };
  duration?: unknown;
};

type ContextsRequest = PathsRequest;

function aggregateToSql(method: string): string {
  switch (method) {
    case "average":
      return "avg(value)";
    case "min":
      return "min(value)";
    case "max":
      return "max(value)";
    case "first":
      return "first(value)";
    case "last":
      return "last(value)";
    case "mid":
      return "(min(value) + max(value)) / 2";
    default:
      return "avg(value)";
  }
}

function needsClientSideAggregation(method: string): boolean {
  return method === "middle_index" || method === "sma" || method === "ema";
}

function computeSMA(values: (number | null)[], n: number): (number | null)[] {
  const result: (number | null)[] = [];
  const window: number[] = [];
  for (const v of values) {
    if (v === null) {
      result.push(null);
      continue;
    }
    window.push(v);
    if (window.length > n) window.shift();
    result.push(window.reduce((a, b) => a + b, 0) / window.length);
  }
  return result;
}

function computeEMA(
  values: (number | null)[],
  alpha: number,
): (number | null)[] {
  const result: (number | null)[] = [];
  let prev: number | null = null;
  for (const v of values) {
    if (v === null) {
      result.push(prev);
      continue;
    }
    if (prev === null) {
      prev = v;
    } else {
      prev = alpha * v + (1 - alpha) * prev;
    }
    result.push(prev);
  }
  return result;
}

function buildRangeWhere(range: ResolvedRange, context?: string): string {
  const from = validateTimestamp(range.from);
  const to = validateTimestamp(range.to);
  let where = `ts >= '${from}' AND ts <= '${to}'`;
  if (context) {
    where += ` AND context = '${validateIdentifier(context)}'`;
  }
  return where;
}

export function createHistoryProviderV2(queryClient: QueryClient) {
  async function getValues(query: ValuesRequest): Promise<ValuesResponse> {
    const range = resolveTimeRange(query as any);
    const context = query.context ?? "self";
    const safeContext = validateIdentifier(context);

    const valuesList: { path: string; method: string }[] = [];
    const columnData: Map<string, [string, unknown][]> = new Map();

    for (const spec of query.pathSpecs) {
      const safePath = validateIdentifier(spec.path);
      valuesList.push({ path: spec.path, method: spec.aggregate });

      const isPosition = spec.path === "navigation.position";
      const table = isPosition ? "signalk_position" : "signalk";

      if (isPosition) {
        const where = buildRangeWhere(range, safeContext);
        let sql: string;
        if (query.resolution && query.resolution > 0) {
          sql = `SELECT ts, first(lat) as lat, first(lon) as lon FROM ${table} WHERE ${where} SAMPLE BY ${Math.floor(query.resolution)}s FILL(NULL) ORDER BY ts`;
        } else {
          sql = `SELECT ts, lat, lon FROM ${table} WHERE ${where} ORDER BY ts LIMIT 10000`;
        }
        const result = await queryClient.exec(sql);
        const rows: [string, unknown][] = result.dataset.map((row) => [
          row[0] as string,
          row[1] !== null && row[2] !== null
            ? { latitude: row[1], longitude: row[2] }
            : null,
        ]);
        columnData.set(spec.path, rows);
        continue;
      }

      const where = `${buildRangeWhere(range, safeContext)} AND path = '${safePath}'`;

      if (needsClientSideAggregation(spec.aggregate)) {
        const sql = `SELECT ts, value FROM ${table} WHERE ${where} ORDER BY ts LIMIT 50000`;
        const result = await queryClient.exec(sql);
        const timestamps = result.dataset.map((r) => r[0] as string);
        const rawValues = result.dataset.map((r) => r[1] as number | null);

        let computed: (number | null)[];
        if (spec.aggregate === "sma") {
          const n = parseInt(spec.parameter[0] ?? "5", 10);
          computed = computeSMA(rawValues, n);
        } else if (spec.aggregate === "ema") {
          const alpha = parseFloat(spec.parameter[0] ?? "0.2");
          computed = computeEMA(rawValues, alpha);
        } else {
          const mid = Math.floor(rawValues.length / 2);
          computed = rawValues.map((_, i) => (i === mid ? rawValues[i] : null));
        }

        const rows: [string, unknown][] = timestamps.map((ts, i) => [
          ts,
          computed[i],
        ]);
        columnData.set(spec.path, rows);
        continue;
      }

      const aggExpr = aggregateToSql(spec.aggregate);
      let sql: string;
      if (query.resolution && query.resolution > 0) {
        sql = `SELECT ts, ${aggExpr} as agg_value FROM ${table} WHERE ${where} SAMPLE BY ${Math.floor(query.resolution)}s FILL(NULL) ORDER BY ts`;
      } else {
        sql = `SELECT ts, value FROM ${table} WHERE ${where} ORDER BY ts LIMIT 10000`;
      }

      const result = await queryClient.exec(sql);
      const rows: [string, unknown][] = result.dataset.map((row) => [
        row[0] as string,
        row[1],
      ]);
      columnData.set(spec.path, rows);
    }

    const allTimestamps = new Set<string>();
    for (const rows of columnData.values()) {
      for (const [ts] of rows) {
        allTimestamps.add(ts);
      }
    }
    const sortedTimestamps = Array.from(allTimestamps).sort();

    const pathOrder = query.pathSpecs.map((s) => s.path);
    const indexMaps = new Map<string, Map<string, unknown>>();
    for (const [path, rows] of columnData) {
      const m = new Map<string, unknown>();
      for (const [ts, val] of rows) {
        m.set(ts, val);
      }
      indexMaps.set(path, m);
    }

    const data: [string, ...unknown[]][] = sortedTimestamps.map((ts) => {
      const row: [string, ...unknown[]] = [ts];
      for (const path of pathOrder) {
        const m = indexMaps.get(path);
        row.push(m?.get(ts) ?? null);
      }
      return row;
    });

    return {
      context,
      range: { from: range.from, to: range.to },
      values: valuesList,
      data,
    };
  }

  async function getPaths(query: PathsRequest): Promise<string[]> {
    const range = resolveTimeRange(query as any);
    const where = buildRangeWhere(range);

    const result = await queryClient.exec(
      `SELECT DISTINCT path FROM signalk WHERE ${where}
       UNION
       SELECT DISTINCT path FROM signalk_str WHERE ${where}
       ORDER BY path`,
    );

    return result.dataset.map((row) => row[0] as string);
  }

  async function getContexts(query: ContextsRequest): Promise<string[]> {
    const range = resolveTimeRange(query as any);
    const where = buildRangeWhere(range);

    const result = await queryClient.exec(
      `SELECT DISTINCT context FROM signalk WHERE ${where}
       UNION
       SELECT DISTINCT context FROM signalk_str WHERE ${where}
       ORDER BY context`,
    );

    return result.dataset.map((row) => row[0] as string);
  }

  return { getValues, getPaths, getContexts };
}
