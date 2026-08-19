import {
  QueryClient,
  isSafeIdentifier,
  validateIdentifier,
  validateTimestamp,
} from "./query-client.js";
import type { QuestDBResult } from "./query-client.js";
import { resolveTimeRange, ResolvedRange } from "./time-range.js";
import {
  isMissingKindColumn,
  isMissingSourceColumn,
  isMissingTable,
} from "./schema-errors.js";

interface PathSpec {
  path: string;
  aggregate: string;
  parameter: string[];
  // Delivered by the server for `paths=<path>|<sourceRef>` requests
  // (signalk-server #2737): restrict the series to rows recorded from that
  // source. Absent = all sources mixed, the pre-source behaviour.
  sourceRef?: string;
  // Set only by sourcePolicy=all expansion, for the column that carries rows
  // whose `source` is NULL (recorded before the column existed, or from a
  // delta with no sourceRef).
  //
  // This is NOT the same as "no filter". Leaving such a column unfiltered
  // returns EVERY source's rows, so the unattributed column silently
  // duplicated all the others under a "no source" label — verified against a
  // live QuestDB, where it returned 60 rows instead of its own 20.
  unattributed?: boolean;
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
  // signalk-server #2817. Only "all" is defined; the server rejects anything
  // else before the request reaches a provider, so an unknown value here is
  // treated the same as absent rather than guessed at.
  sourcePolicy?: string;
  pathSpecs: PathSpec[];
}

/**
 * One column of the response.
 *
 * The metadata key is `$source`, not `sourceRef` — signalk-server #2817
 * renamed it on the RESPONSE side while `PathSpec.sourceRef` (the request
 * side) kept its name. The two are deliberately different words for the same
 * thing, and nothing validates the response, so emitting the old key simply
 * left every consumer unable to tell the columns apart.
 */
interface ValueColumn {
  path: string;
  method: string;
  $source?: string;
}

interface ValuesResponse {
  context: string;
  range: { from: string; to: string };
  values: ValueColumn[];
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

// A source filter deliberately errors loudly on a database whose tables
// predate the `source` column (external QuestDB the migration could not
// touch): returning all sources when one was asked for would silently
// reintroduce exactly the mixed-source ambiguity the filter exists to remove.
function buildSourceWhere(spec: {
  sourceRef?: string;
  unattributed?: boolean;
}): string {
  if (spec.sourceRef) {
    return ` AND source = '${validateIdentifier(spec.sourceRef)}'`;
  }
  // `source IS NULL` selects ONLY the unattributed rows. Returning "" here
  // would select every source instead — see PathSpec.unattributed.
  if (spec.unattributed) return " AND source IS NULL";
  return "";
}

/**
 * Map a Signal K context value to the storage form used by signalk-questdb.
 *
 * Per the v2 History API spec, callers may send the context as `vessels.self`
 * or as a fully-qualified context like `vessels.urn:mrn:imo:mmsi:123456789`.
 * We store the own vessel as the literal string "self" for compactness, so
 * any incoming context that refers to the own vessel is normalized to "self".
 */
function normalizeContext(context: string, selfContext: string): string {
  if (
    context === "self" ||
    context === "vessels.self" ||
    context === selfContext
  ) {
    return "self";
  }
  return context;
}

// Upper bound on SAMPLE BY buckets a single request may generate. The
// SAMPLE BY queries use FILL(NULL), which fabricates a row for EVERY bucket
// in the range whether data exists or not — a caller asking for weeks at 1s
// resolution would stream millions of rows through QuestDB, Node's JSON
// parser, and the HTTP response, wedging Pi-class servers for minutes. The
// well-behaved clients budget ~2000 points per request; a million covers any
// sane chart while keeping the worst case bounded.
const MAX_SAMPLE_BUCKETS = 1_000_000;

// Upper bound on columns ONE path may expand into under sourcePolicy=all.
// The bucket cap above only applies to requests that named a resolution, so
// these are the only things bounding an unresolved expansion.
const MAX_EXPANDED_SOURCES = 16;

// Upper bound on the columns a whole REQUEST may expand into. The per-path
// cap alone is not enough: twenty paths at sixteen sources each is 320
// columns and 320 queries from one HTTP request. Exceeding this fails the
// request rather than truncating it — silently returning some of the asked-for
// series would be worse than saying the request is too broad.
const MAX_EXPANDED_COLUMNS = 64;

// Effective SAMPLE BY period: QuestDB rejects `SAMPLE BY 0s`, so fractional
// resolutions (e.g. 0.5) must clamp up to 1s instead of flooring to zero.
function effectiveResolution(resolution: number): number {
  return Math.max(1, Math.floor(resolution));
}

export function createHistoryProviderV2(
  queryClient: QueryClient,
  selfContext: string,
  // Opt-in, default off. Expansion multiplies one requested path into one
  // query and one column PER recording source, so a request that was cheap
  // becomes N times the work — on a Pi-class host that is the difference
  // between a responsive chart and a stalled one. Callers that name a source
  // explicitly are unaffected either way.
  sourcePolicyAllEnabled = false,
  // Degrading must be visible. Defaults to a no-op so existing callers and
  // tests need no change.
  debug: (msg: string) => void = () => {},
) {
  /**
   * Distinct sources that recorded `path` inside the range.
   *
   * All three tables are consulted because a path lives in whichever one
   * matches its value type — numeric in `signalk`, text/boolean in
   * `signalk_str`, and navigation.position in its own table — and the caller
   * has no way to know which before querying. Asking only the numeric table
   * would expand a boolean channel into nothing and silently collapse it back
   * to one merged column.
   *
   * Returns `null` for rows whose `source` is unset (recorded before the
   * column existed, or from a delta carrying no sourceRef): that is a real
   * series, distinct from any named source, and gets its own column.
   *
   * A database whose tables predate the `source` column has no such column to
   * select, so the query errors; that is reported as "no sources", which
   * leaves the path as a single unexpanded column rather than failing the
   * whole request.
   */
  async function distinctSources(
    path: string,
    range: ResolvedRange,
    safeContext: string,
  ): Promise<(string | null)[]> {
    const safePath = validateIdentifier(path);
    const rangeWhere = buildRangeWhere(range, safeContext);
    const tables =
      path === "navigation.position"
        ? [`SELECT DISTINCT source FROM signalk_position WHERE ${rangeWhere}`]
        : [
            `SELECT DISTINCT source FROM signalk WHERE ${rangeWhere} AND path = '${safePath}'`,
            `SELECT DISTINCT source FROM signalk_str WHERE ${rangeWhere} AND path = '${safePath}'`,
          ];

    const found = new Set<string | null>();
    for (const sql of tables) {
      try {
        const result = await queryClient.exec(sql);
        for (const row of result.dataset) {
          const value = row[0];
          found.add(typeof value === "string" ? value : null);
        }
      } catch (err) {
        const table = /FROM (\w+)/.exec(sql)?.[1] ?? "unknown";
        if (isMissingSourceColumn(err)) {
          // A legacy table predating the `source` migration. Real for
          // databases the plugin did not create, so the request still
          // succeeds with one unexpanded column — but SAYING SO matters:
          // silently returning one column makes sourcePolicy=all look like
          // it did nothing, which is indistinguishable from a path that
          // genuinely has one source.
          debug(
            `sourcePolicy=all: ${table} has no 'source' column, so ` +
              `${path} cannot be split by source — returning one merged ` +
              `column. Re-create or migrate the table to enable expansion.`,
          );
          continue;
        }
        if (isMissingTable(err)) {
          // Nothing recorded of this value type yet. Not a fault, and the
          // other table may still answer.
          continue;
        }
        // A timeout, a 5xx, a dropped connection: NOT "no sources". Reporting
        // those as an empty result would hand the caller a plausible-looking
        // single column built on a failure nobody saw.
        throw err;
      }
    }

    // Named sources first and sorted, so column order is stable across
    // requests — a caller charting several receivers should not see the
    // series swap places between refreshes. The unattributed column goes
    // last, where it reads as the leftover it is.
    const named = [...found].filter((s): s is string => s !== null).sort();
    const all: (string | null)[] = found.has(null) ? [...named, null] : named;

    // Hard ceiling on fan-out. The sample-bucket cap only bites when the
    // caller asked for a resolution; an unresolved request runs one raw query
    // per source with no cap at all, so a path that accumulated dozens of
    // sourceRefs (a bus with many transmitters, or churn in generated refs)
    // would schedule dozens of queries from a single HTTP request.
    if (all.length > MAX_EXPANDED_SOURCES) {
      debug(
        `sourcePolicy=all: ${path} has ${all.length} sources in range; ` +
          `expanding the first ${MAX_EXPANDED_SOURCES} (sorted) only.`,
      );
      return all.slice(0, MAX_EXPANDED_SOURCES);
    }
    return all;
  }

  /**
   * Read a path's rows from `signalk_str`.
   *
   * Numeric aggregates do not apply to text, so a downsampled request takes
   * one representative value per bucket (`last` = the state in force at the
   * bucket's end, which is what a state channel means) instead of averaging.
   * Values are returned verbatim: booleans were stored as "true"/"false", so
   * a consumer can tell them apart from real numbers, and Grafana value
   * mappings work directly.
   *
   * Returns the rows plus whether the source predicate had to be dropped.
   * The two value tables migrate independently, so `signalk` can have a
   * `source` column while `signalk_str` does not — and under sourcePolicy=all
   * the expansion is driven by whichever table answered, then applied to
   * both. Without this the fallback query carried `AND source = '...'` into a
   * table with no such column and failed the WHOLE request with
   * "Invalid column: source" (verified against a live QuestDB). The caller
   * uses `sourceDropped` to withdraw the column's `$source` claim, because
   * rows that were never filtered by source must not be labelled as one
   * source's.
   */
  async function readStringRows(
    where: string,
    resolution?: number,
  ): Promise<{ rows: [string, unknown][]; sourceDropped: boolean }> {
    // `value_kind` marks rows that were recorded as booleans, so v2 replays
    // them as real booleans exactly like v1 — otherwise the same path would
    // read `true` through one API and `"true"` through the other. Untagged
    // rows (plain text, and everything written before the column existed)
    // stay strings; the text is never guessed at.
    const sql = (withKind: boolean, withSource: boolean) => {
      const kind = withKind ? "value_kind" : "NULL";
      // Strip the source predicate for the no-source retry. It is always the
      // trailing ` AND source = '...'` / ` AND source IS NULL` that
      // buildSourceWhere appended, so removing it leaves the range, context
      // and path filters intact.
      const clause = withSource
        ? where
        : where.replace(/ AND source (?:= '[^']*'|IS NULL)/g, "");
      return resolution && resolution > 0
        ? `SELECT ts, last(value_str) as value_str, last(${kind}) as value_kind FROM signalk_str WHERE ${clause} SAMPLE BY ${effectiveResolution(resolution)}s FILL(NULL) ORDER BY ts`
        : `SELECT ts, value_str, ${kind} as value_kind FROM signalk_str WHERE ${clause} ORDER BY ts LIMIT 10000`;
    };
    // A read racing ensureTables()'s migration — or an external QuestDB the
    // plugin does not own — must degrade, not fail the request. Two columns
    // can be missing independently: `value_kind` (degrade to text) and
    // `source` (degrade to unfiltered, and say so).
    let sourceDropped = false;
    const run = async (
      withKind: boolean,
      withSource: boolean,
    ): Promise<QuestDBResult> => {
      try {
        return await queryClient.exec(sql(withKind, withSource));
      } catch (err) {
        if (withKind && isMissingKindColumn(err)) return run(false, withSource);
        if (withSource && isMissingSourceColumn(err)) {
          sourceDropped = true;
          debug(
            `signalk_str has no 'source' column, so this path cannot be ` +
              `filtered by source there — returning its rows unfiltered and ` +
              `dropping the source attribution for that column.`,
          );
          return run(withKind, false);
        }
        throw err;
      }
    };
    const result = await run(true, true);
    return {
      rows: result.dataset.map((row: unknown[]) => [
        row[0] as string,
        row[2] === "boolean" ? row[1] === "true" : row[1],
      ]),
      sourceDropped,
    };
  }

  async function getValues(query: ValuesRequest): Promise<ValuesResponse> {
    const range = resolveTimeRange(query as any);

    // Two gates, both required. The caller asks with sourcePolicy=all; the
    // operator has to have allowed it. An unknown policy string is treated as
    // absent — the server validates the value before a provider ever sees it,
    // so guessing at anything else here would only invent behaviour.
    const expandBySource =
      sourcePolicyAllEnabled && query.sourcePolicy === "all";

    const requestedContext = query.context ?? "vessels.self";
    const storedContext = normalizeContext(requestedContext, selfContext);
    const safeContext = validateIdentifier(storedContext);

    // Expand each spec into the COLUMNS it will produce. Under
    // sourcePolicy=all a path with no explicit sourceRef becomes one column
    // per distinct source that actually recorded it in range; everything else
    // stays a single column. Doing this before the query loop is what lets
    // the loop, the bucket budget and the row assembly all agree on how many
    // columns exist — the pre-#2817 code could assume one column per spec.
    const columns: PathSpec[] = [];
    for (const spec of query.pathSpecs) {
      if (!expandBySource || spec.sourceRef) {
        // An explicit sourceRef stays a FILTER and takes precedence over the
        // policy, per the upstream contract.
        columns.push(spec);
        continue;
      }
      // Checked BEFORE the probe, not only after the loop: discovery is a
      // query per path per table, so a request far past the ceiling would
      // otherwise issue hundreds of DISTINCT probes and only then be told it
      // was too broad. Stopping here makes the reported count a lower bound,
      // which the message says.
      if (columns.length > MAX_EXPANDED_COLUMNS) {
        throw new Error(
          `sourcePolicy=all expands these paths into more than ` +
            `${MAX_EXPANDED_COLUMNS} columns — request fewer paths, or name ` +
            `the sources explicitly with paths=<path>|<sourceRef>`,
        );
      }
      const sources = await distinctSources(spec.path, range, safeContext);
      if (sources.length === 0) {
        // Nothing recorded in range, or a database with no `source` column:
        // keep the unexpanded column so the caller still gets the series
        // (empty, or mixed-source on a pre-source database) instead of the
        // path silently vanishing from the response.
        columns.push(spec);
        continue;
      }
      const before = columns.length;
      for (const source of sources) {
        // `null` source = rows recorded before the column existed, or by a
        // delta that carried no sourceRef. They are a real series and get
        // their own unattributed column rather than being dropped.
        if (source === null) {
          columns.push({ ...spec, unattributed: true });
          continue;
        }
        // A STORED sourceRef is not guaranteed to satisfy the identifier
        // guard — a delta can carry something like "tcp://gw:2000", which is
        // a perfectly ordinary Signal K source but contains characters the
        // guard rejects. Letting it reach buildSourceWhere throws and takes
        // the WHOLE request down, so expansion would turn a query that works
        // today into a hard failure. Skip the column and say so instead.
        if (!isSafeIdentifier(source)) {
          debug(
            `sourcePolicy=all: skipping source '${source}' for ${spec.path} — ` +
              `it contains characters that cannot be used in a query filter.`,
          );
          continue;
        }
        columns.push({ ...spec, sourceRef: source });
      }
      // Every source was unusable: keep the path as one merged column rather
      // than dropping it from the response entirely.
      if (columns.length === before) columns.push(spec);
    }

    if (columns.length > MAX_EXPANDED_COLUMNS) {
      throw new Error(
        `sourcePolicy=all expands these paths into ${columns.length} columns ` +
          `(max ${MAX_EXPANDED_COLUMNS}) — request fewer paths, or name the ` +
          `sources explicitly with paths=<path>|<sourceRef>`,
      );
    }

    // The bucket budget is checked HERE, after expansion, not before it.
    // Under sourcePolicy=all one requested path becomes one SAMPLE BY query
    // per source, so counting requested paths would let a four-receiver path
    // run at four times the ceiling this cap exists to enforce — precisely
    // the case the cap is for.
    //
    // Only columns that actually run a SAMPLE BY query fabricate buckets:
    // client-side aggregates (sma/ema/middle_index) read raw rows under a
    // LIMIT and must not trip the cap.
    //
    // A non-numeric path costs TWO such queries: the numeric one comes back
    // empty and the string-table fallback repeats it against signalk_str.
    // Which paths those are is only known after querying, so the budget
    // assumes the worst case — every sampled column falling back — rather
    // than letting a request built entirely of boolean/string paths quietly
    // run at twice the ceiling.
    const sampledSpecs = columns.filter(
      (spec) =>
        spec.path === "navigation.position" ||
        !needsClientSideAggregation(spec.aggregate),
    ).length;
    // navigation.position is served by its own table and never falls back.
    const fallbackCapableSpecs = columns.filter(
      (spec) =>
        spec.path !== "navigation.position" &&
        !needsClientSideAggregation(spec.aggregate),
    ).length;

    if (sampledSpecs > 0 && query.resolution && query.resolution > 0) {
      const rangeSec = (Date.parse(range.to) - Date.parse(range.from)) / 1000;
      const bucketsPerSeries = Math.ceil(
        rangeSec / effectiveResolution(query.resolution),
      );
      const worstCaseQueries = sampledSpecs + fallbackCapableSpecs;
      const buckets = bucketsPerSeries * worstCaseQueries;
      if (buckets > MAX_SAMPLE_BUCKETS) {
        throw new Error(
          `resolution ${query.resolution}s over this range produces up to ` +
            `${buckets} sample buckets across ${sampledSpecs} paths ` +
            `(max ${MAX_SAMPLE_BUCKETS}) — use a coarser resolution or ` +
            `a shorter range`,
        );
      }
    }

    const valuesList: ValueColumn[] = [];
    // Keyed by COLUMN INDEX, not path: source filtering and source expansion
    // both make the same path appear more than once in one request (one
    // column per receiver), and a path-keyed map would let the second
    // column's rows overwrite the first's.
    const columnData: Map<number, [string, unknown][]> = new Map();

    for (const [specIndex, spec] of columns.entries()) {
      const safePath = validateIdentifier(spec.path);
      const entry: ValueColumn = {
        path: spec.path,
        method: spec.aggregate,
      };
      // Only source-specific columns carry `$source`. An unexpanded column is
      // "all sources merged", which is not the same claim as "this source".
      if (spec.sourceRef) entry.$source = spec.sourceRef;
      valuesList.push(entry);

      const sourceWhere = buildSourceWhere(spec);
      const isPosition = spec.path === "navigation.position";
      const table = isPosition ? "signalk_position" : "signalk";

      if (isPosition) {
        const where = buildRangeWhere(range, safeContext) + sourceWhere;
        // Position is an object-valued lat/lon pair. Only first/last keep a
        // real, co-recorded coordinate; per-axis avg/min/max/mid would
        // fabricate a point the vessel never occupied, so they fall back to
        // first. A silent caller also gets first (the server default).
        const posAgg = spec.aggregate === "last" ? "last" : "first";
        let sql: string;
        if (query.resolution && query.resolution > 0) {
          sql = `SELECT ts, ${posAgg}(lat) as lat, ${posAgg}(lon) as lon FROM ${table} WHERE ${where} SAMPLE BY ${effectiveResolution(query.resolution)}s FILL(NULL) ORDER BY ts`;
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
        columnData.set(specIndex, rows);
        continue;
      }

      const where = `${buildRangeWhere(range, safeContext)} AND path = '${safePath}'${sourceWhere}`;

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
        columnData.set(specIndex, rows);
        continue;
      }

      const aggExpr = aggregateToSql(spec.aggregate);
      let sql: string;
      if (query.resolution && query.resolution > 0) {
        sql = `SELECT ts, ${aggExpr} as agg_value FROM ${table} WHERE ${where} SAMPLE BY ${effectiveResolution(query.resolution)}s FILL(NULL) ORDER BY ts`;
      } else {
        sql = `SELECT ts, value FROM ${table} WHERE ${where} ORDER BY ts LIMIT 10000`;
      }

      const result = await queryClient.exec(sql);
      const rows: [string, unknown][] = result.dataset.map((row) => [
        row[0] as string,
        row[1],
      ]);

      // Non-numeric paths (strings, and booleans stored as "true"/"false")
      // live in signalk_str, which this query never touches — they used to
      // come back empty even though getPaths lists them. Fall back to the
      // string table when the numeric one held nothing for this path.
      // Emptiness is judged on VALUES, not row count: a SAMPLE BY with
      // FILL(NULL) fabricates a row per bucket, so an all-null result is
      // still "no numeric data here".
      if (!rows.some(([, value]) => value !== null)) {
        // Report the aggregate that was actually applied. Downsampled string
        // rows always use last() — averaging text is meaningless — so leaving
        // the caller's requested method in the response would label the
        // series with an aggregation that never ran.
        if (query.resolution && query.resolution > 0) {
          valuesList[specIndex].method = "last";
        }
        const strResult = await readStringRows(where, query.resolution);
        if (strResult.sourceDropped) {
          // The rows came back unfiltered because signalk_str has no `source`
          // column. They are every source's rows, so the column must stop
          // claiming to be one source's — labelling unfiltered data with a
          // $source would be a lie the caller cannot detect.
          delete valuesList[specIndex].$source;
        }
        columnData.set(specIndex, strResult.rows);
        continue;
      }

      columnData.set(specIndex, rows);
    }

    const allTimestamps = new Set<string>();
    for (const rows of columnData.values()) {
      for (const [ts] of rows) {
        allTimestamps.add(ts);
      }
    }
    const sortedTimestamps = Array.from(allTimestamps).sort();

    const indexMaps = new Map<number, Map<string, unknown>>();
    for (const [specIndex, rows] of columnData) {
      const m = new Map<string, unknown>();
      for (const [ts, val] of rows) {
        m.set(ts, val);
      }
      indexMaps.set(specIndex, m);
    }

    const data: [string, ...unknown[]][] = sortedTimestamps.map((ts) => {
      const row: [string, ...unknown[]] = [ts];
      for (let i = 0; i < columns.length; i++) {
        const m = indexMaps.get(i);
        row.push(m?.get(ts) ?? null);
      }
      return row;
    });

    return {
      context: requestedContext,
      range: { from: range.from, to: range.to },
      values: valuesList,
      data,
    };
  }

  async function getPaths(query: PathsRequest): Promise<string[]> {
    const range = resolveTimeRange(query as any);
    const where = buildRangeWhere(range);

    // signalk_position has no `path` column — the whole table IS
    // navigation.position — so it contributes that name as a literal. Without
    // this branch getValues happily serves the track while getPaths never
    // advertised it, so a client enumerating paths could not discover the one
    // series it most likely wants.
    const result = await queryClient.exec(
      `SELECT DISTINCT path FROM signalk WHERE ${where}
       UNION
       SELECT DISTINCT path FROM signalk_str WHERE ${where}
       UNION
       SELECT DISTINCT 'navigation.position' path FROM signalk_position WHERE ${where}
       ORDER BY path`,
    );

    return result.dataset.map((row) => row[0] as string);
  }

  async function getContexts(query: ContextsRequest): Promise<string[]> {
    const range = resolveTimeRange(query as any);
    const where = buildRangeWhere(range);

    // Include the track table: a vessel can be position-only (an AIS target
    // whose other paths are filtered out, or a receiver sending nothing but
    // fixes), and omitting it hides that context entirely.
    const result = await queryClient.exec(
      `SELECT DISTINCT context FROM signalk WHERE ${where}
       UNION
       SELECT DISTINCT context FROM signalk_str WHERE ${where}
       UNION
       SELECT DISTINCT context FROM signalk_position WHERE ${where}
       ORDER BY context`,
    );

    // Translate stored "self" back to the spec-canonical "vessels.self"
    return result.dataset.map((row) => {
      const ctx = row[0] as string;
      return ctx === "self" ? "vessels.self" : ctx;
    });
  }

  return { getValues, getPaths, getContexts };
}
