import {
  QueryClient,
  isSafeIdentifier,
  validateIdentifier,
  validateTimestamp,
} from "./query-client.js";
import type { QuestDBResult } from "./query-client.js";
import { resolveTimeRange, ResolvedRange } from "./time-range.js";
import type {
  Context,
  Path,
  SourceRef,
  Timestamp,
  history as HistoryApi,
} from "@signalk/server-api";
import {
  isMissingKindColumn,
  isMissingSourceColumn,
  isMissingTable,
} from "./schema-errors.js";

/**
 * The server's PathSpec — `sourceRef` (signalk-server #2737) restricts the
 * series to rows recorded from that source, absent = all sources mixed —
 * plus one flag of this provider's own.
 */
type PathSpec = HistoryApi.PathSpec & {
  // Set only by sourcePolicy=all expansion, for the column that carries rows
  // whose `source` is NULL (recorded before the column existed, or from a
  // delta with no sourceRef).
  //
  // This is NOT the same as "no filter". Leaving such a column unfiltered
  // returns EVERY source's rows, so the unattributed column silently
  // duplicated all the others under a "no source" label — verified against a
  // live QuestDB, where it returned 60 rows instead of its own 20.
  unattributed?: boolean;
};

type ValuesRequest = HistoryApi.ValuesRequest;
type ValuesResponse = HistoryApi.ValuesResponse;
type PathsRequest = HistoryApi.PathsRequest;
type ContextsRequest = HistoryApi.ContextsRequest;
type AggregateMethod = HistoryApi.AggregateMethod;

/**
 * One column of the response. Its metadata key is `$source`, not
 * `sourceRef` — signalk-server #2817 renamed it on the RESPONSE side while
 * `PathSpec.sourceRef` (the request side) kept its name; the two are
 * deliberately different words for the same thing. Nothing validates the
 * response at runtime, so the published type is what holds the key in place.
 */
type ValueColumn = HistoryApi.ValueList[number];

function aggregateToSql(method: AggregateMethod): string {
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

/**
 * The middle row of each resolution bucket, by time — of the whole range
 * when the request names no resolution. A recorded row, not a computed
 * value, which is what makes it meaningful for navigation.position too.
 * There is no aggregate for it, so row_number and count over the bucket
 * pick it in one pass; an empty bucket yields no row. Without a resolution
 * the range is read under the LIMIT every raw read here has, so a long
 * range is not staged whole for a single row.
 */
function middleRowSql(
  table: string,
  columns: string,
  where: string,
  range: ResolvedRange,
  resolution?: number,
): string {
  if (resolution && resolution > 0) {
    // Floored from the range start, on the grid sampleBy() puts every other
    // column on.
    const bucket = `timestamp_floor('${effectiveResolution(resolution)}s', ts, '${validateTimestamp(range.from)}')`;
    return `SELECT b AS ts, ${columns} FROM (SELECT b, ts, ${columns}, row_number() OVER (PARTITION BY b ORDER BY ts) AS rn, count(*) OVER (PARTITION BY b) AS n FROM (SELECT ${bucket} AS b, ts, ${columns} FROM ${table} WHERE ${where})) WHERE rn = n / 2 + 1 ORDER BY ts`;
  }
  return `SELECT ts, ${columns} FROM (SELECT ts, ${columns}, row_number() OVER (ORDER BY ts) AS rn, count(*) OVER () AS n FROM (SELECT ts, ${columns} FROM ${table} WHERE ${where} ORDER BY ts LIMIT 50000)) WHERE rn = n / 2 + 1`;
}

function isMovingAverage(method: AggregateMethod): boolean {
  return method === "sma" || method === "ema";
}

/**
 * The window of a moving average, from `path:method:parameter`. A sample is
 * one row of the series the window runs over: a resolution bucket when the
 * request names a resolution, a raw row otherwise. Defaults are the ones the
 * server's API docs suggest.
 */
function movingAverageWindow(
  spec: PathSpec,
): { samples: number } | { alpha: number } {
  const raw = spec.parameter[0];
  if (spec.aggregate === "sma") {
    if (raw === undefined) return { samples: 5 };
    const samples = Number(raw);
    if (!Number.isInteger(samples) || samples < 1) {
      throw new Error(
        `sma:${raw} — the window is a whole number of samples, at least 1; ` +
          `without a parameter it is 5`,
      );
    }
    return { samples };
  }
  if (raw === undefined) return { alpha: 0.2 };
  const alpha = Number(raw);
  if (!(alpha > 0 && alpha <= 1)) {
    throw new Error(
      `ema:${raw} — alpha is a number above 0 and up to 1; ` +
        `without a parameter it is 0.2`,
    );
  }
  return { alpha };
}

/**
 * Moving average over the last `n` samples of a series. An empty sample
 * (null) still takes its place in the window — on a resolution grid it is a
 * bucket in time — so a value drops out `n` samples after it arrived whether
 * or not anything followed it. The result at a sample is the mean of the
 * non-empty ones the window holds, null when it holds none.
 */
function computeSMA(values: (number | null)[], n: number): (number | null)[] {
  const result: (number | null)[] = [];
  let sum = 0;
  let count = 0;
  for (let i = 0; i < values.length; i++) {
    const arrived = values[i];
    if (arrived !== null) {
      sum += arrived;
      count += 1;
    }
    // The sample that was in the window one step ago and is not any more.
    const gone = i >= n ? values[i - n] : null;
    if (gone !== null) {
      sum -= gone;
      count -= 1;
    }
    result.push(count > 0 ? sum / count : null);
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

/**
 * Circular statistics for a path recorded in radians — headings, courses,
 * wind angles. The linear mean of 359° and 1° is 180°; the vector mean, the
 * angle of the mean sine and cosine, is 0°. A result goes back into the
 * convention the samples came in: [0, 2π) when none was negative, (−π, π]
 * otherwise — Signal K uses both, by path.
 */
const TWO_PI = 2 * Math.PI;

function isAngular(units: string | undefined): boolean {
  return units === "rad";
}

function normaliseAngle(mean: number, nonNegative: boolean): number {
  // Modulo rather than one addition: a mean a hair below zero must read as
  // 0, not as 2π less a hair. atan2 can return exactly −π, which the signed
  // convention (−π, π] reports as π.
  if (nonNegative) return ((mean % TWO_PI) + TWO_PI) % TWO_PI;
  return mean <= -Math.PI ? mean + TWO_PI : mean;
}

/**
 * The bucket's vector mean, and its lowest sample for the convention.
 * Samples that cancel — 0° and 180° in equal measure — leave no direction
 * to report, so a resultant shorter than the guard is null rather than
 * whatever atan2 makes of two zeros.
 */
const NO_DIRECTION = 1e-9;
const ANGULAR_BUCKET_SQL = `CASE WHEN sqrt(avg(sin(value)) * avg(sin(value)) + avg(cos(value)) * avg(cos(value))) < ${NO_DIRECTION} THEN NULL ELSE atan2(avg(sin(value)), avg(cos(value))) END as agg_value, min(value) as agg_floor`;

/** The angle of a mean sine and cosine, or null when they cancel. */
function meanAngle(
  sin: number,
  cos: number,
  nonNegative: boolean,
): number | null {
  return Math.hypot(sin, cos) < NO_DIRECTION
    ? null
    : normaliseAngle(Math.atan2(sin, cos), nonNegative);
}

function computeAngularSMA(
  values: (number | null)[],
  n: number,
  nonNegative: boolean,
): (number | null)[] {
  const result: (number | null)[] = [];
  let sumSin = 0;
  let sumCos = 0;
  let count = 0;
  for (let i = 0; i < values.length; i++) {
    const arrived = values[i];
    if (arrived !== null) {
      sumSin += Math.sin(arrived);
      sumCos += Math.cos(arrived);
      count += 1;
    }
    const gone = i >= n ? values[i - n] : null;
    if (gone !== null) {
      sumSin -= Math.sin(gone);
      sumCos -= Math.cos(gone);
      count -= 1;
    }
    result.push(
      count > 0 ? meanAngle(sumSin / count, sumCos / count, nonNegative) : null,
    );
  }
  return result;
}

function computeAngularEMA(
  values: (number | null)[],
  alpha: number,
  nonNegative: boolean,
): (number | null)[] {
  const result: (number | null)[] = [];
  let sin: number | null = null;
  let cos: number | null = null;
  for (const v of values) {
    if (v !== null) {
      if (sin === null || cos === null) {
        sin = Math.sin(v);
        cos = Math.cos(v);
      } else {
        sin = alpha * Math.sin(v) + (1 - alpha) * sin;
        cos = alpha * Math.cos(v) + (1 - alpha) * cos;
      }
    }
    result.push(
      sin === null || cos === null ? null : meanAngle(sin, cos, nonNegative),
    );
  }
  return result;
}

/**
 * Carry a held value across the empty buckets after it.
 *
 * The recorder skips a value equal to the last one it wrote until the
 * unchanged-value heartbeat is due (see ChangeGate), so a bucket with no row
 * usually means "unchanged", not "no data" — a switch that has been on for an
 * hour has one row in that hour, and FILL(NULL) alone would chart it as off
 * the air. Each empty bucket takes the last sample before it, but only while
 * that sample is younger than `holdMs`: a live path writes at least once per
 * heartbeat, so a longer silence is a real gap — a sensor gone stale, the
 * recorder stopped — and stays one.
 *
 * Every row ends in the bucket's `last(ts)` — when its last sample was
 * taken, null for an empty bucket — and the hold is measured from that, not
 * from the bucket start: at a resolution near the hold, a sample late in its
 * bucket would otherwise count as a whole bucket older than it is. `seed` is
 * the last row before the range in the same shape, so the first buckets of a
 * range are held too.
 *
 * Never into a bucket that starts after `now`: a held value says the value
 * had not changed, which nothing can say about a time still to come.
 */
function holdThroughEmptyBuckets(
  rows: unknown[][],
  seed: unknown[] | null,
  holdMs: number,
  carry: (empty: unknown[], held: unknown[]) => unknown[],
  now = Date.now(),
): unknown[][] {
  if (holdMs <= 0) return rows;
  const sampledAt = (row: unknown[]) => row[row.length - 1];
  let held = seed;
  return rows.map((row) => {
    if (sampledAt(row) !== null) {
      held = row;
      return row;
    }
    const bucketStart = Date.parse(row[0] as string);
    if (
      held === null ||
      bucketStart > now ||
      bucketStart - Date.parse(sampledAt(held) as string) >= holdMs
    ) {
      return row;
    }
    return carry(row, held);
  });
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
// Trailing columns of every numeric bucket read, for holdNumericBuckets: the
// bucket's last sample and when it was taken.
const HELD_SQL = "last(value) as held, last(ts) as held_ts";

function effectiveResolution(resolution: number): number {
  return Math.max(1, Math.floor(resolution));
}

/**
 * The SAMPLE BY clause every bucketed read uses. FROM … TO makes FILL(NULL)
 * fabricate the WHOLE grid of the range: without it QuestDB only fills
 * between the first and last bucket that has a row, so leading and trailing
 * empty buckets were missing from the response — and a change-only recorded
 * switch, one row in the middle of the range, came back as a single bucket
 * with nothing to hold across. It also aligns buckets to the range start
 * rather than the calendar. TO is exclusive: a row stamped exactly at the
 * range end starts a bucket past it and is left out.
 */
function sampleBy(range: ResolvedRange, resolution: number): string {
  const from = validateTimestamp(range.from);
  const to = validateTimestamp(range.to);
  return `SAMPLE BY ${effectiveResolution(resolution)}s FROM '${from}' TO '${to}' FILL(NULL)`;
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
  // A path's units per the server's metadata, in the request's context.
  // Unknown means linear, which is right for everything but radians.
  unitsOf: (path: string, context: string) => string | undefined = () =>
    undefined,
  // How long an empty bucket may carry the last sample before it (see
  // holdThroughEmptyBuckets). 0 leaves empty buckets null, as FILL(NULL)
  // makes them.
  holdMs = 0,
): HistoryApi.HistoryProvider {
  /**
   * The range's context predicate over the `holdMs` just before it — where
   * the row a range's first buckets are held from lives, if there is one.
   */
  function seedRangeWhere(range: ResolvedRange, safeContext: string): string {
    const from = validateTimestamp(range.from);
    const since = new Date(Date.parse(from) - holdMs).toISOString();
    return `ts >= '${since}' AND ts < '${from}' AND context = '${safeContext}'`;
  }

  /** The numeric table's last `[value, ts]` before the range, or null. */
  async function numericSeed(seedWhere: string): Promise<unknown[] | null> {
    const result = await queryClient.exec(
      `SELECT value, ts FROM signalk WHERE ${seedWhere} LIMIT -1`,
    );
    return result.dataset[0] ?? null;
  }

  /**
   * Hold a numeric SAMPLE BY result whose last two columns are the bucket's
   * `last(value)` and `last(ts)` (HELD_SQL). Every aggregate of a bucket
   * that held one unchanged value is that value, so an empty bucket takes the
   * held sample in every column. The seed is only fetched when the range
   * opens on an empty bucket.
   */
  async function holdNumericBuckets(
    dataset: unknown[][],
    seedWhere: string,
  ): Promise<unknown[][]> {
    if (holdMs <= 0 || dataset.length === 0) return dataset;
    const width = dataset[0].length;
    let seed: unknown[] | null = null;
    if (dataset[0][width - 1] === null) {
      const found = await numericSeed(seedWhere);
      // Shaped like a bucket row: the value in every value column, the
      // sample's time last.
      if (found) {
        seed = [
          found[1],
          ...Array<unknown>(width - 2).fill(found[0]),
          found[1],
        ];
      }
    }
    return holdThroughEmptyBuckets(dataset, seed, holdMs, (row, held) =>
      row.map((cell, i) =>
        i === 0 ? cell : i === width - 1 ? null : held[width - 2],
      ),
    );
  }

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
    // would schedule dozens of queries from a single HTTP request. Past the
    // ceiling the request fails, as it does past the request-wide one:
    // returning some of the asked-for series without saying so would be
    // worse than refusing a request that is too broad.
    if (all.length > MAX_EXPANDED_SOURCES) {
      throw new Error(
        `sourcePolicy=all: ${path} was recorded by ${all.length} sources in ` +
          `range, more than the ${MAX_EXPANDED_SOURCES} one path may expand ` +
          `into — name the sources with paths=<path>|<sourceRef>, or ask ` +
          `for a shorter range`,
      );
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
    range: ResolvedRange,
    resolution?: number,
    seedWhere?: string,
  ): Promise<{ rows: [string, unknown][]; sourceDropped: boolean }> {
    // `value_kind` marks rows that were recorded as booleans, so v2 replays
    // them as real booleans exactly like v1 — otherwise the same path would
    // read `true` through one API and `"true"` through the other. Untagged
    // rows (plain text, and everything written before the column existed)
    // stay strings; the text is never guessed at.
    // Strip the source predicate for the no-source retry. It is always the
    // trailing ` AND source = '...'` / ` AND source IS NULL` that
    // buildSourceWhere appended, so removing it leaves the range, context
    // and path filters intact.
    const withoutSource = (clause: string) =>
      clause.replace(/ AND source (?:= '[^']*'|IS NULL)/g, "");
    const sql = (withKind: boolean, withSource: boolean) => {
      const kind = withKind ? "value_kind" : "NULL";
      const clause = withSource ? where : withoutSource(where);
      return resolution && resolution > 0
        ? `SELECT ts, last(value_str) as value_str, last(${kind}) as value_kind, last(ts) as held_ts FROM signalk_str WHERE ${clause} ${sampleBy(range, resolution)} ORDER BY ts`
        : `SELECT ts, value_str, ${kind} as value_kind FROM signalk_str WHERE ${clause} ORDER BY ts LIMIT 10000`;
    };
    // A read racing ensureTables()'s migration — or an external QuestDB the
    // plugin does not own — must degrade, not fail the request. Two columns
    // can be missing independently: `value_kind` (degrade to text) and
    // `source` (degrade to unfiltered, and say so).
    let sourceDropped = false;
    // The column set the query ran with, for the seed to run with the same.
    let ranWithKind = true;
    const run = async (
      withKind: boolean,
      withSource: boolean,
    ): Promise<QuestDBResult> => {
      try {
        const result = await queryClient.exec(sql(withKind, withSource));
        ranWithKind = withKind;
        return result;
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
    let dataset = result.dataset;
    // A text bucket's value is already its last sample, so an empty bucket
    // takes the held row's text and kind as they are.
    if (resolution && resolution > 0 && seedWhere && holdMs > 0) {
      let seed: unknown[] | null = null;
      if (dataset.length > 0 && dataset[0][3] === null) {
        const kind = ranWithKind ? "value_kind" : "NULL";
        const clause = sourceDropped ? withoutSource(seedWhere) : seedWhere;
        const seedResult = await queryClient.exec(
          `SELECT ts, value_str, ${kind} as value_kind, ts as held_ts FROM signalk_str WHERE ${clause} LIMIT -1`,
        );
        seed = seedResult.dataset[0] ?? null;
      }
      dataset = holdThroughEmptyBuckets(dataset, seed, holdMs, (row, held) => [
        row[0],
        held[1],
        held[2],
        null,
      ]);
    }
    return {
      rows: dataset.map((row: unknown[]) => [
        row[0] as string,
        row[2] === "boolean" ? row[1] === "true" : row[1],
      ]),
      sourceDropped,
    };
  }

  async function getValues(query: ValuesRequest): Promise<ValuesResponse> {
    const range = resolveTimeRange(query);

    // Two gates, both required. The caller asks with sourcePolicy=all; the
    // operator has to have allowed it. An unknown policy string is treated as
    // absent — the server validates the value before a provider ever sees it,
    // so guessing at anything else here would only invent behaviour.
    const expandBySource =
      sourcePolicyAllEnabled && query.sourcePolicy === "all";

    // Checked before any query runs, so a bad window fails the request whole
    // instead of after the columns ahead of it were already read.
    for (const spec of query.pathSpecs) {
      if (isMovingAverage(spec.aggregate)) movingAverageWindow(spec);
    }

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
        columns.push({ ...spec, sourceRef: source as SourceRef });
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
    // Every column runs a query bounded by the bucket count: SAMPLE BY
    // fabricates one row per bucket, middle_index picks at most one.
    //
    // A non-numeric path costs TWO such queries: the numeric one comes back
    // empty and the string-table fallback repeats it against signalk_str.
    // Which paths those are is only known after querying, so the budget
    // assumes the worst case — every sampled column falling back — rather
    // than letting a request built entirely of boolean/string paths quietly
    // run at twice the ceiling.
    const sampledSpecs = columns.length;
    // navigation.position is served by its own table and never falls back;
    // a moving average and middle_index have no text to fall back to.
    const fallbackCapableSpecs = columns.filter(
      (spec) =>
        spec.path !== "navigation.position" &&
        !isMovingAverage(spec.aggregate) &&
        spec.aggregate !== "middle_index",
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
        // Position is a co-recorded lat/lon pair. first, last and
        // middle_index keep a point the vessel was at; a per-axis
        // avg/min/max/mid would fabricate one it never occupied, so anything
        // else runs first — and the column says so, as the string fallback
        // does. A silent caller also gets first (the server default).
        const posAgg =
          spec.aggregate === "last" || spec.aggregate === "middle_index"
            ? spec.aggregate
            : "first";
        if (posAgg !== spec.aggregate) valuesList[specIndex].method = "first";
        let sql: string;
        if (posAgg === "middle_index") {
          sql = middleRowSql(table, "lat, lon", where, range, query.resolution);
        } else if (query.resolution && query.resolution > 0) {
          sql = `SELECT ts, ${posAgg}(lat) as lat, ${posAgg}(lon) as lon FROM ${table} WHERE ${where} ${sampleBy(range, query.resolution)} ORDER BY ts`;
        } else {
          sql = `SELECT ts, lat, lon FROM ${table} WHERE ${where} ORDER BY ts LIMIT 10000`;
        }
        const result = await queryClient.exec(sql);
        // A position goes out as a `[longitude, latitude]` pair — GeoJSON
        // order, as the History API's OpenAPI schema defines and the other
        // providers emit — not as the data model's `{latitude, longitude}`.
        const rows: [string, unknown][] = result.dataset.map((row) => [
          row[0] as string,
          row[1] !== null && row[2] !== null ? [row[2], row[1]] : null,
        ]);
        columnData.set(specIndex, rows);
        continue;
      }

      const where = `${buildRangeWhere(range, safeContext)} AND path = '${safePath}'${sourceWhere}`;
      const seedWhere = `${seedRangeWhere(range, safeContext)} AND path = '${safePath}'${sourceWhere}`;

      if (isMovingAverage(spec.aggregate)) {
        // The window runs over the series at the requested resolution — one
        // averaged bucket per sample, the grid every other column is on — so
        // sma:5 at 180s is a 15-minute average and the rows line up with the
        // rest of the response. With no resolution the series is the raw
        // rows, as it is for every method. A text path has nothing to
        // average, so this column never falls back to the string table.
        // An empty bucket stays a sample of the window (see computeSMA), so a
        // gap ages values out instead of stretching the window across it.
        const window = movingAverageWindow(spec);
        const angular = isAngular(unitsOf(spec.path, requestedContext));
        const bucket = angular ? ANGULAR_BUCKET_SQL : "avg(value) as agg_value";
        const sampled = !!query.resolution && query.resolution > 0;
        const sql = sampled
          ? `SELECT ts, ${bucket}, ${HELD_SQL} FROM ${table} WHERE ${where} ${sampleBy(range, query.resolution!)} ORDER BY ts`
          : `SELECT ts, value FROM ${table} WHERE ${where} ORDER BY ts LIMIT 10000`;
        const result = await queryClient.exec(sql);
        // Held before averaging: an unchanged value is still a sample of
        // the window, not a hole in it.
        const dataset = sampled
          ? await holdNumericBuckets(result.dataset, seedWhere)
          : result.dataset;
        // An angular series keeps one convention throughout: [0, 2π) unless
        // a sample (or a bucket's lowest sample) was negative.
        const floors = dataset.map((r) =>
          angular && sampled
            ? (r[2] as number | null)
            : (r[1] as number | null),
        );
        const nonNegative = floors.every((f) => f === null || f >= 0);
        const series = dataset.map((r) => {
          const v = r[1] as number | null;
          return v !== null && angular && sampled
            ? normaliseAngle(v, (r[2] as number) >= 0)
            : v;
        });
        const computed = angular
          ? "samples" in window
            ? computeAngularSMA(series, window.samples, nonNegative)
            : computeAngularEMA(series, window.alpha, nonNegative)
          : "samples" in window
            ? computeSMA(series, window.samples)
            : computeEMA(series, window.alpha);
        const rows: [string, unknown][] = dataset.map((r, i) => [
          r[0] as string,
          computed[i],
        ]);
        columnData.set(specIndex, rows);
        continue;
      }

      if (spec.aggregate === "middle_index") {
        // A text path has rows in signalk_str only; this reads the numeric
        // table and leaves the column empty for one, like a moving average.
        const result = await queryClient.exec(
          middleRowSql(table, "value", where, range, query.resolution),
        );
        const rows: [string, unknown][] = result.dataset.map((r) => [
          r[0] as string,
          r[1],
        ]);
        columnData.set(specIndex, rows);
        continue;
      }

      // Only average has a circular form; min, max and mid on an angle are
      // the caller's choice, and first, last and middle_index are samples.
      const angularMean =
        spec.aggregate === "average" &&
        isAngular(unitsOf(spec.path, requestedContext));
      const aggExpr = angularMean
        ? ANGULAR_BUCKET_SQL
        : `${aggregateToSql(spec.aggregate)} as agg_value`;
      const sampled = !!query.resolution && query.resolution > 0;
      const sql = sampled
        ? `SELECT ts, ${aggExpr}, ${HELD_SQL} FROM ${table} WHERE ${where} ${sampleBy(range, query.resolution!)} ORDER BY ts`
        : `SELECT ts, value FROM ${table} WHERE ${where} ORDER BY ts LIMIT 10000`;

      const result = await queryClient.exec(sql);
      const dataset = sampled
        ? await holdNumericBuckets(result.dataset, seedWhere)
        : result.dataset;
      const rows: [string, unknown][] = dataset.map((row) => [
        row[0] as string,
        angularMean && row.length > 2 && row[1] !== null
          ? normaliseAngle(row[1] as number, (row[2] as number) >= 0)
          : row[1],
      ]);

      // Non-numeric paths (strings, and booleans stored as "true"/"false")
      // live in signalk_str, which this query never touches — they used to
      // come back empty even though getPaths lists them. Fall back to the
      // string table when the numeric one held nothing for this path.
      // Emptiness is judged on VALUES, not row count: a SAMPLE BY with
      // FILL(NULL) fabricates a row per bucket, so an all-null result is
      // still "no numeric data here". Not for an angular mean, though: its
      // null is a bucket whose samples cancelled, and the string table
      // holds no angles to fall back to.
      if (!angularMean && !rows.some(([, value]) => value !== null)) {
        // Report the aggregate that was actually applied. Downsampled string
        // rows always use last() — averaging text is meaningless — so leaving
        // the caller's requested method in the response would label the
        // series with an aggregation that never ran.
        if (query.resolution && query.resolution > 0) {
          valuesList[specIndex].method = "last";
        }
        const strResult = await readStringRows(
          where,
          range,
          query.resolution,
          seedWhere,
        );
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

    const data: HistoryApi.DataRow[] = sortedTimestamps.map((ts) => {
      const row: HistoryApi.DataRow = [ts as Timestamp];
      for (let i = 0; i < columns.length; i++) {
        const m = indexMaps.get(i);
        row.push(m?.get(ts) ?? null);
      }
      return row;
    });

    return {
      context: requestedContext as Context,
      range: { from: range.from as Timestamp, to: range.to as Timestamp },
      values: valuesList,
      data,
    };
  }

  async function getPaths(
    query: PathsRequest,
  ): Promise<HistoryApi.PathsResponse> {
    const range = resolveTimeRange(query);
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

    return result.dataset.map((row) => row[0] as Path);
  }

  async function getContexts(
    query: ContextsRequest,
  ): Promise<HistoryApi.ContextsResponse> {
    const range = resolveTimeRange(query);
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
      const ctx = row[0] as Context;
      return ctx === "self" ? ("vessels.self" as Context) : ctx;
    });
  }

  return { getValues, getPaths, getContexts };
}
