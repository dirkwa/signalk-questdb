// Importing history out of an existing InfluxDB into QuestDB.
//
// The Signal K InfluxDB plugins (signalk-to-influxdb and -influxdb2) store one
// measurement per Signal K path, with the value in a field. Which field, and
// what shape, differs between them and across versions, so the reader here
// classifies each row by the value it actually finds rather than by assuming a
// schema. Anything it cannot map is counted as skipped, never silently
// dropped: a migration that quietly loses a third of the history looks
// identical to one that worked.
//
// Reads are windowed by time and drained window by window. A boat's InfluxDB
// is routinely larger than the Pi's RAM, so neither the query nor the result
// may be unbounded — and a window that returns nothing still advances, so a
// multi-year gap costs one empty query per window rather than a stall.

import type { ILPWriter } from "./ilp-writer.js";
import { routeDeltaValue, flattenObjectValue } from "./delta-routing.js";
import type {
  MigrationBucket,
  MigrationMeasurement,
  MigrationProgress,
  MigrationRunState,
} from "./api-contract.js";

/** Nanoseconds per millisecond, the unit gap between `Date` and ILP. */
const NANOS_PER_MS = 1_000_000n;

/**
 * How much time one read covers. Small enough that a dense measurement does
 * not return a result set too large to hold, large enough that a sparse one
 * does not need thousands of round trips. A day is the natural unit for boat
 * data: it bounds a busy passage to a few hundred thousand points.
 */
export const DEFAULT_WINDOW_MS = 24 * 60 * 60 * 1000;

/**
 * Pause reading once the writer has this many lines queued. The ILP writer
 * flushes on its own timer; without backpressure a fast InfluxDB fills the
 * buffer faster than the socket drains it and the import is capped by
 * MAX_BUFFER_LINES dropping the OLDEST lines — i.e. it would silently discard
 * the very history it was asked to copy.
 */
const WRITER_HIGH_WATER = 20_000;
const WRITER_RESUME = 5_000;

/**
 * Give up waiting for the writer to drain after this long.
 *
 * The drain wait is otherwise unbounded, and `pendingLines` only falls when
 * QuestDB actually accepts data. A QuestDB that is down, wedged or refusing
 * writes therefore parks the import in a `sleep` loop forever: the run sits at
 * "running" with its counters frozen, nothing is logged, and the only way out
 * is cancelling it by hand. Failing with a stated reason after five minutes is
 * far more useful than an import that silently never finishes — and because
 * imported rows upsert on their dedup key, re-running once QuestDB is healthy
 * simply resumes the work.
 */
const WRITER_DRAIN_TIMEOUT_MS = 5 * 60 * 1000;

export interface InfluxAuth {
  /** 2.x API token, or 1.x password when `username` is set. */
  token?: string;
  username?: string;
  password?: string;
  /** 2.x organisation, required by the Flux API. */
  org?: string;
}

export interface MigrationRequest {
  url: string;
  /** "influxdb1" or "influxdb2" — decides the query dialect. */
  type: string;
  /** Bucket (2.x) or database (1.x). */
  bucket: string;
  auth?: InfluxAuth;
  /** ISO instants bounding the import. */
  from: string;
  to: string;
  /** Signal K context to write rows under. */
  context: string;
  /** Restrict to these measurements; empty/absent means all of them. */
  measurements?: string[];
  /** `source` tag written on every imported row, so it is distinguishable. */
  sourceLabel?: string;
  windowMs?: number;
}

function authHeaders(type: string, auth?: InfluxAuth): Record<string, string> {
  if (!auth) return {};
  if (type === "influxdb2") {
    return auth.token ? { Authorization: `Token ${auth.token}` } : {};
  }
  // 1.x accepts HTTP Basic; token auth is meaningless there.
  if (auth.username) {
    const basic = Buffer.from(
      `${auth.username}:${auth.password ?? ""}`,
    ).toString("base64");
    return { Authorization: `Basic ${basic}` };
  }
  return {};
}

/** Buckets (2.x) or databases (1.x) available to import from. */
export async function listBuckets(
  req: { url: string; type: string; auth?: InfluxAuth },
  fetchImpl: typeof fetch = fetch,
): Promise<MigrationBucket[]> {
  const headers = authHeaders(req.type, req.auth);
  if (req.type === "influxdb2") {
    type BucketPage = {
      buckets?: {
        id?: string;
        name?: string;
        retentionRules?: { everySeconds?: number }[];
      }[];
      links?: { next?: string };
    };

    // 100 is the API maximum per page, and a server with more buckets returns
    // a relative `links.next` — verified against a live 2.9.1 holding 106
    // buckets, where a single request silently returned only 98 of them. A
    // bucket the operator cannot see is a bucket they cannot import from, so
    // the pages are followed to the end rather than truncated.
    const collected: NonNullable<BucketPage["buckets"]> = [];
    // The FIRST page is built by concatenation, like every other request in
    // this file. Resolving an absolute path against the base instead would
    // drop a configured path prefix — `http://host/influx` would be probed at
    // `http://host/api/v2/buckets` — and a reverse-proxied InfluxDB is a
    // normal setup, which validateInfluxUrl deliberately preserves.
    let url: string | null = `${req.url}/api/v2/buckets?limit=100`;
    // Hard stop on the page count: `links.next` comes from the far end, and a
    // server that always returns one would otherwise loop forever. 100 pages
    // is 10k buckets — far past any real deployment.
    for (let page = 0; url && page < 100; page++) {
      const current: string = url;
      const r = await fetchImpl(current, {
        headers,
        signal: AbortSignal.timeout(10000),
      });
      if (!r.ok) throw new Error(await describeHttpError(r, "list buckets"));
      const body = (await r.json()) as BucketPage;
      collected.push(...(body.buckets ?? []));
      // `links.next` is server-supplied and relative, so it resolves against
      // the page just fetched rather than against the configured base.
      const link = body.links?.next;
      if (typeof link !== "string") {
        url = null;
      } else {
        const resolved = new URL(link, current).toString();
        // A `next` pointing at the page just fetched would spin forever.
        url = resolved === current ? null : resolved;
      }
    }

    return (
      collected
        // The _monitoring/_tasks system buckets hold InfluxDB's own telemetry,
        // never Signal K data; offering them as import sources is noise.
        .filter((b) => b.name && !b.name.startsWith("_"))
        .map((b) => ({
          name: b.name as string,
          id: b.id,
          retentionSeconds: b.retentionRules?.[0]?.everySeconds ?? 0,
        }))
    );
  }

  const r = await fetchImpl(
    `${req.url}/query?q=${encodeURIComponent("SHOW DATABASES")}`,
    { headers, signal: AbortSignal.timeout(10000) },
  );
  if (!r.ok) throw new Error(await describeHttpError(r, "list databases"));
  const body = (await r.json()) as InfluxQlResponse;
  // 1.x answers a REJECTED query with HTTP 200 and an `error` member, so
  // without this an auth failure or a bad query reads as "no databases" —
  // the same trap the measurement and window readers already guard against.
  if (body.results?.[0]?.error) throw new Error(body.results[0].error);
  const values = body.results?.[0]?.series?.[0]?.values ?? [];
  return values
    .map((row) => String(row[0]))
    .filter((name) => name && name !== "_internal")
    .map((name) => ({ name }));
}

interface InfluxQlResponse {
  results?: {
    series?: { name?: string; columns?: string[]; values?: unknown[][] }[];
    error?: string;
  }[];
}

/** Measurements in a bucket/database, with the field keys each carries. */
export async function listMeasurements(
  req: { url: string; type: string; bucket: string; auth?: InfluxAuth },
  fetchImpl: typeof fetch = fetch,
): Promise<MigrationMeasurement[]> {
  const headers = authHeaders(req.type, req.auth);
  if (req.type === "influxdb2") {
    // One request for every (measurement, field) pair in the bucket.
    //
    // schema.fieldKeys() looks like the obvious call but returns only a bare
    // `_value` column with no `_measurement` — verified against a live 2.9.1 —
    // so the pairing is lost and every measurement reads as having no fields.
    // Grouping by both columns and regrouping keeps them together.
    //
    // The unbounded `range(start: 0)` scans the whole bucket, but `keep` drops
    // everything except the two schema columns before `distinct` collapses
    // them, so what comes back is one row per pair, not one per point.
    const flux = `from(bucket: ${JSON.stringify(req.bucket)})
  |> range(start: 0)
  |> keep(columns: ["_measurement", "_field"])
  |> group(columns: ["_measurement", "_field"])
  |> distinct(column: "_field")
  |> group()
  |> keep(columns: ["_measurement", "_value"])`;
    const rows = await runFlux(req, flux, headers, fetchImpl);
    const byMeasurement = new Map<string, Set<string>>();
    for (const row of rows) {
      const m = row.values["_measurement"];
      const f = row.values["_value"];
      if (!m) continue;
      if (!byMeasurement.has(m)) byMeasurement.set(m, new Set());
      if (f) byMeasurement.get(m)!.add(f);
    }
    // An empty result means the bucket has no data in it at all (the pair
    // query is driven by points, not by schema metadata). Fall back to the
    // schema listing so an empty-but-existing measurement still appears.
    if (byMeasurement.size === 0) {
      const flux2 = `import "influxdata/influxdb/schema"
schema.measurements(bucket: ${JSON.stringify(req.bucket)})`;
      const ms = await runFlux(req, flux2, headers, fetchImpl);
      return ms
        .map((r) => r.values["_value"])
        .filter((v): v is string => !!v)
        .map((name) => ({ name, fields: [] }));
    }
    return [...byMeasurement.entries()].map(([name, fields]) => ({
      name,
      fields: [...fields],
    }));
  }

  // SHOW FIELD KEYS returns one series PER MEASUREMENT, each named after it,
  // so a single request yields the same (measurement, fields) pairing the 2.x
  // branch builds — rather than listing names with an always-empty `fields`
  // that the response contract declares as populated.
  const url = `${req.url}/query?db=${encodeURIComponent(req.bucket)}&q=${encodeURIComponent(
    "SHOW FIELD KEYS",
  )}`;
  const r = await fetchImpl(url, {
    headers,
    signal: AbortSignal.timeout(15000),
  });
  if (!r.ok) throw new Error(await describeHttpError(r, "list measurements"));
  const body = (await r.json()) as InfluxQlResponse;
  if (body.results?.[0]?.error) throw new Error(body.results[0].error);
  const series = body.results?.[0]?.series ?? [];
  const out = series
    .filter((s) => s.name)
    .map((s) => ({
      name: s.name as string,
      // Each row is [fieldKey, fieldType].
      fields: (s.values ?? []).map((row) => String(row[0])),
    }));
  if (out.length > 0) return out;

  // A measurement with no field keys (possible on an odd schema) would be
  // missing above, so fall back to the plain listing rather than returning
  // nothing at all.
  const r2 = await fetchImpl(
    `${req.url}/query?db=${encodeURIComponent(req.bucket)}&q=${encodeURIComponent("SHOW MEASUREMENTS")}`,
    { headers, signal: AbortSignal.timeout(15000) },
  );
  if (!r2.ok) throw new Error(await describeHttpError(r2, "list measurements"));
  const body2 = (await r2.json()) as InfluxQlResponse;
  const values = body2.results?.[0]?.series?.[0]?.values ?? [];
  return values.map((row) => ({ name: String(row[0]), fields: [] }));
}

async function describeHttpError(r: Response, what: string): Promise<string> {
  const text = await r.text().catch(() => "");
  // 401/403 against InfluxDB almost always means a missing or wrong token,
  // which is worth saying outright — the raw body is usually just {"code":
  // "unauthorized"} and leaves the user guessing.
  if (r.status === 401 || r.status === 403) {
    return `Cannot ${what}: InfluxDB rejected the credentials (HTTP ${r.status}). Check the API token / username and password.`;
  }
  return `Cannot ${what}: HTTP ${r.status}${text ? ` — ${text.slice(0, 200)}` : ""}`;
}

/**
 * Run a Flux query and return the annotated-CSV rows as records.
 *
 * Written by hand rather than pulled in as a dependency: the plugin ships with
 * four runtime deps and the official client would add a large tree for what is
 * one POST and a CSV parse.
 */
async function runFlux(
  req: { url: string; auth?: InfluxAuth },
  flux: string,
  headers: Record<string, string>,
  fetchImpl: typeof fetch,
  timeoutMs = 120_000,
): Promise<AnnotatedRecord[]> {
  const org = req.auth?.org ?? "";
  const url = `${req.url}/api/v2/query${org ? `?org=${encodeURIComponent(org)}` : ""}`;
  const r = await fetchImpl(url, {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/vnd.flux",
      Accept: "text/csv",
    },
    body: flux,
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!r.ok) throw new Error(await describeHttpError(r, "run query"));
  return parseAnnotatedCsv(await r.text());
}

/**
 * Minimal annotated-CSV reader for Flux responses.
 *
 * Flux emits one or more *tables*, each preceded by annotation lines starting
 * with `#` and then a header row. A response can therefore change its column
 * layout partway through, which is why the header is tracked per block rather
 * than read once at the top.
 */
export function parseAnnotatedCsv(text: string): AnnotatedRecord[] {
  const out: AnnotatedRecord[] = [];
  let header: string[] | null = null;
  // Per-block `#datatype` line. Without it a genuine STRING value of "3.5"
  // is indistinguishable from the number 3.5 in the CSV body — verified
  // against a live 2.9.1 — and would be imported into the numeric table.
  let datatypes: string[] | null = null;
  // A quoted CSV value may contain a newline — verified against a live 2.9.1,
  // which returns `"line1\r\nline2"` for a string value holding one. Splitting
  // on newlines alone would tear that record in half and produce two garbage
  // rows, so physical lines are joined until the quotes balance.
  // \r is stripped per PHYSICAL line before joining: a quoted value spanning
  // two CRLF lines would otherwise keep the \r of the first inside the joined
  // value, so the imported string silently carries a stray carriage return.
  for (const rawLine of joinQuotedLines(
    text.split("\n").map((l) => l.replace(/\r$/, "")),
  )) {
    const line = rawLine;
    if (line === "") {
      // Blank line separates tables; the next non-# line is a fresh header.
      header = null;
      datatypes = null;
      continue;
    }
    if (line.startsWith("#")) {
      // Annotation (#datatype/#group/#default). A new annotation block means
      // the previous header no longer applies.
      if (line.startsWith("#datatype")) datatypes = splitCsvLine(line);
      header = null;
      continue;
    }
    const cells = splitCsvLine(line);
    if (!header) {
      header = cells;
      continue;
    }
    const rec: Record<string, string> = {};
    const types: Record<string, string> = {};
    for (let i = 0; i < header.length; i++) {
      const key = header[i];
      // Flux's leading empty column is the annotation gutter, not data.
      if (key === "") continue;
      rec[key] = cells[i] ?? "";
      // #datatype is positionally aligned with the header row.
      if (datatypes && datatypes[i]) types[key] = datatypes[i];
    }
    out.push({ values: rec, types });
  }
  return out;
}

/**
 * Re-join physical lines that belong to one CSV record.
 *
 * A record is complete when its double quotes are balanced ("" inside a quoted
 * value is an escaped quote and does not change the balance). Annotation and
 * blank lines can never be mid-record, so they pass straight through and the
 * caller's boundary handling is unaffected.
 */
function joinQuotedLines(lines: string[]): string[] {
  const out: string[] = [];
  let pending: string | null = null;
  for (const line of lines) {
    const candidate: string = pending === null ? line : `${pending}\n${line}`;
    if (quotesBalanced(candidate)) {
      out.push(candidate);
      pending = null;
    } else {
      pending = candidate;
    }
  }
  // Unterminated quote at EOF: emit what there is rather than dropping it.
  if (pending !== null) out.push(pending);
  return out;
}

function quotesBalanced(s: string): boolean {
  let inQuotes = false;
  for (let i = 0; i < s.length; i++) {
    if (s[i] !== '"') continue;
    if (inQuotes && s[i + 1] === '"') {
      i++;
      continue;
    }
    inQuotes = !inQuotes;
  }
  return !inQuotes;
}

/** One Flux CSV record plus the `#datatype` of each of its columns. */
export interface AnnotatedRecord {
  values: Record<string, string>;
  types: Record<string, string>;
}

function splitCsvLine(line: string): string[] {
  const cells: string[] = [];
  let cur = "";
  let inQuotes = false;
  for (let i = 0; i < line.length; i++) {
    const c = line[i];
    if (inQuotes) {
      if (c === '"') {
        if (line[i + 1] === '"') {
          cur += '"';
          i++;
        } else {
          inQuotes = false;
        }
      } else {
        cur += c;
      }
    } else if (c === '"') {
      inQuotes = true;
    } else if (c === ",") {
      cells.push(cur);
      cur = "";
    } else {
      cur += c;
    }
  }
  cells.push(cur);
  return cells;
}

/**
 * Parse an RFC3339 instant into nanoseconds since the epoch, preserving digits
 * below the millisecond.
 *
 * `Date.parse` truncates to milliseconds. InfluxDB stores nanoseconds, and the
 * QuestDB tables dedup on (ts, path, context, source) — so two points 200µs
 * apart would collide and one would be lost. The fractional part is therefore
 * read from the string directly.
 */
export function rfc3339ToNanos(value: string): bigint | null {
  const m =
    /^(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2})(?:\.(\d+))?(Z|[+-]\d{2}:\d{2})$/.exec(
      value,
    );
  if (!m) {
    const ms = Date.parse(value);
    return Number.isNaN(ms) ? null : BigInt(ms) * NANOS_PER_MS;
  }
  const [, whole, frac = "", zone] = m;
  const baseMs = Date.parse(`${whole}${zone}`);
  if (Number.isNaN(baseMs)) return null;
  // Pad/truncate the fraction to exactly 9 digits of nanoseconds.
  const nanosPart = BigInt((frac + "000000000").slice(0, 9));
  return BigInt(baseMs) * NANOS_PER_MS + nanosPart;
}

/**
 * The Signal K path a measurement+field pair represents.
 *
 * signalk-to-influxdb writes the path as the measurement name and the value in
 * a field called `value`. Anything else (a measurement with several named
 * fields) is a non-Signal K schema, so the field name is appended to keep the
 * two apart instead of overwriting one with the other.
 */
export function toSignalKPath(measurement: string, field: string): string {
  if (field === "value" || field === "" || field === "_value")
    return measurement;
  return `${measurement}.${field}`;
}

/** Coerce an InfluxDB field value (always text over CSV) to a JS value. */
export function coerceValue(raw: string, dataType?: string): unknown {
  if (raw === "") return null;
  // The DECLARED type wins, and is checked first: a STRING column holding
  // "true" is the word, not a boolean, and would otherwise be recorded as one
  // — losing the distinction the value_kind tag exists to preserve.
  if (dataType === "string") return raw;
  if (dataType === "boolean" || raw === "true" || raw === "false") {
    if (raw === "true") return true;
    if (raw === "false") return false;
  }
  const n = Number(raw);
  // `Number("")` is 0 and `Number("12abc")` is NaN — only accept a clean parse,
  // otherwise treat it as the string it is.
  if (raw.trim() !== "" && Number.isFinite(n)) return n;
  return raw;
}

export interface MigrationRunHandle {
  id: string;
  state: MigrationRunState;
  url: string;
  bucket: string;
  startedAt: string;
  finishedAt?: string;
  progress: MigrationProgress;
  error?: string;
  cancel(): void;
}

/**
 * A single import run. Holds its own cancellation flag and counters so the
 * HTTP layer can poll it without the importer knowing about Express.
 */
export class MigrationRun implements MigrationRunHandle {
  state: MigrationRunState = "running";
  startedAt = new Date().toISOString();
  finishedAt?: string;
  error?: string;
  progress: MigrationProgress = {
    read: 0,
    written: 0,
    skipped: 0,
    measurementsDone: 0,
    measurementsTotal: 0,
  };
  private cancelled = false;

  constructor(
    readonly id: string,
    readonly url: string,
    readonly bucket: string,
  ) {}

  cancel(): void {
    this.cancelled = true;
  }
  get isCancelled(): boolean {
    return this.cancelled;
  }
}

/**
 * Copy history from InfluxDB into QuestDB.
 *
 * Rows are written through the plugin's own ILP writer, so imported history is
 * subject to exactly the same schema, dedup and retention rules as live data —
 * and re-running an import is idempotent, because a row with the same
 * (ts, path, context, source) upserts rather than duplicating.
 */
export async function runMigration(
  req: MigrationRequest,
  writer: Pick<
    ILPWriter,
    | "writeAtNanos"
    | "writeStringAtNanos"
    | "writePositionAtNanos"
    | "pendingLines"
  > & { readonly droppedLineCount?: number },
  run: MigrationRun,
  deps: {
    fetchImpl?: typeof fetch;
    debug?: (msg: string) => void;
    /** Injected so tests don't wait on real backpressure sleeps. */
    sleep?: (ms: number) => Promise<void>;
  } = {},
): Promise<void> {
  const fetchImpl = deps.fetchImpl ?? fetch;
  const debug = deps.debug ?? (() => {});
  const sleep =
    deps.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const headers = authHeaders(req.type, req.auth);
  // A non-positive window would make `start += windowMs` never advance and
  // spin forever. Not reachable from the HTTP API (which does not expose the
  // knob), but an infinite loop inside the Signal K process is severe enough
  // to be worth one comparison.
  const requestedWindow = req.windowMs ?? DEFAULT_WINDOW_MS;
  const windowMs = requestedWindow > 0 ? requestedWindow : DEFAULT_WINDOW_MS;
  const context = req.context;
  const source = req.sourceLabel ?? "influxdb-import";

  // The writer's drop counter is monotonic and shared with the live recorder,
  // so only the delta across this run is attributable to it.
  const droppedAtStart = writer.droppedLineCount ?? 0;

  try {
    // Checked BEFORE discovery: listMeasurements is a network round trip, and
    // running it first means a bad range costs a request and — worse — any
    // discovery failure masks the real problem, so the user is told "Cannot
    // list measurements: HTTP 401" when the actual fault is an inverted range.
    const fromMs = Date.parse(req.from);
    const toMs = Date.parse(req.to);
    if (Number.isNaN(fromMs) || Number.isNaN(toMs)) {
      throw new Error("Invalid from/to range");
    }
    if (toMs <= fromMs) {
      throw new Error("`to` must be after `from`");
    }

    let measurements = req.measurements ?? [];
    if (measurements.length === 0) {
      measurements = (await listMeasurements(req, fetchImpl)).map(
        (m) => m.name,
      );
    }
    run.progress.measurementsTotal = measurements.length;

    // Wait for the writer to fall back to the resume mark.
    //
    // Two marks, not one: resuming at the high-water mark would park the
    // buffer permanently just under the cap, where a single slow flush pushes
    // it over. And the wait is bounded — `pendingLines` only falls when
    // QuestDB actually accepts data, so a QuestDB that is down or wedged would
    // otherwise spin here forever with the run frozen at "running".
    const awaitDrain = async (): Promise<void> => {
      if (writer.pendingLines <= WRITER_HIGH_WATER) return;
      let waited = 0;
      while (writer.pendingLines > WRITER_RESUME && !run.isCancelled) {
        await sleep(200);
        waited += 200;
        if (waited >= WRITER_DRAIN_TIMEOUT_MS) {
          throw new Error(
            `QuestDB is not accepting writes: ${writer.pendingLines} lines still queued after ` +
              `${Math.round(WRITER_DRAIN_TIMEOUT_MS / 1000)}s. Import stopped — ` +
              `re-run it once QuestDB is healthy (already-imported rows are not duplicated).`,
          );
        }
      }
    };

    for (const measurement of measurements) {
      if (run.isCancelled) break;
      run.progress.currentMeasurement = measurement;

      for (let start = fromMs; start < toMs; start += windowMs) {
        if (run.isCancelled) break;
        const end = Math.min(start + windowMs, toMs);
        run.progress.currentWindowStart = new Date(start).toISOString();

        // Backpressure: let the writer drain before reading more.
        await awaitDrain();

        const { rows, dropped } =
          req.type === "influxdb2"
            ? await readWindowFlux(
                req,
                measurement,
                start,
                end,
                headers,
                fetchImpl,
              )
            : await readWindowInfluxQl(
                req,
                measurement,
                start,
                end,
                headers,
                fetchImpl,
              );

        // Half-pair positions never become rows, so they are counted here or
        // not at all — otherwise read - written - skipped silently disagrees.
        run.progress.read += dropped;
        run.progress.skipped += dropped;

        // Checked inside the row loop too, not only between windows: a single
        // dense window can carry far more rows than the writer's cap on its
        // own, and waiting only at the window boundary would let the buffer
        // sail past MAX_BUFFER_LINES mid-window — dropping its OLDEST lines,
        // which is exactly the silent data loss this guard exists to prevent.
        let sinceDrainCheck = 0;
        for (const row of rows) {
          if (run.isCancelled) break;
          if (++sinceDrainCheck >= 1000) {
            sinceDrainCheck = 0;
            await awaitDrain();
          }
          run.progress.read++;
          const written = writeRow(row, measurement, context, source, writer);
          if (written) run.progress.written++;
          else run.progress.skipped++;
        }
      }

      run.progress.measurementsDone++;
      debug(
        `migration: ${measurement} done (${run.progress.written} written, ${run.progress.skipped} skipped)`,
      );
    }

    // The writer drops the OLDEST buffered lines when its cap is hit — while
    // disconnected, or when QuestDB cannot keep up. Those rows were counted as
    // written but never reached the database, so a run that ends there is NOT
    // a clean success and must not be reported as one: the whole point of an
    // import is knowing what actually landed.
    const droppedByWriter = (writer.droppedLineCount ?? 0) - droppedAtStart;
    if (droppedByWriter > 0) {
      throw new Error(
        `QuestDB could not keep up: ${droppedByWriter} buffered rows were dropped before reaching the database. ` +
          `Re-run the import once QuestDB is healthy (already-imported rows are not duplicated).`,
      );
    }

    run.state = run.isCancelled ? "cancelled" : "done";
  } catch (err) {
    run.state = "failed";
    run.error = err instanceof Error ? err.message : String(err);
  } finally {
    run.finishedAt = new Date().toISOString();
    run.progress.currentMeasurement = undefined;
    run.progress.currentWindowStart = undefined;
  }
}

/** One point read out of InfluxDB, normalised across the two dialects. */
export interface SourceRow {
  tsNanos: bigint;
  field: string;
  value: unknown;
}

/**
 * Map one source row onto the plugin's tables, reusing the live path's routing
 * so imported data is classified exactly like recorded data.
 *
 * Returns false when the row could not be represented, which the caller counts
 * as skipped.
 */
function writeRow(
  row: SourceRow,
  measurement: string,
  context: string,
  source: string,
  writer: Pick<
    ILPWriter,
    "writeAtNanos" | "writeStringAtNanos" | "writePositionAtNanos"
  >,
): boolean {
  const path = toSignalKPath(measurement, row.field);
  if (!path) return false;
  const route = routeDeltaValue(path, row.value);
  switch (route) {
    case "number":
      writer.writeAtNanos(
        path,
        context,
        row.value as number,
        row.tsNanos,
        source,
      );
      return true;
    case "string":
      writer.writeStringAtNanos(
        path,
        context,
        row.value as string,
        row.tsNanos,
        undefined,
        source,
      );
      return true;
    case "boolean":
      writer.writeStringAtNanos(
        path,
        context,
        String(row.value),
        row.tsNanos,
        "boolean",
        source,
      );
      return true;
    case "position": {
      const v = row.value as { latitude: number; longitude: number };
      writer.writePositionAtNanos(context, v, row.tsNanos, source);
      return true;
    }
    case "flatten": {
      // An object value stored in InfluxDB is unusual but not impossible
      // (position written as a single field). Record its scalar leaves, the
      // same as the live path does. A leaf may be a string or boolean, not
      // just a number, so each one is routed on its own type — writing them
      // all as numbers would put "true" into the numeric column.
      const { leaves } = flattenObjectValue(path, row.value as object);
      let any = false;
      for (const leaf of leaves) {
        if (typeof leaf.value === "number") {
          writer.writeAtNanos(
            leaf.path,
            context,
            leaf.value,
            row.tsNanos,
            source,
          );
        } else if (typeof leaf.value === "boolean") {
          writer.writeStringAtNanos(
            leaf.path,
            context,
            String(leaf.value),
            row.tsNanos,
            "boolean",
            source,
          );
        } else {
          writer.writeStringAtNanos(
            leaf.path,
            context,
            leaf.value,
            row.tsNanos,
            undefined,
            source,
          );
        }
        any = true;
      }
      return any;
    }
    default:
      return false;
  }
}

/**
 * Read one time window of a measurement from InfluxDB 2.x.
 *
 * `navigation.position` is stored by the Signal K plugins as two fields on one
 * measurement, so the rows are pivoted back into a single object value —
 * otherwise latitude and longitude would land as two meaningless numeric paths
 * instead of a position.
 */
async function readWindowFlux(
  req: MigrationRequest,
  measurement: string,
  startMs: number,
  endMs: number,
  headers: Record<string, string>,
  fetchImpl: typeof fetch,
): Promise<{ rows: SourceRow[]; dropped: number }> {
  const flux = `from(bucket: ${JSON.stringify(req.bucket)})
  |> range(start: ${new Date(startMs).toISOString()}, stop: ${new Date(endMs).toISOString()})
  |> filter(fn: (r) => r._measurement == ${JSON.stringify(measurement)})
  |> keep(columns: ["_time", "_field", "_value"])`;
  const records = await runFlux(req, flux, headers, fetchImpl);
  const rows: SourceRow[] = [];
  // A record with no usable timestamp cannot be placed in time, so it is not
  // importable — but it IS reported. The module's contract is that nothing is
  // dropped silently, and a `read` total that quietly excludes these makes an
  // import look smaller than the source rather than showing what was lost.
  let unusable = 0;
  for (const rec of records) {
    const t = rec.values["_time"];
    if (!t) {
      unusable++;
      continue;
    }
    const tsNanos = rfc3339ToNanos(t);
    if (tsNanos === null) {
      unusable++;
      continue;
    }
    rows.push({
      tsNanos,
      field: rec.values["_field"] ?? "value",
      // The declared type decides: a genuine string "3.5" must stay a string
      // rather than being parsed into the numeric table.
      value: coerceValue(rec.values["_value"] ?? "", rec.types["_value"]),
    });
  }
  const merged = mergePositionRows(measurement, rows);
  return { rows: merged.rows, dropped: merged.dropped + unusable };
}

/** Read one time window of a measurement from InfluxDB 1.x. */
async function readWindowInfluxQl(
  req: MigrationRequest,
  measurement: string,
  startMs: number,
  endMs: number,
  headers: Record<string, string>,
  fetchImpl: typeof fetch,
): Promise<{ rows: SourceRow[]; dropped: number }> {
  // Measurement names come from SHOW MEASUREMENTS on this same server, but
  // they still land inside a query string — quote them as identifiers.
  //
  // Backslashes are escaped FIRST, then quotes. The other order is a bypass:
  // escaping only quotes leaves a name ending in `\` turning its own
  // backslash into the escape for the closing delimiter, so the identifier
  // ends early and the rest of the name becomes query text.
  const quoted = `"${measurement.replace(/\\/g, "\\\\").replace(/"/g, '\\"')}"`;
  // `*::field`, not `*`. A bare `SELECT *` returns TAG columns alongside the
  // fields — verified against 1.8.10 — and the reader treats every non-time
  // column as a field, so a measurement tagged `source=n2k` would import a
  // bogus `measurement.source = "n2k"` string path for every single point.
  // Signal K's own InfluxDB writers tag their points, so this is the common
  // case, not an exotic one.
  const q = `SELECT *::field FROM ${quoted} WHERE time >= ${BigInt(startMs) * NANOS_PER_MS} AND time < ${BigInt(endMs) * NANOS_PER_MS}`;
  const url = `${req.url}/query?db=${encodeURIComponent(req.bucket)}&epoch=ns&q=${encodeURIComponent(q)}`;
  const r = await fetchImpl(url, {
    headers,
    signal: AbortSignal.timeout(120_000),
  });
  if (!r.ok) throw new Error(await describeHttpError(r, "read data"));
  const body = (await r.json()) as InfluxQlResponse;
  if (body.results?.[0]?.error) throw new Error(body.results[0].error);
  const series = body.results?.[0]?.series ?? [];
  const rows: SourceRow[] = [];
  // See readWindowFlux: values that cannot be imported are reported, not
  // dropped in silence.
  let unusable = 0;
  for (const s of series) {
    const columns = s.columns ?? [];
    const timeIdx = columns.indexOf("time");
    if (timeIdx < 0) continue;
    for (const values of s.values ?? []) {
      // epoch=ns returns the time as a number; going through BigInt(String)
      // avoids the precision loss a float64 would suffer past 2^53 ns.
      const rawTime = values[timeIdx];
      let tsNanos: bigint;
      try {
        tsNanos = BigInt(String(rawTime));
      } catch {
        // No usable timestamp: reported rather than dropped in silence.
        unusable++;
        continue;
      }
      for (let i = 0; i < columns.length; i++) {
        if (i === timeIdx) continue;
        const raw = values[i];
        // A wide row has a column per field; nulls are fields absent from
        // this point, not values.
        if (raw === null || raw === undefined) continue;
        // The JSON type IS the declared type here — InfluxQL returns a string
        // field as a JSON string, a float as a JSON number and a boolean as a
        // JSON boolean (verified against 1.8.10). That is unlike the 2.x CSV
        // path, where every cell is text and `#datatype` is the only
        // discriminator, so coerceValue is needed there and actively harmful
        // here: re-parsing put a genuine string "3.5" into the NUMERIC table
        // and recorded a literal "true" as a boolean, in both cases making
        // imported values indistinguishable from real numbers and booleans.
        //
        // An empty string is still mapped to "no reading" rather than being
        // recorded as an empty value, matching coerceValue's own handling —
        // and counted, so the totals account for it.
        if (raw === "") {
          unusable++;
          continue;
        }
        rows.push({
          tsNanos,
          field: columns[i],
          value: raw,
        });
      }
    }
  }
  const merged = mergePositionRows(measurement, rows);
  return { rows: merged.rows, dropped: merged.dropped + unusable };
}

/**
 * Recombine latitude/longitude fields of a position measurement into one
 * object value at the same instant.
 *
 * The Signal K InfluxDB plugins store navigation.position as two fields
 * (`latitude`/`longitude`, or `lat`/`lon`). Left as-is they would import as
 * two numeric paths and the position history would be unusable — QuestDB keeps
 * positions in their own table.
 */
export function mergePositionRows(
  measurement: string,
  rows: SourceRow[],
): { rows: SourceRow[]; dropped: number } {
  if (!measurement.includes("position")) return { rows, dropped: 0 };
  const latKeys = new Set(["latitude", "lat"]);
  const lonKeys = new Set(["longitude", "lon", "lng"]);
  const byTime = new Map<string, { lat?: number; lon?: number }>();
  const passthrough: SourceRow[] = [];
  for (const row of rows) {
    const key = row.tsNanos.toString();
    if (latKeys.has(row.field) && typeof row.value === "number") {
      if (!byTime.has(key)) byTime.set(key, {});
      byTime.get(key)!.lat = row.value;
    } else if (lonKeys.has(row.field) && typeof row.value === "number") {
      if (!byTime.has(key)) byTime.set(key, {});
      byTime.get(key)!.lon = row.value;
    } else {
      passthrough.push(row);
    }
  }
  const merged: SourceRow[] = [];
  let dropped = 0;
  for (const [key, { lat, lon }] of byTime) {
    // A half-pair (lat with no lon at the same instant) is not a position.
    // Emitting it as a bare number would be worse than skipping it, so it is
    // dropped — and REPORTED, so it lands in the run's skipped total instead
    // of vanishing between the read count and the written count.
    if (lat === undefined || lon === undefined) {
      dropped++;
      continue;
    }
    merged.push({
      tsNanos: BigInt(key),
      field: "value",
      value: { latitude: lat, longitude: lon },
    });
  }
  return { rows: [...passthrough, ...merged], dropped };
}
