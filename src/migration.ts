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
// Reads are windowed by time, and each window is streamed: read a batch, write
// it, release it, read the next. A boat's InfluxDB is routinely larger than the
// Pi's RAM, and a single dense day of one path can be too — so nothing here
// holds a whole response, and reading stops while QuestDB is catching up. A
// window that returns nothing still advances, so a multi-year gap costs one
// empty query per window rather than a stall.

import type { ILPWriter } from "./ilp-writer.js";
import { routeDeltaValue, flattenObjectValue } from "./delta-routing.js";
import {
  CheckpointTracker,
  migrationIdentity,
  sameIdentity,
} from "./migration-checkpoint.js";
import type {
  CheckpointStore,
  MigrationCheckpoint,
} from "./migration-checkpoint.js";
import type {
  MigrationBucket,
  MigrationMeasurement,
  MigrationProgress,
  MigrationResumePoint,
  MigrationRunState,
} from "./api-contract.js";

/** Nanoseconds per millisecond, the unit gap between `Date` and ILP. */
const NANOS_PER_MS = 1_000_000n;

/**
 * How much time one query covers. Large enough that a sparse measurement does
 * not need thousands of round trips, small enough that one query stays cheap
 * for InfluxDB and progress moves in visible steps. It does not bound memory:
 * a window is streamed in batches, however many points it holds.
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
  /** Request-level failure, as opposed to a statement's own `error`. */
  error?: string;
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
    // `schema.measurements()` reads InfluxDB's SCHEMA, not its points.
    //
    // The obvious-looking alternative — from(bucket) |> range(start: 0) |>
    // keep([_measurement, _field]) |> distinct — returns the same names but
    // gets them by READING EVERY POINT IN THE BUCKET first. `keep` and
    // `distinct` shrink the RESULT, not the scan. Measured on 2.9.1: 0.15s
    // at 1.2M points, 0.50s at 4.8M — linear in point count — while
    // schema.measurements() stayed at ~8ms for both. On a year of boat data
    // (a user reported ~40 GB on a Pi 5) the scan blew the query timeout, so
    // the import failed at discovery having read nothing: "0 written,
    // 0 skipped, 0/0 measurements".
    //
    // Field keys are deliberately NOT fetched. Nothing consumes them: the
    // import maps this to `.name`, and the panel renders only names. Pairing
    // fields to measurements needs either the full scan above or one request
    // per measurement, and neither is worth paying for a value no one reads.
    // `start` is pinned to the epoch rather than left to the default.
    // schema.measurements() takes a time range, and its default is a
    // documented-as-changeable window (historically -30d in the Flux
    // stdlib). A 2.9.1 instance returns everything either way — verified
    // with a measurement 200 days old — but relying on that would mean an
    // import silently skipping a boat's older history on some other
    // version, which is precisely the class of failure this commit fixes.
    const flux = `import "influxdata/influxdb/schema"
schema.measurements(bucket: ${JSON.stringify(req.bucket)}, start: 1970-01-01T00:00:00Z)`;
    const rows = await runFlux(req, flux, headers, fetchImpl);
    return rows
      .map((r) => r.values["_value"])
      .filter((v): v is string => !!v)
      .map((name) => ({ name, fields: [] }));
  }

  // SHOW FIELD KEYS returns one series PER MEASUREMENT, each named after it,
  // so a single request yields both the names and their field keys. Unlike
  // Flux's schema functions this is metadata, not a point scan — measured at
  // 0.024s over 800k points — so 1.x gets the pairing for free and keeps it.
  // (The 2.x branch above deliberately returns an empty `fields`; see there.)
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
  const r = await fetchImpl(fluxQueryUrl(req), {
    ...fluxQueryInit(flux, headers),
    signal: AbortSignal.timeout(timeoutMs),
  });
  if (!r.ok) throw new Error(await describeHttpError(r, "run query"));
  return parseAnnotatedCsv(await r.text());
}

function fluxQueryUrl(req: { url: string; auth?: InfluxAuth }): string {
  const org = req.auth?.org ?? "";
  return `${req.url}/api/v2/query${org ? `?org=${encodeURIComponent(org)}` : ""}`;
}

/**
 * A Flux query as a request.
 *
 * Sent as JSON, not as a raw `application/vnd.flux` body, because only the JSON
 * form can carry a dialect — and without one InfluxDB answers with bare CSV, no
 * annotation lines at all (verified against a live 2.7.12). The reader needs
 * two of them: `#datatype`, without which a string field holding "3.5" is
 * indistinguishable from the number and lands in the numeric table, and
 * `#group`, which is how a pivoted record's tags are told from its fields.
 */
function fluxQueryInit(
  flux: string,
  headers: Record<string, string>,
): RequestInit {
  return {
    method: "POST",
    headers: {
      ...headers,
      "Content-Type": "application/json",
      Accept: "text/csv",
    },
    body: JSON.stringify({
      query: flux,
      type: "flux",
      dialect: { header: true, annotations: ["datatype", "group", "default"] },
    }),
  };
}

/**
 * How long InfluxDB may stay silent while a read is outstanding.
 *
 * Not a limit on the whole request: a streamed window is read only as fast as
 * QuestDB accepts it, so a dense one legitimately stays open for many minutes
 * while the writer drains. The clock runs only while this side is waiting for
 * bytes it has asked for.
 */
const READ_IDLE_TIMEOUT_MS = 120_000;

/**
 * Run a request and yield its response body one physical line at a time.
 *
 * The body is never held whole — a dense day of one measurement runs to
 * hundreds of megabytes of CSV, and a heap exhausted by it cannot be caught:
 * it ends the Signal K process. Reading is demand-driven, so a consumer that
 * pauses (the import, waiting for the writer to drain) stops the reads, and
 * TCP flow control carries that back to InfluxDB.
 *
 * Stopping early — cancelling a run — aborts the request rather than leaving
 * InfluxDB streaming into a socket nobody reads.
 */
async function* streamLines(
  fetchImpl: typeof fetch,
  url: string,
  init: RequestInit,
  what: string,
  idleTimeoutMs: number,
): AsyncGenerator<string> {
  const controller = new AbortController();
  // Rejects on its own clock rather than waiting for the abort to surface
  // through the pending step: the abort is what frees the connection, but the
  // run must fail on time even if the step never notices it.
  const guarded = <T>(step: () => Promise<T>): Promise<T> =>
    new Promise<T>((resolve, reject) => {
      const timer = setTimeout(() => {
        controller.abort();
        reject(
          new Error(
            `Cannot ${what}: InfluxDB sent no data for ${Math.round(idleTimeoutMs / 1000)}s`,
          ),
        );
      }, idleTimeoutMs);
      step().then(
        (value) => {
          clearTimeout(timer);
          resolve(value);
        },
        (err: unknown) => {
          clearTimeout(timer);
          reject(err instanceof Error ? err : new Error(String(err)));
        },
      );
    });

  const r = await guarded(() =>
    fetchImpl(url, { ...init, signal: controller.signal }),
  );
  if (!r.ok) throw new Error(await guarded(() => describeHttpError(r, what)));
  if (!r.body) return;

  const reader = r.body.getReader();
  const decoder = new TextDecoder();
  let carry = "";
  try {
    for (;;) {
      const { done, value } = await guarded(() => reader.read());
      if (done) break;
      carry += decoder.decode(value, { stream: true });
      let from = 0;
      for (
        let nl = carry.indexOf("\n");
        nl >= 0;
        nl = carry.indexOf("\n", from)
      ) {
        yield carry.slice(from, nl);
        from = nl + 1;
      }
      carry = carry.slice(from);
    }
    carry += decoder.decode();
    if (carry !== "") yield carry;
  } finally {
    controller.abort();
    await reader.cancel().catch(() => {});
  }
}

/**
 * Incremental annotated-CSV reader for Flux responses.
 *
 * Fed one physical line at a time, so a response is never held whole: the
 * caller streams the body through it and sees each record as it completes.
 *
 * Flux emits one or more *tables*, each preceded by annotation lines starting
 * with `#` and then a header row. A response can therefore change its column
 * layout partway through, which is why the header is tracked per block rather
 * than read once at the top.
 */
export class AnnotatedCsvReader {
  private header: string[] | null = null;
  // Per-block `#datatype` line. Without it a genuine STRING value of "3.5"
  // is indistinguishable from the number 3.5 in the CSV body — verified
  // against a live 2.9.1 — and would be imported into the numeric table.
  private datatypes: string[] | null = null;
  private groups: string[] | null = null;
  // Built once per table and shared by its records: a table can run to
  // millions of rows, and a copy per record was most of what a window cost.
  private types: Record<string, string> = {};
  private grouped: ReadonlySet<string> = new Set();
  private pending: string | null = null;

  /**
   * Feed one physical line, without its `\n`. Returns the record it
   * completes, or null for annotations, headers, blanks and partial records.
   *
   * A quoted CSV value may contain a newline — verified against a live 2.9.1,
   * which returns `"line1\r\nline2"` for a string value holding one. Splitting
   * on newlines alone would tear that record in half and produce two garbage
   * rows, so physical lines are joined until the quotes balance ("" inside a
   * quoted value is an escaped quote and does not change the balance).
   * \r is stripped per PHYSICAL line before joining: a quoted value spanning
   * two CRLF lines would otherwise keep the \r of the first inside the joined
   * value, so the imported string silently carries a stray carriage return.
   */
  push(physicalLine: string): AnnotatedRecord | null {
    const stripped = physicalLine.endsWith("\r")
      ? physicalLine.slice(0, -1)
      : physicalLine;
    const line =
      this.pending === null ? stripped : `${this.pending}\n${stripped}`;
    if (!quotesBalanced(line)) {
      this.pending = line;
      return null;
    }
    this.pending = null;
    return this.logicalLine(line);
  }

  /** Call once after the last line. */
  end(): AnnotatedRecord | null {
    if (this.pending === null) return null;
    // Unterminated quote at EOF: emit what there is rather than dropping it.
    const line = this.pending;
    this.pending = null;
    return this.logicalLine(line);
  }

  private logicalLine(line: string): AnnotatedRecord | null {
    if (line === "") {
      // Blank line separates tables; the next non-# line is a fresh header.
      this.header = null;
      this.datatypes = null;
      this.groups = null;
      return null;
    }
    if (line.startsWith("#")) {
      // Annotation (#datatype/#group/#default). A new annotation block means
      // the previous header no longer applies.
      if (line.startsWith("#datatype")) this.datatypes = splitCsvLine(line);
      else if (line.startsWith("#group")) this.groups = splitCsvLine(line);
      this.header = null;
      return null;
    }
    const cells = splitCsvLine(line);
    if (!this.header) {
      this.header = cells;
      const types: Record<string, string> = {};
      const grouped = new Set<string>();
      for (let i = 0; i < cells.length; i++) {
        const key = cells[i];
        if (key === "") continue;
        // Annotations are positionally aligned with the header row.
        if (this.datatypes?.[i]) types[key] = this.datatypes[i];
        if (this.groups?.[i] === "true") grouped.add(key);
      }
      this.types = types;
      this.grouped = grouped;
      return null;
    }
    const values: Record<string, string> = {};
    for (let i = 0; i < this.header.length; i++) {
      const key = this.header[i];
      // Flux's leading empty column is the annotation gutter, not data.
      if (key === "") continue;
      values[key] = cells[i] ?? "";
    }
    return { values, types: this.types, grouped: this.grouped };
  }
}

/** Parse a whole annotated-CSV response. For small results only. */
export function parseAnnotatedCsv(text: string): AnnotatedRecord[] {
  const reader = new AnnotatedCsvReader();
  const out: AnnotatedRecord[] = [];
  for (const line of text.split("\n")) {
    const rec = reader.push(line);
    if (rec) out.push(rec);
  }
  const last = reader.end();
  if (last) out.push(last);
  return out;
}

function quotesBalanced(s: string): boolean {
  if (!s.includes('"')) return true;
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

/**
 * One Flux CSV record, plus what the table's annotations say about its columns.
 * `types` and `grouped` belong to the table and are shared by all its records.
 */
export interface AnnotatedRecord {
  values: Record<string, string>;
  /** `#datatype` per column. */
  types: Readonly<Record<string, string>>;
  /** Columns in the table's group key (`#group` true): tags, never fields. */
  grouped: ReadonlySet<string>;
}

function splitCsvLine(line: string): string[] {
  if (!line.includes('"')) return line.split(",");
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
 * The field signalk-to-influxdb 1.x uses for a value that is neither a number,
 * a string nor a boolean: the value itself, JSON-encoded. `navigation.position`
 * is always stored this way; `lat`/`lon` fields exist only when that plugin's
 * `separateLatLon` option is on, which it is not by default.
 */
const JSON_VALUE_FIELD = "jsonValue";

/**
 * Field names that hold the value of the measurement's own path.
 *
 * signalk-to-influxdb2 writes every value to `value`. signalk-to-influxdb 1.x
 * picks the field by the value's type — `value` for numbers, then
 * `stringValue`, `boolValue` and `jsonValue` — because an InfluxDB 1.x field
 * cannot change type once it has been written.
 */
const PATH_VALUE_FIELDS: ReadonlySet<string> = new Set([
  "value",
  "",
  "_value",
  "stringValue",
  "boolValue",
  JSON_VALUE_FIELD,
]);

/**
 * The Signal K path a measurement+field pair represents.
 *
 * The Signal K InfluxDB plugins write the path as the measurement name and the
 * value in one of PATH_VALUE_FIELDS. Anything else (a measurement with several
 * named fields) is a non-Signal K schema, so the field name is appended to keep
 * the two apart instead of overwriting one with the other.
 */
export function toSignalKPath(measurement: string, field: string): string {
  if (PATH_VALUE_FIELDS.has(field)) return measurement;
  return `${measurement}.${field}`;
}

/**
 * Decode `jsonValue` fields back into the values they encode.
 *
 * Left as the string it arrives as, a position would be recorded in
 * `signalk_str` as text and `signalk_position` would stay empty. Decoded, it
 * routes exactly like the live value did: a position to the position table,
 * any other object to its scalar leaves.
 *
 * A value that does not parse cannot be represented, so it is dropped — and
 * reported, so it lands in the run's skipped total.
 */
export function decodeJsonValueRows(rows: SourceRow[]): {
  rows: SourceRow[];
  dropped: number;
} {
  if (!rows.some((row) => row.field === JSON_VALUE_FIELD))
    return { rows, dropped: 0 };
  let dropped = 0;
  const decoded: SourceRow[] = [];
  for (const row of rows) {
    if (row.field !== JSON_VALUE_FIELD || typeof row.value !== "string") {
      decoded.push(row);
      continue;
    }
    try {
      decoded.push({ ...row, value: JSON.parse(row.value) as unknown });
    } catch {
      dropped++;
    }
  }
  return { rows: decoded, dropped };
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
  resumedFrom?: MigrationResumePoint;
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
  resumedFrom?: MigrationResumePoint;
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
  > & {
    readonly droppedLineCount?: number;
    readonly enqueuedLineCount?: number;
    readonly settledLineCount?: number;
  },
  run: MigrationRun,
  deps: {
    fetchImpl?: typeof fetch;
    debug?: (msg: string) => void;
    /** Injected so tests don't wait on real backpressure sleeps. */
    sleep?: (ms: number) => Promise<void>;
    /** Injected so tests don't wait out a real stalled read. */
    readIdleTimeoutMs?: number;
    /** Where the import's position is kept, so a later run can resume it. */
    checkpoints?: CheckpointStore;
    /**
     * A checkpoint to continue from. Only honoured if it belongs to this same
     * import — anything else is ignored and the run starts from the beginning.
     */
    resumeFrom?: MigrationCheckpoint;
    /** Injected so tests don't wait out the real checkpoint lag. */
    checkpointLagMs?: number;
    now?: () => number;
  } = {},
): Promise<void> {
  const fetchImpl = deps.fetchImpl ?? fetch;
  const debug = deps.debug ?? (() => {});
  const sleep =
    deps.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const headers = authHeaders(req.type, req.auth);
  const readIdleTimeoutMs = deps.readIdleTimeoutMs ?? READ_IDLE_TIMEOUT_MS;
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

    // Before the first await, so a caller that started this run without
    // awaiting it — the HTTP handler — already sees it as resumed, with the
    // earlier totals, in the response it sends straight away.
    const now = deps.now ?? Date.now;
    const identity = migrationIdentity(req, windowMs);
    const resume =
      deps.resumeFrom && sameIdentity(deps.resumeFrom.identity, identity)
        ? deps.resumeFrom
        : undefined;
    const done = new Set(resume?.done ?? []);
    if (resume) {
      run.progress.read = resume.progress.read;
      run.progress.written = resume.progress.written;
      run.progress.skipped = resume.progress.skipped;
      run.resumedFrom = {
        measurement: resume.current?.measurement,
        windowStart:
          resume.current && new Date(resume.current.windowStart).toISOString(),
      };
    }

    let measurements = req.measurements ?? [];
    if (measurements.length === 0) {
      measurements = (await listMeasurements(req, fetchImpl)).map(
        (m) => m.name,
      );
      // Nothing to import is worth SAYING. A run that ends "done, 0 written,
      // 0/0 measurements" is visually identical to the discovery failure this
      // module was just fixed for, so an operator who picked the wrong bucket
      // — or whose token cannot see its contents — would read a silent
      // success and have no idea why nothing arrived.
      if (measurements.length === 0) {
        throw new Error(
          `No measurements found in "${req.bucket}". The bucket may be empty, ` +
            `or the token may not have read access to it.`,
        );
      }
    }
    run.progress.measurementsTotal = measurements.length;

    const tracker = deps.checkpoints
      ? new CheckpointTracker(
          deps.checkpoints,
          writer,
          deps.checkpointLagMs,
          now,
        )
      : undefined;
    // A checkpoint that cannot be written costs a later resume, nothing more.
    // It must not cost the import that is running.
    const offerCheckpoint = async (
      current?: MigrationCheckpoint["current"],
    ): Promise<void> => {
      try {
        await tracker?.offer(() => ({
          version: 1,
          identity,
          done: [...done],
          current,
          progress: {
            read: run.progress.read,
            written: run.progress.written,
            skipped: run.progress.skipped,
          },
          updatedAt: new Date(now()).toISOString(),
        }));
      } catch (err) {
        debug(
          `migration: checkpoint not written: ${err instanceof Error ? err.message : String(err)}`,
        );
      }
    };

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
      if (done.has(measurement)) {
        run.progress.measurementsDone++;
        continue;
      }
      run.progress.currentMeasurement = measurement;

      // Only a start this run would itself arrive at: anything else in the
      // file is not a position of this import, and the measurement is read
      // from its beginning instead.
      let firstStart = fromMs;
      const resumeAt =
        resume?.current?.measurement === measurement
          ? resume.current.windowStart
          : undefined;
      if (
        resumeAt !== undefined &&
        resumeAt > fromMs &&
        resumeAt < toMs &&
        (resumeAt - fromMs) % windowMs === 0
      ) {
        firstStart = resumeAt;
      }

      for (let start = firstStart; start < toMs; start += windowMs) {
        if (run.isCancelled) break;
        const end = Math.min(start + windowMs, toMs);
        run.progress.currentWindowStart = new Date(start).toISOString();

        // Backpressure: let the writer drain before reading more.
        await awaitDrain();

        const batches =
          req.type === "influxdb2"
            ? readWindowFlux(
                req,
                measurement,
                start,
                end,
                headers,
                fetchImpl,
                readIdleTimeoutMs,
              )
            : readWindowInfluxQl(
                req,
                measurement,
                start,
                end,
                headers,
                fetchImpl,
                readIdleTimeoutMs,
              );

        // The window is read a batch at a time and each batch is written
        // before the next is asked for, so what is held never grows with the
        // window. Leaving the loop early — a cancel — ends the read as well.
        let sinceDrainCheck = 0;
        for await (const { rows, dropped } of batches) {
          if (run.isCancelled) break;

          // Half-pair positions never become rows, so they are counted here or
          // not at all — otherwise read - written - skipped silently disagrees.
          run.progress.read += dropped;
          run.progress.skipped += dropped;

          // Checked inside the row loop too, not only between batches: waiting
          // only at a boundary would let the buffer sail past
          // MAX_BUFFER_LINES mid-batch — dropping its OLDEST lines, which is
          // exactly the silent data loss this guard exists to prevent.
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

        // A window left early is not an imported window.
        if (run.isCancelled) break;
        if (start + windowMs < toMs) {
          await offerCheckpoint({
            measurement,
            windowStart: start + windowMs,
          });
        }
      }
      if (run.isCancelled) break;

      done.add(measurement);
      run.progress.measurementsDone++;
      await offerCheckpoint();
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
    // Nothing left to resume. Kept on a cancel or a failure, which is exactly
    // when a later run wants it.
    if (run.state === "done") {
      await deps.checkpoints?.clear().catch((err: unknown) => {
        debug(
          `migration: finished checkpoint not removed: ${err instanceof Error ? err.message : String(err)}`,
        );
      });
    }
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

/** Rows read from one stretch of a window, ready to be written. */
interface WindowBatch {
  rows: SourceRow[];
  /** Points that could not become rows; counted as read and skipped. */
  dropped: number;
}

/**
 * How many Flux records go into one batch. Bounds what a window holds at once:
 * a batch is parsed, written and released before the next is read.
 */
const FLUX_BATCH_RECORDS = 5_000;

/**
 * Whether a measurement may hold a position split across latitude/longitude
 * fields — the measurements mergePositionRows reassembles.
 */
function isPositionMeasurement(measurement: string): boolean {
  return measurement.includes("position");
}

/**
 * Turn the rows gathered so far into a batch.
 *
 * Decoding and position reassembly work within the batch, so a batch must
 * hold WHOLE points: every field of an instant together. Both readers
 * guarantee that — see each for how.
 */
function finishBatch(
  measurement: string,
  rows: SourceRow[],
  unusable: number,
): WindowBatch {
  const decoded = decodeJsonValueRows(rows);
  const merged = mergePositionRows(measurement, decoded.rows);
  return {
    rows: merged.rows,
    dropped: merged.dropped + decoded.dropped + unusable,
  };
}

/**
 * Read one time window of a measurement from InfluxDB 2.x, in batches.
 *
 * Flux returns one table per field, each running the length of the window. For
 * most measurements that is fine — every record is a complete value. A
 * position is not: its latitude and longitude arrive as two separate tables,
 * the second starting only after the first has ended, so pairing them from the
 * stream would mean holding a whole table back. Position measurements are
 * therefore pivoted by InfluxDB, which puts every field of an instant on one
 * record and lets a position be assembled record by record.
 */
async function* readWindowFlux(
  req: MigrationRequest,
  measurement: string,
  startMs: number,
  endMs: number,
  headers: Record<string, string>,
  fetchImpl: typeof fetch,
  idleTimeoutMs: number,
): AsyncGenerator<WindowBatch> {
  const pivoted = isPositionMeasurement(measurement);
  const source = `from(bucket: ${JSON.stringify(req.bucket)})
  |> range(start: ${new Date(startMs).toISOString()}, stop: ${new Date(endMs).toISOString()})
  |> filter(fn: (r) => r._measurement == ${JSON.stringify(measurement)})`;
  const flux = pivoted
    ? `${source}
  |> pivot(rowKey: ["_time"], columnKey: ["_field"], valueColumn: "_value")
  |> drop(columns: ["_start", "_stop", "_measurement"])`
    : `${source}
  |> keep(columns: ["_time", "_field", "_value"])`;
  const csv = new AnnotatedCsvReader();
  let rows: SourceRow[] = [];
  let records = 0;
  // A record with no usable timestamp cannot be placed in time, so it is not
  // importable — but it IS reported. The module's contract is that nothing is
  // dropped silently, and a `read` total that quietly excludes these makes an
  // import look smaller than the source rather than showing what was lost.
  let unusable = 0;
  const take = (rec: AnnotatedRecord): void => {
    records++;
    const t = rec.values["_time"];
    const tsNanos = t ? rfc3339ToNanos(t) : null;
    if (tsNanos === null) {
      unusable++;
      return;
    }
    if (pivoted) {
      pushPivotedFields(rec, tsNanos, rows);
      return;
    }
    rows.push({
      tsNanos,
      field: rec.values["_field"] ?? "value",
      // The declared type decides: a genuine string "3.5" must stay a string
      // rather than being parsed into the numeric table.
      value: coerceValue(rec.values["_value"] ?? "", rec.types["_value"]),
    });
  };

  const lines = streamLines(
    fetchImpl,
    fluxQueryUrl(req),
    fluxQueryInit(flux, headers),
    "read data",
    idleTimeoutMs,
  );
  for await (const line of lines) {
    const rec = csv.push(line);
    if (!rec) continue;
    take(rec);
    if (records >= FLUX_BATCH_RECORDS) {
      yield finishBatch(measurement, rows, unusable);
      rows = [];
      records = 0;
      unusable = 0;
    }
  }
  const last = csv.end();
  if (last) take(last);
  if (records > 0) yield finishBatch(measurement, rows, unusable);
}

/** Columns of a pivoted record that are bookkeeping rather than fields. */
const PIVOT_NON_FIELD_COLUMNS: ReadonlySet<string> = new Set([
  "result",
  "table",
  "_time",
]);

/**
 * The fields of one pivoted record, as rows at its instant.
 *
 * A pivoted record carries the series' tags as columns too. They are what the
 * table is grouped by, which is how they are told from fields: reading them as
 * fields would import a bogus `measurement.source` path for every point.
 *
 * An empty cell is a field this point does not have, not a value.
 */
function pushPivotedFields(
  rec: AnnotatedRecord,
  tsNanos: bigint,
  out: SourceRow[],
): void {
  for (const column of Object.keys(rec.values)) {
    if (PIVOT_NON_FIELD_COLUMNS.has(column) || rec.grouped.has(column))
      continue;
    const raw = rec.values[column];
    if (raw === "") continue;
    out.push({
      tsNanos,
      field: column,
      value: coerceValue(raw, rec.types[column]),
    });
  }
}

/**
 * How many points InfluxDB 1.x puts in one chunk of a chunked response. Each
 * chunk becomes a batch, so this bounds what a window holds at once.
 */
const INFLUXQL_CHUNK_POINTS = 10_000;

/**
 * Read one time window of a measurement from InfluxDB 1.x, in batches.
 *
 * `chunked=true` makes InfluxDB answer with a series of complete JSON
 * documents, one per line, instead of one document for the whole window. A
 * 1.x row already carries every field of its point, so each chunk holds whole
 * points and can be finished on its own.
 */
async function* readWindowInfluxQl(
  req: MigrationRequest,
  measurement: string,
  startMs: number,
  endMs: number,
  headers: Record<string, string>,
  fetchImpl: typeof fetch,
  idleTimeoutMs: number,
): AsyncGenerator<WindowBatch> {
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
  const url = `${req.url}/query?db=${encodeURIComponent(req.bucket)}&epoch=ns&chunked=true&chunk_size=${INFLUXQL_CHUNK_POINTS}&q=${encodeURIComponent(q)}`;
  for await (const line of streamLines(
    fetchImpl,
    url,
    { headers },
    "read data",
    idleTimeoutMs,
  )) {
    if (line.trim() === "") continue;
    let body: InfluxQlResponse;
    try {
      body = JSON.parse(line) as InfluxQlResponse;
    } catch {
      throw new Error(
        "Cannot read data: InfluxDB returned a malformed response",
      );
    }
    if (body.error) throw new Error(body.error);
    if (body.results?.[0]?.error) throw new Error(body.results[0].error);
    const rows: SourceRow[] = [];
    const unusable = pushInfluxQlSeries(body.results?.[0]?.series ?? [], rows);
    if (rows.length > 0 || unusable > 0)
      yield finishBatch(measurement, rows, unusable);
  }
}

/**
 * The points of one InfluxQL result, as rows. Returns how many values could
 * not be imported — reported, like readWindowFlux's, not dropped in silence.
 */
function pushInfluxQlSeries(
  series: NonNullable<
    NonNullable<InfluxQlResponse["results"]>[number]["series"]
  >,
  rows: SourceRow[],
): number {
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
  return unusable;
}

/**
 * Recombine latitude/longitude fields of a position measurement into one
 * object value at the same instant.
 *
 * signalk-to-influxdb2 stores navigation.position as two fields (`lat`/`lon`),
 * as does signalk-to-influxdb 1.x with `separateLatLon` on. Left as-is they
 * would import as two numeric paths and the position history would be unusable
 * — QuestDB keeps positions in their own table. The 1.x default encoding, a
 * single `jsonValue`, is already whole by the time it gets here (see
 * decodeJsonValueRows).
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
  // Instants that already carry a whole position. signalk-to-influxdb 1.x with
  // `separateLatLon` on writes `jsonValue` AND `lat`/`lon` for the same fix, so
  // the pair is a second copy of a row that is already going to be written.
  const wholePositionTimes = new Set<string>();
  for (const row of rows) {
    const key = row.tsNanos.toString();
    // Routed on the mapped path, as writeRow does, so a row is only counted as
    // a whole position here if it is going to be written as one.
    if (
      routeDeltaValue(toSignalKPath(measurement, row.field), row.value) ===
      "position"
    ) {
      wholePositionTimes.add(key);
      passthrough.push(row);
    } else if (latKeys.has(row.field) && typeof row.value === "number") {
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
    if (wholePositionTimes.has(key)) continue;
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
