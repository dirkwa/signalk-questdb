// Normalizing the plugin's own API responses before they reach React state.
//
// Every fetch in the panel casts `res.json()` to a contract type. A cast is
// an assertion, not a check: the plugin's own handlers are well behaved, but
// a reverse proxy, auth gateway or captive portal can answer 200 with a body
// of an entirely different shape. Where the panel then dereferences the
// result during render — `versions.filter(...)` runs with no error boundary
// above it — that throws mid-render and blanks the whole config panel.
//
// These helpers keep the "trust but verify" boundary in one testable place.

import type {
  MigrationBucket,
  MigrationMeasurement,
  MigrationSource,
  MigrationStatusResponse,
} from "../api-contract.js";

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null;

/**
 * GET /api/migration/detect returns `{ sources: [...] }`. A body without it
 * means "nothing detected", which is also what an empty array means — so the
 * user gets the honest "no InfluxDB found" instead of a TypeError surfaced
 * as "Detection failed".
 */
export function toMigrationSources(body: unknown): MigrationSource[] {
  const sources = isRecord(body) ? body.sources : undefined;
  if (!Array.isArray(sources)) return [];
  return sources.filter(
    (s): s is MigrationSource =>
      isRecord(s) &&
      typeof s.type === "string" &&
      typeof s.url === "string" &&
      // Every field below is rendered as a React child. A plain object
      // there throws "Objects are not valid as a React child" and takes
      // the panel down — the same blank-screen failure as an unguarded
      // array, just reached from the detected-sources list.
      typeof s.status === "string" &&
      (s.version === undefined || typeof s.version === "string"),
  );
}

/**
 * POST /api/migration/buckets. Same trust boundary as the sources list: every
 * name below is rendered as a React child and used as a query parameter.
 */
export function toMigrationBuckets(body: unknown): MigrationBucket[] {
  const buckets = isRecord(body) ? body.buckets : undefined;
  if (!Array.isArray(buckets)) return [];
  return buckets.filter(
    (b): b is MigrationBucket =>
      isRecord(b) &&
      typeof b.name === "string" &&
      (b.id === undefined || typeof b.id === "string") &&
      (b.retentionSeconds === undefined ||
        typeof b.retentionSeconds === "number"),
  );
}

/** POST /api/migration/measurements. */
export function toMigrationMeasurements(body: unknown): MigrationMeasurement[] {
  const list = isRecord(body) ? body.measurements : undefined;
  if (!Array.isArray(list)) return [];
  return list.filter(
    (m): m is MigrationMeasurement =>
      isRecord(m) &&
      typeof m.name === "string" &&
      // `fields` is declared required and is mapped over by consumers, so a
      // missing one must not be narrowed to MigrationMeasurement — that would
      // hand `undefined` to a `.map()` that tsc believes is safe.
      Array.isArray(m.fields),
  );
}

/**
 * GET /api/migration/status and POST /api/migration/start.
 *
 * The progress counters drive a percentage and are rendered directly, so a
 * missing or non-numeric counter has to become 0 rather than reaching the
 * render as undefined and printing "NaN%".
 */
export function toMigrationStatus(
  body: unknown,
): MigrationStatusResponse["run"] | undefined {
  const run = isRecord(body) ? body.run : undefined;
  if (!isRecord(run)) return undefined;
  if (typeof run.id !== "string" || typeof run.state !== "string")
    return undefined;
  const state = run.state;
  if (
    state !== "running" &&
    state !== "done" &&
    state !== "failed" &&
    state !== "cancelled"
  )
    return undefined;
  const p = isRecord(run.progress) ? run.progress : {};
  const num = (v: unknown): number =>
    typeof v === "number" && Number.isFinite(v) ? v : 0;
  const str = (v: unknown): string | undefined =>
    typeof v === "string" ? v : undefined;
  return {
    id: run.id,
    state,
    url: str(run.url) ?? "",
    bucket: str(run.bucket) ?? "",
    startedAt: str(run.startedAt) ?? "",
    finishedAt: str(run.finishedAt),
    error: str(run.error),
    progress: {
      read: num(p.read),
      written: num(p.written),
      skipped: num(p.skipped),
      measurementsDone: num(p.measurementsDone),
      measurementsTotal: num(p.measurementsTotal),
      currentMeasurement: str(p.currentMeasurement),
      currentWindowStart: str(p.currentWindowStart),
    },
  };
}

/**
 * Validate the two `datetime-local` values an import range is entered as.
 *
 * A datetime-local input reads back "" while it is incomplete, and
 * `new Date("")` is an Invalid Date whose `.toISOString()` THROWS. Calling it
 * straight from the click handler would kill the handler before any request
 * went out, leaving the button stuck with no explanation — so the conversion
 * happens here, where the failure is a value rather than an exception.
 *
 * Returns the ISO strings to send, or the reason the range is unusable.
 */
export function toMigrationRange(
  from: string,
  to: string,
): { from: string; to: string } | { error: string } {
  const fromDate = new Date(from);
  const toDate = new Date(to);
  if (Number.isNaN(fromDate.getTime()) || Number.isNaN(toDate.getTime())) {
    return { error: "Enter a valid From and To date for the import." };
  }
  if (toDate <= fromDate) {
    return { error: "The To date must be after the From date." };
  }
  return { from: fromDate.toISOString(), to: toDate.toISOString() };
}
