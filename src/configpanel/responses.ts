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

import type { MigrationSource } from "../api-contract.js";

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
