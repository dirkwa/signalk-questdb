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

import type { MigrationSource, QuestdbVersion } from "../api-contract.js";

const isRecord = (v: unknown): v is Record<string, unknown> =>
  typeof v === "object" && v !== null;

/**
 * GET /api/versions returns an array of releases. Anything else becomes an
 * empty list, and malformed members are dropped: the version dropdown is
 * then short or empty rather than the panel being gone.
 *
 * Members are validated, not just the array — `versions.filter(v =>
 * !v.prerelease)` runs during render, so a single null element is as fatal
 * as a non-array body.
 */
export function toVersionList(body: unknown): QuestdbVersion[] {
  if (!Array.isArray(body)) return [];
  return body.filter(
    (v): v is QuestdbVersion =>
      isRecord(v) &&
      typeof v.tag === "string" &&
      // Not merely cosmetic: the two filters below partition on this, so a
      // missing or non-boolean value would list a pre-release as stable —
      // and the stable list is what the version dropdown defaults to.
      typeof v.prerelease === "boolean",
  );
}

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
