// SQL WHERE-clause builder for /api/full-export/:table. Extracted from the
// route handler so we can unit-test the param-parsing without standing up
// the full plugin.

export type FullExportRangeResult =
  | { ok: true; where: string }
  | { ok: false; error: string };

/**
 * Build a SQL `WHERE ts >= … AND ts < …` clause from optional from/to query
 * params. Both must be set together; both must be ISO 8601 timestamps.
 * When neither is set, returns an empty `where` (full-table behavior).
 *
 * The interval is half-open `[from, to)` — natural for week-boundary
 * arithmetic (no row appears in two adjacent weeks).
 */
export function buildFullExportWhere(
  from: string | undefined,
  to: string | undefined,
): FullExportRangeResult {
  if (!from && !to) return { ok: true, where: "" };
  if ((from && !to) || (to && !from)) {
    return {
      ok: false,
      error: "from and to query params must be set together",
    };
  }
  const fromDate = new Date(from!);
  const toDate = new Date(to!);
  if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
    return { ok: false, error: "from and to must be ISO 8601 timestamps" };
  }
  if (fromDate.getTime() >= toDate.getTime()) {
    return { ok: false, error: "from must be strictly before to" };
  }
  return {
    ok: true,
    where: ` WHERE ts >= '${fromDate.toISOString()}' AND ts < '${toDate.toISOString()}'`,
  };
}
