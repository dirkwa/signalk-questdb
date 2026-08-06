import type { QueryClient } from "./query-client.js";

/**
 * Repopulate the Signal K data model from QuestDB at startup.
 *
 * The server's live model is in-memory only: `FullSignalK` starts with
 * `vessels: {}` and the server's delta cache starts empty, so after a restart
 * every AIS target is gone until that vessel transmits again — 30s for a
 * Class B, up to 3 minutes for a Class A at anchor, ~6 minutes before names
 * arrive. This replays the last known value per (context, path) so the chart
 * is populated at once instead of filling in over the next several minutes.
 *
 * The values are historical by definition. Nothing here dead-reckons them
 * forward, so `maxAgeMs` is the safety boundary: a target older than that is
 * not replayed at all. Consumers see ordinary deltas carrying their ORIGINAL
 * recorded timestamp (never `now`), which is what lets Freeboard's own
 * staleness logic age them out normally rather than treating a restored ghost
 * as a fresh contact.
 */

/**
 * Marks a delta as replayed history rather than a live observation. The
 * recorder filters on this: without it, restored values loop back through the
 * streambundle and get re-recorded with the current receive time, inventing
 * present-tense positions for vessels that may be long gone.
 */
export const RESTORE_SOURCE = "signalk-questdb.restore";

export interface RestoreDeps {
  queryClient: Pick<QueryClient, "exec" | "toObjects">;
  handleMessage: (delta: unknown) => void;
  selfContext: string;
  debug: (msg: string) => void;
  /**
   * True if this context has already been seen live since startup. The query
   * is slow enough that vessels transmit while it runs, and a stored fix
   * replayed over a live one would move that vessel backwards on the chart.
   * Keyed on the STORED context ("self" for own vessel), matching the rows.
   */
  hasLiveData?: (context: string) => boolean;
}

export interface RestoreOptions {
  /** Replay window. Values older than this are skipped entirely. */
  maxAgeMs: number;
  /** Restore own-vessel state. */
  restoreSelf: boolean;
  /** Restore AIS targets / other contexts. */
  restoreOthers: boolean;
  /** Clock injection point for tests. */
  now?: () => number;
}

export interface RestoreResult {
  contexts: number;
  values: number;
  skippedStale: number;
  /** Contexts skipped because the vessel transmitted during startup. */
  skippedLive: number;
}

/**
 * Paths worth restoring. This is deliberately a small allowlist rather than
 * "everything recorded": the goal is to put targets on the chart with enough
 * identity to be useful, not to reconstruct the entire data model. Replaying
 * every recorded path would push thousands of stale sensor readings (tank
 * levels, engine temps) into the model as though they were live, which is
 * both misleading and a needless burst of deltas at startup.
 *
 * Position is the reason this feature exists; the rest is what a chart plotter
 * needs to draw and label a contact.
 */
const RESTORE_PATHS = [
  "navigation.position",
  "navigation.courseOverGroundTrue",
  "navigation.speedOverGround",
  "navigation.headingTrue",
  "navigation.headingMagnetic",
  "navigation.state",
  "design.aisShipType",
  "design.length",
  "design.beam",
  "mmsi",
  "name",
] as const;

function sqlList(values: readonly string[]): string {
  return values.map((v) => `'${v.replace(/'/g, "''")}'`).join(", ");
}

/**
 * Last known value per (context, path) within the window.
 *
 * Mirrors getHistory()'s shape in history-v1.ts: LATEST ON is applied PER
 * TABLE and the results unioned, never over a materialized union — the latter
 * makes QuestDB scan instead of using each table's index, which timed out on
 * large installs. Position partitions by context alone because that table has
 * no path column.
 */
function snapshotSQL(sinceIso: string, withKind: boolean): string {
  const at = `ts >= '${sinceIso}'`;
  const paths = sqlList(RESTORE_PATHS);
  const kind = withKind ? `CAST(value_kind AS STRING)` : `CAST(NULL AS STRING)`;

  const num =
    `SELECT ts, path, context, CAST(value AS STRING) valuetext, ` +
    `'number' kind FROM signalk WHERE ${at} AND path IN (${paths})` +
    ` LATEST ON ts PARTITION BY path, context`;

  const str =
    `SELECT ts, path, context, value_str valuetext, ${kind} kind ` +
    `FROM signalk_str WHERE ${at} AND path IN (${paths})` +
    ` LATEST ON ts PARTITION BY path, context`;

  const pos =
    `SELECT ts, 'navigation.position' path, context, ` +
    `concat(CAST(lat AS STRING), ',', CAST(lon AS STRING)) valuetext, ` +
    `'position' kind FROM signalk_position WHERE ${at}` +
    ` LATEST ON ts PARTITION BY context`;

  return `(${num}) UNION ALL (${str}) UNION ALL (${pos})`;
}

// True for the error QuestDB returns when value_kind has not been added yet.
// A read racing an unmigrated table would otherwise fail the whole union,
// turning a missing type tag into no restore at all.
function isMissingKindColumn(err: unknown): boolean {
  return /Invalid column:\s*value_kind/i.test(
    err instanceof Error ? err.message : String(err),
  );
}

function decodeValue(row: Record<string, unknown>): unknown {
  const text = row.valuetext;
  if (text === null || text === undefined) return null;
  switch (row.kind) {
    case "number": {
      const n = Number(text);
      return Number.isFinite(n) ? n : null;
    }
    case "boolean":
      return String(text) === "true";
    case "position": {
      const [lat, lon] = String(text).split(",");
      const latitude = Number(lat);
      const longitude = Number(lon);
      return Number.isFinite(latitude) && Number.isFinite(longitude)
        ? { latitude, longitude }
        : null;
    }
    default:
      return text;
  }
}

export async function restoreFromHistory(
  deps: RestoreDeps,
  options: RestoreOptions,
): Promise<RestoreResult> {
  const { queryClient, handleMessage, selfContext, debug } = deps;
  const now = options.now ? options.now() : Date.now();
  const sinceIso = new Date(now - options.maxAgeMs).toISOString();

  const result = await queryClient
    .exec(snapshotSQL(sinceIso, true))
    .catch((err: unknown) => {
      if (!isMissingKindColumn(err)) throw err;
      return queryClient.exec(snapshotSQL(sinceIso, false));
    });

  const rows = queryClient.toObjects(result);

  // Group by context, keyed per path so a duplicate from another table cannot
  // win on arrival order, and tracking the newest timestamp seen so the
  // replayed delta carries a real recorded time rather than `now`.
  const byContext = new Map<
    string,
    {
      byPath: Map<string, { ts: string; value: unknown; isName: boolean }>;
      latestTs: string;
    }
  >();

  let skippedStale = 0;

  for (const row of rows) {
    const storedContext = (row.context as string) || "self";
    const isSelf = storedContext === "self";
    if (isSelf && !options.restoreSelf) continue;
    if (!isSelf && !options.restoreOthers) continue;

    const ts = row.ts as string;
    // The window is applied in SQL, but a row whose timestamp does not parse
    // cannot be aged out by consumers, so it is not safe to replay.
    const rowTime = Date.parse(ts);
    if (!Number.isFinite(rowTime) || now - rowTime > options.maxAgeMs) {
      skippedStale++;
      continue;
    }

    const value = decodeValue(row);
    if (value === null) continue;

    const path = row.path as string;
    const entry = byContext.get(storedContext) ?? {
      byPath: new Map<
        string,
        { ts: string; value: unknown; isName: boolean }
      >(),
      latestTs: ts,
    };

    // Vessel names are stored under the synthetic path "name" (tagged
    // "identity") but were received as empty-path object deltas, and only
    // that shape is read as a name. The kind gate keeps a data path literally
    // called "name" replaying as the plain string it is.
    const isName =
      path === "name" && row.kind === "identity" && typeof value === "string";

    // LATEST ON runs PER TABLE, so a path whose type changed over time (a
    // value recorded as a number, later as a string) yields one row from each
    // table. Union order is not timestamp order, so keep the newer row rather
    // than letting whichever arrives last win.
    const existing = entry.byPath.get(path);
    if (!existing || ts > existing.ts) {
      entry.byPath.set(path, { ts, value, isName });
    }

    if (ts > entry.latestTs) entry.latestTs = ts;
    byContext.set(storedContext, entry);
  }

  let contexts = 0;
  let values = 0;
  let skippedLive = 0;

  for (const [storedContext, entry] of byContext) {
    // A restored context is only useful if it can be drawn and only honest if
    // it can be aged out. An identity with no fix would be an undrawable
    // target that never expires.
    if (!entry.byPath.has("navigation.position")) continue;

    // A vessel that transmitted while the query ran is already current; its
    // stored fix is stale by comparison and replaying it would move the
    // target backwards.
    if (deps.hasLiveData?.(storedContext)) {
      skippedLive++;
      continue;
    }

    // One update per distinct recorded time. These paths were sampled at
    // different moments — an AIS name repeats every ~6 minutes while position
    // arrives every few seconds — so putting them all under the newest
    // timestamp would present a stale value as being as fresh as the fix.
    const byTimestamp = new Map<string, { path: string; value: unknown }[]>();
    for (const [path, row] of entry.byPath) {
      const group = byTimestamp.get(row.ts) ?? [];
      group.push(
        row.isName
          ? { path: "", value: { name: row.value as string } }
          : { path, value: row.value },
      );
      byTimestamp.set(row.ts, group);
    }

    // Stored "self" is a placeholder for whatever this server's self context
    // is; every other context is already a real Signal K context string.
    const context = storedContext === "self" ? selfContext : storedContext;

    handleMessage({
      context,
      updates: [...byTimestamp].map(([timestamp, values]) => ({
        // Marks the value as replayed history — for consumers, and for the
        // recorder, which filters these out so they are not re-recorded.
        $source: RESTORE_SOURCE,
        timestamp,
        values,
      })),
    });

    contexts++;
    values += entry.byPath.size;
  }

  debug(
    `restored ${contexts} context(s), ${values} value(s) from history` +
      (skippedStale ? `, skipped ${skippedStale} stale row(s)` : "") +
      (skippedLive ? `, skipped ${skippedLive} already live` : ""),
  );

  return { contexts, values, skippedStale, skippedLive };
}
