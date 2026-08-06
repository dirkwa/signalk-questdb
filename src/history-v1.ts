import { QueryClient, validateTimestamp } from "./query-client.js";

interface HistoryOptions {
  startTime: Date;
  playbackRate: number;
  subscribe?: string;
}

interface Delta {
  context: string;
  updates: {
    timestamp: string;
    values: { path: string; value: unknown }[];
  }[];
}

// v1 replays raw deltas rather than aggregating, so all three value tables can
// be read in one pass and interleaved by timestamp. Every branch below emits
// the same unified shape — ts, path, context, valuetext, kind — which
// decodeValue turns back into the real delta value; the CASTs exist to keep
// the union's column types compatible.
//
// One builder per table, shared by BOTH query shapes (windowed streaming and
// the LATEST ON snapshot). `tail` carries what the caller appends inside the
// branch — the snapshot's LATEST ON clause. Keeping every branch here is the
// point: the snapshot query used to re-implement these by hand and drifted,
// silently replaying booleans as text on that path only.
function numBranch(where: string, tail = ""): string {
  return (
    `SELECT ts, path, context, CAST(value AS STRING) valuetext, ` +
    `'number' kind FROM signalk WHERE ${where}${tail}`
  );
}

// `withKind: false` degrades to the pre-migration shape. ensureTables() adds
// value_kind at startup, but a read racing an unmigrated table (or an
// external QuestDB the plugin does not own) would otherwise fail the WHOLE
// union with "Invalid column: value_kind" — turning a missing type tag into
// no history at all. The NULL is cast so the union's column type is stated
// rather than inferred.
function strBranch(where: string, withKind: boolean, tail = ""): string {
  const kind = withKind ? `CAST(value_kind AS STRING)` : `CAST(NULL AS STRING)`;
  return (
    `SELECT ts, path, context, value_str valuetext, ${kind} kind ` +
    `FROM signalk_str WHERE ${where}${tail}`
  );
}

// The track table has no path column, so the path is supplied as a literal.
function posBranch(where: string, tail = ""): string {
  return (
    `SELECT ts, 'navigation.position' path, context, ` +
    `concat(CAST(lat AS STRING), ',', CAST(lon AS STRING)) valuetext, ` +
    `'position' kind FROM signalk_position WHERE ${where}${tail}`
  );
}

function unionValueRowsSQL(
  where: string,
  orderAndLimit: string,
  withKind = true,
): string {
  return (
    `${numBranch(where)} UNION ALL ` +
    `${strBranch(where, withKind)} UNION ALL ` +
    `${posBranch(where)} ${orderAndLimit}`
  );
}

// True for the error QuestDB returns when value_kind has not been added yet.
function isMissingKindColumn(err: unknown): boolean {
  return /Invalid column:\s*value_kind/i.test(
    err instanceof Error ? err.message : String(err),
  );
}

// Turn a unified row back into the value a Signal K delta carries. Numbers
// must not stay strings (consumers do arithmetic on them), positions must be
// objects again, and a value recorded as a boolean must replay as a boolean.
//
// Only rows explicitly tagged 'boolean' are converted: the text is parsed
// from the stored value, never guessed. A path whose value genuinely is the
// word "true" carries no tag and stays a string — as do all rows written
// before the value_kind column existed, whose original type is not
// recoverable.
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

function groupRowsIntoDeltas(rows: Record<string, unknown>[]): Delta[] {
  const byTimestamp = new Map<
    string,
    Map<string, { path: string; value: unknown }[]>
  >();

  for (const row of rows) {
    const ts = row.ts as string;
    const context = (row.context as string) || "self";
    const path = row.path as string;
    // Both call sites query the unified shape, so every row carries
    // `valuetext` + `kind` for decodeValue to reconstruct.
    const value = decodeValue(row);

    if (!byTimestamp.has(ts)) {
      byTimestamp.set(ts, new Map());
    }
    const byContext = byTimestamp.get(ts)!;
    if (!byContext.has(context)) {
      byContext.set(context, []);
    }
    // Vessel-name rows are stored under the synthetic path "name" (tagged
    // kind "identity") but were received as empty-path object deltas —
    // replay them in that original shape, the only one consumers
    // (Freeboard) read names from. The kind gate keeps a data path
    // literally named "name" replaying as the plain string it is.
    byContext
      .get(context)!
      .push(
        path === "name" && row.kind === "identity" && typeof value === "string"
          ? { path: "", value: { name: value } }
          : { path, value },
      );
  }

  const deltas: Delta[] = [];
  for (const [ts, byContext] of byTimestamp) {
    for (const [context, values] of byContext) {
      deltas.push({
        context,
        updates: [{ timestamp: ts, values }],
      });
    }
  }

  return deltas;
}

// Last-known name per context AT the playback start. Names are static: a
// vessel's AIS static report repeats every ~6 minutes and the recorder
// dedupes writes to actual changes, so a playback window almost never
// contains a name row — the name was written when the vessel was first
// seen, possibly days ago. Fetching the latest-before-start per context
// lets playback label every vessel the moment it first appears; bounding
// at the start keeps a LATER rename out of a historical replay (in-window
// renames still play back as rows). The kind filter keeps a data path
// literally named "name" out of identity. Errors degrade to an empty map:
// playback proceeds unlabeled rather than not at all.
async function fetchLatestNames(
  queryClient: QueryClient,
  startTime: string,
): Promise<Map<string, string>> {
  try {
    const result = await queryClient.exec(
      `SELECT context, value_str FROM signalk_str ` +
        `WHERE path = 'name' AND value_kind = 'identity' ` +
        `AND ts <= '${startTime}' LATEST ON ts PARTITION BY context`,
    );
    const names = new Map<string, string>();
    for (const row of queryClient.toObjects(result)) {
      const context = (row.context as string) || "self";
      const name = row.value_str;
      if (typeof name === "string" && name !== "") {
        names.set(context, name);
      }
    }
    return names;
  } catch {
    return new Map();
  }
}

export function createHistoryProviderV1(
  queryClient: QueryClient,
  selfContext: string,
  debug: (msg: string) => void,
) {
  function hasAnyData(
    options: HistoryOptions,
    callback: (hasResults: boolean) => void,
  ): void {
    const startTime = validateTimestamp(options.startTime.toISOString());
    // "Any data" must consider every value table: a vessel recording only
    // string/boolean channels (or only a position track) has history to play
    // back, and answering false here disables playback entirely.
    queryClient
      .exec(
        // Summed via a UNION ALL subquery: QuestDB rejects adding scalar
        // subqueries with `+` ("no matching operator").
        `SELECT sum(c) as cnt FROM (` +
          `SELECT count() c FROM signalk WHERE ts >= '${startTime}'` +
          ` UNION ALL SELECT count() c FROM signalk_str WHERE ts >= '${startTime}'` +
          ` UNION ALL SELECT count() c FROM signalk_position WHERE ts >= '${startTime}')`,
      )
      .then((result) => {
        const count =
          result.dataset.length > 0 ? (result.dataset[0][0] as number) : 0;
        callback(count > 0);
      })
      .catch(() => {
        callback(false);
      });
  }

  function streamHistory(
    spark: {
      write: (data: unknown) => void;
      on: (event: string, cb: (...args: unknown[]) => void) => void;
    },
    options: HistoryOptions,
    // eslint-disable-next-line @typescript-eslint/no-unused-vars
    onChange: () => void,
  ): () => void {
    let stopped = false;
    const startTime = validateTimestamp(options.startTime.toISOString());
    const playbackRate = Math.max(1, options.playbackRate);

    // Vessel labels for this playback: last-known name per context,
    // injected once ahead of a context's first delta so consumers can
    // label the target immediately (a window almost never carries the
    // name row itself — see fetchLatestNames).
    let latestNames: Map<string, string> | null = null;
    const namedContexts = new Set<string>();

    const CHUNK_SECONDS = 60;
    // Rows per read. A window holding more than this is drained across
    // several reads rather than truncated (see the resume logic below).
    const CHUNK_ROW_LIMIT = 10000;
    let currentTime = new Date(startTime);

    async function streamChunk() {
      if (stopped) return;

      const from = validateTimestamp(currentTime.toISOString());
      const chunkEnd = new Date(currentTime.getTime() + CHUNK_SECONDS * 1000);
      const to = validateTimestamp(chunkEnd.toISOString());

      try {
        const window = `ts >= '${from}' AND ts < '${to}'`;
        const order = `ORDER BY ts LIMIT ${CHUNK_ROW_LIMIT}`;
        const result = await queryClient
          .exec(unionValueRowsSQL(window, order))
          .catch((err) => {
            if (!isMissingKindColumn(err)) throw err;
            return queryClient.exec(unionValueRowsSQL(window, order, false));
          });

        if (result.dataset.length === 0) {
          currentTime = chunkEnd;
          if (!stopped) {
            setTimeout(streamChunk, 100);
          }
          return;
        }

        const rows = queryClient.toObjects(result);
        const deltas = groupRowsIntoDeltas(rows);

        if (deltas.length > 0 && latestNames === null) {
          latestNames = await fetchLatestNames(queryClient, startTime);
          if (stopped) return;
        }

        for (const delta of deltas) {
          if (stopped) return;

          const resolvedContext =
            delta.context === "self" ? selfContext : delta.context;

          const name = latestNames?.get(delta.context);
          if (name !== undefined && !namedContexts.has(delta.context)) {
            namedContexts.add(delta.context);
            spark.write({
              context: resolvedContext,
              updates: [
                {
                  timestamp: delta.updates[0].timestamp,
                  values: [{ path: "", value: { name } }],
                },
              ],
            });
          }

          spark.write({
            ...delta,
            context: resolvedContext,
          });
        }

        // A busy interval can hold more rows than one read returns — querying
        // three tables instead of one makes that far likelier (a live install
        // already reaches ~6k rows per 60s window). Advancing to chunkEnd
        // after a truncated read would skip the remainder silently, so drain
        // the window before moving on.
        //
        // Resume AT the last sent row's timestamp, not past it: QuestDB
        // stamps at microsecond precision but JS Date truncates to
        // milliseconds, and a single instrument update commonly stamps
        // several paths within the same millisecond. Stepping past it would
        // drop the siblings that did not fit in this page. Re-reading that
        // millisecond can re-send rows already delivered, which is harmless
        // on replay — losing them is not.
        //
        // Only when the whole page shared currentTime's millisecond (so
        // resuming at it would repeat the identical read forever) does the
        // cursor step forward by 1ms, trading that millisecond's tail for
        // guaranteed progress.
        // Guard on `rows`, not `dataset`: toObjects() returns [] when the
        // response carries no column metadata, and indexing the last row of
        // an empty array would throw into the catch below — turning a
        // metadata hiccup into a permanent 1s retry loop on this window.
        if (rows.length >= CHUNK_ROW_LIMIT) {
          const lastTs = new Date(rows[rows.length - 1].ts as string).getTime();
          const resumeAt =
            lastTs > currentTime.getTime() ? lastTs : currentTime.getTime() + 1;
          currentTime = new Date(Math.min(resumeAt, chunkEnd.getTime()));
          if (!stopped) {
            setTimeout(streamChunk, 0);
          }
          return;
        }

        currentTime = chunkEnd;
        const wallDelay = (CHUNK_SECONDS * 1000) / playbackRate;
        if (!stopped) {
          setTimeout(streamChunk, wallDelay);
        }
      } catch (err) {
        debug(
          `streamHistory error: ${err instanceof Error ? err.message : String(err)}`,
        );
        if (!stopped) {
          setTimeout(streamChunk, 1000);
        }
      }
    }

    streamChunk();

    spark.on("end", () => {
      stopped = true;
    });

    return () => {
      stopped = true;
    };
  }

  // Snapshot of every path at `date`, for the v1 snapshot API.
  //
  // `path` is deliberately unused and must NOT become a filter. Despite the
  // name (and the `string` in the server's provider interface), the server
  // passes the REQUEST URL SEGMENTS — e.g. ["vessels", "<selfId>",
  // "navigation"], or [] for the root snapshot — then feeds every returned
  // delta to buildFullFromDeltas() and walks into the assembled tree with
  // those segments (signalk-server src/interfaces/rest.js). Filtering the
  // query by it would both mismatch the type and starve the snapshot the
  // caller is building.
  function getHistory(
    date: Date,

    path: string,
    callback: (deltas: Delta[]) => void,
  ): void {
    const ts = validateTimestamp(date.toISOString());

    // LATEST ON is applied PER TABLE and the results unioned, not the other
    // way round: running it over a materialized union makes QuestDB scan
    // instead of using each table's index, which timed out (>30s) on a
    // multi-hundred-million-row install. Position partitions by context only
    // — the track table has no path column.
    const at = `ts <= '${ts}'`;
    const byPath = ` LATEST ON ts PARTITION BY path, context`;
    const byContext = ` LATEST ON ts PARTITION BY context`;
    const snapshotSQL = (withKind: boolean) =>
      `(${numBranch(at, byPath)})` +
      ` UNION ALL ` +
      `(${strBranch(at, withKind, byPath)})` +
      ` UNION ALL ` +
      `(${posBranch(at, byContext)})`;

    queryClient
      .exec(snapshotSQL(true))
      .catch((err) => {
        if (!isMissingKindColumn(err)) throw err;
        return queryClient.exec(snapshotSQL(false));
      })
      .then((result) => {
        const rows = queryClient.toObjects(result);
        const deltas = groupRowsIntoDeltas(rows);
        callback(deltas);
      })
      .catch((err) => {
        debug(
          `getHistory error: ${err instanceof Error ? err.message : String(err)}`,
        );
        callback([]);
      });
  }

  return { hasAnyData, streamHistory, getHistory };
}
