import { QueryClient, validateTimestamp } from "./query-client.js";
import { isMissingKindColumn, isMissingSourceColumn } from "./schema-errors.js";

interface HistoryOptions {
  startTime: Date;
  playbackRate: number;
  subscribe?: string;
}

interface Delta {
  context: string;
  updates: {
    timestamp: string;
    $source?: string;
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
// `withSource: false` degrades like `withKind: false` below: the source
// column is a later migration still, so a read racing ensureTables() (or an
// external QuestDB the plugin does not own) replays unattributed rather than
// not at all.
function sourceCol(withSource: boolean): string {
  return withSource ? `CAST(source AS STRING)` : `CAST(NULL AS STRING)`;
}

function numBranch(where: string, withSource: boolean, tail = ""): string {
  return (
    `SELECT ts, path, context, ${sourceCol(withSource)} source, ` +
    `CAST(value AS STRING) valuetext, ` +
    `'number' kind FROM signalk WHERE ${where}${tail}`
  );
}

// `withKind: false` degrades to the pre-migration shape. ensureTables() adds
// value_kind at startup, but a read racing an unmigrated table (or an
// external QuestDB the plugin does not own) would otherwise fail the WHOLE
// union with "Invalid column: value_kind" — turning a missing type tag into
// no history at all. The NULL is cast so the union's column type is stated
// rather than inferred.
function strBranch(
  where: string,
  withKind: boolean,
  withSource: boolean,
  tail = "",
): string {
  const kind = withKind ? `CAST(value_kind AS STRING)` : `CAST(NULL AS STRING)`;
  return (
    `SELECT ts, path, context, ${sourceCol(withSource)} source, ` +
    `value_str valuetext, ${kind} kind ` +
    `FROM signalk_str WHERE ${where}${tail}`
  );
}

// The track table has no path column, so the path is supplied as a literal.
function posBranch(where: string, withSource: boolean, tail = ""): string {
  return (
    `SELECT ts, 'navigation.position' path, context, ` +
    `${sourceCol(withSource)} source, ` +
    `concat(CAST(lat AS STRING), ',', CAST(lon AS STRING)) valuetext, ` +
    `'position' kind FROM signalk_position WHERE ${where}${tail}`
  );
}

function unionValueRowsSQL(
  where: string,
  orderAndLimit: string,
  withKind = true,
  withSource = true,
): string {
  return (
    `${numBranch(where, withSource)} UNION ALL ` +
    `${strBranch(where, withKind, withSource)} UNION ALL ` +
    `${posBranch(where, withSource)} ${orderAndLimit}`
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
  // Grouped by (ts, context, source): mixing two sources' rows into one
  // update would force a single $source label onto both, so each source
  // gets its own update — exactly how the live deltas arrived. Rows with no
  // recorded source (pre-migration data) group under `undefined` and replay
  // without a $source, as before.
  const byTimestamp = new Map<
    string,
    Map<
      string,
      {
        context: string;
        source?: string;
        values: { path: string; value: unknown }[];
      }
    >
  >();

  for (const row of rows) {
    const ts = row.ts as string;
    const context = (row.context as string) || "self";
    const source = typeof row.source === "string" ? row.source : undefined;
    const path = row.path as string;
    // Both call sites query the unified shape, so every row carries
    // `valuetext` + `kind` for decodeValue to reconstruct.
    const value = decodeValue(row);

    if (!byTimestamp.has(ts)) {
      byTimestamp.set(ts, new Map());
    }
    const byGroup = byTimestamp.get(ts)!;
    // A NUL byte can appear in neither a context nor a sourceRef, so the
    // composite key is unambiguous.
    const groupKey = source ? `${context}\u0000${source}` : context;
    if (!byGroup.has(groupKey)) {
      byGroup.set(groupKey, { context, source, values: [] });
    }
    // Vessel-name rows are stored under the synthetic path "name" (tagged
    // kind "identity") but were received as empty-path object deltas —
    // replay them in that original shape, the only one consumers
    // (Freeboard) read names from. The kind gate keeps a data path
    // literally named "name" replaying as the plain string it is.
    byGroup
      .get(groupKey)!
      .values.push(
        path === "name" && row.kind === "identity" && typeof value === "string"
          ? { path: "", value: { name: value } }
          : { path, value },
      );
  }

  const deltas: Delta[] = [];
  for (const [ts, byGroup] of byTimestamp) {
    for (const { context, source, values } of byGroup.values()) {
      deltas.push({
        context,
        updates: [
          source
            ? { timestamp: ts, $source: source, values }
            : { timestamp: ts, values },
        ],
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
  // Run a unified read, retrying without whichever optional column QuestDB
  // reports missing. value_kind and source are separate migrations, so
  // either — or both, on an external database ensureTables() has not
  // touched — can be absent independently; each retry drops only the column
  // actually complained about.
  //
  // `state` lets a caller keep the degraded flags across calls: a playback
  // session issues one read per minute of history, and re-probing from
  // scratch each time pays two rejected statements per chunk on QuestDB's
  // single shared worker. The default (a fresh object per call) keeps
  // one-shot reads re-probing, so a read that raced the migration recovers
  // on the next request rather than staying degraded until restart.
  async function execUnified(
    build: (withKind: boolean, withSource: boolean) => string,
    state = { withKind: true, withSource: true },
  ) {
    for (;;) {
      try {
        return await queryClient.exec(build(state.withKind, state.withSource));
      } catch (err) {
        // Each degradation is announced ONCE per state object, not per query:
        // the retry is deliberate and keeps playback working, but it silently
        // changes what the caller gets back, and "playback works but the data
        // looks wrong" is far harder to diagnose than a line in the log.
        if (state.withKind && isMissingKindColumn(err)) {
          state.withKind = false;
          debug(
            "history v1: signalk_str has no 'value_kind' column, so values " +
              'recorded as booleans replay as the strings "true"/"false" — ' +
              "their original type is not recoverable. Re-create or migrate " +
              "the table to restore boolean replay.",
          );
          continue;
        }
        if (state.withSource && isMissingSourceColumn(err)) {
          state.withSource = false;
          debug(
            "history v1: tables have no 'source' column, so replayed updates " +
              "carry no $source attribution. Re-create or migrate the tables " +
              "to restore it.",
          );
          continue;
        }
        throw err;
      }
    }
  }

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
    // The pending inter-chunk timer. Tracked so stopping actually CANCELS it:
    // clearing `stopped` alone leaves a scheduled callback holding the event
    // loop open for up to CHUNK_SECONDS (60s at playbackRate 1), which kept
    // the test process alive long past its assertions and tripped the plugin
    // registry's 60s test budget.
    let chunkTimer: ReturnType<typeof setTimeout> | null = null;
    const scheduleChunk = (delayMs: number) => {
      if (stopped) return;
      chunkTimer = setTimeout(() => {
        chunkTimer = null;
        streamChunk();
      }, delayMs);
      // Never let a playback timer be the only thing keeping the process
      // alive — the server owns the lifecycle, not this stream.
      chunkTimer.unref?.();
    };
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
    // Session-scoped degraded-column flags: probe the optional columns once,
    // then remember for every chunk of this playback (see execUnified). A
    // new session starts fresh, so a mid-migration race degrades one
    // playback, not the provider's lifetime.
    const columnState = { withKind: true, withSource: true };

    async function streamChunk() {
      if (stopped) return;

      const from = validateTimestamp(currentTime.toISOString());
      const chunkEnd = new Date(currentTime.getTime() + CHUNK_SECONDS * 1000);
      const to = validateTimestamp(chunkEnd.toISOString());

      try {
        const window = `ts >= '${from}' AND ts < '${to}'`;
        const order = `ORDER BY ts LIMIT ${CHUNK_ROW_LIMIT}`;
        const result = await execUnified(
          (withKind, withSource) =>
            unionValueRowsSQL(window, order, withKind, withSource),
          columnState,
        );

        if (result.dataset.length === 0) {
          currentTime = chunkEnd;
          if (!stopped) {
            scheduleChunk(100);
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
            scheduleChunk(0);
          }
          return;
        }

        currentTime = chunkEnd;
        const wallDelay = (CHUNK_SECONDS * 1000) / playbackRate;
        if (!stopped) {
          scheduleChunk(wallDelay);
        }
      } catch (err) {
        debug(
          `streamHistory error: ${err instanceof Error ? err.message : String(err)}`,
        );
        if (!stopped) {
          scheduleChunk(1000);
        }
      }
    }

    streamChunk();

    // Cancel the pending chunk as well as setting the flag: the flag only
    // stops the NEXT scheduling decision, while an already-scheduled timer
    // keeps the event loop alive until it fires.
    const stop = () => {
      stopped = true;
      if (chunkTimer) {
        clearTimeout(chunkTimer);
        chunkTimer = null;
      }
    };

    spark.on("end", stop);

    return stop;
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
    const snapshotSQL = (withKind: boolean, withSource: boolean) =>
      `(${numBranch(at, withSource, byPath)})` +
      ` UNION ALL ` +
      `(${strBranch(at, withKind, withSource, byPath)})` +
      ` UNION ALL ` +
      `(${posBranch(at, withSource, byContext)})`;

    execUnified(snapshotSQL)
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
