import { QueryClient, validateTimestamp } from "./query-client";

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
// be read in one pass and interleaved by timestamp. Values are unified into a
// single `value` column: numbers stay numbers, signalk_str rows come back as
// their stored text (booleans as "true"/"false"), and positions are
// reassembled into the {latitude, longitude} object a delta carries.
//
// Position rows have no `path` column — the table is the vessel track — so the
// path is supplied as a literal. CAST keeps the union's column types
// compatible across branches; the reader turns them back into real values.
// The signalk_str branch, with or without the value_kind tag. ensureTables()
// adds that column at startup, but a read racing an unmigrated table (or an
// external QuestDB the plugin does not own) would otherwise fail the WHOLE
// union with "Invalid column: value_kind" — turning a missing type tag into
// no history at all. `withKind: false` degrades to the pre-migration
// behaviour: every string row replays as text.
function strBranch(where: string, withKind: boolean): string {
  const kind = withKind ? `CAST(value_kind AS STRING)` : `NULL`;
  return `SELECT ts, path, context, value_str valuetext, ${kind} kind FROM signalk_str WHERE ${where}`;
}

function unionValueRowsSQL(
  where: string,
  orderAndLimit: string,
  withKind = true,
): string {
  return (
    `SELECT ts, path, context, CAST(value AS STRING) valuetext, 'number' kind FROM signalk WHERE ${where}` +
    ` UNION ALL ` +
    strBranch(where, withKind) +
    ` UNION ALL ` +
    `SELECT ts, 'navigation.position' path, context, ` +
    `concat(CAST(lat AS STRING), ',', CAST(lon AS STRING)) valuetext, 'position' kind ` +
    `FROM signalk_position WHERE ${where}` +
    ` ${orderAndLimit}`
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
    // Unified rows carry `valuetext` + `kind`; decodeValue reconstructs the
    // real value. Rows that already have a plain `value` (older callers) are
    // passed through unchanged.
    const value =
      "valuetext" in row ? decodeValue(row) : (row.value as unknown);

    if (!byTimestamp.has(ts)) {
      byTimestamp.set(ts, new Map());
    }
    const byContext = byTimestamp.get(ts)!;
    if (!byContext.has(context)) {
      byContext.set(context, []);
    }
    byContext.get(context)!.push({ path, value });
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

        for (const delta of deltas) {
          if (stopped) return;

          const resolvedContext =
            delta.context === "self" ? selfContext : delta.context;
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
    const snapshotSQL = (withKind: boolean) =>
      `(SELECT ts, path, context, CAST(value AS STRING) valuetext, 'number' kind ` +
      `FROM signalk WHERE ts <= '${ts}' LATEST ON ts PARTITION BY path, context)` +
      ` UNION ALL ` +
      `(${strBranch(`ts <= '${ts}'`, withKind)} LATEST ON ts PARTITION BY path, context)` +
      ` UNION ALL ` +
      `(SELECT ts, 'navigation.position' path, context, ` +
      `concat(CAST(lat AS STRING), ',', CAST(lon AS STRING)) valuetext, 'position' kind ` +
      `FROM signalk_position WHERE ts <= '${ts}' LATEST ON ts PARTITION BY context)`;

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
