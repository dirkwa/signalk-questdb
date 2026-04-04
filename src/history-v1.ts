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

function groupRowsIntoDeltas(rows: Record<string, unknown>[]): Delta[] {
  const byTimestamp = new Map<
    string,
    Map<string, { path: string; value: unknown }[]>
  >();

  for (const row of rows) {
    const ts = row.ts as string;
    const context = (row.context as string) || "self";
    const path = row.path as string;
    const value = row.value as unknown;

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
    queryClient
      .exec(
        `SELECT count() as cnt FROM signalk WHERE ts >= '${startTime}' LIMIT 1`,
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
    _onChange: () => void,
  ): () => void {
    let stopped = false;
    const startTime = validateTimestamp(options.startTime.toISOString());
    const playbackRate = Math.max(1, options.playbackRate);

    const CHUNK_SECONDS = 60;
    let currentTime = new Date(startTime);

    async function streamChunk() {
      if (stopped) return;

      const from = validateTimestamp(currentTime.toISOString());
      const chunkEnd = new Date(currentTime.getTime() + CHUNK_SECONDS * 1000);
      const to = validateTimestamp(chunkEnd.toISOString());

      try {
        const result = await queryClient.exec(
          `SELECT ts, path, context, value FROM signalk WHERE ts >= '${from}' AND ts < '${to}' ORDER BY ts LIMIT 10000`,
        );

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

  function getHistory(
    date: Date,
    path: string,
    callback: (deltas: Delta[]) => void,
  ): void {
    const ts = validateTimestamp(date.toISOString());

    queryClient
      .exec(
        `SELECT path, value, ts, context FROM signalk WHERE ts <= '${ts}' LATEST ON ts PARTITION BY path`,
      )
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
