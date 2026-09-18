// Removing what an import made before 2.1.5 left behind.
//
// Against a signalk-to-influxdb 1.x source, those imports filed every string,
// boolean and object under a path suffixed with the InfluxDB field name —
// `navigation.state.stringValue`, `steering.autopilot.engaged.boolValue`,
// `navigation.position.jsonValue` — paths no consumer ever asks for. A re-run
// of the import writes the rows under their real paths, but cannot remove the
// old ones: the path is part of the dedup key, and QuestDB has no row DELETE.
// The only way to drop rows is to rebuild the table without them.

import type { QueryClient } from "./query-client.js";

/** The field names the 1.x writer used, which the old importer appended. */
const LEGACY_SUFFIXES = [".stringValue", ".boolValue", ".jsonValue"] as const;

/**
 * The rows in question: imported, and under a suffixed path. A live row has
 * no source when its delta had none, and the predicate is spelled so that
 * such a row is kept whatever the database makes of a comparison with null —
 * verified on QuestDB, which keeps it either way, but not left to it.
 */
export const LEGACY_IMPORT_ROWS_WHERE =
  `(source IS NOT NULL AND source = 'influxdb-import' AND (` +
  LEGACY_SUFFIXES.map((s) => `path LIKE '%${s}'`).join(" OR ") +
  `))`;

const TABLE = "signalk_str";
const REBUILT = "signalk_str_clean";
const RETIRED = "signalk_str_old";

/** How long the rebuild may take: a table of tens of millions of rows. */
const REBUILD_TIMEOUT_MS = 30 * 60 * 1000;
/** How long to wait for the marker, and so for everything sent before it. */
const BARRIER_TIMEOUT_MS = 60 * 1000;
/**
 * Marker rows: one written before the copy, one written last before the
 * hold. Rows of the old table only — never copied, never carried over,
 * dropped with it. Their source keeps them out of every query the plugin
 * makes for real data.
 */
const MARKER_SOURCE = "signalk-questdb-cleanup";
const MARKER_PATH = "cleanup.marker";
/** Every row the rebuild keeps or carries over: not legacy, not a marker. */
const KEPT_ROWS_WHERE = `NOT ${LEGACY_IMPORT_ROWS_WHERE} AND (source IS NULL OR source != '${MARKER_SOURCE}')`;

export interface CleanupWriter {
  hold(): void;
  release(): void;
  startTimestampWatch(): void;
  endTimestampWatch(): bigint | null;
  writeStringAtNanos(
    path: string,
    context: string,
    value: string,
    tsNanos: bigint,
    kind?: "boolean" | "identity",
    source?: string,
  ): void;
  readonly droppedLineCount?: number;
}

/**
 * How many legacy rows there are — after finishing what a rebuild that failed
 * after its swap left undone, so the count never reads zero while a retired
 * table still holds rows to carry over.
 */
export async function countLegacyImportRows(
  query: Pick<QueryClient, "exec">,
): Promise<number> {
  await recoverRetiredTable(query);
  const r = await query.exec(
    `SELECT count() FROM ${TABLE} WHERE ${LEGACY_IMPORT_ROWS_WHERE}`,
    REBUILD_TIMEOUT_MS,
  );
  return Number(r.dataset[0]?.[0] ?? 0);
}

/**
 * Rebuild the string table without the legacy rows.
 *
 * The copy runs with the writer working as usual. Two markers through the
 * writer's own stream bound what it wrote meanwhile: one before the copy,
 * waited for, so that everything sent earlier is in the snapshot; and one
 * last before the hold, waited for, so that everything sent before the swap
 * is in the old table. Between the two the writer notes the oldest timestamp
 * it wrote, and rows of the old table from there on are carried over before
 * it is dropped — a source with a wrong clock records the present under any
 * date. Only the swap holds the writer. A swap that fails before the rebuilt
 * table is in place puts the old one back; a failure after that leaves the
 * rebuilt table in place, since it has accepted writes since, and the old one
 * is carried over and dropped by the next run.
 *
 * Reports the lines the writer's cap discarded during the hold, if any: the
 * hold lasts seconds, but a recorder fed fast enough could still fill it.
 */
export async function removeLegacyImportRows(
  query: Pick<QueryClient, "exec">,
  writer: CleanupWriter,
  deps: {
    now?: () => number;
    sleep?: (ms: number) => Promise<void>;
    barrierTimeoutMs?: number;
  } = {},
): Promise<{ removed: number; dropped: number }> {
  const now = deps.now ?? Date.now;
  const sleep =
    deps.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const barrierTimeoutMs = deps.barrierTimeoutMs ?? BARRIER_TIMEOUT_MS;

  const before = await countLegacyImportRows(query);
  if (before === 0) return { removed: 0, dropped: 0 };

  // A rebuild that failed earlier may have left its table behind.
  await query.exec(`DROP TABLE IF EXISTS ${REBUILT}`);

  const writeMarker = (): string => {
    const marker = `${now()}-${Math.random().toString(36).slice(2)}`;
    writer.writeStringAtNanos(
      MARKER_PATH,
      "self",
      marker,
      BigInt(now()) * 1_000_000n,
      undefined,
      MARKER_SOURCE,
    );
    return marker;
  };

  // The watch first, then the marker: every line from here on is watched,
  // and once the marker is there, every line from before is in the table
  // and so in the snapshot the copy takes.
  writer.startTimestampWatch();
  await waitForMarker(query, writeMarker(), sleep, barrierTimeoutMs);
  await query.exec(
    `CREATE TABLE ${REBUILT} AS (SELECT * FROM ${TABLE} WHERE ${KEPT_ROWS_WHERE}) TIMESTAMP(ts) PARTITION BY DAY WAL`,
    REBUILD_TIMEOUT_MS,
  );
  await query.exec(
    `ALTER TABLE ${REBUILT} DEDUP ENABLE UPSERT KEYS(ts, path, context, source)`,
  );

  const droppedBefore = writer.droppedLineCount ?? 0;
  // Written and held in one go, so this marker is the last line sent.
  const last = writeMarker();
  writer.hold();
  // Everything the writer sent into the old table after the copy's snapshot
  // it sent between the two markers, and the watch says how old the oldest
  // of it is. Nothing at all means nothing to carry over.
  const oldestWritten = writer.endTimestampWatch();
  try {
    await waitForMarker(query, last, sleep, barrierTimeoutMs);
    await query.exec(`RENAME TABLE ${TABLE} TO ${RETIRED}`);
    try {
      await query.exec(`RENAME TABLE ${REBUILT} TO ${TABLE}`);
    } catch (err) {
      await query.exec(`RENAME TABLE ${RETIRED} TO ${TABLE}`);
      throw err;
    }
    await carryOverAndDrop(
      query,
      oldestWritten === null
        ? undefined
        : new Date(Number(oldestWritten / 1_000_000n)).toISOString(),
      oldestWritten === null,
    );
  } finally {
    writer.release();
  }
  return {
    removed: before,
    dropped: (writer.droppedLineCount ?? 0) - droppedBefore,
  };
}

/**
 * Rows of the retired table that are not legacy rows or markers go into the
 * live table — dedup makes the overlap harmless — and then it is dropped.
 * `since` narrows the copy to the stragglers when the cutoff is known; a
 * recovery of a table left behind takes everything; `nothing` skips the copy
 * when the writer wrote nothing meanwhile.
 */
async function carryOverAndDrop(
  query: Pick<QueryClient, "exec">,
  since?: string,
  nothing = false,
): Promise<void> {
  if (!nothing) {
    const window = since ? `ts >= '${since}' AND ` : "";
    await query.exec(
      `INSERT INTO ${TABLE} SELECT * FROM ${RETIRED} WHERE ${window}${KEPT_ROWS_WHERE}`,
      REBUILD_TIMEOUT_MS,
    );
  }
  await query.exec(`DROP TABLE ${RETIRED}`);
}

/**
 * A rebuild that failed after the swap leaves the retired table behind, with
 * the rows recorded during the copy still in it. Finish that work first.
 */
async function recoverRetiredTable(
  query: Pick<QueryClient, "exec">,
): Promise<void> {
  const r = await query.exec(
    `SELECT count() FROM tables() WHERE table_name = '${RETIRED}'`,
  );
  if (Number(r.dataset[0]?.[0] ?? 0) === 0) return;
  await carryOverAndDrop(query);
}

/** Until the marker can be read back: everything sent before it has landed. */
async function waitForMarker(
  query: Pick<QueryClient, "exec">,
  marker: string,
  sleep: (ms: number) => Promise<void>,
  timeoutMs: number,
): Promise<void> {
  let waited = 0;
  for (;;) {
    const r = await query.exec(
      `SELECT count() FROM ${TABLE} WHERE path = '${MARKER_PATH}' AND source = '${MARKER_SOURCE}' AND value_str = '${marker}'`,
    );
    if (Number(r.dataset[0]?.[0] ?? 0) > 0) return;
    if (waited >= timeoutMs) {
      throw new Error(
        `QuestDB has not taken the last writes to ${TABLE} after ${Math.round(timeoutMs / 1000)}s; try again later`,
      );
    }
    await sleep(500);
    waited += 500;
  }
}
