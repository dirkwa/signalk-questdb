// The QuestDB container's environment, in ONE place.
//
// There are two call sites that create the container — plugin start and
// /api/update/apply — and they used to build this literal independently. They
// had already drifted: the update path silently dropped the low-power worker
// tuning, so an in-place version update quietly moved a Pi onto QuestDB's
// multi-worker defaults. Anything a future option adds here now reaches both
// paths by construction.

import { Config } from "./config/schema";

// QuestDB's ingest path does not fsync anything under its default
// `cairo.commit.mode=nosync`: durability is left to the OS page cache, and a
// power cut can land the `_txn` commit record while the partition data it
// describes is still dirty. The table then claims more rows than exist on
// disk, and TableWriter asserts while OPENING the partition — a state no
// RESUME WAL variant can repair (both remedies run after the open), so it
// costs the whole table. `sync` makes TableWriter.commit00() fsync partition
// columns BEFORE writing the commit record, and makes WalWriter fsync each
// segment on commit, so the on-disk state can never claim data it doesn't
// have.
//
// Boats lose power; that is the deployment, not an edge case. The cost is a
// handful of fsyncs per commit interval (ILP batches at `ilpFlushIntervalMs`,
// 5s by default), which at boat data rates is noise against losing a table.
export const QUESTDB_COMMIT_MODE = "sync";

export function buildContainerEnv(
  config: Partial<Config>,
): Record<string, string> {
  return {
    QDB_TELEMETRY_ENABLED: "false",
    QDB_HTTP_ENABLED: "true",
    QDB_LINE_TCP_ENABLED: "true",
    QDB_CAIRO_COMMIT_MODE: QUESTDB_COMMIT_MODE,
    // Reduce CPU usage on low-power devices (Pi, Cerbo)
    QDB_CAIRO_WAL_APPLY_WORKER_COUNT: "1",
    QDB_SHARED_WORKER_COUNT: "1",
    QDB_LINE_TCP_WRITER_WORKER_COUNT: "1",
    QDB_CAIRO_O3_COLUMN_MEMORY_SIZE: "262144",
    ...(config.compression && config.compression !== "none"
      ? {
          QDB_CAIRO_WAL_SEGMENT_COMPRESSION_CODEC:
            config.compression === "zstd" ? "ZSTD" : "LZ4",
          ...(config.compression === "zstd" && config.compressionLevel
            ? {
                QDB_CAIRO_WAL_SEGMENT_COMPRESSION_LEVEL: String(
                  config.compressionLevel,
                ),
              }
            : {}),
        }
      : {}),
  };
}
