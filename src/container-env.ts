// The QuestDB container's environment, in ONE place.
//
// There are two call sites that create the container — plugin start and
// /api/update/apply — and they used to build this literal independently. They
// had already drifted: the update path silently dropped the low-power worker
// tuning, so an in-place version update quietly moved a Pi onto QuestDB's
// multi-worker defaults. Anything a future option adds here now reaches both
// paths by construction.

import { Config } from "./config/schema.js";

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

// Poll cycles a worker spins before sleeping. QuestDB's default is 10000,
// which keeps threads hot on a database that is idle most of the time. Low
// enough to sleep promptly, high enough that a burst of ingest does not pay a
// wake-up per batch.
const WORKER_SLEEP_THRESHOLD = "100";

export function buildContainerEnv(
  config: Partial<Config>,
): Record<string, string> {
  return {
    QDB_TELEMETRY_ENABLED: "false",
    QDB_HTTP_ENABLED: "true",
    QDB_LINE_TCP_ENABLED: "true",
    QDB_CAIRO_COMMIT_MODE: QUESTDB_COMMIT_MODE,

    // ── Low-power tuning (Pi, Cerbo) ──────────────────────────────────────
    //
    // QuestDB sizes most worker pools from the host core count and defaults
    // each pool to a busy-poll before it sleeps. That is right for a server
    // and wrong for a boat: on a mostly idle database the threads burn CPU
    // waiting for work that never comes, and the cost is a fixed floor rather
    // than something that scales with the data.
    //
    // NOTE ON NAMING: QuestDB ignores an unrecognised QDB_* variable in
    // silence — no warning, no startup failure — so a wrong name is
    // indistinguishable from a setting that worked. Verify any change here
    // against the running server rather than against the docs:
    //
    //   SELECT property_path, value, value_source FROM (SHOW PARAMETERS)
    //     WHERE value_source = 'env'
    //
    // A key that reports `default` there was not applied, whatever was
    // passed. (The startup log names each pool's `workers=N` too, but
    // QDB_LOG_W_STDOUT_LEVEL below suppresses those lines, so the query is
    // the reliable check.)
    //
    // `wal.apply.worker.count` has NO `cairo.` prefix, unlike its neighbours
    // `cairo.wal.apply.*` — the one spelling in this block most likely to be
    // guessed wrong.
    QDB_WAL_APPLY_WORKER_COUNT: "1",
    QDB_SHARED_WORKER_COUNT: "1",
    QDB_LINE_TCP_WRITER_WORKER_COUNT: "1",
    // Zero is this pool's default and does NOT mean "share the common pool"
    // the way it does elsewhere — ILP keys per-worker state by worker id, so
    // it gets a dedicated pool sized 2 regardless of the core count. Leaving
    // it unset was our own gap: the writer pool was pinned to 1 while the io
    // pool beside it still ran two threads.
    QDB_LINE_TCP_IO_WORKER_COUNT: "1",
    // Pools for features this plugin never uses. They are sized and polled
    // regardless, which makes them precisely the ones nobody thinks to set.
    QDB_VIEW_COMPILER_WORKER_COUNT: "1",
    QDB_MAT_VIEW_REFRESH_WORKER_COUNT: "1",
    QDB_LIVE_VIEW_REFRESH_WORKER_COUNT: "1",
    QDB_EXPORT_WORKER_COUNT: "1",

    // Poll cycles a worker spins before sleeping. There is no global default,
    // so every pool needs its own — one left out keeps the full spin.
    QDB_SHARED_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    // The HTTP and pg-wire pools take their own threshold even though both
    // currently draw workers from `shared-network` — QuestDB reports them at
    // the 10000 default there rather than inheriting the shared value. Set
    // one of the two `*.worker.count` keys and each becomes a dedicated pool
    // where its own threshold is what governs, so leaving these unset is a
    // gap that only opens when a default changes.
    QDB_HTTP_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_PG_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_WAL_APPLY_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_LINE_TCP_IO_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_LINE_TCP_WRITER_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_VIEW_COMPILER_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_MAT_VIEW_REFRESH_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_LIVE_VIEW_REFRESH_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,
    QDB_EXPORT_WORKER_SLEEP_THRESHOLD: WORKER_SLEEP_THRESHOLD,

    // QuestDB narrates every WAL commit at INFO — several lines a second of
    // pure noise, written to the container log forever. ERROR is a floor, not
    // an exact set, so CRITICAL and ADVISORY still come through: table
    // suspension, ILP parse failures and the max_map_count warning all
    // survive. What it drops is the INFO band — which also carries the WAL
    // apply memory-pressure backoff that precedes a suspension, so raise this
    // back to INFO when diagnosing a database that has stopped recording.
    QDB_LOG_W_STDOUT_LEVEL: "ERROR",

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
