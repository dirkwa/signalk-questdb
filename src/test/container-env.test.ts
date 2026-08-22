import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildContainerEnv, QUESTDB_COMMIT_MODE } from "../container-env.js";
import type { Config } from "../config/schema.js";

const baseConfig = { compression: "none" } as Partial<Config>;

describe("buildContainerEnv durability", () => {
  it("runs QuestDB in fsync-on-commit mode", () => {
    // Default nosync leaves the ingest path entirely to the page cache: a
    // power cut can land the commit record while the data it describes is
    // still dirty, which corrupts the table beyond what any RESUME WAL can
    // repair. Boats lose power, so this must be on by default.
    assert.equal(buildContainerEnv(baseConfig).QDB_CAIRO_COMMIT_MODE, "sync");
    assert.equal(QUESTDB_COMMIT_MODE, "sync");
  });

  it("keeps durability and low-power tuning on an empty config", () => {
    // The update path passes `currentConfig ?? {}`.
    const env = buildContainerEnv({});
    assert.equal(env.QDB_CAIRO_COMMIT_MODE, "sync");
    assert.equal(env.QDB_WAL_APPLY_WORKER_COUNT, "1");
  });
});

describe("buildContainerEnv low-power tuning", () => {
  // QuestDB ignores an unrecognised QDB_* variable in SILENCE — no warning,
  // no startup failure — so a misspelled key is indistinguishable from one
  // that worked, and only the running server's startup log can tell them
  // apart. These assertions pin the spellings that log confirmed.
  it("names the WAL apply pool without a cairo prefix", () => {
    const env = buildContainerEnv({});
    // `wal.apply.worker.count` has no `cairo.` prefix, unlike its neighbours
    // `cairo.wal.apply.*`.
    assert.equal(env.QDB_WAL_APPLY_WORKER_COUNT, "1");
    assert.ok(
      !("QDB_CAIRO_WAL_APPLY_WORKER_COUNT" in env),
      "the cairo-prefixed spelling is not a QuestDB property and is ignored",
    );
  });

  it("pins every worker pool it can to one thread", () => {
    const env = buildContainerEnv({});
    for (const key of [
      "QDB_WAL_APPLY_WORKER_COUNT",
      "QDB_SHARED_WORKER_COUNT",
      "QDB_LINE_TCP_WRITER_WORKER_COUNT",
      // Zero here does NOT fall back to the shared pool: ILP keys per-worker
      // state by worker id, so it gets a dedicated pool of two instead.
      "QDB_LINE_TCP_IO_WORKER_COUNT",
      // Features this plugin never uses, whose pools poll regardless.
      "QDB_VIEW_COMPILER_WORKER_COUNT",
      "QDB_MAT_VIEW_REFRESH_WORKER_COUNT",
      "QDB_LIVE_VIEW_REFRESH_WORKER_COUNT",
      "QDB_EXPORT_WORKER_COUNT",
    ]) {
      assert.equal(env[key], "1", `${key} should pin the pool to one thread`);
    }
  });

  it("sets a sleep threshold on every pool it sizes", () => {
    const env = buildContainerEnv({});
    // There is no global default, so a pool left out keeps the full spin.
    // Every pool given a count above must also be given a threshold.
    const counts = Object.keys(env).filter((k) => k.endsWith("_WORKER_COUNT"));
    for (const count of counts) {
      const threshold = count.replace(
        "_WORKER_COUNT",
        "_WORKER_SLEEP_THRESHOLD",
      );
      assert.ok(
        threshold in env,
        `${count} is set but ${threshold} is not — that pool keeps spinning`,
      );
    }
    assert.ok(
      counts.length >= 8,
      `expected the full pool set, got ${counts.length}`,
    );
  });

  it("gives every threshold it sets the low-power value", () => {
    const env = buildContainerEnv({});
    // Checked across ALL thresholds, not only those derived from a worker
    // count: http and pg take a threshold without us setting a count for
    // them, so a count-derived check would never look at those two.
    // Presence alone is not enough either — QuestDB's own default is 10000,
    // which would satisfy a key-existence check and change nothing.
    const thresholds = Object.keys(env).filter((k) =>
      k.endsWith("_WORKER_SLEEP_THRESHOLD"),
    );
    for (const key of thresholds) {
      assert.equal(
        env[key],
        "100",
        `${key} must carry the low-power value, not just exist`,
      );
    }
    assert.ok(
      thresholds.length >= 10,
      `expected every pool's threshold, got ${thresholds.length}`,
    );
  });

  it("quiets the per-commit INFO narration without losing failures", () => {
    const env = buildContainerEnv({});
    // A level is a floor, not an exact set: ERROR still carries CRITICAL and
    // ADVISORY, so table suspension and ILP parse failures survive.
    assert.equal(env.QDB_LOG_W_STDOUT_LEVEL, "ERROR");
  });
});

describe("buildContainerEnv compression", () => {
  it("omits compression vars when disabled", () => {
    const env = buildContainerEnv(baseConfig);
    assert.equal(env.QDB_CAIRO_WAL_SEGMENT_COMPRESSION_CODEC, undefined);
    assert.equal(env.QDB_CAIRO_WAL_SEGMENT_COMPRESSION_LEVEL, undefined);
  });

  it("maps lz4 without a level", () => {
    const env = buildContainerEnv({ compression: "lz4" } as Partial<Config>);
    assert.equal(env.QDB_CAIRO_WAL_SEGMENT_COMPRESSION_CODEC, "LZ4");
    assert.equal(env.QDB_CAIRO_WAL_SEGMENT_COMPRESSION_LEVEL, undefined);
  });

  it("maps zstd with its level", () => {
    const env = buildContainerEnv({
      compression: "zstd",
      compressionLevel: 7,
    } as Partial<Config>);
    assert.equal(env.QDB_CAIRO_WAL_SEGMENT_COMPRESSION_CODEC, "ZSTD");
    assert.equal(env.QDB_CAIRO_WAL_SEGMENT_COMPRESSION_LEVEL, "7");
  });
});

describe("buildContainerEnv single source of truth", () => {
  // The start path and /api/update/apply both create the container. They used
  // to build this literal separately and had already drifted: the update path
  // dropped the low-power worker tuning, so an in-place version update moved a
  // Pi onto QuestDB's multi-worker defaults. Both now call this builder; these
  // assertions fail if a future change reintroduces a partial env anywhere.
  it("always emits the full low-power tuning set", () => {
    for (const config of [
      {},
      baseConfig,
      { compression: "zstd", compressionLevel: 3 } as Partial<Config>,
    ]) {
      const env = buildContainerEnv(config);
      assert.equal(env.QDB_TELEMETRY_ENABLED, "false");
      assert.equal(env.QDB_HTTP_ENABLED, "true");
      assert.equal(env.QDB_LINE_TCP_ENABLED, "true");
      assert.equal(env.QDB_CAIRO_COMMIT_MODE, "sync");
      assert.equal(env.QDB_WAL_APPLY_WORKER_COUNT, "1");
      assert.equal(env.QDB_SHARED_WORKER_COUNT, "1");
      assert.equal(env.QDB_LINE_TCP_WRITER_WORKER_COUNT, "1");
      assert.equal(env.QDB_CAIRO_O3_COLUMN_MEMORY_SIZE, "262144");
    }
  });

  it("returns a fresh object so one call site cannot mutate the other's env", () => {
    const first = buildContainerEnv(baseConfig);
    first.QDB_CAIRO_COMMIT_MODE = "nosync";
    assert.equal(buildContainerEnv(baseConfig).QDB_CAIRO_COMMIT_MODE, "sync");
  });
});
