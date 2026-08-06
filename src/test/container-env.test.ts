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
    assert.equal(env.QDB_CAIRO_WAL_APPLY_WORKER_COUNT, "1");
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
      assert.equal(env.QDB_CAIRO_WAL_APPLY_WORKER_COUNT, "1");
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
