import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { normalizeConfig, type Config } from "../config/schema";

// Signal K hands the plugin its stored configuration verbatim — a config
// saved before an option existed simply misses the key, and TypeBox
// defaults are not applied at load time. These tests build such configs the
// way they exist on disk (missing keys), hence the casts.
describe("normalizeConfig", () => {
  it("fills pathFilter and samplingRates missing from a legacy config", () => {
    const legacy = {
      managedContainer: true,
      recordSelf: true,
    } as unknown as Config;

    const normalized = normalizeConfig(legacy);
    assert.deepEqual(normalized.pathFilter, { mode: "exclude", paths: [] });
    assert.deepEqual(normalized.samplingRates, {});
    // The defaults must be usable by the per-delta pipeline: pathFilter.paths
    // is iterable and samplingRates is Object.entries-able.
    assert.equal(normalized.pathFilter.paths.length, 0);
    assert.equal(Object.entries(normalized.samplingRates).length, 0);
  });

  it("backfills a partially-shaped pathFilter per field", () => {
    const partial = {
      pathFilter: { mode: "include" },
    } as unknown as Config;
    const normalized = normalizeConfig(partial);
    assert.deepEqual(normalized.pathFilter, { mode: "include", paths: [] });

    const pathsOnly = {
      pathFilter: { paths: ["navigation.*"] },
    } as unknown as Config;
    assert.deepEqual(normalizeConfig(pathsOnly).pathFilter, {
      mode: "exclude",
      paths: ["navigation.*"],
    });
  });

  it("preserves values that are present", () => {
    const config = {
      pathFilter: { mode: "include", paths: ["navigation.*"] },
      samplingRates: { "environment.wind.*": 200 },
    } as unknown as Config;

    const normalized = normalizeConfig(config);
    assert.deepEqual(normalized.pathFilter, {
      mode: "include",
      paths: ["navigation.*"],
    });
    assert.deepEqual(normalized.samplingRates, {
      "environment.wind.*": 200,
    });
  });

  it("does not mutate the input", () => {
    const legacy = { recordSelf: true } as unknown as Config;
    normalizeConfig(legacy);
    assert.equal(
      (legacy as Record<string, unknown>).pathFilter,
      undefined,
      "input object must stay untouched",
    );
  });
});
