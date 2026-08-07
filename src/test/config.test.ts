import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { normalizeConfig, type Config } from "../config/schema.js";

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

describe("normalizeConfig recording toggles", () => {
  it("defaults missing toggles to enabled, matching the schema", () => {
    // A hand-edited or pre-panel config misses the keys entirely; the
    // runtime guards read them directly, so missing must mean the schema
    // default (both ON), not "disabled".
    const legacy = { managedContainer: true } as unknown as Config;
    const normalized = normalizeConfig(legacy);
    assert.equal(normalized.recordSelf, true);
    assert.equal(normalized.recordOthers, true);
  });

  it("preserves an explicit opt-out", () => {
    const config = {
      recordSelf: false,
      recordOthers: false,
    } as unknown as Config;
    const normalized = normalizeConfig(config);
    assert.equal(normalized.recordSelf, false);
    assert.equal(normalized.recordOthers, false);
  });
});

describe("normalizeConfig startup restore", () => {
  it("defaults a missing restore toggle to OFF", () => {
    // Restore is opt-in — the inverse of the recording toggles. Defaulting a
    // missing key to ON would start replaying history for every existing
    // install on upgrade.
    const legacy = { managedContainer: true } as unknown as Config;
    const normalized = normalizeConfig(legacy);
    assert.equal(normalized.restoreOnStart, false);
  });

  it("preserves an explicit opt-in", () => {
    const config = { restoreOnStart: true } as unknown as Config;
    assert.equal(normalizeConfig(config).restoreOnStart, true);
  });

  it("backfills a missing window with the schema default", () => {
    const legacy = { restoreOnStart: true } as unknown as Config;
    assert.equal(normalizeConfig(legacy).restoreMaxAgeMinutes, 9);
  });

  it("treats a zero or negative window as unset rather than disabling silently", () => {
    // A zero window would replay nothing while the toggle still reads "on",
    // which is indistinguishable from the feature being broken.
    for (const value of [0, -5]) {
      const config = {
        restoreOnStart: true,
        restoreMaxAgeMinutes: value,
      } as unknown as Config;
      assert.equal(normalizeConfig(config).restoreMaxAgeMinutes, 9);
    }
  });

  it("preserves a custom window", () => {
    const config = {
      restoreOnStart: true,
      restoreMaxAgeMinutes: 30,
    } as unknown as Config;
    assert.equal(normalizeConfig(config).restoreMaxAgeMinutes, 30);
  });
});

describe("normalizeConfig resource limits", () => {
  it("backfills limits dropped by pre-fix panel saves (issue #98)", () => {
    // The panel used to replace the config wholesale, stripping the resource
    // keys — which ran QuestDB with NO memory cap instead of the schema's
    // 768m. Missing keys must recover the schema defaults.
    const damaged = { managedContainer: true } as unknown as Config;
    const normalized = normalizeConfig(damaged);
    assert.equal(normalized.questdbMemoryLimit, "768m");
    assert.equal(normalized.questdbCpuLimit, 1.5);
  });

  it("preserves the documented unlimited opt-outs", () => {
    // "" (memory) and 0 (CPU) are explicit choices, not missing keys.
    const config = {
      questdbMemoryLimit: "",
      questdbCpuLimit: 0,
    } as unknown as Config;
    const normalized = normalizeConfig(config);
    assert.equal(normalized.questdbMemoryLimit, "");
    assert.equal(normalized.questdbCpuLimit, 0);
  });

  it("keeps explicit user limits untouched", () => {
    const config = {
      questdbMemoryLimit: "2g",
      questdbCpuLimit: 3,
    } as unknown as Config;
    const normalized = normalizeConfig(config);
    assert.equal(normalized.questdbMemoryLimit, "2g");
    assert.equal(normalized.questdbCpuLimit, 3);
  });
});
