import { describe, it } from "node:test";
import assert from "node:assert";
import {
  extractVesselName,
  flattenObjectValue,
  routeDeltaValue,
  UnstorableTracker,
} from "../delta-routing.js";

describe("routeDeltaValue", () => {
  it("routes numbers to the scalar table", () => {
    assert.strictEqual(
      routeDeltaValue("environment.depth.belowKeel", 4.2),
      "number",
    );
  });

  it("routes booleans to the string table", () => {
    // Switch/relay, pump and valve states, autopilot flags: these used to
    // fall through to null and be dropped without a trace (issue #79).
    assert.equal(
      routeDeltaValue("watermaker.brineomatic.high_pressure_pump_on", true),
      "boolean",
    );
    assert.equal(
      routeDeltaValue("electrical.switches.bilgePump.state", false),
      "boolean",
    );
  });

  it("routes strings to the string table", () => {
    assert.strictEqual(
      routeDeltaValue("navigation.state", "anchored"),
      "string",
    );
  });

  it("routes navigation.position to the position table", () => {
    assert.strictEqual(
      routeDeltaValue("navigation.position", {
        latitude: 52.5,
        longitude: 13.4,
      }),
      "position",
    );
  });

  it("does NOT route other lat/lon-object paths to the position table", () => {
    // navigation.anchor.position is re-emitted on every fix by anchor plugins
    // while watching; letting it into the path-less signalk_position table
    // interleaves it with the real vessel track. They flatten instead, so the
    // anchor's coordinates are still recorded — as their own dotted paths,
    // where they cannot be confused with the vessel track (issue #128).
    for (const path of [
      "navigation.anchor.position",
      "navigation.courseGreatCircle.nextPoint.position",
      "steering.autopilot.target.position",
    ]) {
      assert.strictEqual(
        routeDeltaValue(path, { latitude: 12.05, longitude: -61.75 }),
        "flatten",
        path,
      );
    }
  });

  it("flattens objects that are not a usable position", () => {
    // These used to return null and be dropped without a trace (issue #128).
    // A half-position is not a track point, but its scalar leaves are still
    // real readings, so they are recorded as dotted paths like anything else.
    assert.strictEqual(
      routeDeltaValue("navigation.position", { latitude: 1 }),
      "flatten",
    );
    assert.strictEqual(
      routeDeltaValue("navigation.attitude", { roll: 0.1, pitch: 0 }),
      "flatten",
    );
    assert.strictEqual(routeDeltaValue("navigation.position", null), null);
  });

  it("keeps non-finite or non-numeric coordinates out of the track table", () => {
    // A NaN latitude is not a track point. The object still flattens, so the
    // usable leaf beside it survives — and flattenObjectValue drops the
    // non-finite one, so nothing unrepresentable reaches ILP.
    for (const value of [
      { latitude: NaN, longitude: 13.4 },
      { latitude: "52.5", longitude: 13.4 },
      { latitude: 52.5, longitude: Infinity },
    ]) {
      // Asserts "flatten", not merely "not position": the weaker form would
      // also pass if these regressed to being dropped entirely, which is the
      // bug this whole change exists to fix.
      assert.strictEqual(
        routeDeltaValue("navigation.position", value),
        "flatten",
        JSON.stringify(value),
      );
    }
  });

  it("does not flatten arrays", () => {
    // Array indices are not stable identities, so `foo.0` would mean a
    // different thing from one delta to the next.
    assert.strictEqual(routeDeltaValue("some.list", [1, 2, 3]), null);
    assert.strictEqual(routeDeltaValue("some.list", []), null);
  });
});

describe("flattenObjectValue (issue #128)", () => {
  it("pulls each scalar leaf out as its own dotted path", () => {
    const { leaves, skipped } = flattenObjectValue("navigation.attitude", {
      roll: 0.02,
      pitch: -0.01,
      yaw: 1.57,
    });

    assert.deepStrictEqual(leaves, [
      { path: "navigation.attitude.roll", value: 0.02 },
      { path: "navigation.attitude.pitch", value: -0.01 },
      { path: "navigation.attitude.yaw", value: 1.57 },
    ]);
    assert.deepStrictEqual(skipped, []);
  });

  it("keeps string and boolean leaves, not just numbers", () => {
    const { leaves } = flattenObjectValue("some.thing", {
      count: 3,
      label: "port",
      active: true,
    });

    assert.deepStrictEqual(leaves, [
      { path: "some.thing.count", value: 3 },
      { path: "some.thing.label", value: "port" },
      { path: "some.thing.active", value: true },
    ]);
  });

  it("reports non-finite numbers as skipped rather than writing them", () => {
    // NaN and ±Infinity have no ILP representation and would poison the
    // column. They are reported so the drop is visible, not silent.
    const { leaves, skipped } = flattenObjectValue("sensor.x", {
      good: 1.5,
      bad: NaN,
      worse: Infinity,
    });

    assert.deepStrictEqual(leaves, [{ path: "sensor.x.good", value: 1.5 }]);
    assert.deepStrictEqual(skipped, ["sensor.x.bad", "sensor.x.worse"]);
  });

  it("does not descend into nested objects, and says so", () => {
    // One level deep deliberately: a recursive walk would write out whole
    // notification and resource payloads nobody asked to record.
    const { leaves, skipped } = flattenObjectValue("a.b", {
      flat: 1,
      nested: { deep: 2 },
      list: [1, 2],
    });

    assert.deepStrictEqual(leaves, [{ path: "a.b.flat", value: 1 }]);
    assert.deepStrictEqual(skipped, ["a.b.nested", "a.b.list"]);
  });

  it("reports null and undefined leaves as skipped", () => {
    const { leaves, skipped } = flattenObjectValue("a.b", {
      present: 1,
      empty: null,
      missing: undefined,
    });

    assert.deepStrictEqual(leaves, [{ path: "a.b.present", value: 1 }]);
    assert.deepStrictEqual(skipped, ["a.b.empty", "a.b.missing"]);
  });

  it("yields nothing for a non-object, an array or an empty object", () => {
    for (const value of [null, 42, "x", [1, 2], {}]) {
      const { leaves } = flattenObjectValue("a.b", value);
      assert.deepStrictEqual(leaves, [], JSON.stringify(value));
    }
  });

  it("records the anchor position's coordinates under its own path", () => {
    // The case that must NOT reach signalk_position: the leaves are recorded
    // where they cannot be confused with the vessel track.
    const { leaves } = flattenObjectValue("navigation.anchor.position", {
      latitude: 12.05,
      longitude: -61.75,
    });

    assert.deepStrictEqual(leaves, [
      { path: "navigation.anchor.position.latitude", value: 12.05 },
      { path: "navigation.anchor.position.longitude", value: -61.75 },
    ]);
  });
});

describe("UnstorableTracker (issue #128)", () => {
  it("records each distinct path once", () => {
    const t = new UnstorableTracker();
    t.note("a.b");
    t.note("a.b");
    t.note("c.d");

    assert.strictEqual(t.size, 2);
    assert.strictEqual(t.truncated, false);
    assert.deepStrictEqual(t.examples(5), ["a.b", "c.d"]);
  });

  it("stops retaining paths at the cap and says it truncated", () => {
    // Signal K paths are NOT a bounded vocabulary — they embed instance
    // identifiers (`watermaker.0.*`) and notifications embed per-vessel ones,
    // so an unstorable shape on a busy AIS stream would grow an uncapped set
    // for the lifetime of the process.
    const t = new UnstorableTracker(3);
    for (let i = 0; i < 100; i++) t.note(`notifications.vessel${i}.alarm`);

    assert.strictEqual(t.size, 3, "must not grow past the cap");
    assert.strictEqual(t.truncated, true);
  });

  it("is not truncated while exactly at the cap", () => {
    // Off-by-one guard: the cap is how many are RETAINED, so filling it
    // exactly is not yet a truncation.
    const t = new UnstorableTracker(3);
    t.note("a");
    t.note("b");
    t.note("c");

    assert.strictEqual(t.size, 3);
    assert.strictEqual(t.truncated, false);

    t.note("d");
    assert.strictEqual(t.truncated, true);
  });

  it("keeps deduplicating known paths after the cap is reached", () => {
    // The cap must not turn into a leak of repeated work: a path already
    // tracked stays a no-op, and re-noting it does not set truncated.
    const t = new UnstorableTracker(2);
    t.note("a");
    t.note("b");
    t.note("a");

    assert.strictEqual(t.size, 2);
    assert.strictEqual(t.truncated, false, "a known path is not a new one");
  });

  it("counts an all-unstorable object once per leaf, not per leaf plus parent", () => {
    // The handler reports `skipped` and nothing else. Also noting the parent
    // would double-count one problem and burn a second slot against the cap,
    // hitting it sooner and hiding genuinely distinct paths — measured at 16
    // of 50 slots wasted on a stream of per-vessel wrapper objects.
    const t = new UnstorableTracker();
    const { leaves, skipped } = flattenObjectValue("some.wrapper", {
      nested: { a: 1 },
      alsoNested: { b: 2 },
    });
    for (const p of skipped) t.note(p);

    assert.strictEqual(leaves.length, 0, "nothing storable in this object");
    assert.deepStrictEqual(t.examples(10), [
      "some.wrapper.nested",
      "some.wrapper.alsoNested",
    ]);
    assert.strictEqual(t.size, 2, "two leaves, two entries — not three");
  });

  it("resets fully on clear", () => {
    // The plugin clears this on stop: a path unstorable under one config may
    // not be under the next, so nothing may carry across a restart.
    const t = new UnstorableTracker(1);
    t.note("a");
    t.note("b");
    assert.strictEqual(t.truncated, true);

    t.clear();
    assert.strictEqual(t.size, 0);
    assert.strictEqual(t.truncated, false);
    assert.deepStrictEqual(t.examples(5), []);
  });
});

describe("extractVesselName", () => {
  it("extracts the name from an empty-path object delta", () => {
    assert.strictEqual(
      extractVesselName("", { name: "Sea Breeze" }),
      "Sea Breeze",
    );
    // AIS static reports carry siblings alongside the name.
    assert.strictEqual(
      extractVesselName("", { name: "Sea Breeze", mmsi: "244813000" }),
      "Sea Breeze",
    );
  });

  it("ignores non-empty paths — a data path named name stays data", () => {
    assert.strictEqual(extractVesselName("name", "Sea Breeze"), null);
    assert.strictEqual(
      extractVesselName("navigation.state", { name: "x" }),
      null,
    );
  });

  it("ignores empty-path deltas without a usable name", () => {
    assert.strictEqual(extractVesselName("", { mmsi: "244813000" }), null);
    assert.strictEqual(extractVesselName("", { name: "" }), null);
    assert.strictEqual(extractVesselName("", { name: "   " }), null);
    assert.strictEqual(extractVesselName("", { name: 42 }), null);
    assert.strictEqual(extractVesselName("", null), null);
    assert.strictEqual(extractVesselName("", "just a string"), null);
  });
});
