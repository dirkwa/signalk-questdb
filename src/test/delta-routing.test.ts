import { describe, it } from "node:test";
import assert from "node:assert";
import { routeDeltaValue } from "../delta-routing";

describe("routeDeltaValue", () => {
  it("routes numbers to the scalar table", () => {
    assert.strictEqual(
      routeDeltaValue("environment.depth.belowKeel", 4.2),
      "number",
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
    // interleaves it with the real vessel track.
    for (const path of [
      "navigation.anchor.position",
      "navigation.courseGreatCircle.nextPoint.position",
      "steering.autopilot.target.position",
    ]) {
      assert.strictEqual(
        routeDeltaValue(path, { latitude: 12.05, longitude: -61.75 }),
        null,
        path,
      );
    }
  });

  it("drops objects without both coordinate keys", () => {
    assert.strictEqual(
      routeDeltaValue("navigation.position", { latitude: 1 }),
      null,
    );
    assert.strictEqual(
      routeDeltaValue("navigation.attitude", { roll: 0.1, pitch: 0 }),
      null,
    );
    assert.strictEqual(routeDeltaValue("navigation.position", null), null);
  });

  it("rejects non-finite or non-numeric coordinates", () => {
    assert.strictEqual(
      routeDeltaValue("navigation.position", {
        latitude: NaN,
        longitude: 13.4,
      }),
      null,
    );
    assert.strictEqual(
      routeDeltaValue("navigation.position", {
        latitude: "52.5",
        longitude: 13.4,
      }),
      null,
    );
    assert.strictEqual(
      routeDeltaValue("navigation.position", {
        latitude: 52.5,
        longitude: Infinity,
      }),
      null,
    );
  });
});
