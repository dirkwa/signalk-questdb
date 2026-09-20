import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { unitsResolver } from "../history-units.js";

describe("history-units", () => {
  const SELF = "vessels.urn:mrn:imo:mmsi:123456789";
  const app = (
    live: Record<string, unknown>,
    catalog: Record<string, string>,
  ) => ({
    selfContext: SELF,
    getPath: (p: string) => live[p],
    getMetadata: (p: string) =>
      p in catalog ? { units: catalog[p] } : undefined,
  });

  it("prefers the units the live model carries", () => {
    const units = unitsResolver(
      app(
        { "vessels.self.sensors.vane.angle.meta": { units: "rad" } },
        { "vessels.self.sensors.vane.angle": "deg" },
      ),
    );
    assert.equal(units("sensors.vane.angle", "vessels.self"), "rad");
  });

  it("falls back to the static catalog for a path that is not live", () => {
    const units = unitsResolver(
      app({}, { "vessels.self.navigation.headingTrue": "rad" }),
    );
    assert.equal(units("navigation.headingTrue", "vessels.self"), "rad");
  });

  it("asks both sources for the own vessel as vessels.self", () => {
    const asked: string[] = [];
    const units = unitsResolver({
      selfContext: SELF,
      getPath: (p) => {
        asked.push(p);
        return undefined;
      },
      getMetadata: (p) => {
        asked.push(p);
        return undefined;
      },
    });
    for (const context of ["self", SELF, "vessels.self"]) {
      units("navigation.headingTrue", context);
    }
    assert.deepEqual(
      [...new Set(asked)],
      [
        "vessels.self.navigation.headingTrue.meta",
        "vessels.self.navigation.headingTrue",
      ],
    );
  });

  it("keeps another vessel's context as it is", () => {
    const asked: string[] = [];
    unitsResolver({
      selfContext: SELF,
      getPath: (p) => {
        asked.push(p);
        return undefined;
      },
    })("navigation.headingTrue", "vessels.urn:mrn:imo:mmsi:244813009");
    assert.deepEqual(asked, [
      "vessels.urn:mrn:imo:mmsi:244813009.navigation.headingTrue.meta",
    ]);
  });

  it("is undefined when neither source knows the path, or without either", () => {
    assert.equal(
      unitsResolver(app({}, {}))("custom.thing", "vessels.self"),
      undefined,
    );
    assert.equal(
      unitsResolver({ selfContext: SELF })("custom.thing", "vessels.self"),
      undefined,
    );
    // A meta object without units, or a non-object, is not units either.
    assert.equal(
      unitsResolver(app({ "vessels.self.x.meta": { description: "x" } }, {}))(
        "x",
        "vessels.self",
      ),
      undefined,
    );
  });
});
