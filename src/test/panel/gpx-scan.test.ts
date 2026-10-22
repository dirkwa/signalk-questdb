import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { isOtherVesselName, scanGpx } from "../../configpanel/gpx-scan.js";

const pt = (lat: number, lon: number, time?: string) =>
  `<trkpt lat="${lat}" lon="${lon}">${time ? `<time>${time}</time>` : ""}</trkpt>`;
const trk = (name: string | null, points: string[]) =>
  `<trk>${name === null ? "" : `<name>${name}</name>`}<trkseg>${points.join("")}</trkseg></trk>`;
const gpx = (...tracks: string[]) =>
  `<?xml version="1.0"?><gpx xmlns="http://www.topografix.com/GPX/1/1">${tracks.join("")}</gpx>`;

/**
 * A track from (lat, lon), one point every `stepMin` minutes for `hours`,
 * moving north by `drift` degrees per point.
 */
const passage = (
  name: string,
  start: string,
  lat: number,
  lon: number,
  hours = 2,
  stepMin = 10,
  drift = 0.001,
) =>
  trk(
    name,
    Array.from({ length: (hours * 60) / stepMin }, (_, i) =>
      pt(
        lat + i * drift,
        lon,
        new Date(Date.parse(start) + i * stepMin * 60_000).toISOString(),
      ),
    ),
  );

const scan = (doc: string, chunkBytes?: number) =>
  scanGpx(new Blob([doc]), () => {}, undefined, chunkBytes);

describe("scanGpx", () => {
  it("groups tracks by name with their points and period", async () => {
    const groups = await scan(
      gpx(
        passage("Own Ship", "2024-01-01T00:00:00Z", -17, -151),
        passage("Own Ship", "2024-02-01T00:00:00Z", -17, -151),
        trk("AIS SALLY", [pt(1, 1, "2024-01-01T00:00:00Z")]),
      ),
    );
    const own = groups.find((g) => g.name === "Own Ship")!;
    assert.equal(own.tracks, 2);
    assert.equal(own.points, 24);
    assert.equal(new Date(own.from!).toISOString(), "2024-01-01T00:00:00.000Z");
    assert.equal(new Date(own.to!).toISOString(), "2024-02-01T01:50:00.000Z");
    assert.equal(groups[0]!.name, "Own Ship", "largest first");
  });

  it("reads the same through chunks that cut tags and characters apart", async () => {
    const doc = gpx(
      passage("Fähre Ü", "2024-01-01T00:00:00Z", -17, -151),
      `<!-- <trk><name>AIS FAKE</name></trk> -->`,
      trk("<![CDATA[Own Ship]]>", [pt(1, 2, "2024-03-01T00:00:00Z")]),
      `<trk><name>Self</name><trkseg><trkpt lat="1" lon="2"/></trkseg></trk>`,
    );
    const whole = await scan(doc);
    for (const size of [7, 13, 64]) {
      assert.deepEqual(await scan(doc, size), whole, `chunks of ${size}`);
    }
    assert.deepEqual(whole.map((g) => [g.name, g.points]).sort(), [
      ["Fähre Ü", 12],
      ["Own Ship", 1],
      ["Self", 1],
    ]);
  });

  it("proposes the largest own-looking group and recurring names", async () => {
    const groups = await scan(
      gpx(
        passage("Own Ship", "2024-01-01T00:00:00Z", -17, -151, 6),
        passage("Eigenes Schiff", "2024-02-01T00:00:00Z", -17, -151),
        passage("Eigenes Schiff", "2024-02-02T00:00:00Z", -17, -151),
        passage("Sunset sail", "2024-03-01T00:00:00Z", -17, -151),
        passage("AIS SALLY", "2024-01-01T00:00:00Z", -17, -151),
        passage("AIS SALLY", "2024-01-02T00:00:00Z", -17, -151),
      ),
    );
    const pick = Object.fromEntries(groups.map((g) => [g.name, g.selected]));
    assert.deepEqual(pick, {
      "Own Ship": true,
      "Eigenes Schiff": true,
      // One of a kind: may be imported from elsewhere, so the user decides.
      "Sunset sail": false,
      "AIS SALLY": false,
    });
    assert.equal(groups.find((g) => g.name === "AIS SALLY")!.otherVessel, true);
  });

  it("flags a group that was elsewhere while a larger one recorded", async () => {
    const groups = await scan(
      gpx(
        passage("Own Ship", "2024-04-15T00:00:00Z", -16.73, -151.48, 24, 10, 0),
        passage(
          "Savusavu _ Nabuwalou",
          "2024-04-15T19:30:00Z",
          -16.77,
          179.33,
          2,
          10,
          0,
        ),
        passage(
          "Savusavu _ Nabuwalou",
          "2024-04-16T19:30:00Z",
          -16.77,
          179.33,
          2,
          10,
          0,
        ),
        // Same place, same time: the same vessel from another recorder.
        passage(
          "Own Ship (backup)",
          "2024-04-15T02:00:00Z",
          -16.73,
          -151.48,
          2,
          10,
          0,
        ),
        passage(
          "Own Ship (backup)",
          "2024-04-15T08:00:00Z",
          -16.73,
          -151.48,
          2,
          10,
          0,
        ),
      ),
    );
    const fiji = groups.find((g) => g.name === "Savusavu _ Nabuwalou")!;
    assert.equal(fiji.selected, false);
    assert.equal(fiji.conflict?.name, "Own Ship");
    assert.ok(fiji.conflict!.km > 3000);
    assert.equal(
      new Date(fiji.conflict!.at).toISOString(),
      "2024-04-15T19:30:00.000Z",
    );
    const backup = groups.find((g) => g.name === "Own Ship (backup)")!;
    assert.equal(backup.conflict, null);
    assert.equal(backup.selected, true);
  });

  it("counts points without a time but gives them no period", async () => {
    const [group] = await scan(gpx(trk("Own Ship", [pt(1, 2), pt(1, 2)])));
    assert.equal(group!.points, 2);
    assert.equal(group!.from, null);
  });

  it("names AIS targets the way TimeZero does", () => {
    assert.equal(isOtherVesselName("AIS SALLY"), true);
    assert.equal(isOtherVesselName("211336520"), true);
    assert.equal(isOtherVesselName("AISLING"), false);
    assert.equal(isOtherVesselName("Own Ship"), false);
  });
});
