import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  GpxImport,
  GpxImportCancelled,
  antimeridian,
  GpxParseError,
  WriterGate,
  gpxDateTime,
  gpxDecimal,
  recordedIntervals,
  type GpxImportOptions,
} from "../gpx-import.js";

type Row = [string, number, number];

const encoder = new TextEncoder();

// The own tracks in the fixtures below, as a caller would select them.
const OWN = new Set(["Own Ship", "Eigenes Schiff", "Fähre Ü", ""]);

/** Run a document through an import, `sliceBytes` at a time. */
async function run(
  doc: string,
  options: Partial<GpxImportOptions> = {},
  sliceBytes = Infinity,
) {
  const rows: Row[] = [];
  const importer = new GpxImport({
    tracks: OWN,
    resolutionSeconds: 10,
    write: (ms, lat, lon) => rows.push([new Date(ms).toISOString(), lat, lon]),
    backpressure: async () => {},
    ...options,
  });
  const bytes = encoder.encode(doc);
  const step = Math.min(sliceBytes, bytes.length);
  for (let at = 0; at < bytes.length; at += step) {
    await importer.write(bytes.subarray(at, at + step));
  }
  const summary = await importer.finish();
  return { rows, summary };
}

const pt = (lat: number, lon: number, time?: string) =>
  `<trkpt lat="${lat}" lon="${lon}">${time ? `<time>${time}</time>` : ""}</trkpt>`;

const gpx = (...tracks: string[]) =>
  `<?xml version="1.0"?>\n<gpx version="1.1" xmlns="http://www.topografix.com/GPX/1/1">${tracks.join("")}</gpx>`;

const trk = (name: string | null, ...segments: string[][]) =>
  `<trk>${name === null ? "" : `<name>${name}</name>`}${segments
    .map((points) => `<trkseg>${points.join("")}</trkseg>`)
    .join("")}</trk>`;

describe("gpx value rules", () => {
  it("reads xsd:decimal and nothing looser", () => {
    assert.equal(gpxDecimal("-16.6874148999964"), -16.6874148999964);
    assert.equal(gpxDecimal("+5."), 5);
    assert.ok(Number.isNaN(gpxDecimal("1e1")));
    assert.ok(Number.isNaN(gpxDecimal("0x10")));
    assert.ok(Number.isNaN(gpxDecimal(undefined)));
  });

  it("reads xsd:dateTime, with TimeZero's seven fraction digits", () => {
    assert.equal(
      gpxDateTime("2023-01-01T21:39:35.6764466Z"),
      Date.parse("2023-01-01T21:39:35.676Z"),
    );
    assert.equal(
      gpxDateTime("2024-06-01T12:00:00+02:00"),
      Date.parse("2024-06-01T10:00:00Z"),
    );
    assert.ok(Number.isNaN(gpxDateTime("2024-02-30T00:00:00Z")));
    assert.ok(Number.isNaN(gpxDateTime("2024")));
    assert.ok(Number.isNaN(gpxDateTime("2024-06-01 12:00:00")));
  });

  it("brings longitudes into [-180, 180] and refuses more than a turn", () => {
    assert.equal(antimeridian(-180), -180);
    assert.equal(antimeridian(180), 180);
    assert.ok(Math.abs(antimeridian(-180.25) - 179.75) < 1e-12);
    assert.ok(Math.abs(antimeridian(190) - -170) < 1e-12);
    assert.ok(Number.isNaN(antimeridian(360)));
    assert.ok(Number.isNaN(antimeridian(Number.NaN)));
  });
});

describe("GpxImport", () => {
  const ownTrack = trk("Own Ship", [
    pt(10, 20, "2024-06-01T12:00:01.5Z"),
    pt(10.1, 20.1, "2024-06-01T12:00:04Z"),
    pt(10.2, 20.2, "2024-06-01T12:00:11Z"),
    pt(10.3, 20.3, "2024-06-01T12:00:25Z"),
  ]);

  it("writes one row per interval, stamped on the interval's start", async () => {
    const { rows, summary } = await run(gpx(ownTrack));
    assert.deepEqual(rows, [
      ["2024-06-01T12:00:00.000Z", 10, 20],
      ["2024-06-01T12:00:10.000Z", 10.2, 20.2],
      ["2024-06-01T12:00:20.000Z", 10.3, 20.3],
    ]);
    assert.equal(summary.points, 4);
    assert.equal(summary.imported, 3);
    assert.equal(summary.thinned, 1);
    assert.equal(summary.from, "2024-06-01T12:00:00.000Z");
    assert.equal(summary.to, "2024-06-01T12:00:20.000Z");
  });

  it("keeps every point's own time at resolution 0", async () => {
    const { rows } = await run(gpx(ownTrack), { resolutionSeconds: 0 });
    assert.equal(rows.length, 4);
    assert.equal(rows[0]![0], "2024-06-01T12:00:01.500Z");
  });

  it("reads the same whatever the slices cut through", async () => {
    const doc = gpx(
      trk("Fähre Ü", [pt(1, 2, "2024-06-01T00:00:00Z")]),
      ownTrack,
      trk("AIS ÖRESUND", [pt(3, 4, "2024-06-01T00:00:00Z")]),
    );
    const whole = await run(doc);
    // Seven bytes cut tags, attribute values and the two-byte umlauts apart.
    const sliced = await run(doc, {}, 7);
    assert.deepEqual(sliced.rows, whole.rows);
    assert.deepEqual(sliced.summary, whole.summary);
    assert.deepEqual(sliced.summary.skippedTracks, { "AIS ÖRESUND": 1 });
  });

  it("imports only the selected tracks and tallies every name", async () => {
    const { rows, summary } = await run(
      gpx(
        trk("AIS SALLY", [
          pt(1, 1, "2024-06-01T00:00:00Z"),
          pt(1, 1, "2024-06-01T00:01:00Z"),
        ]),
        trk("Savusavu _ Nabuwalou", [pt(2, 2, "2024-06-01T00:00:00Z")]),
        trk("Eigenes Schiff", [pt(3, 3, "2024-06-01T00:00:00Z")]),
        trk(null, [pt(4, 4, "2024-06-01T01:00:00Z")]),
      ),
    );
    assert.deepEqual(
      rows.map((r) => r[1]),
      [3, 4],
    );
    assert.equal(summary.tracks, 4);
    assert.deepEqual(summary.skippedTracks, {
      "AIS SALLY": 2,
      "Savusavu _ Nabuwalou": 1,
    });
    assert.deepEqual(summary.importedTracks, { "Eigenes Schiff": 1, "": 1 });
  });

  it("names at most 100 tracks and counts the rest together", async () => {
    const many = Array.from({ length: 102 }, (_, i) =>
      trk(`AIS ${i}`, [pt(1, 1, "2024-06-01T00:00:00Z")]),
    );
    const { summary } = await run(gpx(...many));
    assert.equal(Object.keys(summary.skippedTracks).length, 101);
    assert.equal(summary.skippedTracks["(other tracks)"], 2);
  });

  it("takes only GPX's own structure as track data", async () => {
    const { rows } = await run(
      gpx(
        `<trk><name>Own Ship</name><extensions xmlns:v="urn:vendor">` +
          `<v:trkseg>${pt(9, 9, "2024-06-01T00:00:00Z")}</v:trkseg>` +
          `<trkseg>${pt(8, 8, "2024-06-01T00:00:00Z")}</trkseg>` +
          `</extensions><trkseg>` +
          `<trkpt lat="1" lon="1"><name>AIS FAKE</name><time>2024-06-01T00:00:00Z</time></trkpt>` +
          `</trkseg></trk>`,
      ),
    );
    assert.deepEqual(rows, [["2024-06-01T00:00:00.000Z", 1, 1]]);
  });

  it("counts points it cannot place or date", async () => {
    const { rows, summary } = await run(
      gpx(
        trk("Own Ship", [
          pt(91, 0, "2024-06-01T00:00:00Z"),
          pt(0, 360, "2024-06-01T00:00:10Z"),
          `<trkpt lat="1e1" lon="0"><time>2024-06-01T00:00:20Z</time></trkpt>`,
          pt(0, 0, "2024-02-30T00:00:00Z"),
          pt(0, 0),
          pt(0, 0, "2024-06-01T00:00:40Z"),
        ]),
      ),
    );
    assert.equal(summary.invalid, 5);
    assert.equal(rows.length, 1);
  });

  it("wraps a fix written just past the antimeridian", async () => {
    const { rows, summary } = await run(
      gpx(
        trk("Own Ship", [
          pt(-17.1, -180.000017430542, "2024-06-01T00:00:00Z"),
          pt(-17.1, 180.5, "2024-06-01T00:00:10Z"),
        ]),
      ),
    );
    assert.equal(summary.invalid, 0);
    assert.ok(Math.abs(rows[0]![2] - 179.999982569458) < 1e-9);
    assert.ok(Math.abs(rows[1]![2] - -179.5) < 1e-9);
  });

  it("starts thinning afresh in each segment", async () => {
    const { rows } = await run(
      gpx(
        trk(
          "Own Ship",
          [pt(1, 1, "2024-06-01T00:00:01Z")],
          [pt(2, 2, "2024-06-01T00:00:02Z")],
        ),
      ),
    );
    // Both land on the same interval; the archive's dedup keeps one.
    assert.equal(rows.length, 2);
  });

  it("leaves out intervals the vessel already has positions for", async () => {
    const asked: number[] = [];
    const day = Date.parse("2024-06-01T00:00:00Z");
    const { rows, summary } = await run(
      gpx(
        trk("Own Ship", [
          pt(1, 1, "2024-06-01T00:01:00Z"),
          pt(2, 2, "2024-06-01T00:04:59Z"),
          pt(3, 3, "2024-06-01T00:05:00Z"),
          pt(4, 4, "2024-06-02T00:00:00Z"),
        ]),
      ),
      {
        recorded: async (dayStart) => {
          asked.push(dayStart);
          // The first five minutes of June 1st were recorded live.
          return new Set(dayStart === day ? [day / 300_000] : []);
        },
      },
    );
    assert.deepEqual(
      rows.map((r) => r[1]),
      [3, 4],
    );
    assert.equal(summary.alreadyRecorded, 2);
    assert.deepEqual(asked, [day, day + 86_400_000]);
  });

  it("stops at the first XML error and keeps what it wrote before", async () => {
    const doc = gpx(ownTrack) + "<trk>";
    const rows: Row[] = [];
    const importer = new GpxImport({
      tracks: OWN,
      resolutionSeconds: 10,
      write: (ms, lat, lon) =>
        rows.push([new Date(ms).toISOString(), lat, lon]),
      backpressure: async () => {},
    });
    // The second root fails the slice it arrives in, after the three rows
    // read before it have gone out.
    await assert.rejects(importer.write(encoder.encode(doc)), GpxParseError);
    assert.equal(rows.length, 3);

    const truncated = new GpxImport({
      tracks: OWN,
      resolutionSeconds: 10,
      write: () => {},
      backpressure: async () => {},
    });
    await truncated.write(encoder.encode(gpx(ownTrack).slice(0, -6)));
    await assert.rejects(truncated.finish(), GpxParseError);

    const broken = new GpxImport({
      tracks: OWN,
      resolutionSeconds: 10,
      write: () => {},
      backpressure: async () => {},
    });
    await assert.rejects(
      broken.write(encoder.encode("<gpx><trk></gpx>")),
      /not well-formed XML/,
    );
  });

  it("refuses bytes that are not UTF-8", async () => {
    const importer = new GpxImport({
      tracks: OWN,
      resolutionSeconds: 10,
      write: () => {},
      backpressure: async () => {},
    });
    await assert.rejects(
      importer.write(Uint8Array.from([0x3c, 0x67, 0xff, 0xfe])),
      GpxParseError,
    );
  });

  it("waits for the writer after every slice and stops when it fails", async () => {
    let waits = 0;
    await run(gpx(ownTrack), {
      backpressure: async () => {
        waits++;
      },
    });
    assert.ok(waits >= 1);
    await assert.rejects(
      run(gpx(ownTrack), {
        backpressure: async () => {
          throw new Error("QuestDB is not accepting writes");
        },
      }),
      /not accepting writes/,
    );
  });

  it("stops writing once cancelled", async () => {
    const importer = new GpxImport({
      tracks: OWN,
      resolutionSeconds: 10,
      write: () => {},
      backpressure: async () => {},
    });
    importer.cancel();
    await assert.rejects(
      importer.write(encoder.encode(gpx(ownTrack))),
      GpxImportCancelled,
    );
  });
});

describe("WriterGate", () => {
  function writer(pending: number) {
    return {
      pendingLines: pending,
      enqueuedLineCount: 100,
      settledLineCount: 0,
      droppedLineCount: 0,
    };
  }

  it("passes while the writer has room and waits while it is full", async () => {
    const w = writer(10);
    await new WriterGate(w, 1).room();
    w.pendingLines = 30_000;
    const waiting = new WriterGate(w, 1).room();
    setTimeout(() => (w.pendingLines = 4_000), 10);
    await waiting;
  });

  it("gives up on a writer that never drains", async () => {
    await assert.rejects(
      new WriterGate(writer(30_000), 1, 20).room(),
      /not accepting writes/,
    );
  });

  it("fails once the writer has dropped lines", async () => {
    const w = writer(10);
    const gate = new WriterGate(w, 1);
    w.droppedLineCount = 5;
    await assert.rejects(gate.room(), /dropped 5 lines/);
  });

  it("settles once every line enqueued so far has left", async () => {
    const w = writer(0);
    const gate = new WriterGate(w, 1);
    const settled = gate.settled();
    setTimeout(() => (w.settledLineCount = 100), 10);
    await settled;
  });
});

describe("recordedIntervals", () => {
  it("asks for the own vessel's recorded, not imported, positions of the day", async () => {
    const statements: string[] = [];
    const lookup = recordedIntervals(
      {
        exec: async (sql: string) => {
          statements.push(sql);
          return {
            columns: [],
            dataset: [["2024-06-01T00:05:00.000000Z", 3]],
            count: 1,
            timestamp: 0,
          };
        },
      },
      { covers: () => true },
    );
    const found = await lookup(Date.parse("2024-06-01T00:00:00Z"));
    assert.deepEqual(
      [...found],
      [Date.parse("2024-06-01T00:05:00Z") / 300_000],
    );
    const sql = statements[0]!;
    assert.match(
      sql,
      /FROM \(\(SELECT .* signalk_position_archive\) ORDER BY ts\) TIMESTAMP\(ts\) WHERE/,
    );
    assert.match(
      sql,
      /ts >= '2024-06-01T00:00:00\.000Z' AND ts < '2024-06-02T00:00:00\.000Z'/,
    );
    assert.match(
      sql,
      /context = 'self' AND \(source IS NULL OR source != 'import\.gpx'\)/,
    );
    assert.match(sql, /SAMPLE BY 5m ALIGN TO CALENDAR$/);
  });
});
