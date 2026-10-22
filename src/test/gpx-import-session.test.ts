import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { PassThrough } from "node:stream";
import { GpxImport } from "../gpx-import.js";
import {
  GpxImportConflict,
  GpxImportSession,
  GpxImports,
  IDLE_TIMEOUT_MS,
  SliceTooLarge,
  readSlice,
} from "../gpx-import-session.js";

const DOC = new TextEncoder().encode(
  `<gpx xmlns="http://www.topografix.com/GPX/1/1"><trk><name>Own Ship</name><trkseg>` +
    `<trkpt lat="1" lon="2"><time>2024-06-01T00:00:00Z</time></trkpt>` +
    `</trkseg></trk></gpx>`,
);

function session(
  options: {
    size?: number;
    now?: () => number;
    applied?: () => Promise<void>;
  } = {},
) {
  const written: number[] = [];
  const importer = new GpxImport({
    tracks: new Set(["Own Ship"]),
    resolutionSeconds: 10,
    write: (ms) => written.push(ms),
    backpressure: async () => {},
  });
  const s = new GpxImportSession(
    "Tracks_TZ.gpx",
    options.size ?? DOC.length,
    importer,
    options.applied ?? (async () => {}),
    options.now,
  );
  return { s, written };
}

describe("GpxImportSession", () => {
  it("accepts slices in order and finishes once every byte is in", async () => {
    let applied = 0;
    const { s, written } = session({
      applied: async () => {
        applied++;
      },
    });
    const half = Math.floor(DOC.length / 2);
    assert.equal((await s.append(0, DOC.subarray(0, half))).received, half);
    await s.append(half, DOC.subarray(half));
    const done = await s.finish();
    assert.equal(done.state, "finished");
    assert.equal(done.summary.imported, 1);
    assert.equal(written.length, 1);
    assert.equal(applied, 1);
  });

  it("refuses a slice that does not start where the last one ended", async () => {
    const { s } = session();
    await s.append(0, DOC.subarray(0, 10));
    await assert.rejects(s.append(5, DOC.subarray(5, 20)), (err) => {
      assert.ok(err instanceof GpxImportConflict);
      assert.equal(err.status?.received, 10);
      return true;
    });
    // Nothing changed: the right offset still works.
    await s.append(10, DOC.subarray(10));
  });

  it("refuses bytes past the announced size", async () => {
    const { s } = session({ size: 10 });
    await assert.rejects(s.append(0, DOC.subarray(0, 11)), GpxImportConflict);
  });

  it("refuses to finish an incomplete file", async () => {
    const { s } = session();
    await s.append(0, DOC.subarray(0, 10));
    await assert.rejects(s.finish(), /Only 10 of \d+ bytes/);
    assert.equal(s.status.state, "receiving");
  });

  it("fails on a broken document and accepts nothing after", async () => {
    const broken = new TextEncoder().encode("<gpx><trk></gpx>");
    const { s } = session({ size: broken.length });
    await assert.rejects(s.append(0, broken));
    assert.equal(s.status.state, "failed");
    assert.match(s.status.error ?? "", /not well-formed/);
    await assert.rejects(s.append(broken.length, broken), GpxImportConflict);
  });

  it("aborts, keeping its reason, and accepts nothing after", async () => {
    const { s } = session();
    s.abort();
    assert.equal(s.status.state, "aborted");
    assert.equal(s.status.error, "Aborted");
    await assert.rejects(s.append(0, DOC), GpxImportConflict);
  });
});

describe("GpxImports", () => {
  it("runs one import at a time and reports the last one", () => {
    const imports = new GpxImports();
    const first = imports.start(() => session().s);
    assert.throws(() => imports.start(() => session().s), GpxImportConflict);
    assert.equal(imports.get(first.id), first);
    assert.equal(imports.get("other"), null);
    first.abort();
    const second = imports.start(() => session().s);
    assert.equal(imports.latest()?.id, second.id);
  });

  it("abandons an import nobody has sent a slice to", () => {
    let now = 0;
    const imports = new GpxImports();
    const stale = imports.start(() => session({ now: () => now }).s);
    now = IDLE_TIMEOUT_MS + 1;
    const next = imports.start(() => session().s);
    assert.equal(stale.status.state, "aborted");
    assert.match(stale.status.error ?? "", /No slice arrived/);
    assert.equal(next.status.state, "receiving");
  });
});

describe("readSlice", () => {
  const request = (headers: Record<string, string> = {}) =>
    Object.assign(new PassThrough(), { headers });

  it("returns the body", async () => {
    const req = request();
    const read = readSlice(req, 100);
    req.end(Buffer.from("hello"));
    assert.equal((await read).toString(), "hello");
  });

  it("refuses a body announced or sent past the limit", async () => {
    await assert.rejects(
      readSlice(request({ "content-length": "101" }), 100),
      SliceTooLarge,
    );
    const req = request();
    const read = readSlice(req, 4);
    req.end(Buffer.from("hello"));
    await assert.rejects(read, SliceTooLarge);
  });
});
