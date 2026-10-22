// Importing a GPX track into the archive.
//
// A chartplotter's track export runs to gigabytes — a TimeZero export of four
// years is 1.5 GB and 11 million points — so nothing here holds a document.
// The browser sends the file in slices, each slice is parsed as it arrives,
// and its points go to the ILP writer before the next slice is accepted. Memory
// stays at one slice plus the parser's state, whatever the file's size.
//
// Points land in the position archive, the table retention never drops,
// under their own source, so an import can be told apart from what the
// server recorded and a second import of the same file upserts onto the
// first instead of doubling it. They are thinned on the way in to the
// archive's resolution, by stamping each on the start of its interval and
// letting the table's dedup keep one per interval: an export's block seams
// and overlapping tracks then collapse without the importer having to see
// them.

import { SaxesParser, type SaxesTagNS } from "saxes";
import type { QueryClient } from "./query-client.js";
import type { ILPWriter } from "./ilp-writer.js";
import { readFrom, type ArchiveReader } from "./archive.js";

/** The sourceRef imported positions are stored under. */
export const IMPORT_SOURCE = "import.gpx";

// Slices handed to the parser at once. Each runs synchronously, so this
// bounds how long an import holds the event loop between yields.
const SLICE_BYTES = 64 * 1024;

// A point is left out where the vessel already has a recorded position in the
// same five minutes: interleaving two receivers' fixes draws a sawtooth.
const COVERAGE_MS = 5 * 60 * 1000;
const DAY_MS = 24 * 60 * 60 * 1000;
// Days of coverage kept while an import runs. An export is mostly in time
// order, so only the days around the current point are ever asked again.
const COVERAGE_DAYS_CACHED = 8;

// Track names listed individually in the summary; the rest are counted under
// one key. The summary goes out with every slice's reply, and a file can hold
// a whole anchorage's worth of AIS targets.
const MAX_NAMED = 100;
const OTHER_NAMES = "(other tracks)";

const GPX_NAMESPACES = new Set([
  "",
  "http://www.topografix.com/GPX/1/1",
  "http://www.topografix.com/GPX/1/0",
]);

/**
 * An attribute as an `xsd:decimal`, or NaN. `Number()` reads `0x10` and
 * `1e1` too, which GPX does not allow and no exporter means.
 */
export function gpxDecimal(value: string | undefined): number {
  return value !== undefined && /^[+-]?(?:\d+(?:\.\d*)?|\.\d+)$/.test(value)
    ? Number(value)
    : Number.NaN;
}

/**
 * A `<time>` as epoch milliseconds, or NaN. GPX types it `xsd:dateTime`;
 * `Date.parse` alone takes a bare year and rolls February 30th into March.
 */
export function gpxDateTime(value: string): number {
  const match =
    /^(\d{4})-(\d{2})-(\d{2})T\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:Z|[+-]\d{2}:\d{2})$/.exec(
      value,
    );
  if (!match) return Number.NaN;
  const parsed = Date.parse(value);
  if (Number.isNaN(parsed)) return Number.NaN;
  const year = Number(match[1]);
  const month = Number(match[2]);
  const day = Number(match[3]);
  const calendar = new Date(Date.UTC(year, month - 1, day));
  return calendar.getUTCFullYear() === year &&
    calendar.getUTCMonth() === month - 1 &&
    calendar.getUTCDate() === day
    ? parsed
    : Number.NaN;
}

/**
 * A longitude brought into [-180, 180], or NaN when it is not one.
 *
 * Crossing the antimeridian, TimeZero writes the fixes just past it as
 * -180.00002 rather than 179.99998. Those are real positions, so they are
 * wrapped; a value beyond a full turn is not a position at all.
 */
export function antimeridian(longitude: number): number {
  if (!(longitude > -360 && longitude < 360)) return Number.NaN;
  if (longitude < -180) return longitude + 360;
  if (longitude > 180) return longitude - 360;
  return longitude;
}

export interface GpxImportSummary {
  /** Bytes of the document parsed. */
  bytes: number;
  tracks: number;
  /** Points per track name, of the tracks selected for import. */
  importedTracks: Record<string, number>;
  /** Points per track name, of the tracks left out. */
  skippedTracks: Record<string, number>;
  /** Track points read, in every track. */
  points: number;
  /** Rows written to the archive. */
  imported: number;
  /** Points that fell in an interval already written from the same segment. */
  thinned: number;
  /** Points where the vessel already had recorded positions. */
  alreadyRecorded: number;
  /** Points with a position or time GPX cannot express, or none at all. */
  invalid: number;
  /** First and last imported instant, ISO. */
  from: string | null;
  to: string | null;
}

export interface GpxImportOptions {
  /**
   * Names of the tracks to import, "" for a track without one.
   *
   * GPX has no field that says whose a track is, and a chartplotter's export
   * mixes the own track with AIS targets and with passages imported from
   * elsewhere — a TimeZero export put another boat's Fiji passage on the same
   * day as the own track in Polynesia. Written into one series, those draw a
   * line across the ocean, so the caller says which tracks are the vessel's.
   */
  tracks: ReadonlySet<string>;
  /** One row per this many seconds; 0 keeps every point's own time. */
  resolutionSeconds: number;
  /** Write one own-vessel position, stamped `ms`. */
  write(ms: number, latitude: number, longitude: number): void;
  /** Resolves once the writer can take more; rejects if it never will. */
  backpressure(): Promise<void>;
  /**
   * The five-minute intervals of a UTC day (by `floor(ms / 5 min)`) in which
   * the vessel already has positions. Absent, every point is imported.
   */
  recorded?: (dayStartMs: number) => Promise<ReadonlySet<number>>;
}

interface Point {
  ms: number;
  latitude: number;
  longitude: number;
  segment: number;
}

/** Count one point against a track name, bounded at MAX_NAMED names. */
function tally(counts: Record<string, number>, name: string): void {
  const key =
    name in counts || Object.keys(counts).length < MAX_NAMED
      ? name
      : OTHER_NAMES;
  counts[key] = (counts[key] ?? 0) + 1;
}

/** The document is not well-formed XML; rows before the error are kept. */
export class GpxParseError extends Error {}

/** The import was aborted while a slice was being written. */
export class GpxImportCancelled extends Error {}

/**
 * One import, fed the document slice by slice.
 *
 * Only GPX's own structure counts: a `trk` under `gpx`, a `trkseg` under it,
 * a `trkpt` under that. A vendor `<trkseg>` inside `<extensions>` is not a
 * segment, and a `<name>` inside a point is not the track's name.
 */
export class GpxImport {
  readonly summary: GpxImportSummary = {
    bytes: 0,
    tracks: 0,
    importedTracks: {},
    skippedTracks: {},
    points: 0,
    imported: 0,
    thinned: 0,
    alreadyRecorded: 0,
    invalid: 0,
    from: null,
    to: null,
  };

  private readonly parser = new SaxesParser({ xmlns: true });
  private readonly decoder = new TextDecoder("utf-8", { fatal: true });
  private parseError: Error | null = null;
  private cancelled = false;

  // Local names of the open GPX elements, "" for a foreign one, so a
  // structural element is recognised only at its place in the hierarchy.
  private readonly path: string[] = [];
  private trackName: string | null = null;
  private nameText = "";
  private selected = false;
  private segment = 0;
  private point: { latitude: number; longitude: number } | null = null;
  private timeText = "";
  private pending: Point[] = [];

  private lastSegment = -1;
  private lastBucket = Number.NaN;
  private readonly coverage = new Map<number, Promise<ReadonlySet<number>>>();
  private coveredDay = Number.NaN;
  private covered: ReadonlySet<number> = new Set();

  constructor(private readonly options: GpxImportOptions) {
    this.parser.on("opentag", (tag) => this.open(tag));
    this.parser.on("closetag", () => this.close());
    this.parser.on("text", (text) => this.text(text));
    this.parser.on("cdata", (text) => this.text(text));
    // saxes reports and carries on; an import stops at the first error.
    this.parser.on("error", (err) => {
      this.parseError ??= err;
    });
  }

  /** Stop writing: the slice in progress ends at its next point. */
  cancel(): void {
    this.cancelled = true;
  }

  /** Parse the next bytes of the document and write the points they hold. */
  async write(chunk: Uint8Array): Promise<void> {
    for (let at = 0; at < chunk.length; at += SLICE_BYTES) {
      const slice = chunk.subarray(at, at + SLICE_BYTES);
      let text: string;
      try {
        text = this.decoder.decode(slice, { stream: true });
      } catch {
        throw new GpxParseError(
          `The file is not UTF-8 near byte ${this.summary.bytes}`,
        );
      }
      this.parse(text);
      this.summary.bytes += slice.length;
      // The points read before an error are sound; they go out first.
      await this.drain();
      this.throwParseError();
      await new Promise((resolve) => setImmediate(resolve));
    }
  }

  /** The document has ended: anything left unclosed is an error. */
  async finish(): Promise<GpxImportSummary> {
    let rest: string;
    try {
      rest = this.decoder.decode();
    } catch {
      throw new GpxParseError("The file ends inside a UTF-8 character");
    }
    this.parse(rest);
    this.parser.close();
    await this.drain();
    this.throwParseError();
    return this.summary;
  }

  private parse(text: string): void {
    if (text !== "" && !this.parseError) this.parser.write(text);
  }

  private throwParseError(): void {
    if (this.parseError) {
      throw new GpxParseError(
        `The file is not well-formed XML: ${this.parseError.message}`,
      );
    }
  }

  private at(...names: string[]): boolean {
    if (this.path.length !== names.length) return false;
    return names.every((name, i) => this.path[i] === name);
  }

  private open(tag: SaxesTagNS): void {
    if (this.parseError) return;
    this.path.push(GPX_NAMESPACES.has(tag.uri) ? tag.local : "");
    if (this.at("gpx", "trk")) {
      this.summary.tracks++;
      this.trackName = null;
      this.selected = false;
    } else if (this.at("gpx", "trk", "name")) {
      this.nameText = "";
    } else if (this.at("gpx", "trk", "trkseg")) {
      // GPX puts <name> before the segments; a track whose name is missing
      // or comes later is selected as the track without one.
      if (this.trackName === null) this.name("");
      this.segment++;
    } else if (this.at("gpx", "trk", "trkseg", "trkpt")) {
      this.point = {
        latitude: gpxDecimal(tag.attributes.lat?.value),
        longitude: gpxDecimal(tag.attributes.lon?.value),
      };
      this.timeText = "";
    }
  }

  private text(text: string): void {
    if (this.at("gpx", "trk", "name")) this.nameText += text;
    else if (this.at("gpx", "trk", "trkseg", "trkpt", "time")) {
      this.timeText += text;
    }
  }

  private close(): void {
    // saxes carries on past an error, so the rest of the slice it was in
    // still produces events; none of them is trusted.
    if (this.parseError) return;
    if (this.at("gpx", "trk", "name") && this.trackName === null) {
      this.name(this.nameText.trim());
    } else if (this.at("gpx", "trk", "trkseg", "trkpt") && this.point) {
      this.summary.points++;
      const { latitude, longitude } = this.point;
      this.point = null;
      const name = this.trackName ?? "";
      if (!this.selected) {
        tally(this.summary.skippedTracks, name);
      } else {
        tally(this.summary.importedTracks, name);
        const ms = gpxDateTime(this.timeText.trim());
        const lon = antimeridian(longitude);
        if (
          Number.isNaN(ms) ||
          !(latitude >= -90 && latitude <= 90) ||
          Number.isNaN(lon)
        ) {
          this.summary.invalid++;
        } else {
          this.pending.push({
            ms,
            latitude,
            longitude: lon,
            segment: this.segment,
          });
        }
      }
    }
    this.path.pop();
  }

  private name(name: string): void {
    this.trackName = name;
    this.selected = this.options.tracks.has(name);
  }

  private async recordedIn(ms: number): Promise<ReadonlySet<number> | null> {
    const lookup = this.options.recorded;
    if (!lookup) return null;
    const day = Math.floor(ms / DAY_MS) * DAY_MS;
    if (day === this.coveredDay) return this.covered;
    let found = this.coverage.get(day);
    if (!found) {
      found = lookup(day);
      this.coverage.set(day, found);
      if (this.coverage.size > COVERAGE_DAYS_CACHED) {
        const oldest = this.coverage.keys().next().value;
        if (oldest !== undefined) this.coverage.delete(oldest);
      }
    }
    this.covered = await found;
    this.coveredDay = day;
    return this.covered;
  }

  private async drain(): Promise<void> {
    if (this.pending.length === 0) return;
    const points = this.pending;
    this.pending = [];
    const resolution = Math.floor(this.options.resolutionSeconds) * 1000;
    for (const point of points) {
      if (this.cancelled)
        throw new GpxImportCancelled("The import was aborted");
      const recorded = await this.recordedIn(point.ms);
      if (recorded?.has(Math.floor(point.ms / COVERAGE_MS))) {
        this.summary.alreadyRecorded++;
        continue;
      }
      const bucket =
        resolution > 0 ? Math.floor(point.ms / resolution) : point.ms;
      if (point.segment === this.lastSegment && bucket === this.lastBucket) {
        this.summary.thinned++;
        continue;
      }
      this.lastSegment = point.segment;
      this.lastBucket = bucket;
      const ms = resolution > 0 ? bucket * resolution : point.ms;
      this.options.write(ms, point.latitude, point.longitude);
      this.summary.imported++;
      const iso = new Date(ms).toISOString();
      if (this.summary.from === null || iso < this.summary.from) {
        this.summary.from = iso;
      }
      if (this.summary.to === null || iso > this.summary.to) {
        this.summary.to = iso;
      }
    }
    await this.options.backpressure();
  }
}

type ImportWriter = Pick<
  ILPWriter,
  "pendingLines" | "enqueuedLineCount" | "settledLineCount" | "droppedLineCount"
>;

// Same marks as the InfluxDB migration: far below the writer's buffer cap,
// which drops the OLDEST lines once reached — the very rows being imported.
const WRITER_HIGH_WATER = 20_000;
const WRITER_RESUME = 5_000;
const WRITER_TIMEOUT_MS = 60_000;
const WRITER_POLL_MS = 200;

/**
 * What an import needs from the shared ILP writer: to wait while it is full,
 * to know when everything written has left it, and to notice when it had to
 * drop lines, which may have been the import's.
 */
export class WriterGate {
  private readonly droppedAtStart: number;

  constructor(
    private readonly writer: ImportWriter,
    private readonly pollMs = WRITER_POLL_MS,
    private readonly timeoutMs = WRITER_TIMEOUT_MS,
  ) {
    this.droppedAtStart = writer.droppedLineCount;
  }

  /** Resolves once the writer has room; rejects if it does not drain. */
  async room(): Promise<void> {
    this.checkDrops();
    if (this.writer.pendingLines <= WRITER_HIGH_WATER) return;
    await this.until(
      () => this.writer.pendingLines <= WRITER_RESUME,
      "QuestDB is not accepting writes",
    );
  }

  /** Resolves once every line enqueued so far has left the writer. */
  async settled(): Promise<void> {
    const mark = this.writer.enqueuedLineCount;
    await this.until(
      () => this.writer.settledLineCount + this.writer.droppedLineCount >= mark,
      "QuestDB has not taken the last rows",
    );
    this.checkDrops();
  }

  private async until(done: () => boolean, failure: string): Promise<void> {
    const deadline = Date.now() + this.timeoutMs;
    while (!done()) {
      this.checkDrops();
      if (Date.now() > deadline) {
        throw new Error(
          `${failure} after ${Math.round(this.timeoutMs / 1000)} s. Run the ` +
            `import again once QuestDB is healthy: rows already imported are ` +
            `not duplicated.`,
        );
      }
      await new Promise((resolve) => setTimeout(resolve, this.pollMs));
    }
  }

  private checkDrops(): void {
    const dropped = this.writer.droppedLineCount - this.droppedAtStart;
    if (dropped > 0) {
      throw new Error(
        `The writer dropped ${dropped} lines while QuestDB was behind, so ` +
          `part of the import may be missing. Run it again: rows already ` +
          `imported are not duplicated.`,
      );
    }
  }
}

/**
 * The five-minute intervals of a day in which the own vessel has positions
 * that did not come from an import, read from the base table and, where it
 * covers the day, the archive.
 */
export function recordedIntervals(
  queryClient: Pick<QueryClient, "exec">,
  archive: ArchiveReader,
): (dayStartMs: number) => Promise<ReadonlySet<number>> {
  return async (dayStartMs) => {
    const from = new Date(dayStartMs).toISOString();
    const to = new Date(dayStartMs + DAY_MS).toISOString();
    const table = readFrom(
      "signalk_position",
      archive.covers("signalk_position", from, to),
    );
    const result = await queryClient.exec(
      `SELECT ts, count() FROM ${table} WHERE ts >= '${from}' AND ts < '${to}' ` +
        `AND context = 'self' AND (source IS NULL OR source != '${IMPORT_SOURCE}') ` +
        `SAMPLE BY 5m ALIGN TO CALENDAR`,
    );
    return new Set(
      result.dataset.map((row) =>
        Math.floor(Date.parse(String(row[0])) / COVERAGE_MS),
      ),
    );
  };
}
