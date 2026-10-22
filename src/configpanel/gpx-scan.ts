// A quick look at a GPX file in the browser, before any of it is uploaded:
// which tracks it holds, by name, and which of them are the own vessel's.
//
// GPX does not say whose a track is. A chartplotter export mixes the own track
// with the AIS targets it followed and with passages imported into it from
// elsewhere, and a real TimeZero export put another boat's Fiji passage on the
// same day as the own track in Polynesia. The user picks; this proposes a
// pick and points at tracks that cannot both be the vessel's.
//
// It reads the file as text and picks out tags, without an XML parser: it
// runs over a gigabyte in seconds, where parsing would take a minute. The
// server parses the upload properly and imports only the names picked here,
// so a file odd enough to fool this scan costs a track in the preview, never
// a wrong row.

const CHUNK_BYTES = 8 * 1024 * 1024;

// One position kept per this much track time, for comparing tracks.
const SAMPLE_MS = 5 * 60 * 1000;
// Two tracks a sample apart in time are compared at that sample.
const MATCH_MS = 5 * 60 * 1000;
// Fixes of one vessel from two recorders lie metres apart; another vessel
// at the same time is kilometres away.
const CONFLICT_KM = 1;

export interface GpxTrackGroup {
  /** The tracks' `<name>`, "" for tracks without one. */
  name: string;
  tracks: number;
  points: number;
  /** First and last point time, epoch ms; null when no point had one. */
  from: number | null;
  to: number | null;
  /** Named the way TimeZero names an AIS target. */
  otherVessel: boolean;
  /** A larger group this one contradicts: same time, another place. */
  conflict: { name: string; km: number; at: number } | null;
  /** Proposed for import. */
  selected: boolean;
}

interface Group {
  name: string;
  tracks: number;
  points: number;
  from: number | null;
  to: number | null;
  // Flat [ms, lat, lon, ms, lat, lon, ...], in file order.
  samples: number[];
}

/** Named like an AIS target in a TimeZero export: "AIS <name>", or the MMSI. */
export function isOtherVesselName(name: string): boolean {
  return /^AIS\b/.test(name) || /^\d{9}$/.test(name);
}

const NAME = /[^\s/>]+/y;
// The names that make up nearly every tag of a track file, matched in place
// rather than cut out of the text.
const COMMON = ["trkpt", "time", "trkseg", "trk", "name", "gpx"];

/** Local name of the tag starting at `from`, e.g. "trkpt" for `<gpx:trkpt …>`. */
function tagName(buffer: string, from: number): string {
  for (const name of COMMON) {
    if (buffer.startsWith(name, from)) {
      const after = buffer.charCodeAt(from + name.length);
      // space, tab, newline, carriage return, "/" or ">"
      if (after <= 0x20 || after === 0x2f || after === 0x3e) return name;
    }
  }
  NAME.lastIndex = from;
  const name = NAME.exec(buffer)?.[0] ?? "";
  const colon = name.indexOf(":");
  return colon >= 0 ? name.slice(colon + 1) : name;
}

const enum Where {
  Other,
  Track,
  Name,
  Segment,
  Point,
  Time,
}

/** A numeric attribute of the tag spanning [from, to), or NaN. */
function attribute(buffer: string, name: string, from: number, to: number) {
  let at = buffer.indexOf(name, from);
  while (at >= 0 && at < to) {
    const before = buffer.charCodeAt(at - 1);
    let eq = at + name.length;
    while (buffer.charCodeAt(eq) === 0x20) eq++;
    if (
      (before === 0x20 ||
        before === 0x09 ||
        before === 0x0a ||
        before === 0x0d) &&
      buffer.charCodeAt(eq) === 0x3d /* = */
    ) {
      let q = eq + 1;
      while (buffer.charCodeAt(q) === 0x20) q++;
      const quote = buffer[q];
      if (quote !== '"' && quote !== "'") return Number.NaN;
      const close = buffer.indexOf(quote, q + 1);
      return close > 0 && close < to
        ? Number(buffer.slice(q + 1, close))
        : Number.NaN;
    }
    at = buffer.indexOf(name, at + 1);
  }
  return Number.NaN;
}

/**
 * The tracks of a GPX file, grouped by name, with a proposed selection.
 * `onProgress` gets the bytes read so far.
 */
export async function scanGpx(
  file: Blob,
  onProgress: (bytes: number) => void = () => {},
  signal?: AbortSignal,
  chunkBytes = CHUNK_BYTES,
): Promise<GpxTrackGroup[]> {
  const groups = new Map<string, Group>();
  const decoder = new TextDecoder("utf-8");
  const path: string[] = [];
  let carry = "";
  let text = "";
  // Text matters only inside a track's <name> and a point's <time>; the
  // whitespace between the other tags is most of the file.
  let collecting = false;
  let current: Group | null = null;
  let trackName: string | null = null;
  // The open point's tag, kept as a place in the text: its coordinates are
  // read only for the few points that become samples.
  let pointText: string | null = null;
  let pointFrom = 0;
  let pointTo = 0;
  let time = "";
  let lastSample = Number.NEGATIVE_INFINITY;
  // Points come seconds apart, so a time is parsed only when its minute
  // differs from the last one parsed; within a minute the last parsed value
  // stands in, which moves a group's span by under a minute at most.
  let lastMinute = "";
  let lastMs = Number.NaN;

  const startTrack = (name: string) => {
    trackName = name;
    let group = groups.get(name);
    if (!group) {
      group = { name, tracks: 0, points: 0, from: null, to: null, samples: [] };
      groups.set(name, group);
    }
    group.tracks++;
    current = group;
    lastSample = Number.NEGATIVE_INFINITY;
  };

  const endPoint = () => {
    const group = current as Group | null;
    const text = pointText;
    pointText = null;
    if (!group || text === null) return;
    group.points++;
    const value = time.trim();
    const minute = value.slice(0, 16);
    if (minute !== lastMinute) {
      lastMinute = minute;
      lastMs = Date.parse(value);
    }
    const ms = lastMs;
    if (!Number.isFinite(ms)) return;
    if (group.from === null || ms < group.from) group.from = ms;
    if (group.to === null || ms > group.to) group.to = ms;
    if (ms - lastSample >= SAMPLE_MS) {
      const lat = attribute(text, "lat", pointFrom, pointTo);
      const lon = attribute(text, "lon", pointFrom, pointTo);
      if (Number.isFinite(lat) && Number.isFinite(lon)) {
        group.samples.push(Date.parse(value), lat, lon);
        lastSample = ms;
      }
    }
  };

  // Where in GPX's structure the open element is. Only five places matter,
  // and this runs for every tag in the file.
  let where = Where.Other;
  const locate = (): Where => {
    const d = path.length;
    if (d < 2 || path[0] !== "gpx" || path[1] !== "trk") return Where.Other;
    if (d === 2) return Where.Track;
    if (d === 3) {
      return path[2] === "name"
        ? Where.Name
        : path[2] === "trkseg"
          ? Where.Segment
          : Where.Other;
    }
    if (path[2] !== "trkseg" || path[3] !== "trkpt") return Where.Other;
    if (d === 4) return Where.Point;
    return d === 5 && path[4] === "time" ? Where.Time : Where.Other;
  };

  const consume = (chunk: string, final: boolean) => {
    const buffer = carry + chunk;
    let i = 0;
    for (;;) {
      const lt = buffer.indexOf("<", i);
      if (lt < 0) {
        if (collecting) text += buffer.slice(i);
        carry = "";
        return;
      }
      if (collecting) text += buffer.slice(i, lt);
      // Comments, CDATA and declarations end on their own markers.
      let close: string | null = null;
      const next = buffer.charCodeAt(lt + 1);
      if (next === 0x21 /* ! */ || next === 0x3f /* ? */) {
        if (buffer.startsWith("<!--", lt)) close = "-->";
        else if (buffer.startsWith("<![CDATA[", lt)) close = "]]>";
        else if (next === 0x3f) close = "?>";
      }
      const end = close ? buffer.indexOf(close, lt) : buffer.indexOf(">", lt);
      if (end < 0) {
        carry = final ? "" : buffer.slice(lt);
        return;
      }
      const stop = end + (close ? close.length : 1);
      if (close === "]]>" && collecting) text += buffer.slice(lt + 9, end);
      i = stop;
      if (close) continue;

      if (next === 0x2f /* / */) {
        if (where === Where.Name && trackName === null) {
          startTrack(text.trim());
        } else if (where === Where.Time) {
          time = text;
        } else if (where === Where.Point) {
          endPoint();
        } else if (where === Where.Track) {
          current = null;
        }
        path.pop();
      } else if (next !== 0x21 /* ! */) {
        path.push(tagName(buffer, lt + 1));
        where = locate();
        if (where === Where.Track) {
          trackName = null;
          current = null;
        } else if (where === Where.Segment && trackName === null) {
          startTrack("");
        } else if (where === Where.Point) {
          pointText = buffer;
          pointFrom = lt;
          pointTo = end;
          time = "";
        }
        if (buffer.charCodeAt(end - 1) === 0x2f /* / */) {
          if (where === Where.Point) endPoint();
          path.pop();
        }
      }
      where = locate();
      text = "";
      collecting = where === Where.Name || where === Where.Time;
    }
  };

  for (let offset = 0; offset < file.size; offset += chunkBytes) {
    if (signal?.aborted) throw new DOMException("Scan aborted", "AbortError");
    const bytes = new Uint8Array(
      await file.slice(offset, offset + chunkBytes).arrayBuffer(),
    );
    consume(decoder.decode(bytes, { stream: true }), false);
    onProgress(Math.min(file.size, offset + chunkBytes));
  }
  consume(decoder.decode(), true);

  return propose([...groups.values()]);
}

function distanceKm(lat1: number, lon1: number, lat2: number, lon2: number) {
  const rad = Math.PI / 180;
  const a =
    Math.sin(((lat2 - lat1) * rad) / 2) ** 2 +
    Math.cos(lat1 * rad) *
      Math.cos(lat2 * rad) *
      Math.sin(((lon2 - lon1) * rad) / 2) ** 2;
  return 6371 * 2 * Math.asin(Math.min(1, Math.sqrt(a)));
}

/** Samples as [ms, lat, lon] triples, in time order. */
function sorted(samples: number[]): [number, number, number][] {
  const triples: [number, number, number][] = [];
  for (let i = 0; i < samples.length; i += 3) {
    triples.push([samples[i]!, samples[i + 1]!, samples[i + 2]!]);
  }
  return triples.sort((a, b) => a[0] - b[0]);
}

/**
 * Where `a` and `b` were at the same times, the median distance between
 * them and the first time they were that far apart; null if they never
 * overlap in time.
 */
function compare(
  a: [number, number, number][],
  b: [number, number, number][],
): { km: number; at: number } | null {
  const distances: { km: number; at: number }[] = [];
  let j = 0;
  for (const [ms, lat, lon] of a) {
    while (j < b.length - 1 && b[j + 1]![0] <= ms) j++;
    const near = [b[j], b[j + 1]].find(
      (s) => s !== undefined && Math.abs(s[0] - ms) <= MATCH_MS,
    );
    if (near)
      distances.push({ km: distanceKm(lat, lon, near[1], near[2]), at: ms });
  }
  if (distances.length === 0) return null;
  const median = [...distances].sort((x, y) => x.km - y.km)[
    Math.floor(distances.length / 2)
  ]!;
  return { km: median.km, at: distances[0]!.at };
}

/**
 * The proposal: the own vessel's track is the group with the most points
 * among those not named like AIS targets, and so is any other name that
 * recurs — a plotter names every recording alike, a renamed or imported
 * passage is one of a kind. A group that was somewhere else while a larger
 * group was recording is not the vessel's, whatever its name.
 */
export function propose(groups: Group[]): GpxTrackGroup[] {
  const bySize = [...groups].sort((a, b) => b.points - a.points);
  const samples = new Map(groups.map((g) => [g, sorted(g.samples)]));
  const own = bySize.find((g) => !isOtherVesselName(g.name) && g.name !== "");

  return bySize.map((group) => {
    const otherVessel = isOtherVesselName(group.name);
    let conflict: GpxTrackGroup["conflict"] = null;
    if (!otherVessel) {
      for (const larger of bySize) {
        if (larger === group) break;
        if (isOtherVesselName(larger.name)) continue;
        const found = compare(samples.get(group)!, samples.get(larger)!);
        if (found && found.km > CONFLICT_KM) {
          conflict = { name: larger.name, ...found };
          break;
        }
      }
    }
    const selected =
      !otherVessel &&
      conflict === null &&
      (group === own || (group.tracks >= 2 && group.name !== ""));
    return {
      name: group.name,
      tracks: group.tracks,
      points: group.points,
      from: group.from,
      to: group.to,
      otherVessel,
      conflict,
      selected,
    };
  });
}
