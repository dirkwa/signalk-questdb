// Decides which table a delta value belongs in, keyed by JS type — with one
// path-based exception: signalk_position is a pure vessel-track table (rows
// are keyed on ts+context only, no path column), so it must hold
// navigation.position exclusively. Other lat/lon-object paths — e.g.
// navigation.anchor.position, which anchor plugins re-emit on every fix
// while watching — would interleave with the real track and make history
// reads zig-zag between boat and anchor.
//
// Objects that are not navigation.position route to "flatten": each scalar
// leaf is recorded as its own dotted path (issue #128). Before that,
// every such object was dropped, silently — navigation.attitude
// ({roll, pitch, yaw}) was never stored at all, and nothing said so.
export type DeltaRoute =
  "number" | "string" | "boolean" | "position" | "flatten" | null;

/** One scalar leaf pulled out of an object value, ready to record. */
export interface FlattenedLeaf {
  /** Parent path plus the key, e.g. `navigation.attitude.roll`. */
  path: string;
  value: number | string | boolean;
}

/**
 * Remembers which paths carried a value no table can hold, so the drop can be
 * reported instead of happening in silence — the underlying bug behind #128.
 *
 * Capped, because the path vocabulary is NOT bounded: paths embed instance
 * identifiers (`watermaker.0.*`) and notifications embed per-vessel ones, so
 * an unstorable shape on a busy AIS stream would grow an uncapped set for the
 * lifetime of the process. Past the cap the count still rises but no new path
 * is retained, and `truncated` marks the count as a lower bound rather than
 * letting it read as exact.
 *
 * Deliberately does no logging: `note()` runs per delta on a stream carrying
 * 100+ values/sec, and building a message string there costs whether or not
 * debug is enabled.
 */
export class UnstorableTracker {
  private readonly paths = new Set<string>();
  private capped = false;

  constructor(private readonly cap = 50) {}

  /** Records a path. Cheap and idempotent; safe to call per delta. */
  note(path: string): void {
    if (this.paths.has(path)) return;
    if (this.paths.size >= this.cap) {
      this.capped = true;
      return;
    }
    this.paths.add(path);
  }

  get size(): number {
    return this.paths.size;
  }

  get truncated(): boolean {
    return this.capped;
  }

  examples(limit: number): string[] {
    return [...this.paths].slice(0, limit);
  }

  clear(): void {
    this.paths.clear();
    this.capped = false;
  }
}

// Static vessel identity arrives as EMPTY-path object deltas —
// `{path: "", value: {name: "..."}}` is how AIS static reports reach the
// server, and the exact shape Freeboard reads names from. These never make
// it past the recorder's path guard, so vessel names were absent from
// history (issue #91). Returns the name when the delta carries a usable
// one, null otherwise.
export function extractVesselName(path: string, value: unknown): string | null {
  if (path !== "") return null;
  if (value === null || typeof value !== "object") return null;
  const name = (value as { name?: unknown }).name;
  return typeof name === "string" && name.trim() !== "" ? name : null;
}

export function routeDeltaValue(path: string, value: unknown): DeltaRoute {
  // NaN and ±Infinity have no ILP representation. QuestDB does NOT reject
  // them — verified against a live instance, `value=NaN` is accepted and
  // stored — so an unguarded sensor fault silently poisons the numeric
  // column, and every aggregate over that path afterwards. A source
  // reporting a non-finite number is reporting "no reading", which is what
  // recording nothing means. flattenObjectValue already applied this to
  // leaves; the top-level path has to agree.
  if (typeof value === "number")
    return Number.isFinite(value) ? "number" : null;
  if (typeof value === "string") return "string";
  // Booleans are everywhere in Signal K — switch and relay states, pump and
  // valve states, autopilot flags — and used to fall through to null, so a
  // whole class of history was dropped without a trace. They go to
  // signalk_str as "true"/"false" rather than 0/1 in the numeric table: the
  // string round-trips unambiguously (a 1.0 double cannot be told apart from
  // a real numeric channel), and it needs no schema change, so dedup keys,
  // retention and the history API all apply unchanged.
  if (typeof value === "boolean") return "boolean";
  if (
    path === "navigation.position" &&
    value !== null &&
    typeof value === "object" &&
    "latitude" in value &&
    "longitude" in value &&
    Number.isFinite((value as { latitude: unknown }).latitude) &&
    Number.isFinite((value as { longitude: unknown }).longitude)
  )
    return "position";
  // Any other non-null, non-array object: record its scalar leaves
  // individually. Arrays are excluded deliberately — their indices are not
  // stable identities, so `foo.0` would silently mean a different thing from
  // one delta to the next.
  if (value !== null && typeof value === "object" && !Array.isArray(value))
    return "flatten";
  return null;
}

/**
 * Scalar leaves of an object value, as dotted paths.
 *
 *   navigation.attitude {roll: 0.02, yaw: 1.57}
 *     -> navigation.attitude.roll  0.02
 *     -> navigation.attitude.yaw   1.57
 *
 * **One level deep, deliberately.** That covers attitude and effectively every
 * real Signal K object, while a recursive walk would happily write out whole
 * nested payloads (notifications, resource documents) that nobody asked to
 * record. A nested object is therefore skipped, not descended into — and
 * reported, so it is not another silent drop.
 *
 * The leaves are ordinary scalar paths, which is what makes this cheap: no
 * schema change, and dedup, retention, sampling, path filtering and both
 * history APIs apply to them unchanged.
 */
export function flattenObjectValue(
  path: string,
  value: unknown,
): { leaves: FlattenedLeaf[]; skipped: string[] } {
  const leaves: FlattenedLeaf[] = [];
  const skipped: string[] = [];
  if (value === null || typeof value !== "object" || Array.isArray(value)) {
    return { leaves, skipped };
  }
  for (const [key, leaf] of Object.entries(value as Record<string, unknown>)) {
    const leafPath = `${path}.${key}`;
    if (typeof leaf === "number") {
      // NaN and ±Infinity have no ILP representation and would poison the
      // column; a sensor reporting them is reporting "no reading".
      if (Number.isFinite(leaf)) leaves.push({ path: leafPath, value: leaf });
      else skipped.push(leafPath);
    } else if (typeof leaf === "string" || typeof leaf === "boolean") {
      leaves.push({ path: leafPath, value: leaf });
    } else {
      // Nested object, array, null or undefined.
      skipped.push(leafPath);
    }
  }
  return { leaves, skipped };
}
