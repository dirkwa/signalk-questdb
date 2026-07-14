// Decides which table a delta value belongs in, keyed by JS type — with one
// path-based exception: signalk_position is a pure vessel-track table (rows
// are keyed on ts+context only, no path column), so it must hold
// navigation.position exclusively. Other lat/lon-object paths — e.g.
// navigation.anchor.position, which anchor plugins re-emit on every fix
// while watching — would interleave with the real track and make history
// reads zig-zag between boat and anchor. Those (and all other objects)
// return null: not recorded.
export type DeltaRoute = "number" | "string" | "position" | null;

export function routeDeltaValue(path: string, value: unknown): DeltaRoute {
  if (typeof value === "number") return "number";
  if (typeof value === "string") return "string";
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
  return null;
}
