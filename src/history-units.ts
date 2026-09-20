/**
 * What the history provider needs to know about a path: its units, so a
 * path recorded in radians is averaged as a vector.
 */
interface MetadataSource {
  selfContext: string;
  /** The full data model; `<context>.<path>.meta` holds what a user set. */
  getPath?: (path: string) => unknown;
  /** The static catalog of standard paths. */
  getMetadata?: (path: string) => { units?: string } | undefined;
}

/**
 * A path's units, in the request's context. The live model's meta comes
 * first — that is where units set on a custom path live — and the static
 * catalog answers for a standard path that is not live at the moment. The
 * own vessel may be asked for as vessels.self, as self, or by its identity;
 * both sources know it as vessels.self.
 */
export function unitsResolver(
  app: MetadataSource,
): (path: string, context: string) => string | undefined {
  return (path, context) => {
    const ctx =
      context === "self" || context === app.selfContext
        ? "vessels.self"
        : context;
    const full = `${ctx}.${path}`;
    const live = app.getPath?.(`${full}.meta`);
    const liveUnits =
      typeof live === "object" && live !== null
        ? (live as { units?: unknown }).units
        : undefined;
    return typeof liveUnits === "string"
      ? liveUnits
      : app.getMetadata?.(full)?.units;
  };
}
