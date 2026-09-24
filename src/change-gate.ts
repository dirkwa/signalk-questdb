// Change-only recording gate.
//
// Most of a boat's paths do not change from one sample to the next: a switch
// that flips twice a day, an energy counter at rest, a notification's state,
// a tank that is not being used. The throttle alone writes every one of them
// at the sampling rate forever — measured on a live install, 45% of numeric
// rows and 76% of string rows were a path that held one value all day.
//
// So a value equal to the last one WRITTEN for its path, context and source
// is skipped until the heartbeat is due. Every change is written as it
// arrives (subject to the throttle), and the heartbeat bounds how old the
// newest row of a live path can be. That bound is what lets the history API
// carry a held value across the empty buckets in between without also
// carrying a dead sensor's last value forever — see holdMs in history-v2.
//
// Compared against the last value written, not the last one seen: a change
// the throttle drops leaves the gate on the old value, so the next delta
// carrying the new one is still a change and gets written.

interface Written {
  path: string;
  value: unknown;
  at: number;
}

export class ChangeGate {
  // Keyed by context first, so forget() — run for every null on the stream,
  // and some sources send nulls routinely — only walks one vessel's paths.
  private readonly byContext = new Map<string, Map<string, Written>>();
  private count = 0;

  // Same pressure valve as Throttle: transient AIS contexts churn for months,
  // and every (path, context, source) would otherwise stay until shutdown.
  // An entry older than the heartbeat gates nothing — the next value is due
  // anyway — so those are swept first; a full clear only costs one extra
  // row per path.
  constructor(
    private readonly heartbeatMs: number,
    private readonly maxEntries = 20_000,
  ) {}

  get size(): number {
    return this.count;
  }

  /**
   * True when `value` is what this path, context and source last wrote and
   * the heartbeat is not yet due — the value adds nothing a reader cannot
   * carry forward. A non-positive heartbeat turns the gate off.
   */
  isRepeat(
    path: string,
    context: string,
    source: string | undefined,
    value: unknown,
    now: number,
  ): boolean {
    if (this.heartbeatMs <= 0) return false;
    const entry = this.byContext.get(context)?.get(key(path, source));
    return (
      entry !== undefined &&
      entry.value === value &&
      now - entry.at < this.heartbeatMs
    );
  }

  /** Records that `value` was written, so later equal values are repeats. */
  wrote(
    path: string,
    context: string,
    source: string | undefined,
    value: unknown,
    now: number,
  ): void {
    if (this.heartbeatMs <= 0) return;
    const k = key(path, source);
    let paths = this.byContext.get(context);
    if (!paths?.has(k)) {
      if (this.count >= this.maxEntries) {
        this.evict(now);
        paths = this.byContext.get(context);
      }
      if (!paths) {
        paths = new Map();
        this.byContext.set(context, paths);
      }
      this.count += 1;
    }
    paths.set(k, { path, value, at: now });
  }

  /**
   * Forgets a path in a context, for every source, along with any leaves it
   * was flattened into. Signal K reports a path going stale as a null value;
   * without this, the reading that comes back after the gap would be skipped
   * whenever it equals the one before it, and the gap would read as a value
   * held straight through it.
   */
  forget(path: string, context: string): void {
    const paths = this.byContext.get(context);
    if (!paths) return;
    const leafPrefix = `${path}.`;
    for (const [k, entry] of paths) {
      if (entry.path === path || entry.path.startsWith(leafPrefix)) {
        paths.delete(k);
        this.count -= 1;
      }
    }
    if (paths.size === 0) this.byContext.delete(context);
  }

  clear(): void {
    this.byContext.clear();
    this.count = 0;
  }

  private evict(now: number): void {
    for (const [context, paths] of this.byContext) {
      for (const [k, entry] of paths) {
        if (now - entry.at >= this.heartbeatMs) {
          paths.delete(k);
          this.count -= 1;
        }
      }
      if (paths.size === 0) this.byContext.delete(context);
    }
    // Cleared unless the sweep got well below the cap: a sweep that frees a
    // handful would run again after that many new keys, walking every entry
    // on nearly every new key of a busy AIS stream.
    if (this.count >= this.maxEntries * 0.75) this.clear();
  }
}

// A Signal K path cannot contain `|`, so two (path, source) pairs never share
// a key; contexts are kept apart one level up, in byContext.
function key(path: string, source: string | undefined): string {
  return `${path}|${source ?? ""}`;
}
