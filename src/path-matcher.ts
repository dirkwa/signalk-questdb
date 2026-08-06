// Glob matching for the per-delta hot path.
//
// The filter and throttle checks run on EVERY incoming delta — 100+/s on a
// busy vessel. Calling minimatch(path, pattern) per pattern per delta parses
// and compiles the glob into a RegExp every single time. With a real filter
// list from the field (89 patterns) that measured ~874µs per delta, i.e. ~87%
// of one core at 100 deltas/s, which pinned Node's event loop at 100% CPU and
// starved everything else in the server.
//
// Three things fix it, in increasing order of payoff:
//   1. Exact patterns (no glob metacharacters) go in a Set — most real filter
//      lists are mostly literals, and a Set hit is O(1) with no regex at all.
//   2. Real globs are compiled to Minimatch objects ONCE, at construction.
//   3. Results are memoized per path. Signal K paths are a small, stable set
//      that repeats forever, so after the first delta of each path the answer
//      is a single Map lookup.

import { Minimatch } from "minimatch";

// Characters that make a pattern a glob rather than a literal. Anything
// without one of these can be compared with ===, skipping minimatch entirely.
const GLOB_CHARS = /[*?[\]{}!+@()|]/;

// Ceiling on memoized paths, so a source emitting unbounded distinct paths
// (a misbehaving plugin, or AIS contexts folded into paths) cannot grow this
// without limit on a Pi. Far above any real vessel's path count.
const MAX_CACHE_ENTRIES = 5000;

export class PathMatcher {
  private readonly literals: Set<string>;
  private readonly globs: Minimatch[];
  private readonly cache = new Map<string, boolean>();

  constructor(patterns: string[]) {
    this.literals = new Set();
    this.globs = [];
    for (const pattern of patterns ?? []) {
      if (GLOB_CHARS.test(pattern)) {
        this.globs.push(new Minimatch(pattern));
      } else {
        this.literals.add(pattern);
      }
    }
  }

  get isEmpty(): boolean {
    return this.literals.size === 0 && this.globs.length === 0;
  }

  matches(path: string): boolean {
    const cached = this.cache.get(path);
    if (cached !== undefined) return cached;

    const result =
      this.literals.has(path) || this.globs.some((g) => g.match(path));

    // Simple size guard rather than an LRU: the cache is a pure function of
    // (patterns, path), so dropping it wholesale only costs a recompute.
    if (this.cache.size >= MAX_CACHE_ENTRIES) this.cache.clear();
    this.cache.set(path, result);
    return result;
  }
}

// Throttle rates are pattern -> interval, and the winning pattern's interval
// is what matters, so this resolves a path to its rate rather than a boolean.
// Same construction cost and same memoization; null means "no pattern
// matched", i.e. use the default rate.
export class RateMatcher {
  private readonly literals = new Map<string, number>();
  private readonly globs: { matcher: Minimatch; rate: number }[] = [];
  private readonly cache = new Map<string, number | null>();

  constructor(rates: Record<string, number>) {
    for (const [pattern, rate] of Object.entries(rates ?? {})) {
      // A non-positive rate means "no throttling", which the old code
      // skipped; keeping that here avoids paying for it per delta.
      if (!(rate > 0)) continue;
      if (GLOB_CHARS.test(pattern)) {
        this.globs.push({ matcher: new Minimatch(pattern), rate });
      } else {
        this.literals.set(pattern, rate);
      }
    }
  }

  get isEmpty(): boolean {
    return this.literals.size === 0 && this.globs.length === 0;
  }

  // First match wins, literals before globs. Object.entries order decided
  // this before; with a literal and a glob both matching, the exact pattern
  // is the more specific intent.
  rateFor(path: string): number | null {
    const cached = this.cache.get(path);
    if (cached !== undefined) return cached;

    let result: number | null = this.literals.get(path) ?? null;
    if (result === null) {
      for (const { matcher, rate } of this.globs) {
        if (matcher.match(path)) {
          result = rate;
          break;
        }
      }
    }

    if (this.cache.size >= MAX_CACHE_ENTRIES) this.cache.clear();
    this.cache.set(path, result);
    return result;
  }
}

// Sampling gate, keyed by path AND context: the rate limits each vessel's
// stream individually. A per-path-only bucket lets whichever AIS target
// transmits first claim the slot and drops every other vessel's update
// inside the window — with recordOthers and a fleet in range, that is one
// position per interval fleet-wide instead of per target (issue #93).
// Rate overrides stay per PATH (resolved by the caller via RateMatcher):
// an override describes the channel, not the vessel.
export class Throttle {
  private readonly lastWrite = new Map<string, number>();
  private maxRateMs = 0;

  // Transient AIS contexts churn for months on a fleet install, and every
  // (path, context) pair would otherwise stay in the map until shutdown.
  // The cap triggers a sweep of pairs older than every sampling window —
  // those can never throttle again — and falls back to a full clear when
  // that many pairs are genuinely active inside one window (a momentary
  // under-throttle beats unbounded growth; same pressure valve as
  // RateMatcher's MAX_CACHE_ENTRIES).
  constructor(private readonly maxEntries = 10_000) {}

  get size(): number {
    return this.lastWrite.size;
  }

  /**
   * True when this path+context pair already wrote within the past
   * `minMs`; records `now` as the pair's last write otherwise. A
   * non-positive rate disables throttling for the call.
   */
  shouldDrop(
    path: string,
    context: string,
    minMs: number,
    now: number,
  ): boolean {
    if (minMs <= 0) return false;
    if (minMs > this.maxRateMs) this.maxRateMs = minMs;
    const key = `${path}|${context}`;
    const last = this.lastWrite.get(key) ?? 0;
    if (now - last < minMs) return true;
    if (this.lastWrite.size >= this.maxEntries && !this.lastWrite.has(key)) {
      this.evict(now);
    }
    this.lastWrite.set(key, now);
    return false;
  }

  private evict(now: number): void {
    for (const [key, last] of this.lastWrite) {
      if (now - last >= this.maxRateMs) this.lastWrite.delete(key);
    }
    if (this.lastWrite.size >= this.maxEntries) this.lastWrite.clear();
  }

  clear(): void {
    this.lastWrite.clear();
    this.maxRateMs = 0;
  }
}
