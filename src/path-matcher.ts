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
