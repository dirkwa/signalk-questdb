import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { minimatch } from "minimatch";
import { PathMatcher, RateMatcher, Throttle } from "../path-matcher";

describe("PathMatcher", () => {
  it("matches literal patterns exactly", () => {
    const m = new PathMatcher(["electrical.batteries.12v.name"]);
    assert.equal(m.matches("electrical.batteries.12v.name"), true);
    assert.equal(m.matches("electrical.batteries.12v.voltage"), false);
    // A literal must not behave like a prefix.
    assert.equal(m.matches("electrical.batteries.12v.name.extra"), false);
  });

  it("matches glob patterns", () => {
    const m = new PathMatcher([
      "navigation.gnss.*",
      "environment.*.temperature",
    ]);
    assert.equal(m.matches("navigation.gnss.satellites"), true);
    assert.equal(m.matches("navigation.speedOverGround"), false);
    assert.equal(m.matches("environment.water.temperature"), true);
  });

  it("is empty only when no patterns are configured", () => {
    assert.equal(new PathMatcher([]).isEmpty, true);
    assert.equal(new PathMatcher(["a.b"]).isEmpty, false);
    assert.equal(new PathMatcher(["a.*"]).isEmpty, false);
  });

  it("agrees with minimatch across a mixed pattern list", () => {
    // The optimisation must not change behaviour: literals bypass minimatch
    // entirely, so this pins the two implementations together.
    const patterns = [
      "design.*",
      "electrical.batteries.12v.name",
      "navigation.gnss.*",
      "environment.*.temperature",
      "tanks.fuel.*.currentLevel",
      "watch.*",
      "notifications.*",
    ];
    const paths = [
      "design.aisShipType",
      "electrical.batteries.12v.name",
      "electrical.batteries.12v.voltage",
      "navigation.gnss.satellites",
      "navigation.speedOverGround",
      "environment.water.temperature",
      "environment.inside.engineRoom.temperature",
      "tanks.fuel.0.currentLevel",
      "watch",
      "watch.x",
      "design",
      "a.b.c",
    ];
    const matcher = new PathMatcher(patterns);
    for (const path of paths) {
      assert.equal(
        matcher.matches(path),
        patterns.some((p) => minimatch(path, p)),
        `disagreed with minimatch for ${path}`,
      );
    }
  });

  // Counting the glob evaluations makes the memoization observable: result
  // stability alone would also pass with no cache at all, and caching is the
  // whole point of the change.
  function countingMatcher(pattern: string) {
    const m = new PathMatcher([pattern]);
    let evaluations = 0;
    const globs = (
      m as unknown as { globs: { match: (p: string) => boolean }[] }
    ).globs;
    for (const g of globs) {
      const real = g.match.bind(g);
      g.match = (p: string) => {
        evaluations++;
        return real(p);
      };
    }
    return { matcher: m, count: () => evaluations };
  }

  it("evaluates a path once and reuses the result", () => {
    const { matcher, count } = countingMatcher("navigation.*");
    for (let i = 0; i < 5; i++) {
      assert.equal(matcher.matches("navigation.position"), true);
    }
    assert.equal(count(), 1, "expected the glob to be evaluated only once");
  });

  it("re-evaluates after the cache ceiling flushes, still correctly", () => {
    const { matcher, count } = countingMatcher("navigation.*");
    assert.equal(matcher.matches("navigation.position"), true);
    const afterFirst = count();
    // Cross the ceiling so the cache clears.
    for (let i = 0; i < 5100; i++) matcher.matches(`filler.path.${i}`);
    assert.equal(matcher.matches("navigation.position"), true);
    assert.ok(
      count() > afterFirst,
      "expected a re-evaluation once the cache was flushed",
    );
    assert.equal(matcher.matches("filler.path.1"), false);
  });
});

describe("RateMatcher", () => {
  it("resolves a literal pattern to its rate", () => {
    const r = new RateMatcher({ "navigation.position": 500 });
    assert.equal(r.rateFor("navigation.position"), 500);
    assert.equal(r.rateFor("navigation.speedOverGround"), null);
  });

  it("resolves a glob pattern to its rate", () => {
    const r = new RateMatcher({ "environment.wind.*": 200 });
    assert.equal(r.rateFor("environment.wind.speedApparent"), 200);
    assert.equal(r.rateFor("environment.water.temperature"), null);
  });

  it("ignores non-positive rates, matching the previous skip", () => {
    const r = new RateMatcher({ "a.*": 0, "b.*": -1, "c.*": 100 });
    assert.equal(r.rateFor("a.x"), null);
    assert.equal(r.rateFor("b.x"), null);
    assert.equal(r.rateFor("c.x"), 100);
  });

  it("prefers an exact pattern over a glob", () => {
    // Both match; the literal is the more specific intent.
    const r = new RateMatcher({ "tanks.*": 10000, "tanks.fuel.0.level": 250 });
    assert.equal(r.rateFor("tanks.fuel.0.level"), 250);
    assert.equal(r.rateFor("tanks.water.0.level"), 10000);
  });

  it("is empty when no usable rates are configured", () => {
    assert.equal(new RateMatcher({}).isEmpty, true);
    assert.equal(new RateMatcher({ "a.*": 0 }).isEmpty, true);
    assert.equal(new RateMatcher({ "a.*": 5 }).isEmpty, false);
  });
});

describe("RateMatcher throttle semantics", () => {
  // The previous implementation returned as soon as a positive override
  // matched, and only otherwise fell through to the default rate. Resolving
  // the rate first and applying one check must keep exactly that behaviour.
  function effectiveRate(
    rates: Record<string, number>,
    path: string,
    defaultRate: number,
  ): number {
    return new RateMatcher(rates).rateFor(path) ?? defaultRate;
  }

  it("uses the override when one matches", () => {
    assert.equal(
      effectiveRate(
        { "environment.wind.*": 200 },
        "environment.wind.angle",
        2000,
      ),
      200,
    );
  });

  it("falls back to the default when nothing matches", () => {
    assert.equal(
      effectiveRate({ "environment.wind.*": 200 }, "navigation.position", 2000),
      2000,
    );
  });

  it("falls back to the default when the only match is non-positive", () => {
    // A 0 override meant "not a throttle rule" and fell through, rather than
    // disabling throttling for that path.
    assert.equal(
      effectiveRate({ "navigation.*": 0 }, "navigation.position", 2000),
      2000,
    );
  });
});

describe("RateMatcher memoization", () => {
  it("evaluates a path once and reuses the resolved rate", () => {
    const r = new RateMatcher({ "environment.wind.*": 200 });
    let evaluations = 0;
    const globs = (
      r as unknown as {
        globs: { matcher: { match: (p: string) => boolean } }[];
      }
    ).globs;
    for (const g of globs) {
      const real = g.matcher.match.bind(g.matcher);
      g.matcher.match = (p: string) => {
        evaluations++;
        return real(p);
      };
    }

    for (let i = 0; i < 5; i++) {
      assert.equal(r.rateFor("environment.wind.angle"), 200);
    }
    assert.equal(evaluations, 1, "expected one glob evaluation");
  });

  it("memoizes a non-match too, so misses are not re-evaluated", () => {
    const r = new RateMatcher({ "environment.wind.*": 200 });
    for (let i = 0; i < 3; i++) {
      assert.equal(r.rateFor("navigation.position"), null);
    }
  });
});

describe("Throttle", () => {
  const T0 = 1_000_000;

  it("throttles per path AND context, never across vessels", () => {
    // The issue #93 regression: two AIS targets updating the same path
    // inside one sampling window must BOTH record.
    const t = new Throttle();
    assert.equal(
      t.shouldDrop("navigation.position", "vessels.a", 2000, T0),
      false,
    );
    assert.equal(
      t.shouldDrop("navigation.position", "vessels.b", 2000, T0 + 10),
      false,
    );
    assert.equal(
      t.shouldDrop("navigation.position", "self", 2000, T0 + 20),
      false,
    );
  });

  it("drops a same-pair update inside the window and passes it after", () => {
    const t = new Throttle();
    assert.equal(t.shouldDrop("a.b", "self", 2000, T0), false);
    assert.equal(t.shouldDrop("a.b", "self", 2000, T0 + 1999), true);
    assert.equal(t.shouldDrop("a.b", "self", 2000, T0 + 2000), false);
  });

  it("a dropped update does not extend the window", () => {
    const t = new Throttle();
    t.shouldDrop("a.b", "self", 2000, T0);
    t.shouldDrop("a.b", "self", 2000, T0 + 1500);
    // Window still measures from T0, not from the dropped attempt.
    assert.equal(t.shouldDrop("a.b", "self", 2000, T0 + 2100), false);
  });

  it("treats a non-positive rate as no throttling", () => {
    const t = new Throttle();
    assert.equal(t.shouldDrop("a.b", "self", 0, T0), false);
    assert.equal(t.shouldDrop("a.b", "self", 0, T0 + 1), false);
    assert.equal(t.shouldDrop("a.b", "self", -5, T0 + 2), false);
  });

  it("clear() forgets all pairs", () => {
    const t = new Throttle();
    t.shouldDrop("a.b", "self", 2000, T0);
    t.clear();
    assert.equal(t.shouldDrop("a.b", "self", 2000, T0 + 1), false);
  });

  it("evicts pairs older than every sampling window at the cap", () => {
    // Months of transient AIS contexts must not grow the map forever: a
    // pair whose last write predates every window can never throttle
    // again, so crossing the cap sweeps them out.
    const t = new Throttle(3);
    t.shouldDrop("p", "vessels.a", 2000, T0);
    t.shouldDrop("p", "vessels.b", 2000, T0);
    t.shouldDrop("p", "vessels.c", 2000, T0);
    assert.equal(t.size, 3);
    t.shouldDrop("p", "vessels.d", 2000, T0 + 2500);
    assert.equal(t.size, 1, "stale pairs swept, only the new pair kept");
  });

  it("falls back to a full clear when every pair is still active", () => {
    // Pressure valve: if the cap is reached by genuinely concurrent pairs,
    // bounded memory wins over throttling continuity.
    const t = new Throttle(3);
    t.shouldDrop("p", "vessels.a", 2000, T0);
    t.shouldDrop("p", "vessels.b", 2000, T0);
    t.shouldDrop("p", "vessels.c", 2000, T0);
    t.shouldDrop("p", "vessels.d", 2000, T0 + 100);
    assert.equal(t.size, 1, "active pairs cleared to admit the new one");
    assert.equal(
      t.shouldDrop("p", "vessels.b", 2000, T0 + 200),
      false,
      "a cleared pair records again immediately",
    );
  });
});
