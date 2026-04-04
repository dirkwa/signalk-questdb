import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { Temporal } from "@js-temporal/polyfill";
import { resolveTimeRange } from "../time-range";

describe("resolveTimeRange", () => {
  it("resolves from + to", () => {
    const from = Temporal.Instant.from("2024-01-01T00:00:00Z");
    const to = Temporal.Instant.from("2024-01-02T00:00:00Z");
    const result = resolveTimeRange({ from, to });
    assert.ok(result.from.includes("2024-01-01"));
    assert.ok(result.to.includes("2024-01-02"));
  });

  it("resolves from + duration", () => {
    const from = Temporal.Instant.from("2024-01-01T00:00:00Z");
    const duration = Temporal.Duration.from({ hours: 1 });
    const result = resolveTimeRange({ from, duration });
    assert.ok(result.from.includes("2024-01-01"));
    assert.ok(result.to.includes("2024-01-01T01:00:00"));
  });

  it("resolves to + duration", () => {
    const to = Temporal.Instant.from("2024-01-02T00:00:00Z");
    const duration = Temporal.Duration.from({ hours: 1 });
    const result = resolveTimeRange({ to, duration });
    assert.ok(result.from.includes("2024-01-01T23:00:00"));
    assert.ok(result.to.includes("2024-01-02"));
  });

  it("resolves from only (to defaults to now)", () => {
    const from = Temporal.Instant.from("2024-01-01T00:00:00Z");
    const result = resolveTimeRange({ from });
    assert.ok(result.from.includes("2024-01-01"));
    // to should be close to now
    const toDate = new Date(result.to);
    assert.ok(Date.now() - toDate.getTime() < 5000);
  });

  it("resolves duration only (from = now - duration)", () => {
    const duration = Temporal.Duration.from({ minutes: 30 });
    const result = resolveTimeRange({ duration });
    const from = new Date(result.from);
    const to = new Date(result.to);
    const diffMs = to.getTime() - from.getTime();
    // Should be approximately 30 minutes
    assert.ok(
      Math.abs(diffMs - 30 * 60 * 1000) < 5000,
      `Expected ~30min diff, got ${diffMs}ms`,
    );
  });

  it("resolves numeric duration (seconds)", () => {
    const from = Temporal.Instant.from("2024-01-01T00:00:00Z");
    const result = resolveTimeRange({ from, duration: 3600 });
    assert.ok(result.to.includes("2024-01-01T01:00:00"));
  });

  it("throws on empty params", () => {
    assert.throws(() => resolveTimeRange({}));
  });
});
