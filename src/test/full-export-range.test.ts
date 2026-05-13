import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { buildFullExportWhere } from "../full-export-range";

describe("buildFullExportWhere", () => {
  it("returns empty WHERE when both params are unset (full-table behavior)", () => {
    const r = buildFullExportWhere(undefined, undefined);
    assert.deepStrictEqual(r, { ok: true, where: "" });
  });

  it("builds a half-open range when both params are valid ISO 8601", () => {
    const r = buildFullExportWhere(
      "2026-05-04T00:00:00Z",
      "2026-05-11T00:00:00Z",
    );
    assert.equal(r.ok, true);
    if (!r.ok) return;
    assert.equal(
      r.where,
      " WHERE ts >= '2026-05-04T00:00:00.000Z' AND ts < '2026-05-11T00:00:00.000Z'",
    );
  });

  it("rejects when only `from` is set", () => {
    const r = buildFullExportWhere("2026-05-04T00:00:00Z", undefined);
    assert.equal(r.ok, false);
    if (r.ok) return;
    assert.match(r.error, /set together/);
  });

  it("rejects when only `to` is set", () => {
    const r = buildFullExportWhere(undefined, "2026-05-11T00:00:00Z");
    assert.equal(r.ok, false);
    if (r.ok) return;
    assert.match(r.error, /set together/);
  });

  it("rejects when `from` is not parseable", () => {
    const r = buildFullExportWhere("not-a-date", "2026-05-11T00:00:00Z");
    assert.equal(r.ok, false);
    if (r.ok) return;
    assert.match(r.error, /ISO 8601/);
  });

  it("rejects when `to` is not parseable", () => {
    const r = buildFullExportWhere("2026-05-04T00:00:00Z", "garbage");
    assert.equal(r.ok, false);
    if (r.ok) return;
    assert.match(r.error, /ISO 8601/);
  });

  it("rejects when from >= to (would yield empty or backwards range)", () => {
    const equal = buildFullExportWhere(
      "2026-05-04T00:00:00Z",
      "2026-05-04T00:00:00Z",
    );
    assert.equal(equal.ok, false);
    if (equal.ok) return;
    assert.match(equal.error, /strictly before/);

    const reversed = buildFullExportWhere(
      "2026-05-11T00:00:00Z",
      "2026-05-04T00:00:00Z",
    );
    assert.equal(reversed.ok, false);
    if (reversed.ok) return;
    assert.match(reversed.error, /strictly before/);
  });

  it("normalises both to ISO-with-millis (round-trip via Date)", () => {
    // Input without millis; output should still have them — proves we go
    // through Date.toISOString().
    const r = buildFullExportWhere(
      "2026-05-04T12:34:56Z",
      "2026-05-04T12:34:57Z",
    );
    assert.equal(r.ok, true);
    if (!r.ok) return;
    assert.ok(r.where.includes("2026-05-04T12:34:56.000Z"));
    assert.ok(r.where.includes("2026-05-04T12:34:57.000Z"));
  });
});
