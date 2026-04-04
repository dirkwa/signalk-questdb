import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  validateIdentifier,
  validateTimestamp,
  isReadOnlySQL,
} from "../query-client";

describe("validateIdentifier", () => {
  it("accepts valid Signal K paths", () => {
    assert.equal(
      validateIdentifier("navigation.speedOverGround"),
      "navigation.speedOverGround",
    );
    assert.equal(validateIdentifier("self"), "self");
    assert.equal(
      validateIdentifier("vessels.urn:mrn:imo:mmsi:123456789"),
      "vessels.urn:mrn:imo:mmsi:123456789",
    );
  });

  it("rejects SQL injection attempts", () => {
    assert.throws(() => validateIdentifier("'; DROP TABLE signalk;--"));
    assert.throws(() => validateIdentifier("path OR 1=1"));
    assert.throws(() => validateIdentifier("path\nSELECT"));
  });
});

describe("validateTimestamp", () => {
  it("accepts valid ISO timestamps", () => {
    const result = validateTimestamp("2024-06-15T12:00:00.000Z");
    assert.equal(result, "2024-06-15T12:00:00.000Z");
  });

  it("normalizes various timestamp formats to ISO", () => {
    const result = validateTimestamp("2024-06-15");
    assert.ok(result.startsWith("2024-06-15"), `Got: ${result}`);
  });

  it("rejects invalid timestamps", () => {
    assert.throws(() => validateTimestamp("not-a-date"));
    assert.throws(() => validateTimestamp(""));
  });
});

describe("isReadOnlySQL", () => {
  it("allows SELECT queries", () => {
    assert.ok(isReadOnlySQL("SELECT count() FROM signalk"));
    assert.ok(
      isReadOnlySQL("  SELECT ts, value FROM signalk WHERE path = 'nav'"),
    );
    assert.ok(isReadOnlySQL("WITH cte AS (SELECT 1) SELECT * FROM cte"));
  });

  it("allows SHOW queries", () => {
    assert.ok(isReadOnlySQL("SHOW TABLES"));
    assert.ok(isReadOnlySQL("SHOW COLUMNS FROM signalk"));
  });

  it("blocks DDL and DML", () => {
    assert.ok(!isReadOnlySQL("DROP TABLE signalk"));
    assert.ok(!isReadOnlySQL("ALTER TABLE signalk ADD COLUMN x INT"));
    assert.ok(!isReadOnlySQL("INSERT INTO signalk VALUES (1)"));
    assert.ok(!isReadOnlySQL("DELETE FROM signalk WHERE ts < now()"));
    assert.ok(!isReadOnlySQL("CREATE TABLE evil (x INT)"));
    assert.ok(!isReadOnlySQL("TRUNCATE TABLE signalk"));
  });

  it("blocks DDL hidden in SELECT", () => {
    assert.ok(
      !isReadOnlySQL("SELECT 1; DROP TABLE signalk"),
      "Should block DROP even after SELECT",
    );
  });
});
