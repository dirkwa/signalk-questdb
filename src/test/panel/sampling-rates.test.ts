import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  formatSamplingRates,
  parseSamplingRates,
} from "../../configpanel/sampling-rates.js";

describe("per-path sampling rates in the panel", () => {
  it("parses one pattern and rate per line, wire speed included", () => {
    const { rates, invalid } = parseSamplingRates(
      "environment.wind.* 0\n  tanks.*\t10000  \n\nnavigation.attitude 200",
    );
    assert.deepEqual(rates, {
      "environment.wind.*": 0,
      "tanks.*": 10000,
      "navigation.attitude": 200,
    });
    assert.deepEqual(invalid, []);
  });

  it("keeps the order of the lines, which is the order patterns are tried", () => {
    const { rates } = parseSamplingRates("b.* 1\na.* 2");
    assert.deepEqual(Object.keys(rates), ["b.*", "a.*"]);
  });

  it("reports a line without a rate, with a negative rate or with extra words", () => {
    const { rates, invalid } = parseSamplingRates(
      "environment.wind.*\ntanks.* -5\nnavigation.* 200 ms\nelectrical.* fast\nok.* 5",
    );
    assert.deepEqual(rates, { "ok.*": 5 });
    assert.deepEqual(invalid, [
      "environment.wind.*",
      "tanks.* -5",
      "navigation.* 200 ms",
      "electrical.* fast",
    ]);
  });

  it("round-trips the stored rates", () => {
    const stored = { "environment.wind.*": 0, "tanks.*": 10000 };
    assert.equal(
      formatSamplingRates(stored),
      "environment.wind.* 0\ntanks.* 10000",
    );
    assert.deepEqual(
      parseSamplingRates(formatSamplingRates(stored)).rates,
      stored,
    );
  });

  it("shows nothing for a config without rates", () => {
    assert.equal(formatSamplingRates(undefined), "");
    assert.deepEqual(parseSamplingRates(""), { rates: {}, invalid: [] });
  });
});
