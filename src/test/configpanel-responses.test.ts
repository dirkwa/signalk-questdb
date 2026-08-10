import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { toMigrationSources } from "../configpanel/responses.js";

// The panel casts every `res.json()` to a contract type, which asserts a
// shape rather than checking one. The plugin's own handlers are well
// behaved; a reverse proxy, auth gateway or captive portal answering 200 on
// their behalf is not. These are the two responses the panel dereferences
// without a guard downstream, so a wrong shape reaches React as state.

/**
 * Both call sites read `await res.json().catch(() => null)`. A 200 carrying
 * HTML — captive portal, auth redirect, proxy error page — rejects in json()
 * before any shape check could run, so the null has to come from there
 * rather than from the normalizer. This mirrors that call.
 */
const parsedBody = async (raw: string, contentType = "application/json") => {
  const res = new Response(raw, {
    status: 200,
    headers: { "content-type": contentType },
  });
  return res.json().catch(() => null);
};

describe("a 200 response whose body is not JSON", () => {
  it("degrades to 'nothing detected' for migration sources", async () => {
    const body = await parsedBody("<html>login</html>", "text/html");
    // The user sees "No InfluxDB found" rather than
    // "Detection failed: Unexpected token '<'".
    assert.deepEqual(toMigrationSources(body), []);
  });

  it("handles an empty body the same way", async () => {
    const body = await parsedBody("");
    assert.deepEqual(toMigrationSources(body), []);
  });
});

describe("toMigrationSources", () => {
  it("unwraps the sources array", () => {
    const sources = [
      { type: "influxdb2", url: "http://localhost:8086", status: "reachable" },
    ];
    assert.deepEqual(toMigrationSources({ sources }), sources);
  });

  it("preserves an empty result, which means 'nothing detected'", () => {
    assert.deepEqual(toMigrationSources({ sources: [] }), []);
  });

  for (const [label, body] of [
    ["a body with no sources key", {}],
    ["an error envelope", { error: "Invalid URL" }],
    ["sources of the wrong type", { sources: "none" }],
    ["null", null],
    ["undefined", undefined],
    ["a bare array", []],
  ] as const) {
    it(`yields an empty list for ${label}`, () => {
      const result = toMigrationSources(body);
      assert.ok(Array.isArray(result), "must always return an array");
      assert.equal(result.length, 0);
      // Without this the panel reported "Detection failed: Cannot read
      // properties of undefined" instead of "No InfluxDB found", and put
      // `undefined` into state the type declares as an array.
      assert.doesNotThrow(() => result.length === 0);
    });
  }

  it("drops malformed members but keeps the valid ones", () => {
    // Members are rendered field-by-field (src.type, src.url, src.status),
    // so a null entry would throw while drawing the detected-sources list.
    const body = {
      sources: [
        { type: "influxdb2", url: "http://localhost:8086", status: "ok" },
        null,
        { type: "influxdb1" },
        { type: "influxdb1", url: "http://boat:8086", status: "ok" },
      ],
    };
    const result = toMigrationSources(body);

    assert.deepEqual(
      result.map((s) => s.url),
      ["http://localhost:8086", "http://boat:8086"],
    );
    assert.doesNotThrow(() => result.map((s) => s.type === "influxdb2"));
  });

  it("drops sources whose rendered fields are not strings", () => {
    // status and version are rendered as React children; a plain object
    // there throws "Objects are not valid as a React child" and unmounts
    // the panel, the same blank screen an unguarded array causes.
    const result = toMigrationSources({
      sources: [
        {
          type: "influxdb2",
          url: "http://ok:8086",
          status: "ok",
          version: "2.7",
        },
        { type: "influxdb2", url: "http://a:8086", status: { code: 200 } },
        { type: "influxdb1", url: "http://b:8086" },
        {
          type: "influxdb1",
          url: "http://c:8086",
          status: "ok",
          version: { major: 1 },
        },
        // version is genuinely optional in the contract
        { type: "influxdb1", url: "http://d:8086", status: "unreachable" },
      ],
    });

    assert.deepEqual(
      result.map((s) => s.url),
      ["http://ok:8086", "http://d:8086"],
    );
    for (const s of result) {
      assert.equal(typeof s.status, "string");
      assert.ok(s.version === undefined || typeof s.version === "string");
    }
  });
});
