import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  toMigrationBuckets,
  toMigrationMeasurements,
  toMigrationRange,
  toMigrationSources,
  toMigrationStatus,
} from "../configpanel/responses.js";

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

describe("toMigrationBuckets", () => {
  it("keeps well-formed buckets", () => {
    const out = toMigrationBuckets({
      buckets: [{ name: "boatdata", id: "abc", retentionSeconds: 0 }],
    });
    assert.strictEqual(out.length, 1);
    assert.strictEqual(out[0].name, "boatdata");
  });

  // Every field is rendered as a React child; an object there throws
  // "Objects are not valid as a React child" and blanks the whole panel.
  it("drops entries whose name is not a string", () => {
    assert.deepStrictEqual(
      toMigrationBuckets({ buckets: [{ name: { evil: true } }] }),
      [],
    );
  });

  it("a body without the field is empty, not a throw", () => {
    assert.deepStrictEqual(toMigrationBuckets(null), []);
    assert.deepStrictEqual(toMigrationBuckets({ buckets: "nope" }), []);
  });
});

describe("toMigrationMeasurements", () => {
  it("keeps well-formed measurements", () => {
    const out = toMigrationMeasurements({
      measurements: [{ name: "navigation.position", fields: ["lat", "lon"] }],
    });
    assert.strictEqual(out[0].fields.length, 2);
  });

  // `fields` is mapped over during render.
  it("drops an entry whose fields is not an array", () => {
    assert.deepStrictEqual(
      toMigrationMeasurements({
        measurements: [{ name: "x", fields: "not-an-array" }],
      }),
      [],
    );
  });
});

describe("toMigrationStatus", () => {
  it("reads a running run", () => {
    const run = toMigrationStatus({
      run: {
        id: "m1",
        state: "running",
        url: "http://localhost:8086",
        bucket: "boatdata",
        startedAt: "2024-03-01T00:00:00Z",
        progress: { read: 5, written: 4, skipped: 1 },
      },
    });
    assert.strictEqual(run?.state, "running");
    assert.strictEqual(run?.progress.written, 4);
  });

  // The counters drive a rendered number; undefined would print as NaN.
  it("missing counters become 0 rather than undefined", () => {
    const run = toMigrationStatus({
      run: { id: "m1", state: "done", progress: {} },
    });
    assert.strictEqual(run?.progress.read, 0);
    assert.strictEqual(run?.progress.measurementsTotal, 0);
  });

  it("an unknown state is rejected outright", () => {
    assert.strictEqual(
      toMigrationStatus({ run: { id: "m1", state: "exploded" } }),
      undefined,
    );
  });

  it("no run yields undefined", () => {
    assert.strictEqual(toMigrationStatus({}), undefined);
    assert.strictEqual(toMigrationStatus(null), undefined);
  });
});

describe("toMigrationRange", () => {
  it("converts a valid datetime-local pair to ISO", () => {
    const r = toMigrationRange("2024-03-01T12:00", "2024-03-02T12:00");
    assert.ok(!("error" in r));
    assert.ok("from" in r);
    // datetime-local is LOCAL time; the conversion must produce the same
    // instants the browser means, not a naive string append.
    assert.strictEqual(r.from, new Date("2024-03-01T12:00").toISOString());
    assert.strictEqual(r.to, new Date("2024-03-02T12:00").toISOString());
  });

  // The bug this guards: new Date("").toISOString() throws a RangeError, so
  // calling it inline would kill the click handler before any request went
  // out and leave the button stuck with nothing shown to the user.
  it("an empty date is an error value, never a throw", () => {
    const r = toMigrationRange("", "2024-03-02T12:00");
    assert.ok("error" in r);
  });

  it("a half-typed date is an error value too", () => {
    const r = toMigrationRange("2024-13-45T99:99", "2024-03-02T12:00");
    assert.ok("error" in r);
  });

  it("an inverted range is refused before the request", () => {
    const r = toMigrationRange("2024-03-02T12:00", "2024-03-01T12:00");
    assert.ok("error" in r);
  });
});
