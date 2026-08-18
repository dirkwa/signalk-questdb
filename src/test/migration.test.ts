import { test, describe } from "node:test";
import assert from "node:assert";
import {
  coerceValue,
  listBuckets,
  mergePositionRows,
  parseAnnotatedCsv,
  rfc3339ToNanos,
  toSignalKPath,
  MigrationRun,
  runMigration,
  type SourceRow,
} from "../migration.js";

/** Minimal stand-in for the ILP writer, recording what would be written. */
class FakeWriter {
  numbers: {
    path: string;
    context: string;
    value: number;
    ts: bigint;
    source?: string;
  }[] = [];
  strings: {
    path: string;
    context: string;
    value: string;
    ts: bigint;
    kind?: string;
    source?: string;
  }[] = [];
  positions: {
    context: string;
    lat: number;
    lon: number;
    ts: bigint;
    source?: string;
  }[] = [];

  /** Mirrors the real writer: every enqueued line counts until drained. */
  get pendingLines(): number {
    return this.numbers.length + this.strings.length + this.positions.length;
  }

  writeAtNanos(
    path: string,
    context: string,
    value: number,
    ts: bigint,
    source?: string,
  ) {
    this.numbers.push({ path, context, value, ts, source });
  }
  writeStringAtNanos(
    path: string,
    context: string,
    value: string,
    ts: bigint,
    kind?: "boolean" | "identity",
    source?: string,
  ) {
    this.strings.push({ path, context, value, ts, kind, source });
  }
  writePositionAtNanos(
    context: string,
    position: { latitude: number; longitude: number },
    ts: bigint,
    source?: string,
  ) {
    this.positions.push({
      context,
      lat: position.latitude,
      lon: position.longitude,
      ts,
      source,
    });
  }
}

describe("timestamp precision", () => {
  // The tables dedup on (ts, path, context, source). Truncating to
  // milliseconds would make sub-millisecond points collide and upsert.
  test("sub-millisecond digits survive the conversion", () => {
    const a = rfc3339ToNanos("2024-03-01T12:00:00.000200Z");
    const b = rfc3339ToNanos("2024-03-01T12:00:00.000400Z");
    assert.notStrictEqual(a, b);
    assert.strictEqual(b! - a!, 200_000n);
  });

  test("nanosecond precision is preserved exactly", () => {
    const ns = rfc3339ToNanos("2024-03-01T12:00:00.123456789Z");
    assert.strictEqual(ns! % 1_000_000_000n, 123456789n);
  });

  test("a plain second-resolution instant still parses", () => {
    const ns = rfc3339ToNanos("2024-03-01T12:00:00Z");
    assert.strictEqual(
      ns,
      BigInt(Date.parse("2024-03-01T12:00:00Z")) * 1_000_000n,
    );
  });

  test("garbage yields null rather than a bogus instant", () => {
    assert.strictEqual(rfc3339ToNanos("not-a-time"), null);
  });
});

describe("annotated CSV parsing", () => {
  test("reads Flux records, ignoring annotations and the gutter column", () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      "#group,false,false,false,false,true",
      "#default,_result,,,,",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,3.5,value",
      ",,0,2024-03-01T12:00:01Z,3.6,value",
    ].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[0].values["_value"], "3.5");
    assert.strictEqual(rows[1].values["_time"], "2024-03-01T12:00:01Z");
    // The unnamed gutter column must not become a key.
    assert.ok(!("" in rows[0].values));
  });

  test("a second table with a different header is read correctly", () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double",
      ",result,table,_time,_value",
      ",,0,2024-03-01T12:00:00Z,1.0",
      "",
      "#datatype,string,long,dateTime:RFC3339,string",
      ",result,table,_time,_value",
      ",,1,2024-03-01T12:00:00Z,hello",
    ].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows.length, 2);
    assert.strictEqual(rows[1].values["_value"], "hello");
  });

  // Verified against a live 2.9.1: a string value holding a newline comes back
  // as a quoted cell spanning two physical lines.
  test("a quoted value containing a newline stays one record", () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string",
      ",result,table,_time,_value,_field",
      ',,0,2024-03-01T12:00:00Z,"line1',
      'line2",value',
    ].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].values["_value"], "line1\nline2");
    assert.strictEqual(rows[0].values["_field"], "value");
  });

  // The \r is stripped per PHYSICAL line before joining. A CRLF file whose
  // quoted value spans two lines would otherwise keep the first line's \r
  // inside the joined value — a stray carriage return in the imported string.
  test("a quoted value spanning two CRLF lines keeps no stray carriage return", () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string",
      ",result,table,_time,_value,_field",
      ',,0,2024-03-01T12:00:00Z,"line1',
      'line2",value',
    ].join("\r\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].values["_value"], "line1\nline2");
    assert.ok(
      !rows[0].values["_value"].includes("\r"),
      "a stray carriage return survived the join",
    );
  });

  test("an escaped double quote does not unbalance the record", () => {
    const csv = [",result,table,_value", ',,0,"say ""hi"" now"'].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows.length, 1);
    assert.strictEqual(rows[0].values["_value"], 'say "hi" now');
  });

  // A genuine STRING value of "3.5" is byte-identical to the number 3.5 in the
  // CSV body — verified against a live 2.9.1. Only the #datatype annotation
  // tells them apart, so it has to reach coerceValue or the string lands in
  // the numeric table.
  test("the #datatype of each column is carried alongside the values", () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,3.5,value",
    ].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows[0].types["_value"], "string");
    assert.strictEqual(
      coerceValue(rows[0].values["_value"], rows[0].types["_value"]),
      "3.5",
    );
  });

  test("a double column still coerces to a number", () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,3.5,value",
    ].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(
      coerceValue(rows[0].values["_value"], rows[0].types["_value"]),
      3.5,
    );
  });

  test("quoted cells containing commas stay intact", () => {
    const csv = [",result,table,_value", ',,0,"a,b"'].join("\n");
    const rows = parseAnnotatedCsv(csv);
    assert.strictEqual(rows[0].values["_value"], "a,b");
  });
});

describe("value and path mapping", () => {
  test("the conventional `value` field maps to the measurement as the path", () => {
    assert.strictEqual(
      toSignalKPath("navigation.speedOverGround", "value"),
      "navigation.speedOverGround",
    );
  });

  test("a named field is appended so two fields cannot collide", () => {
    assert.strictEqual(
      toSignalKPath("environment.wind", "speedApparent"),
      "environment.wind.speedApparent",
    );
  });

  test("numeric text becomes a number, other text stays a string", () => {
    assert.strictEqual(coerceValue("3.5"), 3.5);
    assert.strictEqual(coerceValue("hello"), "hello");
    assert.strictEqual(coerceValue("true"), true);
    assert.strictEqual(coerceValue("false"), false);
  });

  // A STRING column holding "true" is the word, not a boolean. Detecting the
  // literal before honouring the declared type recorded it as a boolean and
  // lost the distinction value_kind exists to preserve.
  test("a declared string type wins over boolean detection", () => {
    assert.strictEqual(coerceValue("true", "string"), "true");
    assert.strictEqual(coerceValue("false", "string"), "false");
    // With no declared type the literal still reads as a boolean.
    assert.strictEqual(coerceValue("true"), true);
    assert.strictEqual(coerceValue("true", "boolean"), true);
  });

  test("an empty value is null rather than 0", () => {
    // Number("") is 0 — importing a gap as a real zero reading would be a
    // silent data corruption.
    assert.strictEqual(coerceValue(""), null);
  });
});

describe("position reassembly", () => {
  test("lat/lon fields at one instant become a single position", () => {
    const ts = 1_700_000_000_000_000_000n;
    const rows: SourceRow[] = [
      { tsNanos: ts, field: "latitude", value: 52.1 },
      { tsNanos: ts, field: "longitude", value: 4.3 },
    ];
    const merged = mergePositionRows("navigation.position", rows);
    assert.strictEqual(merged.rows.length, 1);
    assert.strictEqual(merged.dropped, 0);
    assert.deepStrictEqual(merged.rows[0].value, {
      latitude: 52.1,
      longitude: 4.3,
    });
  });

  test("a lat with no matching lon is dropped, not written as a number", () => {
    const rows: SourceRow[] = [{ tsNanos: 1n, field: "latitude", value: 52.1 }];
    const merged = mergePositionRows("navigation.position", rows);
    assert.deepStrictEqual(merged.rows, []);
    // Reported, not silently vanished — it lands in the run's skipped total.
    assert.strictEqual(merged.dropped, 1);
  });

  test("non-position measurements pass through untouched", () => {
    const rows: SourceRow[] = [{ tsNanos: 1n, field: "value", value: 3.5 }];
    assert.deepStrictEqual(
      mergePositionRows("navigation.speedOverGround", rows),
      {
        rows,
        dropped: 0,
      },
    );
  });
});

describe("bucket listing", () => {
  const page = (names: string[], next?: string) =>
    new Response(
      JSON.stringify({
        buckets: names.map((name, i) => ({ id: `id${i}`, name })),
        links: next ? { self: "/x", next } : { self: "/x" },
      }),
      { status: 200 },
    );

  // 100 is the API maximum per page. A live 2.9.1 holding 106 buckets returned
  // only 98 from a single request — eight silently missing, and a bucket the
  // operator cannot see is one they cannot import from.
  test("follows links.next until every page is collected", async () => {
    const urls: string[] = [];
    const fakeFetch = (async (url: string | URL) => {
      const u = String(url);
      urls.push(u);
      if (urls.length === 1)
        return page(["a", "b"], "/api/v2/buckets?offset=2");
      return page(["c"]);
    }) as unknown as typeof fetch;

    const buckets = await listBuckets(
      { url: "http://x", type: "influxdb2", auth: { token: "t" } },
      fakeFetch,
    );
    assert.deepStrictEqual(
      buckets.map((b) => b.name),
      ["a", "b", "c"],
    );
    assert.strictEqual(urls.length, 2);
  });

  // `links.next` is server-supplied. One that points at the page just fetched
  // would otherwise loop until the page cap.
  test("stops when links.next repeats the current page", async () => {
    let calls = 0;
    const fakeFetch = (async (url: string | URL) => {
      calls++;
      // Always name the very URL being requested.
      return page(["a"], String(url));
    }) as unknown as typeof fetch;

    const buckets = await listBuckets(
      { url: "http://x", type: "influxdb2" },
      fakeFetch,
    );
    assert.strictEqual(calls, 1, `looped ${calls} times`);
    assert.deepStrictEqual(
      buckets.map((b) => b.name),
      ["a"],
    );
  });

  test("stops at the page cap when next never ends", async () => {
    let calls = 0;
    const fakeFetch = (async () => {
      calls++;
      // A distinct link every time, so only the cap can stop it.
      return page([`b${calls}`], `/api/v2/buckets?offset=${calls}`);
    }) as unknown as typeof fetch;

    await listBuckets({ url: "http://x", type: "influxdb2" }, fakeFetch);
    assert.strictEqual(calls, 100, `expected the 100-page cap, got ${calls}`);
  });

  // validateInfluxUrl deliberately preserves a sub-path for reverse-proxied
  // instances. Resolving an absolute path against the base would drop it and
  // probe the wrong host root.
  test("keeps a configured path prefix on the first page", async () => {
    const urls: string[] = [];
    const fakeFetch = (async (url: string | URL) => {
      urls.push(String(url));
      return page(["a"]);
    }) as unknown as typeof fetch;

    await listBuckets(
      { url: "http://host:8086/influx", type: "influxdb2" },
      fakeFetch,
    );
    assert.strictEqual(
      urls[0],
      "http://host:8086/influx/api/v2/buckets?limit=100",
    );
  });

  test("system buckets are hidden", async () => {
    const fakeFetch = (async () =>
      page(["_monitoring", "_tasks", "boatdata"])) as unknown as typeof fetch;
    const buckets = await listBuckets(
      { url: "http://x", type: "influxdb2" },
      fakeFetch,
    );
    assert.deepStrictEqual(
      buckets.map((b) => b.name),
      ["boatdata"],
    );
  });

  // 1.x answers a rejected query with HTTP 200 and an `error` member, so
  // without the check an auth failure reads as "no databases".
  test("a 1.x error member is surfaced, not read as an empty list", async () => {
    const fakeFetch = (async () =>
      new Response(
        JSON.stringify({ results: [{ error: "authorization failed" }] }),
        { status: 200 },
      )) as unknown as typeof fetch;

    await assert.rejects(
      () => listBuckets({ url: "http://x", type: "influxdb1" }, fakeFetch),
      /authorization failed/,
    );
  });
});

describe("import run", () => {
  const fluxCsv = (rows: [string, string, string][]) =>
    [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ...rows.map(([t, v, f]) => `,,0,${t},${v},${f}`),
    ].join("\n");

  test("numeric rows land in the numeric table with their original timestamps", async () => {
    const fakeFetch = (async () =>
      new Response(fluxCsv([["2024-03-01T12:00:00.000500Z", "3.5", "value"]]), {
        status: 200,
      })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t1", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["navigation.speedOverGround"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(run.state, "done");
    assert.strictEqual(writer.numbers.length, 1);
    assert.strictEqual(writer.numbers[0].path, "navigation.speedOverGround");
    assert.strictEqual(writer.numbers[0].value, 3.5);
    assert.strictEqual(writer.numbers[0].source, "influxdb-import");
    // The 500µs must survive; a Date round-trip would have flattened it.
    assert.strictEqual(
      writer.numbers[0].ts,
      rfc3339ToNanos("2024-03-01T12:00:00.000500Z"),
    );
    assert.strictEqual(run.progress.written, 1);
  });

  test("string and boolean rows go to the string table, booleans tagged", async () => {
    const fakeFetch = (async () =>
      new Response(
        fluxCsv([
          ["2024-03-01T12:00:00Z", "docked", "value"],
          ["2024-03-01T12:00:01Z", "true", "value"],
        ]),
        { status: 200 },
      )) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t2", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["navigation.state"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(writer.strings.length, 2);
    assert.strictEqual(writer.strings[0].value, "docked");
    assert.strictEqual(writer.strings[0].kind, undefined);
    assert.strictEqual(writer.strings[1].value, "true");
    assert.strictEqual(writer.strings[1].kind, "boolean");
  });

  test("a lat/lon pair imports as one position tagged with the source", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,52.1,latitude",
      ",,0,2024-03-01T12:00:00Z,4.3,longitude",
    ].join("\n");
    const fakeFetch = (async () =>
      new Response(csv, { status: 200 })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tpos", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["navigation.position"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(writer.positions.length, 1);
    assert.strictEqual(writer.positions[0].lat, 52.1);
    assert.strictEqual(writer.positions[0].source, "influxdb-import");
    // The pair became ONE position, not two numeric paths.
    assert.strictEqual(writer.numbers.length, 0);
  });

  // A half-pair never becomes a row, so it has to be counted explicitly or
  // read - written - skipped stops adding up.
  test("a dropped half-pair position is counted as skipped", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,52.1,latitude",
    ].join("\n");
    const fakeFetch = (async () =>
      new Response(csv, { status: 200 })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("thalf", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["navigation.position"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(writer.positions.length, 0);
    assert.strictEqual(run.progress.skipped, 1);
    assert.strictEqual(run.progress.read, 1);
  });

  // Reading must pause while the writer is backed up, or the ILP buffer cap
  // discards the OLDEST queued lines — losing the history being imported.
  test("reading waits for the writer to drain past the resume mark", async () => {
    const writer = new FakeWriter();
    // Start above the high-water mark so the loop must wait.
    for (let i = 0; i < 25_000; i++) {
      writer.numbers.push({
        path: "p",
        context: "c",
        value: 1,
        ts: 1n,
        source: "s",
      });
    }
    let sleeps = 0;
    const sleep = async () => {
      sleeps++;
      // Drain a chunk per tick; the loop must keep waiting until it is under
      // the RESUME mark, not merely under the HIGH mark.
      writer.numbers.splice(0, 6_000);
    };

    const fakeFetch = (async () =>
      new Response(
        [
          "#datatype,string,long,dateTime:RFC3339,double,string",
          ",result,table,_time,_value,_field",
        ].join("\n"),
        { status: 200 },
      )) as unknown as typeof fetch;

    const run = new MigrationRun("tbp", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch, sleep },
    );

    assert.ok(sleeps > 0, "should have waited at least once");
    // 25k -> under 5k at 6k per tick takes more than the single tick that
    // would have sufficed to merely fall under the 20k high-water mark.
    assert.ok(
      sleeps >= 4,
      `expected to drain to the resume mark, only slept ${sleeps}x`,
    );
  });

  // A bare `SELECT *` returns TAG columns next to the fields, and every
  // non-time column is read as a field — so a point tagged source=n2k would
  // import a bogus `<measurement>.source = "n2k"` path. Signal K's InfluxDB
  // writers tag their points, so this is the normal case.
  test("the 1.x query asks for fields only, not tags", async () => {
    let seenQuery = "";
    const fakeFetch = (async (url: string | URL) => {
      seenQuery = decodeURIComponent(String(url));
      return new Response(JSON.stringify({ results: [{ series: [] }] }), {
        status: 200,
      });
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tql", "http://x", "signalk");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb1",
        bucket: "signalk",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["navigation.speedOverGround"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.match(seenQuery, /SELECT \*::field FROM/);
  });

  // The own vessel is stored as the literal "self" by the live recorder and
  // both history providers normalise to it. An import that wrote
  // `vessels.<uuid>` instead would file rows in a context no query looks at:
  // present in the table, invisible through the API.
  test("rows are written under exactly the context they are given", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,3.5,value",
    ].join("\n");
    const fakeFetch = (async () =>
      new Response(csv, { status: 200 })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tctx", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "self",
        measurements: ["navigation.speedOverGround"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(writer.numbers[0].context, "self");
  });

  // The numeric assertion above would still pass if only the string path
  // regressed, so the string table is checked on its own context.
  test("string rows are written under the given context too", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,docked,value",
    ].join("\n");
    const fakeFetch = (async () =>
      new Response(csv, { status: 200 })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tctxstr", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "self",
        measurements: ["navigation.state"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(writer.strings.length, 1);
    assert.strictEqual(writer.strings[0].context, "self");
  });

  // `pendingLines` only falls when QuestDB actually accepts data, so a
  // QuestDB that is down or wedged left the drain wait spinning forever: the
  // run sat at "running" with frozen counters and nothing logged, and the
  // only way out was cancelling by hand.
  test("a writer that never drains fails the run instead of hanging", async () => {
    const stuck = {
      pendingLines: 999_999,
      writeAtNanos() {},
      writeStringAtNanos() {},
      writePositionAtNanos() {},
    };
    let slept = 0;
    const run = new MigrationRun("tdrain", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      stuck,
      run,
      {
        fetchImpl: (async () =>
          new Response("", { status: 200 })) as unknown as typeof fetch,
        // Virtual clock: the loop counts elapsed ms itself, so an
        // instant sleep still reaches the timeout deterministically.
        sleep: async () => {
          slept++;
        },
      },
    );
    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /not accepting writes/);
    // Bounded, not spinning: 5 min at 200ms is 1500 iterations.
    assert.ok(slept <= 1600, `slept ${slept} times`);
  });

  // A dense window can carry far more rows than the writer's cap, so waiting
  // only at window boundaries would let the buffer sail past MAX_BUFFER_LINES
  // mid-window — dropping its oldest lines.
  test("backpressure is applied inside a window, not only between windows", async () => {
    // Deliberately BELOW the high-water mark: the window-boundary check must
    // pass straight through, so any wait that happens is proof the check
    // inside the row loop fired. Starting above it made this test vacuous —
    // the boundary check alone satisfied both assertions and the test passed
    // with the intra-window block deleted.
    let pending = 0;
    const writer = {
      writtenCount: 0,
      get pendingLines() {
        return pending;
      },
      writeAtNanos() {
        pending++;
        this.writtenCount++;
      },
      writeStringAtNanos() {},
      writePositionAtNanos() {},
    };
    let drainedAfterRows = 0;
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ...Array.from(
        { length: 25_000 },
        (_, i) =>
          `,,0,2024-03-01T12:00:00.${String(i).padStart(6, "0")}Z,${i},value`,
      ),
    ].join("\n");

    const run = new MigrationRun("tintra", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      {
        fetchImpl: (async () =>
          new Response(csv, { status: 200 })) as unknown as typeof fetch,
        // Draining is what a healthy writer does; count that it was needed.
        sleep: async () => {
          // Only a drain that happens once rows have been written can have
          // come from the intra-window check.
          if (writer.writtenCount > 0) drainedAfterRows++;
          pending = 0;
        },
      },
    );

    assert.strictEqual(run.state, "done");
    assert.ok(
      drainedAfterRows > 0,
      "expected a drain triggered from INSIDE the row loop",
    );
    // Every row still landed rather than being dropped by the buffer cap.
    assert.strictEqual(run.progress.written, 25_000);
  });

  // InfluxQL returns the field's ACTUAL JSON type — a string field as a JSON
  // string, a float as a number, a boolean as a boolean (verified against
  // 1.8.10). Re-parsing strings through coerceValue destroyed that: a genuine
  // string "3.5" landed in the NUMERIC table and a string "true" was recorded
  // as a boolean, both indistinguishable from the real thing.
  test("1.x preserves the JSON field type instead of re-parsing it", async () => {
    const body = {
      results: [
        {
          series: [
            {
              name: "m",
              columns: [
                "time",
                "strnum",
                "strbool",
                "realnum",
                "realbool",
                "empty",
              ],
              values: [[1709294400000000000, "3.5", "true", 3.5, true, ""]],
            },
          ],
        },
      ],
    };
    const fakeFetch = (async () =>
      new Response(JSON.stringify(body), {
        status: 200,
      })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t1xtype", "http://x", "db");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb1",
        bucket: "db",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["m"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    // Only the genuine number reaches the numeric table.
    assert.deepStrictEqual(
      writer.numbers.map((n) => n.path),
      ["m.realnum"],
    );
    // An empty 1.x string field is a gap, not an empty reading: it must not
    // be recorded at all.
    assert.ok(
      !writer.strings.some((w) => w.path === "m.empty"),
      "an empty string field should be skipped, not recorded",
    );
    const strings = Object.fromEntries(
      writer.strings.map((w) => [w.path, { value: w.value, kind: w.kind }]),
    );
    // A string field holding "3.5" stays a string.
    assert.deepStrictEqual(strings["m.strnum"], {
      value: "3.5",
      kind: undefined,
    });
    // A string field holding "true" is the word, not a boolean.
    assert.deepStrictEqual(strings["m.strbool"], {
      value: "true",
      kind: undefined,
    });
    // A real boolean is still tagged as one.
    assert.deepStrictEqual(strings["m.realbool"], {
      value: "true",
      kind: "boolean",
    });
  });

  // The last window must not read past the requested end. An unclamped
  // `start + windowMs` would query beyond `to` and import history the
  // operator did not ask for.
  test("the final window is clamped to the requested end", async () => {
    const stops: string[] = [];
    const fakeFetch = (async (_url: string | URL, init?: RequestInit) => {
      const m = /stop: ([^)]+)\)/.exec(String(init?.body ?? ""));
      if (m) stops.push(m[1]);
      return new Response("", { status: 200 });
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tclamp", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        // A 6h range with the default 24h window: one window, clamped.
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-01T06:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(stops.length, 1);
    assert.strictEqual(stops[0], "2024-03-01T06:00:00.000Z");
  });

  // A value no table can hold must be COUNTED, not quietly ignored — the
  // module's contract is that read = written + skipped.
  test("an unmappable value increments the skipped counter", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string",
      ",result,table,_time,_value,_field",
      // An empty value coerces to null, which no table can hold.
      ",,0,2024-03-01T12:00:00Z,,value",
    ].join("\n");
    const fakeFetch = (async () =>
      new Response(csv, { status: 200 })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tskip", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(run.progress.written, 0);
    assert.strictEqual(run.progress.skipped, 1);
    assert.strictEqual(
      run.progress.read,
      run.progress.written + run.progress.skipped,
    );
  });

  // Cancellation is checked per ROW, not only per window. Plugin stop relies
  // on it: a run that kept going would write to a disconnected writer.
  test("cancelling mid-window stops within a few rows", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ...Array.from(
        { length: 3000 },
        (_, i) =>
          `,,0,2024-03-01T12:00:00.${String(i).padStart(6, "0")}Z,${i},value`,
      ),
    ].join("\n");
    const fakeFetch = (async () =>
      new Response(csv, { status: 200 })) as unknown as typeof fetch;

    const run = new MigrationRun("tcancelrow", "http://x", "b");
    const writer = new (class extends FakeWriter {
      writeAtNanos(
        path: string,
        context: string,
        value: number,
        ts: bigint,
        source?: string,
      ) {
        super.writeAtNanos(path, context, value, ts, source);
        if (this.numbers.length === 5) run.cancel();
      }
    })();
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(run.state, "cancelled");
    // Stopped promptly rather than draining all 3000 rows.
    assert.ok(
      writer.numbers.length < 20,
      `wrote ${writer.numbers.length} rows after cancel`,
    );
  });

  // The ILP writer drops its OLDEST buffered lines when the cap is reached
  // (disconnected, or QuestDB not keeping up). Those rows counted as written
  // but never reached the database, so the run must not report clean success.
  test("rows dropped by the writer fail the run rather than reporting success", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,1,value",
    ].join("\n");

    const writer = Object.assign(new FakeWriter(), {
      // Monotonic counter, as the real writer exposes it.
      droppedLineCount: 0,
    });

    const run = new MigrationRun("tdrop", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      {
        fetchImpl: (async () => {
          // Simulate the buffer overflowing during the run.
          writer.droppedLineCount += 42;
          return new Response(csv, { status: 200 });
        }) as unknown as typeof fetch,
      },
    );

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /42 buffered rows were dropped/);
  });

  test("a run with no writer drops still reports success", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,1,value",
    ].join("\n");
    // A NON-zero starting count: only the delta across this run counts, since
    // the counter is shared with the live recorder.
    const writer = Object.assign(new FakeWriter(), { droppedLineCount: 17 });
    const run = new MigrationRun("tnodrop", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      {
        fetchImpl: (async () =>
          new Response(csv, { status: 200 })) as unknown as typeof fetch,
      },
    );
    assert.strictEqual(run.state, "done");
  });

  test("cancelling stops the run and reports it as cancelled", async () => {
    const fakeFetch = (async () =>
      new Response(fluxCsv([["2024-03-01T12:00:00Z", "1", "value"]]), {
        status: 200,
      })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t3", "http://x", "b");
    run.cancel();
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a", "b"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(run.state, "cancelled");
    assert.strictEqual(writer.numbers.length, 0);
  });

  test("a failing read marks the run failed and keeps the reason", async () => {
    const fakeFetch = (async () =>
      new Response("boom", { status: 500 })) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t4", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /500/);
  });

  // `start += windowMs` with a non-positive window never advances.
  test("a non-positive window does not spin forever", async () => {
    let calls = 0;
    const fakeFetch = (async () => {
      calls++;
      return new Response(
        [
          "#datatype,string,long,dateTime:RFC3339,double,string",
          ",result,table,_time,_value,_field",
        ].join("\n"),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t6", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
        windowMs: 0,
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );
    assert.strictEqual(run.state, "done");
    // One day at the default 24h window is a single read.
    assert.strictEqual(calls, 1);
  });

  test("an inverted range is rejected before any read", async () => {
    const writer = new FakeWriter();
    const run = new MigrationRun("t5", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-02T00:00:00Z",
        to: "2024-03-01T00:00:00Z",
        context: "vessels.self",
        measurements: ["a"],
      },
      writer,
      run,
      {
        fetchImpl: (async () => {
          throw new Error("should not be called");
        }) as unknown as typeof fetch,
      },
    );
    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /after/);
  });
});
