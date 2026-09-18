import { test, describe } from "node:test";
import assert from "node:assert";
import {
  coerceValue,
  decodeJsonValueRows,
  listBuckets,
  listMeasurements,
  mergePositionRows,
  parseAnnotatedCsv,
  rfc3339ToNanos,
  toSignalKPath,
  MigrationRun,
  runMigration,
  type SourceRow,
} from "../migration.js";
import {
  migrationIdentity,
  type CheckpointStore,
  type MigrationCheckpoint,
} from "../migration-checkpoint.js";

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

  // signalk-to-influxdb 1.x names the field after the value's type. Appending
  // those names would file every string, boolean and object under a path no
  // Signal K consumer ever asks for.
  test("the 1.x typed value fields map to the measurement as the path", () => {
    for (const field of ["stringValue", "boolValue", "jsonValue"]) {
      assert.strictEqual(
        toSignalKPath("navigation.state", field),
        "navigation.state",
      );
    }
  });

  test("a jsonValue field is decoded into the value it encodes", () => {
    const rows: SourceRow[] = [
      { tsNanos: 1n, field: "jsonValue", value: '{"a":1,"b":"x"}' },
      { tsNanos: 2n, field: "value", value: 3.5 },
    ];
    const decoded = decodeJsonValueRows(rows);
    assert.strictEqual(decoded.dropped, 0);
    assert.deepStrictEqual(decoded.rows[0].value, { a: 1, b: "x" });
    assert.strictEqual(decoded.rows[1].value, 3.5);
  });

  test("an unparseable jsonValue is dropped and reported", () => {
    const rows: SourceRow[] = [
      { tsNanos: 1n, field: "jsonValue", value: "{not json" },
    ];
    assert.deepStrictEqual(decodeJsonValueRows(rows), { rows: [], dropped: 1 });
  });

  // Only the field NAME marks a value as JSON. A genuine string that happens
  // to look like JSON must stay the string it was recorded as.
  test("a JSON-looking string in another field is left alone", () => {
    const rows: SourceRow[] = [
      { tsNanos: 1n, field: "stringValue", value: '{"a":1}' },
    ];
    assert.deepStrictEqual(decodeJsonValueRows(rows), { rows, dropped: 0 });
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

  // signalk-to-influxdb 1.x with `separateLatLon` on writes the same fix both
  // ways. Importing both would count one position twice.
  test("a lat/lon pair duplicating a whole position at that instant is not repeated", () => {
    const whole: SourceRow = {
      tsNanos: 1n,
      field: "jsonValue",
      value: { latitude: 52.1, longitude: 4.3 },
    };
    const rows: SourceRow[] = [
      whole,
      { tsNanos: 1n, field: "lat", value: 52.1 },
      { tsNanos: 1n, field: "lon", value: 4.3 },
      { tsNanos: 2n, field: "lat", value: 52.2 },
      { tsNanos: 2n, field: "lon", value: 4.4 },
    ];
    const merged = mergePositionRows("navigation.position", rows);
    assert.strictEqual(merged.dropped, 0);
    assert.deepStrictEqual(merged.rows, [
      whole,
      {
        tsNanos: 2n,
        field: "value",
        value: { latitude: 52.2, longitude: 4.4 },
      },
    ]);
  });

  // A position-shaped object in a NAMED field is written under a child path
  // and flattened, not recorded as a position — so it must not stand in for
  // the lat/lon pair at that instant, or the fix is lost altogether.
  test("a position-shaped object in a named field does not suppress the lat/lon pair", () => {
    const named: SourceRow = {
      tsNanos: 1n,
      field: "anchor",
      value: { latitude: 1, longitude: 2 },
    };
    const rows: SourceRow[] = [
      named,
      { tsNanos: 1n, field: "lat", value: 52.1 },
      { tsNanos: 1n, field: "lon", value: 4.3 },
    ];
    const merged = mergePositionRows("navigation.position", rows);
    assert.strictEqual(merged.dropped, 0);
    assert.deepStrictEqual(merged.rows, [
      named,
      {
        tsNanos: 1n,
        field: "value",
        value: { latitude: 52.1, longitude: 4.3 },
      },
    ]);
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

describe("measurement discovery", () => {
  // A user with ~1 year of data on a Pi 5 hit "The operation was aborted due
  // to a timeout" with "0 written, 0/0 measurements" — the import died at
  // discovery having read nothing. The cause was a Flux query that reached
  // for the points (`from(bucket) |> range(start: 0) |> keep(...)`) to derive
  // names that InfluxDB already keeps as schema. `keep`/`distinct` shrink the
  // RESULT, not the scan: measured on 2.9.1, 0.15s at 1.2M points and 0.50s
  // at 4.8M, while schema.measurements() stayed ~8ms for both.
  test("2.x discovery reads the schema, never the points", async () => {
    let sent = "";
    const fakeFetch = (async (_url: string | URL, init?: RequestInit) => {
      sent = String(init?.body ?? "");
      return new Response(
        [
          "#datatype,string,long,string",
          ",result,table,_value",
          ",,0,navigation.speedOverGround",
        ].join("\n"),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const out = await listMeasurements(
      { url: "http://x", type: "influxdb2", bucket: "b" },
      fakeFetch,
    );

    assert.deepEqual(
      out.map((m) => m.name),
      ["navigation.speedOverGround"],
    );
    assert.match(sent, /schema\.measurements/);
    // Pinned to the epoch: the default range is a changeable window, and
    // inheriting it would silently hide a boat's older history.
    assert.match(sent, /start:\s*1970-01-01T00:00:00Z/);
    // The two markers of a point scan. Either one reintroduces a query whose
    // cost grows with the bucket's size instead of its schema.
    assert.ok(
      !/from\(bucket:/.test(sent),
      `discovery must not read points: ${sent}`,
    );
    assert.ok(
      !/range\(/.test(sent),
      `discovery must not range over the data: ${sent}`,
    );
  });

  // A blank name would become `path = ''` in the per-window read — a filter
  // matching nothing, so the column would import as silently empty.
  test("blank measurement names are dropped, not queried", async () => {
    const fakeFetch = (async () =>
      new Response(
        [
          "#datatype,string,long,string",
          ",result,table,_value",
          ",,0,",
          ",,0,navigation.speedOverGround",
        ].join("\n"),
        { status: 200 },
      )) as unknown as typeof fetch;

    const out = await listMeasurements(
      { url: "http://x", type: "influxdb2", bucket: "b" },
      fakeFetch,
    );
    assert.deepEqual(
      out.map((m) => m.name),
      ["navigation.speedOverGround"],
    );
  });

  test("2.x discovery costs exactly one request", async () => {
    let calls = 0;
    const fakeFetch = (async () => {
      calls++;
      return new Response(
        [
          "#datatype,string,long,string",
          ",result,table,_value",
          ",,0,a",
          ",,0,b",
          ",,0,c",
        ].join("\n"),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const out = await listMeasurements(
      { url: "http://x", type: "influxdb2", bucket: "b" },
      fakeFetch,
    );
    assert.equal(out.length, 3);
    // Not one per measurement: schema.measurementFieldKeys would pair fields
    // to names but costs a round trip each, and nothing consumes `fields`.
    assert.equal(calls, 1, `expected 1 request, made ${calls}`);
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

  // Position measurements are read pivoted: InfluxDB puts every field of an
  // instant on one record, with the series' tags as grouped columns.
  test("a lat/lon pair imports as one position tagged with the source", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,double,double",
      "#group,false,false,false,true,false,false",
      "#default,_result,,,,,",
      ",result,table,_time,source,latitude,longitude",
      ",,0,2024-03-01T12:00:00Z,gps,52.1,4.3",
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
      "#datatype,string,long,dateTime:RFC3339,string,double",
      "#group,false,false,false,true,false",
      "#default,_result,,,,",
      ",result,table,_time,source,latitude",
      ",,0,2024-03-01T12:00:00Z,gps,52.1",
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

  // Captured from a live InfluxDB 2.7.12 answering the pivoted position query:
  // one table per series, CRLF line ends, tags as grouped columns, and three
  // cases in one response — a half pair, a signalk-to-influxdb 1.x point
  // carrying `jsonValue` AND `lat`/`lon`, and two fixes whose series differ
  // only by the `s2_cell_id` tag signalk-to-influxdb2 adds.
  test("a live pivoted position response imports whole, once, without its tags", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string,double",
      "#group,false,false,false,true,true,false",
      "#default,_result,,,,,",
      ",result,table,_time,context,source,lat",
      ",,0,2024-03-01T12:00:02Z,vessels.self,gps2,59",
      "",
      "#datatype,string,long,dateTime:RFC3339,string,string,string,double,double",
      "#group,false,false,false,true,true,false,false,false",
      "#default,_result,,,,,,,",
      ",result,table,_time,context,source,jsonValue,lat,lon",
      ',,1,2024-03-01T12:00:03Z,vessels.self,legacy,"{""longitude"":25.5,""latitude"":61.5}",61.5,25.5',
      "",
      "#datatype,string,long,dateTime:RFC3339,string,string,string,double,double",
      "#group,false,false,false,true,true,true,false,false",
      "#default,_result,,,,,,,",
      ",result,table,_time,context,s2_cell_id,source,lat,lon",
      ",,2,2024-03-01T12:00:00Z,vessels.self,abc,gps1,60.1,24.9",
      ",,3,2024-03-01T12:00:01Z,vessels.self,abd,gps1,60.2,24.8",
      "",
    ].join("\r\n");
    let sent = "";
    const fakeFetch = (async (_url: string, init?: RequestInit) => {
      sent = String(init?.body ?? "");
      return new Response(csv, { status: 200 });
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tpivot", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "self",
        measurements: ["navigation.position"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(run.state, "done");
    assert.match(sent, /pivot\(/, "position measurements are read pivoted");
    assert.deepStrictEqual(
      writer.positions.map((p) => [p.lat, p.lon]),
      [
        [61.5, 25.5],
        [60.1, 24.9],
        [60.2, 24.8],
      ],
    );
    // Tags are what the table is grouped by, never fields.
    assert.deepStrictEqual(writer.strings, []);
    assert.deepStrictEqual(writer.numbers, []);
    // The lat with no lon.
    assert.strictEqual(run.progress.skipped, 1);
    assert.strictEqual(run.progress.read, 4);
  });

  // InfluxDB sends annotations only when the request asks for them, and only
  // the JSON request form can ask. Bare CSV has no `#datatype`, so a string
  // field holding "3.5" reads as a number — verified against a live 2.7.12.
  test("2.x reads ask for the annotations the reader depends on", async () => {
    const bodies: { contentType: string; body: string }[] = [];
    const fakeFetch = (async (_url: string, init?: RequestInit) => {
      bodies.push({
        contentType: String(
          (init?.headers as Record<string, string>)["Content-Type"],
        ),
        body: String(init?.body ?? ""),
      });
      return new Response(
        [
          "#datatype,string,long,dateTime:RFC3339,string,string",
          "#group,false,false,false,false,true",
          "#default,_result,,,,",
          ",result,table,_time,_value,_field",
          ",,0,2024-03-01T12:00:00Z,3.5,value",
        ].join("\n"),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tdialect", "http://x", "b");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "b",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "self",
        measurements: ["design.name"],
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(bodies.length, 1);
    assert.strictEqual(bodies[0].contentType, "application/json");
    const request = JSON.parse(bodies[0].body) as {
      query: string;
      dialect: { annotations: string[] };
    };
    assert.match(request.query, /_measurement == "design\.name"/);
    assert.ok(request.dialect.annotations.includes("datatype"));
    assert.ok(request.dialect.annotations.includes("group"));
    // And with them, the string stays a string.
    assert.deepStrictEqual(writer.numbers, []);
    assert.deepStrictEqual(
      writer.strings.map((w) => w.value),
      ["3.5"],
    );
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

  // The layout signalk-to-influxdb 1.x actually writes: one field per value
  // type, and the position as a JSON string.
  test("a signalk-to-influxdb 1.x database imports under its real paths", async () => {
    const ts = 1709294400000000000;
    const series: Record<string, { columns: string[]; values: unknown[][] }> = {
      "navigation.position": {
        columns: ["time", "jsonValue"],
        values: [[ts, '{"longitude":24.95,"latitude":60.17}']],
      },
      "navigation.state": {
        columns: ["time", "stringValue"],
        values: [[ts, "sailing"]],
      },
      "steering.autopilot.engaged": {
        columns: ["time", "boolValue"],
        values: [[ts, true]],
      },
      "environment.depth.belowKeel": {
        columns: ["time", "value"],
        values: [[ts, 4.2]],
      },
      "environment.current": {
        columns: ["time", "jsonValue"],
        values: [[ts, '{"drift":0.5,"setTrue":1.2}']],
      },
    };
    const fakeFetch = (async (input: string | URL | Request) => {
      const q = new URL(String(input)).searchParams.get("q") ?? "";
      const name = Object.keys(series).find((m) => q.includes(`"${m}"`));
      return new Response(
        JSON.stringify({
          results: [{ series: name ? [{ name, ...series[name] }] : [] }],
        }),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t1xschema", "http://x", "db");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb1",
        bucket: "db",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "self",
        measurements: Object.keys(series),
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(run.state, "done");
    assert.strictEqual(run.progress.skipped, 0);
    assert.deepStrictEqual(
      writer.positions.map((p) => ({ lat: p.lat, lon: p.lon })),
      [{ lat: 60.17, lon: 24.95 }],
    );
    assert.deepStrictEqual(
      writer.strings.map((s) => ({
        path: s.path,
        value: s.value,
        kind: s.kind,
      })),
      [
        { path: "navigation.state", value: "sailing", kind: undefined },
        { path: "steering.autopilot.engaged", value: "true", kind: "boolean" },
      ],
    );
    assert.deepStrictEqual(
      writer.numbers.map((n) => ({ path: n.path, value: n.value })),
      [
        { path: "environment.depth.belowKeel", value: 4.2 },
        { path: "environment.current.drift", value: 0.5 },
        { path: "environment.current.setTrue", value: 1.2 },
      ],
    );
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

  // "done, 0 written, 0/0 measurements" is visually identical to the
  // discovery failure this module was fixed for. An operator who picked the
  // wrong bucket would read a silent success.
  test("an empty bucket fails the run with a reason, not a clean 0/0", async () => {
    const fakeFetch = (async () =>
      new Response(
        ["#datatype,string,long,string", ",result,table,_value"].join("\n"),
        { status: 200 },
      )) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tempty", "http://x", "boatdata");
    await runMigration(
      {
        url: "http://x",
        type: "influxdb2",
        bucket: "boatdata",
        from: "2024-03-01T00:00:00Z",
        to: "2024-03-02T00:00:00Z",
        context: "vessels.self",
      },
      writer,
      run,
      { fetchImpl: fakeFetch },
    );

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /No measurements found in "boatdata"/);
    assert.match(run.error ?? "", /empty|read access/);
  });

  // An explicit measurement list must still bypass discovery entirely.
  test("an explicit measurement list skips the empty-bucket check", async () => {
    let discoveryCalls = 0;
    const fakeFetch = (async (_u: string | URL, init?: RequestInit) => {
      if (/schema\.measurements/.test(String(init?.body ?? "")))
        discoveryCalls++;
      return new Response(
        [
          "#datatype,string,long,dateTime:RFC3339,double,string",
          ",result,table,_time,_value,_field",
          ",,0,2024-03-01T12:00:00Z,1.5,value",
        ].join("\n"),
        { status: 200 },
      );
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("texplicit", "http://x", "b");
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
    assert.strictEqual(discoveryCalls, 0, "discovery must be skipped");
    assert.strictEqual(run.progress.written, 1);
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

/**
 * A response whose body is handed over one chunk per read, only when asked.
 * `highWaterMark: 0` stops the stream reading ahead, so `pulled` is exactly
 * how much of the body the import has asked for so far.
 */
function streamedResponse(
  chunks: Uint8Array[],
  signal?: AbortSignal | null,
): { response: Response; pulled: () => number; cancelled: () => boolean } {
  let pulled = 0;
  let cancelled = false;
  const body = new ReadableStream<Uint8Array>(
    {
      start(controller) {
        // What fetch does with an aborted request: the body errors.
        signal?.addEventListener("abort", () => {
          try {
            controller.error(new DOMException("aborted", "AbortError"));
          } catch {
            // Already closed or cancelled.
          }
        });
      },
      pull(controller) {
        if (pulled < chunks.length) controller.enqueue(chunks[pulled++]);
        else controller.close();
      },
      cancel() {
        cancelled = true;
      },
    },
    { highWaterMark: 0 },
  );
  return {
    response: new Response(body, { status: 200 }),
    pulled: () => pulled,
    cancelled: () => cancelled,
  };
}

const FLUX_VALUE_HEADER = [
  "#datatype,string,long,dateTime:RFC3339,double,string",
  "#group,false,false,false,false,true",
  "#default,_result,,,,",
  ",result,table,_time,_value,_field",
].join("\r\n");

/** `count` numeric Flux records, one per second from `first`. */
function fluxValueRows(first: number, count: number): string {
  const t0 = Date.parse("2024-03-01T00:00:00Z");
  let out = "";
  for (let i = first; i < first + count; i++) {
    out += `,,0,${new Date(t0 + i * 1000).toISOString()},${i},value\r\n`;
  }
  return out;
}

const streamedRequest = (type: string, measurement = "m") => ({
  url: "http://x",
  type,
  bucket: "b",
  from: "2024-03-01T00:00:00Z",
  to: "2024-03-02T00:00:00Z",
  context: "self",
  measurements: [measurement],
});

describe("streamed reads", () => {
  // The network cuts a body wherever it likes: mid-line, mid-record, and in
  // the middle of a multi-byte character.
  test("a body split at arbitrary byte boundaries reads the same", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,string,string",
      "#group,false,false,false,false,true",
      "#default,_result,,,,",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,Ålesund ⚓,value",
      ',,0,2024-03-01T12:00:01Z,"two',
      'lines",value',
      "",
    ].join("\r\n");
    const bytes = new TextEncoder().encode(csv);
    const chunks: Uint8Array[] = [];
    for (let i = 0; i < bytes.length; i += 7)
      chunks.push(bytes.slice(i, i + 7));
    const fakeFetch = (async () =>
      streamedResponse(chunks).response) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tsplit", "http://x", "b");
    await runMigration(streamedRequest("influxdb2"), writer, run, {
      fetchImpl: fakeFetch,
    });

    assert.strictEqual(run.state, "done");
    assert.deepStrictEqual(
      writer.strings.map((w) => w.value),
      ["Ålesund ⚓", "two\nlines"],
    );
  });

  test("a window larger than one batch is imported in full", async () => {
    const total = 12_001;
    const chunks = [new TextEncoder().encode(FLUX_VALUE_HEADER + "\r\n")];
    for (let i = 0; i < total; i += 1000) {
      chunks.push(
        new TextEncoder().encode(fluxValueRows(i, Math.min(1000, total - i))),
      );
    }
    const fakeFetch = (async () =>
      streamedResponse(chunks).response) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tbatches", "http://x", "b");
    await runMigration(streamedRequest("influxdb2"), writer, run, {
      fetchImpl: fakeFetch,
      // The fake writer never drains by itself.
      sleep: async () => {
        writer.numbers.length = 0;
      },
    });

    assert.strictEqual(run.state, "done");
    assert.strictEqual(run.progress.read, total);
    assert.strictEqual(run.progress.written, total);
    assert.strictEqual(run.progress.skipped, 0);
  });

  // What bounds memory: while QuestDB is backed up the import stops ASKING for
  // data, rather than reading the window and holding it.
  test("a backed-up writer stops the body being read ahead", async () => {
    const chunkCount = 60;
    const chunks = [new TextEncoder().encode(FLUX_VALUE_HEADER + "\r\n")];
    for (let i = 0; i < chunkCount; i++) {
      chunks.push(new TextEncoder().encode(fluxValueRows(i * 1000, 1000)));
    }
    let body: ReturnType<typeof streamedResponse> | undefined;
    const fakeFetch = (async () => {
      body = streamedResponse(chunks);
      return body.response;
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const pulledAtFirstWait: number[] = [];
    const run = new MigrationRun("tdemand", "http://x", "b");
    await runMigration(streamedRequest("influxdb2"), writer, run, {
      fetchImpl: fakeFetch,
      sleep: async () => {
        pulledAtFirstWait.push(body!.pulled());
        writer.numbers.length = 0;
      },
    });

    assert.strictEqual(run.state, "done");
    assert.strictEqual(run.progress.written, chunkCount * 1000);
    // The first wait comes once 20k lines are queued. By then 20-odd of the
    // 60 thousand-row chunks have been asked for — not the whole window.
    assert.ok(pulledAtFirstWait.length > 0, "the writer never backed up");
    assert.ok(
      pulledAtFirstWait[0] < 30,
      `read ahead to chunk ${pulledAtFirstWait[0]} of ${chunks.length} before waiting`,
    );
  });

  test("cancelling mid-window abandons the rest of the body", async () => {
    const chunks = [new TextEncoder().encode(FLUX_VALUE_HEADER + "\r\n")];
    for (let i = 0; i < 40; i++) {
      chunks.push(new TextEncoder().encode(fluxValueRows(i * 1000, 1000)));
    }
    let body: ReturnType<typeof streamedResponse> | undefined;
    let signal: AbortSignal | null | undefined;
    const fakeFetch = (async (_url: string, init?: RequestInit) => {
      signal = init?.signal;
      body = streamedResponse(chunks, init?.signal);
      return body.response;
    }) as unknown as typeof fetch;

    const run = new MigrationRun("tcancel", "http://x", "b");
    const writer = new FakeWriter();
    const write = writer.writeAtNanos.bind(writer);
    writer.writeAtNanos = (...args) => {
      write(...args);
      if (writer.numbers.length === 1500) run.cancel();
    };
    await runMigration(streamedRequest("influxdb2"), writer, run, {
      fetchImpl: fakeFetch,
    });

    assert.strictEqual(run.state, "cancelled");
    assert.ok(body!.pulled() < chunks.length, "the whole body was still read");
    // The request is given up, not left streaming into a socket nobody reads.
    assert.ok(signal?.aborted, "the request was not aborted");
  });

  // An InfluxDB that accepts the query and then goes quiet must fail the run
  // with a reason, not hold it at "running" forever.
  test("a body that stalls fails the run instead of hanging it", async () => {
    const fakeFetch = (async (_url: string, init?: RequestInit) => {
      const stalled = new ReadableStream<Uint8Array>({
        start(controller) {
          controller.enqueue(new TextEncoder().encode(FLUX_VALUE_HEADER));
          init?.signal?.addEventListener("abort", () =>
            controller.error(new DOMException("aborted", "AbortError")),
          );
        },
      });
      return new Response(stalled, { status: 200 });
    }) as unknown as typeof fetch;

    const run = new MigrationRun("tstall", "http://x", "b");
    await runMigration(streamedRequest("influxdb2"), new FakeWriter(), run, {
      fetchImpl: fakeFetch,
      readIdleTimeoutMs: 30,
    });

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /sent no data/);
  });

  // The idle clock runs only while a read is outstanding. A long pause for the
  // writer is not InfluxDB being slow, and must not be mistaken for it.
  test("time spent waiting for the writer does not count as a stalled read", async () => {
    const chunks = [new TextEncoder().encode(FLUX_VALUE_HEADER + "\r\n")];
    for (let i = 0; i < 30; i++) {
      chunks.push(new TextEncoder().encode(fluxValueRows(i * 1000, 1000)));
    }
    const fakeFetch = (async (_url: string, init?: RequestInit) =>
      streamedResponse(chunks, init?.signal)
        .response) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("tpause", "http://x", "b");
    await runMigration(streamedRequest("influxdb2"), writer, run, {
      fetchImpl: fakeFetch,
      readIdleTimeoutMs: 40,
      // Each drain wait outlasts the idle timeout several times over.
      sleep: async () => {
        await new Promise((r) => setTimeout(r, 150));
        writer.numbers.length = 0;
      },
    });

    assert.strictEqual(run.error, undefined);
    assert.strictEqual(run.state, "done");
    assert.strictEqual(run.progress.written, 30_000);
  });

  // Captured shape from a live InfluxDB 1.8.10 with chunked=true: one complete
  // JSON document per line, `partial` set on all but the last.
  test("1.x is read chunk by chunk, every chunk imported", async () => {
    const doc = (values: number[][], partial: boolean) =>
      JSON.stringify({
        results: [
          {
            statement_id: 0,
            series: [
              { name: "m", columns: ["time", "value"], values, partial },
            ],
            partial,
          },
        ],
      }) + "\n";
    const t = 1709294400000000000;
    const ndjson =
      doc(
        [
          [t, 1],
          [t + 1000, 2],
        ],
        true,
      ) +
      doc([[t + 2000, 3]], true) +
      doc([[t + 3000, 4]], false);
    let url = "";
    const fakeFetch = (async (input: string) => {
      url = input;
      return new Response(ndjson, { status: 200 });
    }) as unknown as typeof fetch;

    const writer = new FakeWriter();
    const run = new MigrationRun("t1xchunks", "http://x", "db");
    await runMigration(streamedRequest("influxdb1"), writer, run, {
      fetchImpl: fakeFetch,
    });

    assert.strictEqual(run.state, "done");
    assert.deepStrictEqual(
      writer.numbers.map((n) => n.value),
      [1, 2, 3, 4],
    );
    const params = new URL(url).searchParams;
    assert.strictEqual(params.get("chunked"), "true");
    assert.ok(Number(params.get("chunk_size")) > 0);
  });

  // 1.x reports failures inside an HTTP 200, and with chunking one can arrive
  // after good chunks have already been read.
  test("a 1.x error in a later chunk fails the run", async () => {
    const t = 1709294400000000000;
    const ndjson =
      JSON.stringify({
        results: [
          {
            series: [
              { name: "m", columns: ["time", "value"], values: [[t, 1]] },
            ],
            partial: true,
          },
        ],
      }) +
      "\n" +
      JSON.stringify({ results: [{ error: "engine: shard closed" }] }) +
      "\n";
    const fakeFetch = (async () =>
      new Response(ndjson, { status: 200 })) as unknown as typeof fetch;

    const run = new MigrationRun("t1xlate", "http://x", "db");
    await runMigration(streamedRequest("influxdb1"), new FakeWriter(), run, {
      fetchImpl: fakeFetch,
    });

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /shard closed/);
  });

  test("a 1.x request-level error fails the run", async () => {
    const fakeFetch = (async () =>
      new Response(JSON.stringify({ error: "database not found: db" }) + "\n", {
        status: 200,
      })) as unknown as typeof fetch;

    const run = new MigrationRun("t1xreq", "http://x", "db");
    await runMigration(streamedRequest("influxdb1"), new FakeWriter(), run, {
      fetchImpl: fakeFetch,
    });

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /database not found/);
  });
});

/** A writer whose lines leave at once, as a healthy QuestDB's do. */
class SettledWriter extends FakeWriter {
  override get pendingLines(): number {
    return 0;
  }
  get enqueuedLineCount(): number {
    return this.numbers.length + this.strings.length + this.positions.length;
  }
  get settledLineCount(): number {
    return this.enqueuedLineCount;
  }
}

class MemoryCheckpoints implements CheckpointStore {
  current: MigrationCheckpoint | null = null;
  saves = 0;
  async save(cp: MigrationCheckpoint) {
    this.current = cp;
    this.saves++;
  }
  async clear() {
    this.current = null;
  }
}

/**
 * A 1.x source holding one point per measurement per day, the value naming its
 * day. `failAt` makes one window's request fail, the way an import dies.
 */
function dailySource(failAt?: { measurement: string; day: number }) {
  const t0 = Date.parse("2024-03-01T00:00:00Z");
  const requests: { measurement: string; day: number }[] = [];
  const fetchImpl = (async (input: string) => {
    const q = new URL(input).searchParams.get("q") ?? "";
    const measurement = /FROM "([^"]+)"/.exec(q)?.[1] ?? "";
    const startNs = BigInt(/time >= (\d+)/.exec(q)?.[1] ?? "0");
    const day = Number((startNs / 1_000_000n - BigInt(t0)) / 86_400_000n);
    requests.push({ measurement, day });
    if (failAt && failAt.measurement === measurement && failAt.day === day) {
      return new Response("engine: shard closed", { status: 500 });
    }
    const body = {
      results: [
        {
          series: [
            {
              name: measurement,
              columns: ["time", "value"],
              values: [[String(startNs + 1n), day]],
            },
          ],
        },
      ],
    };
    // The timestamp is a string above only to survive JSON; send it bare.
    return new Response(
      JSON.stringify(body).replace(/"(\d{19})"/, "$1") + "\n",
      { status: 200 },
    );
  }) as unknown as typeof fetch;
  return { fetchImpl, requests };
}

const resumeRequest = () => ({
  url: "http://x",
  type: "influxdb1",
  bucket: "db",
  from: "2024-03-01T00:00:00Z",
  to: "2024-03-05T00:00:00Z",
  context: "self",
  measurements: ["a", "b", "c"],
});

describe("resuming an import", () => {
  // The property the whole feature exists for. An import that dies part-way
  // and is resumed must end with every row written — none skipped because a
  // checkpoint claimed it, and the finished part not read again.
  test("an import that dies and is resumed ends with every row written", async () => {
    const checkpoints = new MemoryCheckpoints();
    const writer = new SettledWriter();

    const first = dailySource({ measurement: "b", day: 2 });
    const run1 = new MigrationRun("r1", "http://x", "db");
    await runMigration(resumeRequest(), writer, run1, {
      fetchImpl: first.fetchImpl,
      checkpoints,
      checkpointLagMs: 0,
    });
    assert.strictEqual(run1.state, "failed");
    assert.ok(checkpoints.current, "nothing was saved to resume from");

    const second = dailySource();
    const run2 = new MigrationRun("r2", "http://x", "db");
    await runMigration(resumeRequest(), writer, run2, {
      fetchImpl: second.fetchImpl,
      checkpoints,
      resumeFrom: checkpoints.current,
      checkpointLagMs: 0,
    });
    assert.strictEqual(run2.state, "done");

    const written = new Set(writer.numbers.map((n) => `${n.path}:${n.value}`));
    for (const m of ["a", "b", "c"]) {
      for (let day = 0; day < 4; day++) {
        assert.ok(
          written.has(`${m}:${day}`),
          `${m} day ${day} was never written`,
        );
      }
    }
    // The finished measurement is not read again.
    assert.ok(
      !second.requests.some((r) => r.measurement === "a"),
      "a finished measurement was read again",
    );
    assert.ok(
      second.requests.length < 12,
      "the resumed run read everything again",
    );
    // Finished: nothing left to resume.
    assert.strictEqual(checkpoints.current, null);
  });

  test("a resumed run starts at the saved window and carries the totals", async () => {
    const source = dailySource();
    const run = new MigrationRun("r", "http://x", "db");
    const resumeFrom: MigrationCheckpoint = {
      version: 1,
      identity: migrationIdentity(resumeRequest(), 86_400_000),
      done: ["a"],
      current: {
        measurement: "b",
        windowStart: Date.parse("2024-03-03T00:00:00Z"),
      },
      progress: { read: 600, written: 590, skipped: 10 },
      updatedAt: "2024-06-01T00:00:00.000Z",
    };
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: source.fetchImpl,
      resumeFrom,
    });

    assert.strictEqual(run.state, "done");
    assert.deepStrictEqual(source.requests, [
      { measurement: "b", day: 2 },
      { measurement: "b", day: 3 },
      { measurement: "c", day: 0 },
      { measurement: "c", day: 1 },
      { measurement: "c", day: 2 },
      { measurement: "c", day: 3 },
    ]);
    assert.strictEqual(run.progress.read, 606);
    assert.strictEqual(run.progress.written, 596);
    assert.strictEqual(run.progress.skipped, 10);
    assert.strictEqual(run.progress.measurementsDone, 3);
    assert.deepStrictEqual(run.resumedFrom, {
      measurement: "b",
      windowStart: "2024-03-03T00:00:00.000Z",
    });
  });

  // The HTTP handler starts a run without awaiting it and answers at once. A
  // run that only learns it was resumed after measurement discovery — a
  // network round trip — would be reported as a fresh one, at zero.
  test("a resumed run says so before anything is awaited", () => {
    const run = new MigrationRun("r", "http://x", "db");
    const pending = runMigration(
      { ...resumeRequest(), measurements: undefined },
      new SettledWriter(),
      run,
      {
        fetchImpl: (() => new Promise(() => {})) as unknown as typeof fetch,
        resumeFrom: {
          version: 1,
          identity: migrationIdentity(
            { ...resumeRequest(), measurements: undefined },
            86_400_000,
          ),
          done: ["a"],
          current: {
            measurement: "b",
            windowStart: Date.parse("2024-03-03T00:00:00Z"),
          },
          progress: { read: 600, written: 590, skipped: 10 },
          updatedAt: "2024-06-01T00:00:00.000Z",
        },
      },
    );
    void pending;
    assert.strictEqual(run.resumedFrom?.measurement, "b");
    assert.strictEqual(run.progress.written, 590);
  });

  // A position belongs to one import. Applied to another range it would skip
  // windows that range has never imported.
  test("a checkpoint of a different import is ignored", async () => {
    const source = dailySource();
    const run = new MigrationRun("r", "http://x", "db");
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: source.fetchImpl,
      resumeFrom: {
        version: 1,
        identity: migrationIdentity(
          { ...resumeRequest(), to: "2024-03-09T00:00:00Z" },
          86_400_000,
        ),
        done: ["a", "b"],
        progress: { read: 1, written: 1, skipped: 0 },
        updatedAt: "2024-06-01T00:00:00.000Z",
      },
    });
    assert.strictEqual(source.requests.length, 12);
    assert.strictEqual(run.resumedFrom, undefined);
    assert.strictEqual(run.progress.written, 12);
  });

  test("a saved window that is not one of this run's is not trusted", async () => {
    for (const windowStart of [
      Date.parse("2024-03-02T06:00:00Z"), // off the grid
      Date.parse("2024-02-20T00:00:00Z"), // before the range
      Date.parse("2024-03-05T00:00:00Z"), // at its end
    ]) {
      const source = dailySource();
      await runMigration(
        { ...resumeRequest(), measurements: ["a"] },
        new SettledWriter(),
        new MigrationRun("r", "http://x", "db"),
        {
          fetchImpl: source.fetchImpl,
          resumeFrom: {
            version: 1,
            identity: migrationIdentity(
              { ...resumeRequest(), measurements: ["a"] },
              86_400_000,
            ),
            done: [],
            current: { measurement: "a", windowStart },
            progress: { read: 0, written: 0, skipped: 0 },
            updatedAt: "2024-06-01T00:00:00.000Z",
          },
        },
      );
      assert.deepStrictEqual(
        source.requests.map((r) => r.day),
        [0, 1, 2, 3],
        new Date(windowStart).toISOString(),
      );
    }
  });

  // A window the user cancelled out of was not imported. Saving a position
  // past it would make the resumed run skip the rest of it.
  test("a cancelled window is not saved as done", async () => {
    const checkpoints = new MemoryCheckpoints();
    const source = dailySource();
    const run = new MigrationRun("r", "http://x", "db");
    const writer = new SettledWriter();
    const write = writer.writeAtNanos.bind(writer);
    writer.writeAtNanos = (...args) => {
      write(...args);
      // Third row: measurement "a", its day-2 window.
      if (writer.numbers.length === 3) run.cancel();
    };
    await runMigration(resumeRequest(), writer, run, {
      fetchImpl: source.fetchImpl,
      checkpoints,
      checkpointLagMs: 0,
    });

    assert.strictEqual(run.state, "cancelled");
    // Kept, so the import can be continued.
    assert.ok(checkpoints.current);
    assert.deepStrictEqual(checkpoints.current.done, []);
    assert.ok(
      (checkpoints.current.current?.windowStart ?? 0) <=
        Date.parse("2024-03-03T00:00:00Z"),
      "the saved position is past the window that was cancelled",
    );
    assert.strictEqual(run.progress.measurementsDone, 0);
  });

  // Writing on after a drop would leave a gap with a complete window behind
  // it, and the row that closes that window would then vouch for rows that
  // were never stored. So a drop ends the run where it happens.
  test("a dropped line stops the import at once, not at its end", async () => {
    const source = dailySource();
    const checkpoints = new MemoryCheckpoints();
    const writer = Object.assign(new SettledWriter(), { droppedLineCount: 0 });
    const write = writer.writeAtNanos.bind(writer);
    writer.writeAtNanos = (...args) => {
      write(...args);
      if (writer.numbers.length === 2) writer.droppedLineCount = 1;
    };
    const run = new MigrationRun("r", "http://x", "db");
    await runMigration(resumeRequest(), writer, run, {
      fetchImpl: source.fetchImpl,
      checkpoints,
      checkpointLagMs: 0,
    });

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /dropped/);
    // Two of twelve windows were read: the one with the drop, and no more.
    assert.strictEqual(source.requests.length, 2);
    // And nothing was saved past it.
    assert.ok(
      !checkpoints.current || checkpoints.current.done.length === 0,
      "a position was saved after the drop",
    );
  });

  // Within one batch too: the rows after a drop must not be written, or the
  // window's last row could land in QuestDB behind a gap.
  test("no row is written after a drop, even within the same batch", async () => {
    const csv = [
      "#datatype,string,long,dateTime:RFC3339,double,string",
      "#group,false,false,false,false,true",
      "#default,_result,,,,",
      ",result,table,_time,_value,_field",
      ",,0,2024-03-01T12:00:00Z,1,value",
      ",,0,2024-03-01T12:00:01Z,2,value",
      ",,0,2024-03-01T12:00:02Z,3,value",
      ",,0,2024-03-01T12:00:03Z,4,value",
      ",,0,2024-03-01T12:00:04Z,5,value",
    ].join("\n");
    const checkpoints = new MemoryCheckpoints();
    const writer = Object.assign(new SettledWriter(), { droppedLineCount: 0 });
    const write = writer.writeAtNanos.bind(writer);
    writer.writeAtNanos = (...args) => {
      write(...args);
      // The writer's cap discards an older line as this one is enqueued.
      if (writer.numbers.length === 2) writer.droppedLineCount = 1;
    };
    const run = new MigrationRun("r", "http://x", "b");
    await runMigration(
      { ...streamedRequest("influxdb2"), measurements: ["m"] },
      writer,
      run,
      {
        fetchImpl: (async () =>
          new Response(csv, { status: 200 })) as unknown as typeof fetch,
        checkpoints,
        checkpointLagMs: 0,
      },
    );

    assert.strictEqual(run.state, "failed");
    assert.deepStrictEqual(
      writer.numbers.map((n) => n.value),
      [1, 2],
      "rows were written after the drop",
    );
    assert.strictEqual(checkpoints.current, null);
  });

  // The write that fills the buffer past its cap is the one behind the gap.
  // It must be refused before it is enqueued, not noticed afterwards.
  test("a row is not written into a buffer that is at capacity", async () => {
    const source = dailySource();
    const writer = Object.assign(new SettledWriter(), { atCapacity: false });
    const write = writer.writeAtNanos.bind(writer);
    writer.writeAtNanos = (...args) => {
      write(...args);
      if (writer.numbers.length === 3) writer.atCapacity = true;
    };
    const run = new MigrationRun("r", "http://x", "db");
    await runMigration(resumeRequest(), writer, run, {
      fetchImpl: source.fetchImpl,
    });

    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /buffer is full/);
    assert.strictEqual(
      writer.numbers.length,
      3,
      "a row was written into a full buffer",
    );
  });

  // "Done" must mean QuestDB has the rows, not that the writer was handed
  // them: the last minute's rows are still in flight when the loop ends, and
  // a cleared checkpoint would leave nothing to resume if they were lost.
  test("done waits for the last rows to be confirmed", async () => {
    const checkpoints = new MemoryCheckpoints();
    let answers = 0;
    const run = new MigrationRun("r", "http://x", "db");
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: dailySource().fetchImpl,
      checkpoints,
      sleep: async () => {},
      // QuestDB says no twice, then has the rows.
      confirmStored: async () => ++answers > 2,
    });
    assert.strictEqual(run.state, "done");
    assert.strictEqual(answers, 3);
    assert.strictEqual(checkpoints.current, null);
  });

  test("a run whose last rows are never confirmed fails and keeps its position", async () => {
    const checkpoints = new MemoryCheckpoints();
    const run = new MigrationRun("r", "http://x", "db");
    let slept = 0;
    let answers = 0;
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: dailySource().fetchImpl,
      checkpoints,
      checkpointLagMs: 0,
      sleep: async () => {
        slept++;
      },
      // QuestDB confirms the first positions, then stops answering yes.
      confirmStored: async () => ++answers <= 3,
    });
    assert.strictEqual(run.state, "failed");
    assert.match(run.error ?? "", /not confirmed the last rows/);
    assert.ok(slept > 0, "gave up without waiting");
    // The last position that was confirmed is still there to resume from;
    // a false "done" would have cleared it.
    assert.ok(checkpoints.current, "the saved position was cleared");
    assert.ok(checkpoints.current.done.length < 3);
  });

  test("a cancel during the final wait ends it as a cancel", async () => {
    const checkpoints = new MemoryCheckpoints();
    const run = new MigrationRun("r", "http://x", "db");
    let answers = 0;
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: dailySource().fetchImpl,
      checkpoints,
      checkpointLagMs: 0,
      sleep: async () => {},
      confirmStored: async () => {
        // Positions confirm; the final rows never do, and the user gives up.
        if (++answers > 3) run.cancel();
        return answers <= 3;
      },
    });
    assert.strictEqual(run.state, "cancelled");
    assert.strictEqual(run.error, undefined);
    assert.ok(checkpoints.current, "the saved position was cleared");
  });

  test("done waits for the last rows to leave the writer", async () => {
    const writer = new SettledWriter();
    // Everything enqueued is still in the writer until a sleep lets it go.
    let held = true;
    Object.defineProperty(writer, "settledLineCount", {
      get: () => (held ? 0 : writer.enqueuedLineCount),
    });
    const run = new MigrationRun("r", "http://x", "db");
    let slept = 0;
    await runMigration(resumeRequest(), writer, run, {
      fetchImpl: dailySource().fetchImpl,
      sleep: async () => {
        slept++;
        held = false;
      },
    });
    assert.strictEqual(run.state, "done");
    assert.strictEqual(slept, 1);
  });

  test("a checkpoint that cannot be written does not fail the import", async () => {
    const failing: CheckpointStore = {
      save: async () => {
        throw new Error("ENOSPC: no space left on device");
      },
      clear: async () => {
        throw new Error("EACCES");
      },
    };
    const run = new MigrationRun("r", "http://x", "db");
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: dailySource().fetchImpl,
      checkpoints: failing,
      checkpointLagMs: 0,
    });
    assert.strictEqual(run.state, "done");
    assert.strictEqual(run.progress.written, 12);
  });

  // With the real lag a short import never saves anything — and must still
  // finish cleanly, leaving nothing behind.
  test("an import shorter than the lag saves nothing and leaves nothing", async () => {
    const checkpoints = new MemoryCheckpoints();
    const run = new MigrationRun("r", "http://x", "db");
    await runMigration(resumeRequest(), new SettledWriter(), run, {
      fetchImpl: dailySource().fetchImpl,
      checkpoints,
    });
    assert.strictEqual(run.state, "done");
    assert.strictEqual(checkpoints.saves, 0);
    assert.strictEqual(checkpoints.current, null);
  });
});
