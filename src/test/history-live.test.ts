// The history providers against a real QuestDB. The other suites fake the
// /exec endpoint, so a statement QuestDB itself would reject passes there;
// this one runs the SQL the providers build and checks what comes back.
//
// Skipped unless QUESTDB_URL names a QuestDB HTTP endpoint (ILP on
// QUESTDB_ILP_PORT, default 9009). It creates the plugin's tables, writes
// through the real ILP writer and drops the tables when it finishes, so it
// refuses an instance whose tables already hold rows:
//
//   docker run -d --rm -p 9000:9000 -p 9009:9009 questdb/questdb:latest
//   QUESTDB_URL=http://127.0.0.1:9000 node --test dist/test/history-live.test.js

import { after, before, describe, it } from "node:test";
import assert from "node:assert/strict";
import { Temporal } from "@js-temporal/polyfill";
import type {
  Path,
  SourceRef,
  history as HistoryApi,
} from "@signalk/server-api";
import { ARCHIVE_TABLES, ArchiveSpans } from "../archive.js";
import { createHistoryProviderV2 } from "../history-v2.js";
import { ILPWriter } from "../ilp-writer.js";
import { QueryClient } from "../query-client.js";

const QUESTDB_URL = process.env.QUESTDB_URL;
const ILP_PORT = Number(process.env.QUESTDB_ILP_PORT ?? 9009);
const SELF_CONTEXT = "vessels.urn:mrn:imo:mmsi:123456789";
const TABLES = [
  "signalk",
  "signalk_str",
  "signalk_position",
  ARCHIVE_TABLES.signalk,
  ARCHIVE_TABLES.signalk_position,
];
// WAL apply runs behind the ILP commit, so rows appear after the write
// returns.
const APPLY_TIMEOUT_MS = 30_000;

const FROM = "2024-01-01T00:00:00Z";
const TO = "2024-01-01T00:03:00Z";
const at = (seconds: number) => new Date(Date.parse(FROM) + seconds * 1000);
const DEG = Math.PI / 180;

type ValuesArg = HistoryApi.ValuesRequest;

describe(
  "history-v2 against a live QuestDB",
  { skip: QUESTDB_URL ? false : "set QUESTDB_URL to run" },
  () => {
    const url = new URL(QUESTDB_URL ?? "http://127.0.0.1:9000");
    const client = new QueryClient(url.hostname, Number(url.port || 9000));

    /** The table's row count, or null when it does not exist. */
    async function rowCount(table: string): Promise<number | null> {
      try {
        const result = await client.exec(`SELECT count() FROM ${table}`);
        return Number(result.dataset[0][0]);
      } catch (err) {
        if (/does not exist/.test(String(err))) return null;
        throw err;
      }
    }

    async function waitForRows(table: string, expected: number) {
      const deadline = Date.now() + APPLY_TIMEOUT_MS;
      let count = 0;
      while (Date.now() < deadline) {
        count = (await rowCount(table)) ?? 0;
        if (count >= expected) return;
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      assert.fail(`${table}: ${count} of ${expected} rows after apply`);
    }

    // Set once none of the tables exists, so the suite drops only tables it
    // created: `after` runs even when `before` throws.
    let ownsTables = false;

    before(async () => {
      for (const table of TABLES) {
        if ((await rowCount(table)) !== null) {
          throw new Error(
            `${table} already exists; point QUESTDB_URL at a throwaway instance`,
          );
        }
      }
      ownsTables = true;
      await client.ensureTables();

      const writer = new ILPWriter(url.hostname, ILP_PORT);
      try {
        await writer.connect();
        writeFixtures(writer);
      } finally {
        await writer.disconnect();
      }
      // What retention or a GPX import would have kept: speeds of 8 and 1.5
      // beside bucket 0's recorded ones and a speed of 1 alone in bucket 2,
      // a heading of 3° beside the recorded 359° and 1°, a fix inside bucket
      // 0 and the only fix of bucket 1. Only the provider given the archive
      // reads them.
      const kept = (seconds: number, path: string, value: number) =>
        `('${at(seconds).toISOString()}', '${path}', 'self', 'gps.main', ${value})`;
      await client.exec(
        `INSERT INTO ${ARCHIVE_TABLES.signalk} (ts, path, context, source, value) VALUES ` +
          [
            kept(40, "navigation.speedOverGround", 8),
            kept(55, "navigation.speedOverGround", 1.5),
            kept(130, "navigation.speedOverGround", 1),
            kept(30, "navigation.headingTrue", 3 * DEG),
          ].join(", "),
      );
      const archiveWriter = new ILPWriter(url.hostname, ILP_PORT);
      try {
        await archiveWriter.connect();
        for (const [seconds, latitude, longitude] of [
          [20, 60.15, 24.95],
          [100, 60.3, 25.1],
        ]) {
          archiveWriter.writePositionAtNanos(
            "self",
            { latitude, longitude },
            BigInt(at(seconds).getTime()) * 1_000_000n,
            "import.gpx",
            ARCHIVE_TABLES.signalk_position,
          );
        }
      } finally {
        await archiveWriter.disconnect();
      }

      await waitForRows("signalk", 6);
      await waitForRows("signalk_str", 3);
      await waitForRows("signalk_position", 2);
      await waitForRows(ARCHIVE_TABLES.signalk_position, 2);
      await waitForRows(ARCHIVE_TABLES.signalk, 4);
    });

    function writeFixtures(writer: ILPWriter): void {
      const sog = "navigation.speedOverGround";
      // Bucket 0: 2, 4, 9 — bucket 1: 6 — bucket 2: empty.
      writer.write(sog, "self", 2, at(10), "gps.main");
      writer.write(sog, "self", 4, at(30), "gps.main");
      writer.write(sog, "self", 9, at(50), "gps.aux");
      writer.write(sog, "self", 6, at(70), "gps.main");
      // 359° and 1° in one bucket.
      writer.write("navigation.headingTrue", "self", 359 * DEG, at(10));
      writer.write("navigation.headingTrue", "self", 1 * DEG, at(20));
      writer.writeString("navigation.state", "self", "anchored", at(10));
      writer.writeString("navigation.state", "self", "sailing", at(50));
      writer.writeString(
        "electrical.switches.bilgePump.state",
        "self",
        "true",
        at(20),
        "boolean",
      );
      writer.writePosition(
        "self",
        { latitude: 60.1, longitude: 24.9 },
        at(10),
        "gps.main",
      );
      writer.writePosition(
        "self",
        { latitude: 60.2, longitude: 25.0 },
        at(40),
        "gps.main",
      );
    }

    after(async () => {
      if (!ownsTables) return;
      for (const table of TABLES) {
        await client.exec(`DROP TABLE IF EXISTS ${table}`);
      }
    });

    const provider = createHistoryProviderV2(
      client,
      SELF_CONTEXT,
      true,
      undefined,
      (path) => (path === "navigation.headingTrue" ? "rad" : undefined),
    );

    const request = (
      path: string,
      aggregate: string,
      extra: Partial<ValuesArg> = {},
    ): ValuesArg =>
      ({
        from: Temporal.Instant.from(FROM),
        to: Temporal.Instant.from(TO),
        context: "vessels.self",
        resolution: 60,
        pathSpecs: [{ path: path as Path, aggregate, parameter: [] }],
        ...extra,
      }) as ValuesArg;

    const column = async (
      path: string,
      aggregate: string,
      extra: Partial<ValuesArg> = {},
    ) =>
      (await provider.getValues(request(path, aggregate, extra))).data.map(
        (row) => row[1],
      );

    it("aggregates numbers per bucket, empty buckets null", async () => {
      const sog = "navigation.speedOverGround";
      assert.deepEqual(await column(sog, "average"), [5, 6, null]);
      assert.deepEqual(await column(sog, "min"), [2, 6, null]);
      assert.deepEqual(await column(sog, "max"), [9, 6, null]);
      assert.deepEqual(await column(sog, "mid"), [5.5, 6, null]);
      assert.deepEqual(await column(sog, "first"), [2, 6, null]);
      assert.deepEqual(await column(sog, "last"), [9, 6, null]);
    });

    it("returns the raw rows without a resolution", async () => {
      const response = await provider.getValues(
        request("navigation.speedOverGround", "average", {
          resolution: undefined,
        }),
      );
      assert.deepEqual(
        response.data.map((row) => row[1]),
        [2, 4, 9, 6],
      );
    });

    it("picks the middle recorded row of each bucket", async () => {
      assert.deepEqual(
        await column("navigation.speedOverGround", "middle_index"),
        [4, 6],
      );
    });

    it("runs a moving average over the bucket averages", async () => {
      const response = await provider.getValues({
        ...request("navigation.speedOverGround", "sma"),
        pathSpecs: [
          {
            path: "navigation.speedOverGround" as Path,
            aggregate: "sma",
            parameter: ["2"],
          },
        ],
      });
      assert.deepEqual(
        response.data.map((row) => row[1]),
        [5, 5.5, 6],
      );
    });

    it("averages an angle as a vector", async () => {
      const [mean] = await column("navigation.headingTrue", "average");
      assert.equal(typeof mean, "number");
      const fromNorth = Math.min(
        Math.abs(mean as number),
        Math.abs((mean as number) - 2 * Math.PI),
      );
      assert.ok(fromNorth < 1e-9, `expected 0 rad, got ${mean}`);
    });

    it("reads text from the string table, booleans as booleans", async () => {
      const state = await provider.getValues(
        request("navigation.state", "average"),
      );
      assert.equal(state.values[0].method, "last");
      assert.deepEqual(
        state.data.map((row) => row[1]),
        ["sailing", null, null],
      );
      assert.deepEqual(
        await column("electrical.switches.bilgePump.state", "last"),
        [true, null, null],
      );
    });

    it("returns a position as a [longitude, latitude] pair", async () => {
      assert.deepEqual(await column("navigation.position", "last"), [
        [25.0, 60.2],
        null,
        null,
      ]);
    });

    it("filters by source and splits by source", async () => {
      const response = await provider.getValues({
        ...request("navigation.speedOverGround", "average"),
        pathSpecs: [
          {
            path: "navigation.speedOverGround" as Path,
            aggregate: "average",
            parameter: [],
            sourceRef: "gps.aux" as SourceRef,
          },
        ],
      });
      assert.equal(response.values[0].$source, "gps.aux");
      assert.deepEqual(
        response.data.map((row) => row[1]),
        [9, null, null],
      );

      const split = await provider.getValues({
        ...request("navigation.speedOverGround", "average"),
        sourcePolicy: "all",
      } as ValuesArg);
      assert.deepEqual(
        split.values.map((value) => value.$source),
        ["gps.aux", "gps.main"],
      );
      assert.deepEqual(split.data, [
        ["2024-01-01T00:00:00.000000Z", 9, 3],
        ["2024-01-01T00:01:00.000000Z", null, 6],
        ["2024-01-01T00:02:00.000000Z", null, null],
      ]);
    });

    it("lists the recorded paths and contexts", async () => {
      const range = {
        from: Temporal.Instant.from(FROM),
        to: Temporal.Instant.from(TO),
      };
      assert.deepEqual(
        await provider.getPaths(range as HistoryApi.PathsRequest),
        [
          "electrical.switches.bilgePump.state",
          "navigation.headingTrue",
          "navigation.position",
          "navigation.speedOverGround",
          "navigation.state",
        ],
      );
      assert.deepEqual(
        await provider.getContexts(range as HistoryApi.ContextsRequest),
        ["vessels.self"],
      );
    });

    describe("with the archive", () => {
      const spans = new ArchiveSpans(client);
      const archived = createHistoryProviderV2(
        client,
        SELF_CONTEXT,
        false,
        undefined,
        (path) => (path === "navigation.headingTrue" ? "rad" : undefined),
        0,
        spans,
      );
      before(() => spans.refresh());
      const archivedColumn = async (
        path: string,
        aggregate: string,
        extra: Partial<ValuesArg> = {},
      ) =>
        (await archived.getValues(request(path, aggregate, extra))).data.map(
          (row) => row[1],
        );

      it("weighs a bucket's average by both tables' rows", async () => {
        // (2 + 4 + 9 + 8 + 1.5) / 5, then 6 recorded alone, then 1 archived
        // alone.
        assert.deepEqual(
          await archivedColumn("navigation.speedOverGround", "average"),
          [4.9, 6, 1],
        );
      });

      it("takes min, max, mid, first and last over both tables", async () => {
        const sog = "navigation.speedOverGround";
        // Bucket 0 is recorded 2, 4, 9 and archived 8, 1.5 (at 55 s, last).
        for (const [method, expected] of [
          ["min", [1.5, 6, 1]],
          ["max", [9, 6, 1]],
          ["mid", [5.25, 6, 1]],
          ["first", [2, 6, 1]],
          ["last", [1.5, 6, 1]],
        ] as const) {
          assert.deepEqual(await archivedColumn(sog, method), expected, method);
        }
      });

      it("averages an angle over both tables as one vector", async () => {
        const [mean, ...rest] = await archivedColumn(
          "navigation.headingTrue",
          "average",
        );
        const angles = [359 * DEG, 1 * DEG, 3 * DEG];
        const expected = Math.atan2(
          angles.reduce((sum, a) => sum + Math.sin(a), 0),
          angles.reduce((sum, a) => sum + Math.cos(a), 0),
        );
        assert.ok(
          Math.abs((mean as number) - expected) < 1e-9,
          `expected ${expected} rad, got ${mean}`,
        );
        assert.deepEqual(rest, [null, null]);
      });

      it("takes the earlier fix for first and the later for last", async () => {
        assert.deepEqual(await archivedColumn("navigation.position", "first"), [
          [24.9, 60.1],
          [25.1, 60.3],
          null,
        ]);
        assert.deepEqual(await archivedColumn("navigation.position", "last"), [
          [25.0, 60.2],
          [25.1, 60.3],
          null,
        ]);
      });

      it("reads raw positions from both tables in time order", async () => {
        const raw = await archived.getValues(
          request("navigation.position", "first", { resolution: undefined }),
        );
        assert.deepEqual(
          raw.data.map((row) => row[0]),
          [10, 20, 40, 100].map((s) =>
            at(s).toISOString().replace(".000Z", ".000000Z"),
          ),
        );
      });
    });
  },
);
