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
import { ARCHIVE_TABLES } from "../archive.js";
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

    async function rowCount(table: string): Promise<number> {
      try {
        const result = await client.exec(`SELECT count() FROM ${table}`);
        return Number(result.dataset[0][0]);
      } catch (err) {
        if (/does not exist/.test(String(err))) return 0;
        throw err;
      }
    }

    async function waitForRows(table: string, expected: number) {
      const deadline = Date.now() + APPLY_TIMEOUT_MS;
      let count = 0;
      while (Date.now() < deadline) {
        count = await rowCount(table);
        if (count >= expected) return;
        await new Promise((resolve) => setTimeout(resolve, 200));
      }
      assert.fail(`${table}: ${count} of ${expected} rows after apply`);
    }

    // Set once the tables are known to hold nothing of anyone else's, so a
    // refused instance keeps its tables: `after` runs even when `before`
    // throws.
    let ownsTables = false;

    before(async () => {
      for (const table of TABLES) {
        if ((await rowCount(table)) > 0) {
          throw new Error(
            `${table} already holds rows; point QUESTDB_URL at a throwaway instance`,
          );
        }
      }
      ownsTables = true;
      await client.ensureTables();

      const writer = new ILPWriter(url.hostname, ILP_PORT);
      await writer.connect();
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
      await writer.disconnect();

      await waitForRows("signalk", 6);
      await waitForRows("signalk_str", 3);
      await waitForRows("signalk_position", 2);
    });

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
  },
);
