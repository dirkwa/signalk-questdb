import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { ArchiveSpans, readFrom } from "../archive.js";

type SpansClient = ConstructorParameters<typeof ArchiveSpans>[0];

function client(answer: (sql: string) => unknown[][] | Error): SpansClient {
  return {
    exec: async (sql: string) => {
      const dataset = answer(sql);
      if (dataset instanceof Error) throw dataset;
      return { columns: [], dataset, count: dataset.length, timestamp: 0 };
    },
  };
}

describe("archive readFrom", () => {
  it("reads the base table alone when the archive is not needed", () => {
    assert.equal(readFrom("signalk_position", false), "signalk_position");
  });

  it("merges base and archive in time order as a timestamped source", () => {
    const sql = readFrom("signalk_position", true);
    assert.match(
      sql,
      /^\(\(SELECT ts, context, source, lat, lon FROM signalk_position UNION ALL SELECT ts, context, source, lat, lon FROM signalk_position_archive\) ORDER BY ts\) TIMESTAMP\(ts\)$/,
    );
    assert.match(readFrom("signalk", true), /FROM signalk_archive\)/);
  });
});

describe("ArchiveSpans", () => {
  it("covers nothing before the first refresh", () => {
    const spans = new ArchiveSpans(client(() => []));
    assert.equal(
      spans.covers("signalk_position", "2020-01-01T00:00:00Z", "2030-01-01"),
      false,
    );
  });

  it("covers a range that overlaps what the archive holds", async () => {
    const spans = new ArchiveSpans(
      client((sql) =>
        sql.includes("signalk_position_archive")
          ? [["2024-03-01T00:00:00.000000Z", "2024-06-30T23:59:50.000000Z"]]
          : [[null, null]],
      ),
    );
    await spans.refresh();

    const covers = (from: string, to: string) =>
      spans.covers("signalk_position", from, to);
    assert.equal(covers("2024-06-30T00:00:00Z", "2024-07-02T00:00:00Z"), true);
    assert.equal(covers("2024-01-01T00:00:00Z", "2024-03-01T00:00:00Z"), true);
    assert.equal(covers("2024-07-01T00:00:00Z", "2024-07-02T00:00:00Z"), false);
    assert.equal(covers("2023-01-01T00:00:00Z", "2024-02-28T00:00:00Z"), false);
    // An empty archive table has a null span.
    assert.equal(
      spans.covers("signalk", "2024-01-01T00:00:00Z", "2024-12-31T00:00:00Z"),
      false,
    );
  });

  it("treats a missing archive table as empty", async () => {
    const spans = new ArchiveSpans(
      client(() => new Error("table does not exist [table=signalk_archive]")),
    );
    await spans.refresh();
    assert.equal(spans.covers("signalk", "2000-01-01", "2100-01-01"), false);
  });

  it("is known only once a refresh has read every table", async () => {
    let fail = true;
    const spans = new ArchiveSpans(
      client(() =>
        fail ? new Error("QuestDB query failed (503)") : [[null, null]],
      ),
    );
    await assert.rejects(spans.refresh());
    assert.equal(spans.known, false);
    fail = false;
    await spans.refresh();
    assert.equal(spans.known, true);
  });

  it("keeps the known span when a refresh fails for another reason", async () => {
    let fail = false;
    const spans = new ArchiveSpans(
      client(() =>
        fail
          ? new Error("QuestDB query failed (503)")
          : [["2024-03-01T00:00:00.000000Z", "2024-06-30T00:00:00.000000Z"]],
      ),
    );
    await spans.refresh();
    fail = true;
    await assert.rejects(spans.refresh(), /503/);
    assert.equal(
      spans.covers("signalk_position", "2024-04-01T00:00:00Z", "2024-04-02"),
      true,
    );
  });
});
