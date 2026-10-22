import { describe, it } from "node:test";
import assert from "node:assert/strict";
import { GpxUploadError, uploadGpx } from "../../configpanel/gpx-upload.js";
import type { GpxImportStatus } from "../../api-contract.js";

const SIZE = 10;

/** A fake import endpoint that accepts slices in order. */
function server(
  behaviour: {
    failPutOnce?: "network" | "stale";
    refuse?: number;
    /** Answer this many PUTs "still writing the previous slice". */
    busy?: number;
  } = {},
) {
  const calls: string[] = [];
  let received = 0;
  let failed = false;
  let state: GpxImportStatus["state"] = "receiving";
  const status = (): GpxImportStatus => ({
    id: "imp-1",
    filename: "t.gpx",
    size: SIZE,
    received,
    state,
    summary: {
      bytes: received,
      tracks: 0,
      importedTracks: {},
      skippedTracks: {},
      points: 0,
      imported: 0,
      thinned: 0,
      alreadyRecorded: 0,
      invalid: 0,
      from: null,
      to: null,
    },
    startedAt: "",
    updatedAt: "",
  });
  const reply = (code: number, body: unknown) =>
    new Response(JSON.stringify(body), { status: code });
  const fetchFake = (async (url: string, init?: RequestInit) => {
    const method = init?.method ?? "GET";
    calls.push(`${method} ${url}`);
    if (method === "POST" && !url.endsWith("/finish")) {
      return behaviour.refuse
        ? reply(behaviour.refuse, { error: "QuestDB writer not connected" })
        : reply(201, { import: status() });
    }
    if (method === "GET") return reply(200, { import: status() });
    if (method === "DELETE") {
      state = "aborted";
      return reply(200, { import: status() });
    }
    if (method === "PUT") {
      const offset = Number(
        new URL(url, "http://x").searchParams.get("offset"),
      );
      if ((behaviour.busy ?? 0) > 0) {
        behaviour.busy!--;
        return reply(409, { error: "busy", import: status() });
      }
      const bytes = (init!.body as ArrayBuffer).byteLength;
      if (!failed && behaviour.failPutOnce === "network") {
        failed = true;
        received += bytes; // it arrived, the reply did not
        throw new TypeError("fetch failed");
      }
      if (!failed && behaviour.failPutOnce === "stale") {
        failed = true;
        received += 3; // the server is ahead of the client
      }
      if (offset !== received)
        return reply(409, { error: "offset", import: status() });
      received += bytes;
      return reply(200, { import: status() });
    }
    state = "finished";
    return reply(200, { import: status() });
  }) as typeof fetch;
  return { fetchFake, calls };
}

const run = (
  fetchFake: typeof fetch,
  signal = new AbortController().signal,
) => {
  const seen: number[] = [];
  return {
    seen,
    done: uploadGpx({
      file: new Blob(["0123456789"]),
      filename: "t.gpx",
      tracks: ["Own Ship"],
      fillGapsOnly: true,
      onStatus: (s) => seen.push(s.received),
      signal,
      fetch: fetchFake,
      sleep: async () => {},
      sliceBytes: 4,
    }),
  };
};

describe("uploadGpx", () => {
  it("sends the file in slices at their offsets, then finishes", async () => {
    const { fetchFake, calls } = server();
    const { done, seen } = run(fetchFake);
    assert.equal((await done).state, "finished");
    assert.deepEqual(calls, [
      "POST /plugins/signalk-questdb/api/gpx-import",
      "PUT /plugins/signalk-questdb/api/gpx-import/imp-1?offset=0",
      "PUT /plugins/signalk-questdb/api/gpx-import/imp-1?offset=4",
      "PUT /plugins/signalk-questdb/api/gpx-import/imp-1?offset=8",
      "POST /plugins/signalk-questdb/api/gpx-import/imp-1/finish",
    ]);
    assert.deepEqual(seen, [0, 4, 8, 10, 10]);
  });

  it("carries on from the server's offset after a lost reply", async () => {
    const { fetchFake, calls } = server({ failPutOnce: "network" });
    assert.equal((await run(fetchFake).done).state, "finished");
    assert.ok(calls.includes("GET /plugins/signalk-questdb/api/gpx-import"));
    assert.ok(
      calls.includes(
        "PUT /plugins/signalk-questdb/api/gpx-import/imp-1?offset=4",
      ),
    );
  });

  it("follows the server when it is ahead", async () => {
    const { fetchFake, calls } = server({ failPutOnce: "stale" });
    assert.equal((await run(fetchFake).done).state, "finished");
    assert.ok(
      calls.includes(
        "PUT /plugins/signalk-questdb/api/gpx-import/imp-1?offset=3",
      ),
    );
  });

  it("waits while the server is still writing the previous slice", async () => {
    const { fetchFake } = server({ busy: 2 });
    assert.equal((await run(fetchFake).done).state, "finished");
  });

  it("gives up on a server that never takes the slice", async () => {
    const { fetchFake } = server({ busy: 100 });
    await assert.rejects(run(fetchFake).done, (err) => {
      assert.ok(err instanceof GpxUploadError);
      assert.equal(err.message, "busy");
      return true;
    });
  });

  it("reports why the server would not start", async () => {
    const { fetchFake } = server({ refuse: 503 });
    await assert.rejects(run(fetchFake).done, (err) => {
      assert.ok(err instanceof GpxUploadError);
      assert.equal(err.message, "QuestDB writer not connected");
      return true;
    });
  });

  it("aborts the import on the server when cancelled", async () => {
    const { fetchFake, calls } = server();
    const controller = new AbortController();
    controller.abort();
    await assert.rejects(run(fetchFake, controller.signal).done, {
      name: "AbortError",
    });
    assert.ok(
      calls.includes("DELETE /plugins/signalk-questdb/api/gpx-import/imp-1"),
    );
  });
});
