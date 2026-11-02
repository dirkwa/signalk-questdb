import { describe, it } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import { drained } from "../drained.js";

type Ending = "drained-out" | "gone" | "stuck";

// A server that writes until the client stops reading, as the parquet export
// loops do, and reports how its loop ended: "stuck" when it did not end
// within 5 s.
interface Streamed {
  ended: Ending;
  listeners: number;
}

async function streamTo(
  client: (port: number) => Promise<void>,
  chunks: number,
  signal?: AbortSignal,
): Promise<Streamed> {
  let finish!: (r: Streamed) => void;
  const result = new Promise<Streamed>((resolve) => (finish = resolve));
  const chunk = Buffer.alloc(64 * 1024, 1);
  const server = http.createServer(async (_req, res) => {
    res.setHeader("connection", "close");
    for (let i = 0; i < chunks; i++) {
      if (!res.write(chunk) && !(await drained(res, signal))) {
        finish({ ended: "gone", listeners: res.listenerCount("drain") });
        return;
      }
    }
    res.end();
    finish({ ended: "drained-out", listeners: res.listenerCount("close") });
  });
  await new Promise<void>((resolve) => server.listen(0, "127.0.0.1", resolve));
  try {
    await client((server.address() as { port: number }).port);
    let timer: NodeJS.Timeout | undefined;
    return await Promise.race([
      result,
      new Promise<Streamed>((resolve) => {
        timer = setTimeout(
          () => resolve({ ended: "stuck", listeners: -1 }),
          5000,
        );
      }),
    ]).finally(() => clearTimeout(timer));
  } finally {
    server.closeAllConnections();
    server.close();
  }
}

describe("drained", () => {
  it("lets a stream to a reading client run to its end", async () => {
    const r = await streamTo(
      (port) =>
        new Promise<void>((resolve, reject) => {
          http
            .get({ host: "127.0.0.1", port }, (res) => {
              res.resume();
              res.on("end", () => resolve());
            })
            .on("error", reject);
        }),
      64,
    );
    assert.equal(r.ended, "drained-out");
  });

  it("ends the wait when a client that stopped reading goes away", async () => {
    // Without the close wake-up the loop would wait for a drain that never
    // comes, holding the export and the watchdog's export count for good.
    const r = await streamTo(
      (port) =>
        new Promise<void>((resolve) => {
          const req = http.get({ host: "127.0.0.1", port }, (res) => {
            res.pause();
            setTimeout(() => {
              req.destroy();
              resolve();
            }, 200);
          });
          req.on("error", () => {});
        }),
      // Far more than a socket buffers, so the loop meets backpressure
      // whatever the kernel's buffer sizes.
      1 << 16,
    );
    assert.equal(r.ended, "gone");
    assert.equal(r.listeners, 0, "no drain listener left behind");
  });

  it("ends the wait at the deadline for a client that stays but stops reading", async () => {
    // No close ever comes from a suspended client that keeps its connection.
    let req: http.ClientRequest | undefined;
    const r = await streamTo(
      (port) =>
        new Promise<void>((resolve) => {
          req = http.get({ host: "127.0.0.1", port }, (res) => {
            res.pause();
            resolve();
          });
          req.on("error", () => {});
        }),
      1 << 16,
      AbortSignal.timeout(300),
    );
    req?.destroy();
    assert.equal(r.ended, "gone");
    assert.equal(r.listeners, 0);
  });
});
