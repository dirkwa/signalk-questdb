import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as net from "net";
import { ILPWriter, DEFAULT_FLUSH_INTERVAL_MS } from "../ilp-writer.js";

describe("ILPWriter", () => {
  it("sends correctly formatted ILP lines over TCP", async () => {
    const received: string[] = [];

    const server = net.createServer((socket) => {
      socket.on("data", (data) => {
        received.push(data.toString());
      });
    });

    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 6.4, ts);

    // Wait for flush timer
    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();

    server.close();

    const all = received.join("");
    assert.ok(
      all.includes(
        "signalk,path=navigation.speedOverGround,context=self value=6.4",
      ),
      `Expected ILP line in: ${all}`,
    );
    assert.ok(all.endsWith("\n"), "ILP line must end with newline");
  });

  it("sends string values to signalk_str table", async () => {
    const received: string[] = [];

    const server = net.createServer((socket) => {
      socket.on("data", (data) => {
        received.push(data.toString());
      });
    });

    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.writeString("navigation.state", "self", "motoring", ts);

    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();

    server.close();

    const all = received.join("");
    assert.ok(
      all.includes(
        'signalk_str,path=navigation.state,context=self value_str="motoring"',
      ),
      `Expected string ILP line in: ${all}`,
    );
  });

  it("tags booleans with value_kind and leaves text untagged", async () => {
    // value_kind is what keeps a recorded boolean distinguishable from a
    // path whose text value is literally "true". Text must stay untagged so
    // it reads back exactly like rows written before the column existed.
    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (chunk) => received.push(chunk.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.writeString("switches.bilge.state", "self", "true", ts, "boolean");
    writer.writeString("some.text.path", "self", "true", ts);

    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();
    server.close();

    const all = received.join("");
    assert.ok(
      all.includes(
        'signalk_str,path=switches.bilge.state,context=self,value_kind=boolean value_str="true"',
      ),
      `Expected a tagged boolean line in: ${all}`,
    );
    assert.ok(
      all.includes(
        'signalk_str,path=some.text.path,context=self value_str="true"',
      ),
      `Expected an untagged text line in: ${all}`,
    );
  });

  it("tags rows with their source and leaves sourceless writes untagged", async () => {
    // The source tag is what lets interleaved multi-receiver streams be told
    // apart afterwards. A write without one must omit the tag entirely so the
    // column stays null, exactly like rows written before it existed.
    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (chunk) => received.push(chunk.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 6.4, ts, "gps.main");
    writer.writeString(
      "navigation.state",
      "self",
      "true",
      ts,
      "boolean",
      "n2k-on-ve.can0.115",
    );
    writer.writePosition(
      "self",
      { latitude: 60.1, longitude: 24.9 },
      ts,
      "gps.main",
    );
    writer.write("environment.depth.belowKeel", "self", 3.2, ts);

    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();
    server.close();

    const all = received.join("");
    assert.ok(
      all.includes(
        "signalk,path=navigation.speedOverGround,context=self,source=gps.main value=6.4",
      ),
      `Expected a source-tagged numeric line in: ${all}`,
    );
    assert.ok(
      all.includes(
        "signalk_str,path=navigation.state,context=self," +
          'source=n2k-on-ve.can0.115,value_kind=boolean value_str="true"',
      ),
      `Expected a source-tagged string line in: ${all}`,
    );
    assert.ok(
      all.includes(
        "signalk_position,context=self,source=gps.main lat=60.1,lon=24.9",
      ),
      `Expected a source-tagged position line in: ${all}`,
    );
    assert.ok(
      all.includes(
        "signalk,path=environment.depth.belowKeel,context=self value=3.2",
      ),
      `Expected the sourceless line to carry no source tag in: ${all}`,
    );
  });

  it("sends position data to signalk_position table", async () => {
    const received: string[] = [];

    const server = net.createServer((socket) => {
      socket.on("data", (data) => {
        received.push(data.toString());
      });
    });

    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.writePosition("self", { latitude: 52.5, longitude: 13.4 }, ts);

    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();

    server.close();

    const all = received.join("");
    assert.ok(
      all.includes("signalk_position,context=self lat=52.5,lon=13.4"),
      `Expected position ILP line in: ${all}`,
    );
  });

  it("retries a batch after the connection drops mid-flush", async () => {
    const received: string[] = [];
    let firstSocket = true;

    // First connection accepts then immediately destroys the socket (mimicking
    // an overloaded QuestDB dropping us mid-write); the second keeps the data.
    const server = net.createServer((socket) => {
      if (firstSocket) {
        firstSocket = false;
        socket.on("data", () => socket.destroy());
        // Destroy shortly after connect even with no data, so a buffered batch
        // written into this socket is lost and must be re-queued.
        setTimeout(() => socket.destroy(), 50);
      } else {
        socket.on("data", (data) => received.push(data.toString()));
      }
    });

    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
      timing: { initialReconnectDelay: 100, stableConnectionMs: 50 },
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 6.4, ts);

    // Long enough for: first socket destroyed, reconnect (100ms backoff),
    // re-flush of the retained batch onto the second socket.
    await new Promise((resolve) => setTimeout(resolve, 1200));
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));

    const all = received.join("");
    assert.ok(
      all.includes(
        "signalk,path=navigation.speedOverGround,context=self value=6.4",
      ),
      `Re-queued batch should arrive on reconnect, got: ${all}`,
    );
  });

  it("reports unhealthy after repeated instant-drop flaps", async () => {
    // Server accepts every connection then instantly destroys it — the exact
    // flap pattern Kees saw (ILP connected, dropped, reconnected, on repeat).
    const server = net.createServer((socket) => socket.destroy());
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    let unhealthyMsg: string | null = null;
    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      onUnhealthy: (msg) => {
        unhealthyMsg = msg;
      },
      timing: {
        initialReconnectDelay: 20,
        maxReconnectDelay: 40,
        stableConnectionMs: 1000,
        unhealthyAfterFlaps: 3,
      },
    });
    await writer.connect().catch(() => {});

    // 3 flaps at ~20-40ms backoff each resolve well within this window.
    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));

    assert.ok(
      unhealthyMsg !== null,
      "onUnhealthy should fire after repeated flaps",
    );
    assert.match(unhealthyMsg!, /dropping the write connection/);
  });

  it("recovers health when a connection stays up, without a later close", async () => {
    // First few connections instant-drop (driving the writer unhealthy), then
    // the server stops dropping and just holds the socket open. Health must be
    // restored by the stability timer alone — there is no subsequent close to
    // trigger it. This is the exact regression CR caught: markHealthy gated on
    // the close handler would leave the writer stuck unhealthy forever.
    let connCount = 0;
    const heldSockets: net.Socket[] = [];
    const server = net.createServer((socket) => {
      connCount++;
      if (connCount <= 3) {
        socket.destroy();
      } else {
        // Keep the socket open so the connection proves stable.
        heldSockets.push(socket);
        socket.on("data", () => {});
      }
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    let unhealthyMsg: string | null = null;
    let healthyFired = false;
    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      onUnhealthy: (msg) => {
        unhealthyMsg = msg;
      },
      onHealthy: () => {
        healthyFired = true;
      },
      timing: {
        initialReconnectDelay: 20,
        maxReconnectDelay: 40,
        stableConnectionMs: 150,
        unhealthyAfterFlaps: 3,
      },
    });
    await writer.connect().catch(() => {});

    // Window covers: 3 flaps (~20-40ms each) → unhealthy, then a connection
    // that survives stableConnectionMs (150ms) → onHealthy via the timer.
    await new Promise((resolve) => setTimeout(resolve, 800));
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));
    heldSockets.forEach((s) => s.destroy());

    assert.ok(
      unhealthyMsg !== null,
      "writer should have gone unhealthy from the initial flaps",
    );
    assert.ok(
      healthyFired,
      "onHealthy should fire once a connection stays up past stableConnectionMs, with no subsequent close",
    );
  });

  it("escapes special characters in tags", async () => {
    const received: string[] = [];

    const server = net.createServer((socket) => {
      socket.on("data", (data) => {
        received.push(data.toString());
      });
    });

    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("path with spaces", "ctx,with,commas", 1.0, ts);

    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();

    server.close();

    const all = received.join("");
    assert.ok(
      all.includes("path\\ with\\ spaces"),
      `Spaces should be escaped in: ${all}`,
    );
    assert.ok(
      all.includes("ctx\\,with\\,commas"),
      `Commas should be escaped in: ${all}`,
    );
  });

  it("defaults the flush interval to 5s (batching, not the old 500ms)", async () => {
    assert.equal(DEFAULT_FLUSH_INTERVAL_MS, 5000);

    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (data) => received.push(data.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    // No flushIntervalMs option: nothing may be flushed inside a window that
    // the previous 500ms default would have flushed in.
    const writer = new ILPWriter("127.0.0.1", port);
    await writer.connect();
    writer.write("navigation.speedOverGround", "self", 6.4, new Date());
    await new Promise((resolve) => setTimeout(resolve, 700));
    assert.equal(
      received.join(""),
      "",
      "default-configured writer must not flush within 700ms",
    );
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("honors a custom flushIntervalMs", async () => {
    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (data) => received.push(data.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();
    writer.write("navigation.speedOverGround", "self", 6.4, new Date());
    await new Promise((resolve) => setTimeout(resolve, 300));
    assert.ok(
      received.join("").includes("navigation.speedOverGround"),
      "custom 100ms interval should have flushed within 300ms",
    );
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));
  });

  it("assigns strictly increasing microsecond timestamps to same-ms writes", async () => {
    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (data) => received.push(data.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    // A burst of writes to the SAME path within one tick. Without a monotonic
    // tie-breaker these share a millisecond timestamp and collide on the
    // signalk dedup key KEYS(ts, path, context); the later would upsert over
    // the earlier. Each must get a distinct timestamp instead.
    for (let i = 0; i < 5; i++) {
      writer.write("navigation.speedOverGround", "self", i, undefined);
    }
    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));

    // Parse the trailing nanosecond timestamp of each emitted line.
    const timestamps = received
      .join("")
      .trim()
      .split("\n")
      .filter((l) => l.length > 0)
      .map((l) => BigInt(l.slice(l.lastIndexOf(" ") + 1)));
    assert.equal(timestamps.length, 5, "all five rows must be sent");
    for (let i = 1; i < timestamps.length; i++) {
      assert.ok(
        timestamps[i] > timestamps[i - 1],
        `timestamps must strictly increase, got ${timestamps[i - 1]} then ${timestamps[i]}`,
      );
      // Distinct at microsecond resolution (QuestDB's storage granularity),
      // not merely at nanosecond resolution.
      assert.ok(
        timestamps[i] / 1000n > timestamps[i - 1] / 1000n,
        "timestamps must differ by at least 1µs",
      );
    }
  });

  it("honours an explicit timestamp verbatim (no monotonic bump)", async () => {
    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (data) => received.push(data.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 6.4, ts);
    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));

    const expectedNanos = BigInt(ts.getTime()) * 1_000_000n;
    assert.ok(
      received.join("").includes(` ${expectedNanos}\n`),
      `explicit timestamp must be used verbatim (${expectedNanos})`,
    );
  });

  it("floors monotonic timestamps against a prior explicit timestamp", async () => {
    const received: string[] = [];
    const server = net.createServer((socket) => {
      socket.on("data", (data) => received.push(data.toString()));
    });
    await new Promise<void>((resolve) =>
      server.listen(0, "127.0.0.1", resolve),
    );
    const port = (server.address() as net.AddressInfo).port;

    const writer = new ILPWriter("127.0.0.1", port, undefined, {
      flushIntervalMs: 100,
    });
    await writer.connect();

    // An explicit timestamp far in the future, then an omitted-timestamp
    // write. The omitted write's receive-time ns is well below the explicit
    // one, so without flooring it would emit a smaller ts and collide with a
    // future replay. It must instead advance past the explicit value.
    const future = new Date("2099-01-01T00:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 1, future);
    writer.write("navigation.speedOverGround", "self", 2, undefined);
    await new Promise((resolve) => setTimeout(resolve, 300));
    await writer.disconnect();
    await new Promise<void>((resolve) => server.close(() => resolve()));

    const timestamps = received
      .join("")
      .trim()
      .split("\n")
      .filter((l) => l.length > 0)
      .map((l) => BigInt(l.slice(l.lastIndexOf(" ") + 1)));
    assert.equal(timestamps.length, 2);
    assert.ok(
      timestamps[1] > timestamps[0],
      `omitted-timestamp write must advance past a prior explicit one, got ${timestamps[0]} then ${timestamps[1]}`,
    );
  });
});

describe("ILPWriter droppedLineCount", () => {
  it("counts buffer-overflow drops monotonically, never resetting", () => {
    // The recorder's vessel-name dedupe treats an enqueued name as written;
    // a buffer overflow silently discards enqueued lines, so the counter is
    // its only signal to invalidate that assumption. It must only ever grow.
    const writer = new ILPWriter("127.0.0.1", 1, undefined, {
      maxBufferLines: 3,
    });
    assert.equal(writer.droppedLineCount, 0);
    for (let i = 0; i < 5; i++) {
      writer.write("a.b", "self", i, new Date());
    }
    assert.equal(writer.droppedLineCount, 2);
    writer.write("a.b", "self", 9, new Date());
    assert.equal(writer.droppedLineCount, 3, "monotonic across overflows");
  });
});
