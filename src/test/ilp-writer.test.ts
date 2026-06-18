import { describe, it } from "node:test";
import assert from "node:assert/strict";
import * as net from "net";
import { ILPWriter } from "../ilp-writer";

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

    const writer = new ILPWriter("127.0.0.1", port);
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 6.4, ts);

    // Wait for flush timer
    await new Promise((resolve) => setTimeout(resolve, 600));
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

    const writer = new ILPWriter("127.0.0.1", port);
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.writeString("navigation.state", "self", "motoring", ts);

    await new Promise((resolve) => setTimeout(resolve, 600));
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

    const writer = new ILPWriter("127.0.0.1", port);
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.writePosition(
      "navigation.position",
      "self",
      { latitude: 52.5, longitude: 13.4 },
      ts,
    );

    await new Promise((resolve) => setTimeout(resolve, 600));
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
      timing: { initialReconnectDelay: 100, stableConnectionMs: 50 },
    });
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("navigation.speedOverGround", "self", 6.4, ts);

    // Long enough for: first socket destroyed, reconnect (100ms backoff),
    // re-flush of the retained batch onto the second socket.
    await new Promise((resolve) => setTimeout(resolve, 1200));
    await writer.disconnect();
    server.close();

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
    await new Promise((resolve) => setTimeout(resolve, 600));
    await writer.disconnect();
    server.close();

    assert.ok(
      unhealthyMsg !== null,
      "onUnhealthy should fire after repeated flaps",
    );
    assert.match(unhealthyMsg!, /dropping the write connection/);
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

    const writer = new ILPWriter("127.0.0.1", port);
    await writer.connect();

    const ts = new Date("2024-06-15T12:00:00.000Z");
    writer.write("path with spaces", "ctx,with,commas", 1.0, ts);

    await new Promise((resolve) => setTimeout(resolve, 600));
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
});
