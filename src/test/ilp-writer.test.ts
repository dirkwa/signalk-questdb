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
