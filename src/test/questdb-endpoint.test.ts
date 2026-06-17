import { describe, it } from "node:test";
import assert from "node:assert/strict";
import {
  splitHostPort,
  resolveManagedEndpoints,
  resolveLanExposureHost,
  lanExposureEndpoints,
  HOST_GATEWAY,
  QUESTDB_INTERNAL_HTTP_PORT,
  QUESTDB_INTERNAL_ILP_PORT,
} from "../questdb-endpoint";

describe("splitHostPort", () => {
  it("parses host:port", () => {
    assert.deepEqual(splitHostPort("127.0.0.1:9000", 1), {
      host: "127.0.0.1",
      port: 9000,
    });
    assert.deepEqual(splitHostPort("sk-signalk-questdb:9009", 1), {
      host: "sk-signalk-questdb",
      port: 9009,
    });
  });

  it("falls back to fallbackPort for a bare host", () => {
    assert.deepEqual(splitHostPort("questdb.local", 8812), {
      host: "questdb.local",
      port: 8812,
    });
  });

  it("parses bracketed IPv6 with and without a port", () => {
    assert.deepEqual(splitHostPort("[::1]:9000", 1), {
      host: "::1",
      port: 9000,
    });
    assert.deepEqual(splitHostPort("[fe80::1]", 9000), {
      host: "fe80::1",
      port: 9000,
    });
  });

  it("treats a bare unbracketed IPv6 literal as a host", () => {
    // Multiple colons, no brackets — ambiguous, so the whole string is the
    // host and the fallback port applies.
    assert.deepEqual(splitHostPort("fe80::1", 9000), {
      host: "fe80::1",
      port: 9000,
    });
  });

  it("returns null for empty/whitespace/malformed input", () => {
    assert.equal(splitHostPort("", 9000), null);
    assert.equal(splitHostPort("   ", 9000), null);
    assert.equal(splitHostPort(null, 9000), null);
    assert.equal(splitHostPort(undefined, 9000), null);
    assert.equal(splitHostPort("host:notaport", 9000), null);
    assert.equal(splitHostPort("host:0", 9000), null);
    assert.equal(splitHostPort(":9000", 9000), null);
    assert.equal(splitHostPort("[]:9000", 9000), null);
  });
});

describe("resolveManagedEndpoints", () => {
  it("uses resolveContainerAddress for each internal port", async () => {
    const calls: Array<[string, number]> = [];
    const containers = {
      resolveContainerAddress: async (name: string, port: number) => {
        calls.push([name, port]);
        return port === QUESTDB_INTERNAL_HTTP_PORT
          ? "sk-signalk-questdb:9000"
          : "sk-signalk-questdb:9009";
      },
    };
    const ep = await resolveManagedEndpoints(
      containers,
      "signalk-questdb",
      "127.0.0.1",
    );
    assert.deepEqual(ep.http, { host: "sk-signalk-questdb", port: 9000 });
    assert.deepEqual(ep.ilp, { host: "sk-signalk-questdb", port: 9009 });
    assert.deepEqual(calls, [
      ["signalk-questdb", QUESTDB_INTERNAL_HTTP_PORT],
      ["signalk-questdb", QUESTDB_INTERNAL_ILP_PORT],
    ]);
  });

  it("honors a remapped host port from bare-metal allocation", async () => {
    const containers = {
      resolveContainerAddress: async (_name: string, port: number) =>
        port === QUESTDB_INTERNAL_HTTP_PORT
          ? "127.0.0.1:9100" // declared 9000 was taken, allocated 9100
          : "127.0.0.1:9009",
    };
    const ep = await resolveManagedEndpoints(
      containers,
      "signalk-questdb",
      "127.0.0.1",
    );
    assert.deepEqual(ep.http, { host: "127.0.0.1", port: 9100 });
  });

  it("falls back when resolveContainerAddress is absent (old container plugin)", async () => {
    const ep = await resolveManagedEndpoints({}, "signalk-questdb", "10.0.0.5");
    assert.deepEqual(ep.http, { host: "10.0.0.5", port: 9000 });
    assert.deepEqual(ep.ilp, { host: "10.0.0.5", port: 9009 });
  });

  it("falls back when resolveContainerAddress returns null", async () => {
    const containers = {
      resolveContainerAddress: async () => null,
    };
    const ep = await resolveManagedEndpoints(
      containers,
      "signalk-questdb",
      "127.0.0.1",
    );
    assert.deepEqual(ep.http, { host: "127.0.0.1", port: 9000 });
    assert.deepEqual(ep.ilp, { host: "127.0.0.1", port: 9009 });
  });

  it("falls back when resolveContainerAddress throws", async () => {
    const containers = {
      resolveContainerAddress: async () => {
        throw new Error("not yet available");
      },
    };
    const ep = await resolveManagedEndpoints(
      containers,
      "signalk-questdb",
      "127.0.0.1",
    );
    assert.deepEqual(ep.http, { host: "127.0.0.1", port: 9000 });
  });
});

describe("resolveLanExposureHost", () => {
  // selfDeployment lives on containers.doctor, NOT at the top level.
  it("returns the gateway when Signal K is containerized", async () => {
    const host = await resolveLanExposureHost({
      doctor: { selfDeployment: async () => ({ isContainerized: true }) },
    });
    assert.equal(host, HOST_GATEWAY);
  });

  it("returns loopback when Signal K is bare-metal", async () => {
    const host = await resolveLanExposureHost({
      doctor: { selfDeployment: async () => ({ isContainerized: false }) },
    });
    assert.equal(host, "127.0.0.1");
  });

  it("falls back to loopback when the doctor API is absent", async () => {
    assert.equal(await resolveLanExposureHost({}), "127.0.0.1");
  });

  it("falls back to loopback when doctor.selfDeployment is absent", async () => {
    assert.equal(await resolveLanExposureHost({ doctor: {} }), "127.0.0.1");
  });

  it("falls back to loopback when doctor.selfDeployment throws", async () => {
    const host = await resolveLanExposureHost({
      doctor: {
        selfDeployment: async () => {
          throw new Error("probe failed");
        },
      },
    });
    assert.equal(host, "127.0.0.1");
  });

  it("does NOT consult a top-level selfDeployment (wrong API shape)", async () => {
    let called = false;
    const host = await resolveLanExposureHost({
      // Deliberately the wrong nesting — must be ignored.
      selfDeployment: async () => {
        called = true;
        return { isContainerized: true };
      },
    } as never);
    assert.equal(called, false);
    assert.equal(host, "127.0.0.1");
  });
});

describe("lanExposureEndpoints", () => {
  it("uses the supplied host and the configured host ports", () => {
    assert.deepEqual(lanExposureEndpoints("127.0.0.1", 9000, 9009), {
      http: { host: "127.0.0.1", port: 9000 },
      ilp: { host: "127.0.0.1", port: 9009 },
    });
    assert.deepEqual(lanExposureEndpoints(HOST_GATEWAY, 9500, 9509), {
      http: { host: HOST_GATEWAY, port: 9500 },
      ilp: { host: HOST_GATEWAY, port: 9509 },
    });
  });
});
