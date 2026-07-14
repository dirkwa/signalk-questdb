import { describe, it } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";
import {
  splitHostPort,
  resolveManagedEndpoints,
  resolveLanExposureHost,
  lanExposureEndpoints,
  probeQuestdbPing,
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

describe("probeQuestdbPing", () => {
  // A real local HTTP server exercises the URL construction, status check,
  // and timeout path without mocking fetch.
  function serve(
    handler: (req: http.IncomingMessage, res: http.ServerResponse) => void,
  ): Promise<{ port: number; close: () => Promise<void> }> {
    const server = http.createServer(handler);
    return new Promise((resolve) =>
      server.listen(0, "127.0.0.1", () =>
        resolve({
          port: (server.address() as AddressInfo).port,
          close: () =>
            new Promise((r) => {
              server.closeAllConnections();
              server.close(() => r(undefined));
            }),
        }),
      ),
    );
  }

  it("accepts the documented 204 on /ping", async () => {
    const { port, close } = await serve((req, res) => {
      assert.equal(req.url, "/ping");
      res.writeHead(204).end();
    });
    try {
      assert.equal(await probeQuestdbPing("127.0.0.1", port), true);
    } finally {
      await close();
    }
  });

  it("rejects any other status (an unrelated service must not win)", async () => {
    const { port, close } = await serve((_req, res) => {
      res.writeHead(200).end("not questdb");
    });
    try {
      assert.equal(await probeQuestdbPing("127.0.0.1", port), false);
    } finally {
      await close();
    }
  });

  it("returns false on a refused connection", async () => {
    const { port, close } = await serve((_req, res) => res.end());
    await close();
    assert.equal(await probeQuestdbPing("127.0.0.1", port), false);
  });

  it("returns false when the server never answers within the timeout", async () => {
    const { port, close } = await serve(() => {
      /* hold the request open */
    });
    try {
      assert.equal(await probeQuestdbPing("127.0.0.1", port, 200), false);
    } finally {
      await close();
    }
  });
});

describe("resolveLanExposureHost", () => {
  // Probes never involve the network in these tests: a stub records the
  // candidates and scripts which host answers.
  function recordingProbe(answers: string[]) {
    const calls: string[] = [];
    return {
      calls,
      probe: async (host: string, port: number) => {
        calls.push(`${host}:${port}`);
        return answers.includes(host);
      },
    };
  }
  const containerized = {
    doctor: { selfDeployment: async () => ({ isContainerized: true }) },
  };
  const noRetry = { deadlineMs: 0, intervalMs: 0 };

  it("returns loopback for bare-metal Signal K without probing", async () => {
    const { calls, probe } = recordingProbe([]);
    const host = await resolveLanExposureHost(
      {
        doctor: { selfDeployment: async () => ({ isContainerized: false }) },
      },
      9000,
      undefined,
      { probe, ...noRetry },
    );
    assert.equal(host, "127.0.0.1");
    assert.deepEqual(calls, []);
  });

  it("containerized + loopback answers: host-networked SK gets 127.0.0.1 (issue #67)", async () => {
    const { calls, probe } = recordingProbe(["127.0.0.1", HOST_GATEWAY]);
    const host = await resolveLanExposureHost(containerized, 9000, undefined, {
      probe,
      ...noRetry,
    });
    assert.equal(host, "127.0.0.1");
    assert.deepEqual(calls, ["127.0.0.1:9000"]);
  });

  it("containerized + only the gateway answers: bridge-networked SK gets the gateway", async () => {
    const { calls, probe } = recordingProbe([HOST_GATEWAY]);
    const host = await resolveLanExposureHost(containerized, 9500, undefined, {
      probe,
      ...noRetry,
    });
    assert.equal(host, HOST_GATEWAY);
    assert.deepEqual(calls, ["127.0.0.1:9500", `${HOST_GATEWAY}:9500`]);
  });

  // A fake clock that only advances inside sleep() makes the retry cadence
  // fully deterministic — wall-clock scheduling never decides how many
  // rounds fit before the deadline.
  function fakeClock() {
    let t = 0;
    return {
      now: () => t,
      sleep: async (ms: number) => {
        t += ms;
      },
    };
  }

  it("retries the candidate pair until the deadline, then falls back to the gateway", async () => {
    const { calls, probe } = recordingProbe([]);
    const host = await resolveLanExposureHost(containerized, 9000, undefined, {
      probe,
      deadlineMs: 2500,
      intervalMs: 1000,
      ...fakeClock(),
    });
    assert.equal(host, HOST_GATEWAY);
    // Rounds at t=0, 1000, 2000; the sleep ending at t=3000 crosses the
    // deadline, so no fourth round starts.
    assert.equal(calls.length, 6);
  });

  it("a late success on a retry round wins over the fallback", async () => {
    let round = 0;
    const host = await resolveLanExposureHost(containerized, 9000, undefined, {
      probe: async (candidate: string) => {
        if (candidate === "127.0.0.1") round++;
        return candidate === "127.0.0.1" && round >= 3;
      },
      deadlineMs: 60_000,
      intervalMs: 1000,
      ...fakeClock(),
    });
    assert.equal(host, "127.0.0.1");
    assert.equal(round, 3);
  });

  it("clamps each probe's budget to the remaining deadline, with a floor", async () => {
    const budgets: (number | undefined)[] = [];
    const clock = fakeClock();
    await resolveLanExposureHost(containerized, 9000, undefined, {
      probe: async (_host: string, _port: number, timeoutMs?: number) => {
        budgets.push(timeoutMs);
        return false;
      },
      deadlineMs: 2000,
      intervalMs: 1500,
      ...clock,
    });
    // Round 1 (t=0): full 2000ms remain, capped at the 3000ms default → 2000.
    // Round 2 (t=1500): 500ms remain → floored at 1000.
    assert.deepEqual(budgets, [2000, 2000, 1000, 1000]);
  });

  it("falls back to loopback when the doctor API is absent", async () => {
    assert.equal(await resolveLanExposureHost({}, 9000), "127.0.0.1");
  });

  it("falls back to loopback when doctor.selfDeployment is absent", async () => {
    assert.equal(
      await resolveLanExposureHost({ doctor: {} }, 9000),
      "127.0.0.1",
    );
  });

  it("falls back to loopback when doctor.selfDeployment throws", async () => {
    const { calls, probe } = recordingProbe(["127.0.0.1"]);
    const host = await resolveLanExposureHost(
      {
        doctor: {
          selfDeployment: async () => {
            throw new Error("probe failed");
          },
        },
      },
      9000,
      undefined,
      { probe, ...noRetry },
    );
    assert.equal(host, "127.0.0.1");
    assert.deepEqual(calls, []);
  });

  it("does NOT consult a top-level selfDeployment (wrong API shape)", async () => {
    let called = false;
    const host = await resolveLanExposureHost(
      {
        // Deliberately the wrong nesting — must be ignored.
        selfDeployment: async () => {
          called = true;
          return { isContainerized: true };
        },
      } as never,
      9000,
    );
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
