import { describe, it, before, after } from "node:test";
import assert from "node:assert/strict";
import http from "node:http";
import type { AddressInfo } from "node:net";
import { createConsoleProxy } from "../console-proxy.js";

// A stand-in for QuestDB: records what the proxy forwarded so the assertions
// can be about the REQUEST the upstream saw, not just the response echoed back.
interface Seen {
  method?: string;
  url?: string;
  headers: http.IncomingHttpHeaders;
  body: string;
}

let upstream: http.Server;
let upstreamUrl: string;
let seen: Seen[] = [];

before(async () => {
  upstream = http.createServer((req, res) => {
    let body = "";
    req.on("data", (c) => (body += c));
    req.on("end", () => {
      seen.push({
        method: req.method,
        url: req.url,
        headers: req.headers,
        body,
      });
      if (req.url?.startsWith("/teapot")) {
        res.writeHead(418, { "content-type": "text/plain" });
        res.end("no coffee");
        return;
      }
      res.writeHead(200, { "content-type": "application/json" });
      res.end(JSON.stringify({ ok: true }));
    });
  });
  await new Promise<void>((r) => upstream.listen(0, "127.0.0.1", r));
  const { port } = upstream.address() as AddressInfo;
  upstreamUrl = `http://127.0.0.1:${port}`;
});

after(async () => {
  await new Promise<void>((r) => upstream.close(() => r()));
});

// Drive the proxy through a real server so req/res are genuine streams —
// piping is the part most likely to break, and a hand-rolled mock would not
// exercise it.
async function throughProxy(
  path: string,
  init: { method?: string; body?: string; baseUrl?: () => string | null } = {},
) {
  seen = [];
  const proxy = createConsoleProxy({
    baseUrl: init.baseUrl ?? (() => upstreamUrl),
    debug: () => {},
  });
  const front = http.createServer((req, res) => proxy(req, res));
  await new Promise<void>((r) => front.listen(0, "127.0.0.1", r));
  const { port } = front.address() as AddressInfo;
  try {
    const res = await fetch(`http://127.0.0.1:${port}${path}`, {
      method: init.method ?? "GET",
      body: init.body,
    });
    return {
      status: res.status,
      text: await res.text(),
      csp: res.headers.get("content-security-policy"),
      nosniff: res.headers.get("x-content-type-options"),
      seen,
    };
  } finally {
    await new Promise<void>((r) => front.close(() => r()));
  }
}

describe("console proxy forwarding", () => {
  it("forwards the path to the upstream", async () => {
    const r = await throughProxy("/index.html");
    assert.equal(r.status, 200);
    assert.equal(r.seen[0].url, "/index.html");
  });

  it("preserves the query string", async () => {
    // The console's whole API is query-driven (`exec?query=...`); dropping the
    // query would leave every request syntactically valid and useless.
    const r = await throughProxy("/exec?query=SELECT%201&limit=0%2C1000");
    assert.equal(r.seen[0].url, "/exec?query=SELECT%201&limit=0%2C1000");
  });

  it("forwards the method and body", async () => {
    const r = await throughProxy("/imp", { method: "POST", body: "csvdata" });
    assert.equal(r.seen[0].method, "POST");
    assert.equal(r.seen[0].body, "csvdata");
  });

  it("passes the upstream status and content-type through", async () => {
    const r = await throughProxy("/teapot");
    assert.equal(r.status, 418);
    assert.equal(r.text, "no coffee");
  });

  it("rewrites Host so it names the upstream, not Signal K", async () => {
    const r = await throughProxy("/");
    const host = r.seen[0].headers.host;
    assert.ok(
      host?.startsWith("127.0.0.1:"),
      `expected the upstream host, got ${host}`,
    );
  });

  it("does not leak Signal K credentials to QuestDB", async () => {
    // The admin session cookie authenticates the caller to Signal K. QuestDB
    // has no use for it and should never see it.
    const proxy = createConsoleProxy({
      baseUrl: () => upstreamUrl,
      debug: () => {},
    });
    seen = [];
    const front = http.createServer((req, res) => proxy(req, res));
    await new Promise<void>((r) => front.listen(0, "127.0.0.1", r));
    const { port } = front.address() as AddressInfo;
    try {
      await fetch(`http://127.0.0.1:${port}/`, {
        headers: {
          cookie: "JAUTHENTICATION=secret",
          authorization: "Bearer x",
        },
      });
    } finally {
      await new Promise<void>((r) => front.close(() => r()));
    }
    assert.equal(seen[0].headers.cookie, undefined);
    assert.equal(seen[0].headers.authorization, undefined);
  });
});

describe("console proxy header hygiene", () => {
  it("drops headers nominated by Connection, not just the fixed set", async () => {
    // RFC 7230 §6.1: Connection may list ADDITIONAL single-hop headers. A
    // fixed blocklist forwards those, leaking connection-scoped state across
    // the hop.
    const proxy = createConsoleProxy({
      baseUrl: () => upstreamUrl,
      debug: () => {},
    });
    seen = [];
    const front = http.createServer((req, res) => proxy(req, res));
    await new Promise<void>((r) => front.listen(0, "127.0.0.1", r));
    const { port } = front.address() as AddressInfo;
    try {
      // Raw socket: fetch() refuses to set Connection.
      await new Promise<void>((resolve) => {
        const r = http.request(
          {
            // Explicit host. `http.request` defaults to `localhost`, which
            // resolves differently inside the plugin registry's firejail
            // sandbox and failed with ECONNREFUSED — while `fetch`, used by
            // every other test here, was unaffected. The server listens on
            // 127.0.0.1, so name it.
            host: "127.0.0.1",
            port,
            path: "/",
            // `close`, not `keep-alive`: the header still nominates
            // x-hop-secret (which is what this test is about), but the socket
            // is not held open afterwards. With keep-alive the agent keeps it
            // alive, server.close() waits for it, and the test hangs — which
            // it did, but only under the registry's sandbox where the timing
            // differed enough to expose it.
            headers: {
              connection: "close, x-hop-secret",
              "x-hop-secret": "must-not-travel",
              "x-normal": "must-travel",
            },
          },
          (res) => {
            res.resume();
            res.on("end", () => resolve());
          },
        );
        r.end();
      });
    } finally {
      // closeAllConnections() so a lingering socket cannot stall teardown.
      front.closeAllConnections?.();
      await new Promise<void>((r) => front.close(() => r()));
    }
    assert.equal(seen[0].headers["x-hop-secret"], undefined);
    assert.equal(seen[0].headers["x-normal"], "must-travel");
  });

  it("sends a CSP narrowing what the same-origin console may do", async () => {
    const r = await throughProxy("/");
    assert.match(r.csp ?? "", /form-action 'self'/);
    assert.match(r.csp ?? "", /frame-ancestors 'self'/);
    assert.match(r.csp ?? "", /base-uri 'self'/);
  });

  it("does not send connect-src, which would break the SQL editor", async () => {
    // Monaco is loaded from a CDN path baked into QuestDB's bundle. A
    // `connect-src 'self'` here reads as safer but blanks out the query
    // editor — the console's entire purpose. Pinned so a future tightening is
    // a deliberate decision, taken with the Monaco question re-checked.
    const r = await throughProxy("/");
    assert.doesNotMatch(r.csp ?? "", /connect-src/);
  });

  it("does not let QuestDB set cookies on the Signal K origin", async () => {
    // The console shares Signal K's origin, so a proxied Set-Cookie would be
    // applied to it — a `Path=/` cookie from QuestDB could collide with Signal
    // K's own session handling. The request side already strips `cookie`, so
    // no QuestDB cookie could complete a round trip regardless.
    const cookieUpstream = http.createServer((_req, res) => {
      res.writeHead(200, {
        "set-cookie": "questdb_session=abc; Path=/",
        "content-type": "text/plain",
      });
      res.end("ok");
    });
    await new Promise<void>((r) => cookieUpstream.listen(0, "127.0.0.1", r));
    const upPort = (cookieUpstream.address() as AddressInfo).port;

    const proxy = createConsoleProxy({
      baseUrl: () => `http://127.0.0.1:${upPort}`,
      debug: () => {},
    });
    const front = http.createServer((req, res) => proxy(req, res));
    await new Promise<void>((r) => front.listen(0, "127.0.0.1", r));
    const { port } = front.address() as AddressInfo;

    try {
      const res = await fetch(`http://127.0.0.1:${port}/`);
      assert.equal(res.headers.get("set-cookie"), null);
    } finally {
      await new Promise<void>((r) => front.close(() => r()));
      await new Promise<void>((r) => cookieUpstream.close(() => r()));
    }
  });

  it("marks responses nosniff", async () => {
    const r = await throughProxy("/");
    assert.equal(r.nosniff, "nosniff");
  });
});

describe("console proxy HEAD handling", () => {
  it("answers HEAD without hanging", async () => {
    // QuestDB replies to HEAD with `405 Transfer-Encoding: chunked` AND a
    // body, which is protocol-illegal. Piping that through left the client
    // waiting for chunks that never legally arrive, so the connection died
    // with an empty reply — every HEAD probe of the console reported it broken
    // while the console itself worked perfectly.
    const badHead = http.createServer((req, res) => {
      res.writeHead(405, {
        "transfer-encoding": "chunked",
        "content-type": "text/plain",
      });
      res.end("Method HEAD not supported");
    });
    await new Promise<void>((r) => badHead.listen(0, "127.0.0.1", r));
    const badPort = (badHead.address() as AddressInfo).port;

    const proxy = createConsoleProxy({
      baseUrl: () => `http://127.0.0.1:${badPort}`,
      debug: () => {},
    });
    const front = http.createServer((req, res) => proxy(req, res));
    await new Promise<void>((r) => front.listen(0, "127.0.0.1", r));
    const { port } = front.address() as AddressInfo;

    try {
      const res = await fetch(`http://127.0.0.1:${port}/`, {
        method: "HEAD",
        signal: AbortSignal.timeout(4000),
      });
      assert.equal(res.status, 405);
      assert.equal(await res.text(), "", "a HEAD response carries no body");
    } finally {
      await new Promise<void>((r) => front.close(() => r()));
      await new Promise<void>((r) => badHead.close(() => r()));
    }
  });
});

describe("console proxy failure handling", () => {
  it("returns 503 when QuestDB is not running", async () => {
    const r = await throughProxy("/", { baseUrl: () => null });
    assert.equal(r.status, 503);
    assert.match(r.text, /not running/i);
  });

  it("returns a readable 502 when the upstream refuses", async () => {
    // Port 1 is reserved and never listening: a stopped container in practice.
    // Bind an ephemeral port and immediately release it: guarantees a closed
    // port on this host, rather than assuming a well-known one (port 1) is
    // free — on a machine where something holds it, the test would silently
    // stop testing refusal.
    const probe = http.createServer();
    await new Promise<void>((r) => probe.listen(0, "127.0.0.1", r));
    const deadPort = (probe.address() as AddressInfo).port;
    await new Promise<void>((r) => probe.close(() => r()));

    const r = await throughProxy("/", {
      baseUrl: () => `http://127.0.0.1:${deadPort}`,
    });
    assert.equal(r.status, 502);
    assert.match(r.text, /not reachable/i);
    assert.doesNotMatch(r.text, /ECONNREFUSED/, "must not leak a raw error");
  });
});

describe("console mount gating", () => {
  // Regression: the enableConsole check first sat AROUND the router.use(...)
  // call. registerWithRouter runs at plugin registration, before start() has
  // loaded any config, so currentConfig was still null and a saved
  // `enableConsole: false` was ignored — the console stayed reachable. Caught
  // only by end-to-end testing against a real server; every unit test passed.
  //
  // This models the mount as index.ts now does it: always registered, decided
  // per request against live config.
  function mountedHandler(getConfig: () => { enableConsole?: boolean } | null) {
    const proxy = createConsoleProxy({
      baseUrl: () => upstreamUrl,
      debug: () => {},
    });
    return (req: http.IncomingMessage, res: http.ServerResponse) => {
      if (getConfig()?.enableConsole === false) {
        // Express would fall through to the 404 handler here.
        res.writeHead(404);
        res.end();
        return;
      }
      proxy(req, res);
    };
  }

  async function callMount(
    getConfig: () => { enableConsole?: boolean } | null,
  ) {
    const front = http.createServer(mountedHandler(getConfig));
    await new Promise<void>((r) => front.listen(0, "127.0.0.1", r));
    const { port } = front.address() as AddressInfo;
    try {
      return (await fetch(`http://127.0.0.1:${port}/`)).status;
    } finally {
      await new Promise<void>((r) => front.close(() => r()));
    }
  }

  it("serves the console when config has not loaded yet", async () => {
    // null config == registration time. Must not 404: the default is on, and
    // the request will be re-evaluated once config arrives.
    assert.equal(await callMount(() => null), 200);
  });

  it("serves the console when the key is absent", async () => {
    assert.equal(await callMount(() => ({})), 200);
  });

  it("does NOT serve the console when explicitly disabled", async () => {
    assert.equal(await callMount(() => ({ enableConsole: false })), 404);
  });

  it("serves the console when explicitly enabled", async () => {
    assert.equal(await callMount(() => ({ enableConsole: true })), 200);
  });
});

describe("console proxy target confinement", () => {
  it("refuses a path that would re-target another origin", async () => {
    // `new URL("//evil.example.com/", base)` resolves to that host — without
    // an origin check the admin-only route becomes an open relay.
    const r = await throughProxy("//example.com/exec");
    assert.equal(r.status, 400);
    assert.equal(r.seen.length, 0, "nothing may reach the upstream");
  });
});
