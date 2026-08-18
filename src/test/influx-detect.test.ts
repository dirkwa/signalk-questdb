import { test, describe } from "node:test";
import assert from "node:assert";
import { detectInflux, validateInfluxUrl } from "../influx-detect.js";

describe("InfluxDB version detection", () => {
  // The reported bug: InfluxDB 2.x answers the 1.x-compat /ping with 204, so
  // probing both endpoints independently reported one server twice — the
  // config panel showed "InfluxDB 1.x found" and "InfluxDB 2.x found" side by
  // side, both pointing at the same v2.9.1 instance.
  test("a 2.x server that also answers /ping is reported once, as 2.x", async () => {
    const fakeFetch = (async (url: string | URL) => {
      const u = String(url);
      if (u.endsWith("/health"))
        return new Response(
          JSON.stringify({ status: "pass", version: "v2.9.1" }),
          { status: 200 },
        );
      if (u.endsWith("/ping"))
        return new Response(null, {
          status: 204,
          headers: { "X-Influxdb-Version": "v2.9.1" },
        });
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].type, "influxdb2");
  });

  // The panel renders "v{version}". InfluxDB already reports "v2.9.1", which
  // is how the screenshot ended up reading "vv2.9.1".
  test("version is reported without the leading v", async () => {
    const fakeFetch = (async (url: string | URL) => {
      if (String(url).endsWith("/health"))
        return new Response(
          JSON.stringify({ status: "pass", version: "v2.9.1" }),
          { status: 200 },
        );
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(sources[0].version, "2.9.1");
  });

  // InfluxDB 1.8 ALSO serves /health with {"status":"pass"} — verified against
  // a live 1.8.10. Treating a healthy /health as proof of 2.x would flip every
  // real 1.x server to "InfluxDB 2.x", which matters beyond the label: the two
  // majors need different query dialects.
  test("a 1.8 server that answers /health is still detected as 1.x", async () => {
    const fakeFetch = (async (url: string | URL) => {
      if (String(url).endsWith("/health"))
        return new Response(
          JSON.stringify({ status: "pass", version: "1.8.10" }),
          { status: 200 },
        );
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].type, "influxdb1");
    assert.strictEqual(sources[0].version, "1.8.10");
  });

  test("a 1.x server with no /health is detected via /ping", async () => {
    const fakeFetch = (async (url: string | URL) => {
      const u = String(url);
      if (u.endsWith("/health")) return new Response(null, { status: 404 });
      if (u.endsWith("/ping"))
        return new Response(null, {
          status: 204,
          headers: { "X-Influxdb-Version": "1.8.10" },
        });
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].type, "influxdb1");
  });

  test("a server answering /ping without a version header is still reported", async () => {
    const fakeFetch = (async (url: string | URL) => {
      if (String(url).endsWith("/ping"))
        return new Response(null, { status: 204 });
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].version, "unknown");
  });

  // 3.x drops the /health and Flux APIs both supported paths assume. Claiming
  // it is 2.x (or 1.x) would send the caller into a dialect it cannot answer,
  // so it is reported as nothing at all until 3.x is genuinely supported.
  test("a 3.x server is not reported as a version it is not", async () => {
    const fakeFetch = (async (url: string | URL) => {
      if (String(url).endsWith("/health"))
        return new Response(
          JSON.stringify({ status: "pass", version: "v3.0.1" }),
          { status: 200 },
        );
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.deepStrictEqual(sources, []);
  });

  // The allowlist checks the URL it is given; a followed redirect is a second
  // request to a host nothing checked. Without redirect:"manual" a 302 from an
  // allowlisted server would walk the guard straight to a public address.
  test("redirects are not followed on either probe", async () => {
    const seen: (RequestRedirect | undefined)[] = [];
    const fakeFetch = (async (_url: string | URL, init?: RequestInit) => {
      seen.push(init?.redirect);
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(seen.length, 2, "both probes should run");
    assert.ok(
      seen.every((r) => r === "manual"),
      `expected every probe to set redirect:manual, got ${JSON.stringify(seen)}`,
    );
  });

  test("an unhealthy /health falls through to /ping rather than reporting nothing", async () => {
    const fakeFetch = (async (url: string | URL) => {
      const u = String(url);
      if (u.endsWith("/health"))
        return new Response(JSON.stringify({ status: "fail" }), {
          status: 200,
        });
      if (u.endsWith("/ping"))
        return new Response(null, {
          status: 204,
          headers: { "X-Influxdb-Version": "v2.9.1" },
        });
      return new Response(null, { status: 404 });
    }) as unknown as typeof fetch;

    const sources = await detectInflux("http://localhost:8086", fakeFetch);
    assert.strictEqual(sources.length, 1);
    assert.strictEqual(sources[0].type, "influxdb2");
  });

  test("nothing running yields no sources", async () => {
    const fakeFetch = (async () => {
      throw new Error("ECONNREFUSED");
    }) as unknown as typeof fetch;
    assert.deepStrictEqual(
      await detectInflux("http://localhost:8086", fakeFetch),
      [],
    );
  });
});

describe("InfluxDB URL validation", () => {
  // The URL is fetched by the SERVER, so an unrestricted value would let a
  // config-panel user probe hosts the boat can reach but they cannot.
  test("a public host is refused", () => {
    assert.strictEqual(validateInfluxUrl("http://example.com:8086"), null);
  });

  test("the cloud metadata address is refused", () => {
    assert.strictEqual(validateInfluxUrl("http://169.254.169.254/"), null);
  });

  test("a non-http scheme is refused", () => {
    assert.strictEqual(validateInfluxUrl("file:///etc/passwd"), null);
  });

  test("garbage is refused rather than thrown on", () => {
    assert.strictEqual(validateInfluxUrl("not a url"), null);
  });

  test("localhost and private ranges are allowed", () => {
    assert.strictEqual(
      validateInfluxUrl("http://localhost:8086"),
      "http://localhost:8086",
    );
    assert.strictEqual(
      validateInfluxUrl("http://192.168.1.100:8086"),
      "http://192.168.1.100:8086",
    );
    assert.strictEqual(
      validateInfluxUrl("http://10.0.0.5:8086"),
      "http://10.0.0.5:8086",
    );
  });

  test("172.16-31 is private but 172.32 is not", () => {
    assert.ok(validateInfluxUrl("http://172.16.0.1:8086"));
    assert.ok(validateInfluxUrl("http://172.31.255.1:8086"));
    assert.strictEqual(validateInfluxUrl("http://172.32.0.1:8086"), null);
  });

  // URL.hostname returns "[::1]" for a bracketed IPv6 literal, so comparing
  // against a bare "::1" silently refused every IPv6 loopback URL.
  test("bracketed IPv6 loopback is allowed", () => {
    assert.strictEqual(
      validateInfluxUrl("http://[::1]:8086"),
      "http://[::1]:8086",
    );
  });

  test("the whole 127.0.0.0/8 loopback range is allowed", () => {
    assert.ok(validateInfluxUrl("http://127.0.0.1:8086"));
    assert.ok(validateInfluxUrl("http://127.1.2.3:8086"));
  });

  // A non-loopback IPv6 address must still be refused.
  test("a routable IPv6 address is refused", () => {
    assert.strictEqual(validateInfluxUrl("http://[2001:db8::1]:8086"), null);
  });

  // The private-range patterns are prefix matches. Applied to a NAME they let
  // any domain whose label happens to start with "10." or "192.168." through,
  // and DNS resolves it wherever its owner likes — the exact hosts the guard
  // exists to keep the server away from.
  test("a DNS name that merely looks private is refused", () => {
    assert.strictEqual(validateInfluxUrl("http://10.evil.com:8086"), null);
    assert.strictEqual(
      validateInfluxUrl("http://192.168.attacker.net:8086"),
      null,
    );
    assert.strictEqual(
      validateInfluxUrl("http://127.0.0.1.evil.com:8086"),
      null,
    );
  });

  test("a public IP literal is still refused", () => {
    assert.strictEqual(validateInfluxUrl("http://8.8.8.8:8086"), null);
    assert.strictEqual(validateInfluxUrl("http://172.32.0.1:8086"), null);
  });

  // Probes append their own path, so a query string or fragment left on the
  // value would end up in the middle of the probe URL:
  // "http://localhost:8086/?x=1#frag" + "/health".
  test("a query string and fragment are dropped", () => {
    assert.strictEqual(
      validateInfluxUrl("http://localhost:8086/?x=1#frag"),
      "http://localhost:8086",
    );
    assert.strictEqual(
      validateInfluxUrl("http://192.168.1.5:8086/?a=b"),
      "http://192.168.1.5:8086",
    );
  });

  test("a sub-path is preserved for reverse-proxied instances", () => {
    assert.strictEqual(
      validateInfluxUrl("http://192.168.1.5:8086/influx"),
      "http://192.168.1.5:8086/influx",
    );
  });

  // `${url}/health` would otherwise become `http://host:8086//health`.
  test("a trailing slash is stripped", () => {
    assert.strictEqual(
      validateInfluxUrl("http://localhost:8086/"),
      "http://localhost:8086",
    );
  });
});
