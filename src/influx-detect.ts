// Identifying an InfluxDB server behind a URL.
//
// Neither of the two probe endpoints identifies the major version on its own —
// verified against live 1.8.10 and 2.9.1 servers:
//
//   - 2.x still serves the 1.x-compat `HEAD /ping` and answers 204.
//   - 1.8 also serves `GET /health` and answers `{"status":"pass"}`.
//
// Probing both and reporting a hit from each is what made one 2.9.1 server
// appear as "InfluxDB 1.x" AND "InfluxDB 2.x" side by side in the config
// panel. The reported VERSION STRING is the only reliable discriminator.

import { isIP } from "net";

import type { MigrationSource } from "./api-contract.js";

/**
 * Identify the InfluxDB (if any) at `baseUrl`.
 *
 * Returns at most one source, because one server is one server. Callers merge
 * across candidate URLs.
 */
export async function detectInflux(
  baseUrl: string,
  fetchImpl: typeof fetch = fetch,
): Promise<MigrationSource[]> {
  // Two separate facts, deliberately not folded into one variable: whether an
  // InfluxDB answered at all, and what version it claimed. Using `undefined`
  // for both meant a server that answered /health with {"status":"pass"} but
  // no `version` field (a reverse proxy or gateway can strip it) fell through
  // to /ping and, if that was disabled too, was reported as NOT RUNNING —
  // "no InfluxDB found" pointing at a live server.
  let responded = false;
  let version = "";

  // /health carries the version in its body on both majors.
  try {
    const r = await fetchImpl(`${baseUrl}/health`, {
      signal: AbortSignal.timeout(3000),
      // The URL was checked against the loopback/RFC1918 allowlist, but a
      // followed redirect is a SECOND request to a host nothing checked —
      // a 302 to a public address would defeat the guard entirely. Node's
      // default is "follow", so this has to be explicit.
      redirect: "manual",
    });
    if (r.ok) {
      const data = (await r.json()) as { status?: string; version?: string };
      if (data.status === "pass") {
        responded = true;
        // The body is whatever the far end chose to send. A non-string
        // `version` (number, object) would reach `.replace()` below and throw
        // a TypeError the route reports as a 500 — so anything that is not a
        // string is treated as "answered, version unknown".
        version = typeof data.version === "string" ? data.version : "";
      }
    }
  } catch {
    // not running, no /health, or a 200 that was not JSON — /ping may answer
  }

  // /ping carries it in a header. Worth probing both when /health answered
  // without a version: /ping may still name it, and knowing the major version
  // is what keeps 1.x and 2.x from being confused for each other.
  if (!responded || version === "") {
    try {
      const r = await fetchImpl(`${baseUrl}/ping`, {
        method: "HEAD",
        signal: AbortSignal.timeout(3000),
        // See the /health probe: an allowlisted URL must not be able to
        // redirect the server somewhere the allowlist never saw.
        redirect: "manual",
      });
      if (r.status === 204) {
        responded = true;
        // A server that answers /ping but reports no version is still an
        // InfluxDB; the empty string keeps it "found, version unknown".
        version = r.headers.get("X-Influxdb-Version") ?? version;
      }
    } catch {
      // not running
    }
  }

  if (!responded) return [];

  // 3.x is reported as nothing rather than as a version it is not. It drops
  // the /health and Flux APIs this code path assumes, so labelling it either
  // "2.x" or "1.x" would promise an interface it does not serve and send the
  // caller into a query dialect it cannot answer. "No InfluxDB found here"
  // is the honest answer until 3.x is genuinely supported.
  //
  // The major is matched with an explicit boundary (dot, dash or end of
  // string) rather than a bare `3\.`: a version reported as exactly "3" — or
  // "3-alpha" — would otherwise miss this check and fall through to the 1.x
  // label, which is the very misidentification this module exists to prevent.
  const major = /^v?(\d+)(?:[.\-+]|$)/.exec(version)?.[1];
  if (major === "3") return [];

  const isTwo = major === "2";
  return [
    {
      type: isTwo ? "influxdb2" : "influxdb1",
      url: baseUrl,
      status: "found",
      // The version already carries its own "v"; strip it so the panel's own
      // "v" prefix does not render "vv2.9.1".
      version: version.replace(/^v/, "") || "unknown",
    },
  ];
}

/**
 * Restrict a user-supplied InfluxDB URL to what the server may fetch.
 *
 * The URL comes from the config panel and is fetched server-side, so it must
 * not become an SSRF gadget into the boat's own network or a cloud metadata
 * endpoint. Loopback and RFC1918 cover the whole realistic range for "the
 * InfluxDB I am migrating off".
 *
 * Returns the normalised URL, or null when it must be refused.
 */
export function validateInfluxUrl(raw: string): string | null {
  try {
    const urlObj = new URL(raw);
    if (urlObj.protocol !== "http:" && urlObj.protocol !== "https:")
      return null;
    // URL.hostname keeps the brackets on an IPv6 literal, so a bracketed
    // loopback arrives as "[::1]" and never matched the bare "::1" this
    // compared against. Strip them before comparing.
    const host = urlObj.hostname.replace(/^\[|\]$/g, "");

    // Probes append their own path (`${url}/health`), so any query string or
    // fragment on the input would land in the MIDDLE of the resulting URL —
    // "http://h:8086/?x=1#frag/health" is not the endpoint anyone meant. Only
    // origin + path survive, and the path keeps its trailing slash stripped
    // so the join never doubles it.
    urlObj.search = "";
    urlObj.hash = "";
    // Credentials are stripped rather than passed through. Node's fetch
    // REFUSES a URL carrying them ("Request cannot be constructed from a URL
    // that includes credentials"), and that TypeError is swallowed by the
    // probe's catch — so a pasted `http://user:pw@host:8086` reported "no
    // InfluxDB found" against a perfectly healthy server, with nothing saying
    // why. Dropping them makes the probe work; InfluxDB auth belongs in the
    // token / username fields, which is where the import reads it from.
    urlObj.username = "";
    urlObj.password = "";
    const normalised = urlObj.toString().replace(/\/+$/, "");

    // "localhost" is the one NAME allowed; everything else must be an IP
    // literal. Matching the private-range patterns against a name would let
    // "10.evil.com" or "192.168.attacker.net" — both perfectly valid DNS
    // names that resolve wherever their owner points them — walk straight
    // through a guard whose whole purpose is to keep the server on the local
    // network. isIP() returns 0 for anything that is not a literal.
    if (host === "localhost") return normalised;
    const family = isIP(host);
    if (family === 0) return null;

    const isLocal =
      family === 6
        ? // ::1 is the only IPv6 loopback. Normalised forms of it (0:0:0:0:0:0:0:1)
          // are equivalent, so compare on the collapsed value too.
          host === "::1" || host === "0:0:0:0:0:0:0:1"
        : // IPv4 loopback is the whole 127.0.0.0/8, not just 127.0.0.1.
          /^127\./.test(host);
    const isPrivate =
      family === 4 && /^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)/.test(host);
    if (!isLocal && !isPrivate) return null;
    return normalised;
  } catch {
    return null;
  }
}
