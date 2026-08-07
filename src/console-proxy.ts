import type { IncomingMessage, ServerResponse } from "node:http";
import http from "node:http";

/**
 * Reverse proxy for QuestDB's own web console.
 *
 * The console is a separate app served by the QuestDB container on its HTTP
 * port. Rather than rebuild it, this forwards requests to it so the Signal K
 * admin UI can embed it — keeping Signal K's nav panel in place instead of
 * sending the user off to `:9000`, which is unreachable from another device
 * unless the operator publishes the port on the LAN.
 *
 * Plain `node:http` piping rather than a proxy library: the plugin's dependency
 * tree is deliberately lean, and what is needed here (method, path, query,
 * headers, body, streamed response) is a few lines.
 *
 * Path handling is why this works without patching QuestDB's bundle: the
 * console requests its assets and its API relatively (`./assets/…`, `exec?…`),
 * so serving it under a prefix needs no rewriting. If a future QuestDB image
 * switches to absolute paths that breaks visibly as 404s — see the plan's
 * manual-verification step — rather than silently.
 *
 * SECURITY: the console is a full SQL client. Unlike `/api/query`, which is
 * gated by isReadOnlySQL, it can drop tables. It is reachable only because
 * routes registered directly on a plugin router are admin-only by default in
 * signalk-server. Do NOT register the mount through `router.access(...)` —
 * that would downgrade it to readwrite/readonly and hand non-admins a way to
 * delete the vessel's recorded history.
 */

export interface ConsoleProxyDeps {
  /** Base URL of QuestDB's HTTP interface, e.g. `http://127.0.0.1:9000`. */
  baseUrl: () => string | null;
  debug: (msg: string) => void;
}

// Hop-by-hop headers must not be forwarded (RFC 7230 §6.1). `host` is dropped
// separately because it has to name the upstream, not the Signal K server.
const HOP_BY_HOP = new Set([
  "connection",
  "keep-alive",
  "proxy-authenticate",
  "proxy-authorization",
  "te",
  "trailer",
  "transfer-encoding",
  "upgrade",
]);

/**
 * Header names `Connection` nominates for removal. RFC 7230 §6.1 lets a hop
 * list additional single-hop headers there, so the fixed set above is not the
 * whole story — forwarding a nominated header leaks connection-scoped state
 * across the hop.
 */
function connectionNominated(headers: IncomingMessage["headers"]): Set<string> {
  const raw = Array.isArray(headers.connection)
    ? headers.connection.join(",")
    : (headers.connection ?? "");
  return new Set(
    raw
      .split(",")
      .map((v) => v.trim().toLowerCase())
      .filter(Boolean),
  );
}

function stripHopByHop(
  headers: IncomingMessage["headers"],
): Record<string, string | string[]> {
  const nominated = connectionNominated(headers);
  const out: Record<string, string | string[]> = {};
  for (const [key, value] of Object.entries(headers)) {
    if (value === undefined) continue;
    const lower = key.toLowerCase();
    if (HOP_BY_HOP.has(lower) || nominated.has(lower)) continue;
    out[key] = value;
  }
  return out;
}

function forwardableHeaders(
  headers: IncomingMessage["headers"],
): Record<string, string | string[]> {
  const out = stripHopByHop(headers);
  // `host` must name the upstream, not the Signal K server.
  delete out.host;
  // Signal K's session cookie is not QuestDB's business, and forwarding it
  // would leak an admin credential to a process that has no use for it.
  delete out.cookie;
  delete out.authorization;
  return out;
}

/**
 * Response headers must be filtered too. The upstream's own hop-by-hop
 * headers describe ITS connection to us, not ours to the browser; relaying
 * them lets QuestDB's framing decisions override the ones Node is making for
 * the client's connection.
 */
function returnableHeaders(
  headers: IncomingMessage["headers"],
): Record<string, string | string[]> {
  const out = stripHopByHop(headers);
  // The console shares the Signal K origin, so a proxied Set-Cookie would be
  // applied to it — letting a misconfigured or compromised QuestDB write
  // cookies that collide with Signal K's own session handling. Nothing is
  // lost by dropping it: the request side already strips `cookie`, so no
  // QuestDB cookie could ever have completed a round trip anyway.
  delete out["set-cookie"];
  return out;
}

export function createConsoleProxy(deps: ConsoleProxyDeps) {
  return function consoleProxy(req: IncomingMessage, res: ServerResponse) {
    const base = deps.baseUrl();
    if (!base) {
      res.writeHead(503, { "content-type": "text/plain" });
      res.end("QuestDB is not running.");
      return;
    }

    let target: URL;
    try {
      // `req.url` here is already relative to the mount point (Express strips
      // the matched prefix for `router.use`), so "/" is the console root.
      target = new URL(req.url ?? "/", base);
    } catch {
      res.writeHead(400, { "content-type": "text/plain" });
      res.end("Bad request path.");
      return;
    }

    // Confine the proxy to the resolved QuestDB origin. `new URL` would
    // otherwise let a path like `//evil.example.com/` re-target the request,
    // turning an admin-only route into an open relay.
    const origin = new URL(base);
    if (target.origin !== origin.origin) {
      res.writeHead(400, { "content-type": "text/plain" });
      res.end("Bad request path.");
      return;
    }

    const upstream = http.request(
      {
        protocol: origin.protocol,
        hostname: origin.hostname,
        port: origin.port,
        method: req.method,
        path: target.pathname + target.search,
        headers: forwardableHeaders(req.headers),
      },
      (proxied) => {
        const headers = returnableHeaders(proxied.headers);

        // The console is served under the Signal K origin, so its scripts run
        // with the admin's session. Signal K's auth cookie is httpOnly, so
        // script cannot read it — but it IS attached to same-origin requests,
        // which would let a tampered console drive Signal K's admin API as the
        // logged-in user. A plugin cannot mint a separate origin, so narrow
        // what the console may do instead.
        //
        // Deliberately NOT `connect-src 'self'`: the console's SQL editor is
        // Monaco, and the bundle carries a jsdelivr loader path for it. Static
        // analysis could not settle whether that path is actually taken, and
        // shipping a policy that blanks out the query editor would be a worse
        // outcome than the risk it mitigates. `form-action` and `base-uri`
        // still block the two cheapest ways to redirect an admin's submission
        // or rewrite relative URLs, and `frame-ancestors` blocks clickjacking
        // from another site.
        //
        // Revisit if QuestDB ever bundles Monaco locally — then `connect-src`
        // and `default-src 'self'` become safe to tighten, which is the real
        // fix for a same-origin console.
        headers["content-security-policy"] =
          "form-action 'self'; frame-ancestors 'self'; base-uri 'self'";
        // The console has no reason to be sniffed into a different type.
        headers["x-content-type-options"] = "nosniff";

        res.writeHead(proxied.statusCode ?? 502, headers);
        // A HEAD response carries no body by definition. QuestDB nonetheless
        // answers HEAD with `405 Transfer-Encoding: chunked` AND a body, which
        // is protocol-illegal; piping that upstream response through leaves
        // the client waiting for chunks that never legally arrive and the
        // connection dies with an empty reply. Terminate the response
        // ourselves and discard whatever the upstream sent.
        if (req.method === "HEAD") {
          proxied.resume();
          res.end();
          return;
        }
        proxied.pipe(res);
      },
    );

    upstream.on("error", (err) => {
      deps.debug(`console proxy error: ${err.message}`);
      if (res.headersSent) {
        res.destroy();
        return;
      }
      // The overwhelmingly common cause is the container being stopped, so say
      // that rather than surfacing an ECONNREFUSED stack to the browser.
      res.writeHead(502, { "content-type": "text/plain" });
      res.end("QuestDB is not reachable. Is the container running?");
    });

    // A client that goes away mid-request must not leave the upstream socket
    // open — the console streams large query results.
    res.on("close", () => {
      if (!upstream.destroyed) upstream.destroy();
    });

    req.pipe(upstream);
  };
}
