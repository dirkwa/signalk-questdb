// Resolving the host:port the Signal K process uses to reach QuestDB.
//
// Two managed-mode connectivity paths (see src/index.ts):
//
//   - Default (exposeToContainers=false): signalk-container owns the
//     networking via `signalkAccessiblePorts`, so the endpoint is whatever
//     `resolveContainerAddress()` returns for the current topology (loopback
//     bare-metal, container DNS when SK is containerized). Secure by default.
//
//   - LAN exposure (exposeToContainers=true): QuestDB's ports are published on
//     0.0.0.0 so another machine (or a separate-Docker Grafana) can reach
//     them. The Signal K process reaches the published port over the host
//     loopback when bare-metal, or via the `host.containers.internal` gateway
//     when SK itself runs in a container.
//
// External mode (managedContainer=false) is authoritative on the configured
// host/ports and never enters this module.
//
// Isolated here so the decisions are unit-testable without a container runtime.

// QuestDB always listens on these ports *inside* the container, regardless of
// any host-side remapping. signalk-container allocates the host binding and
// returns the reachable endpoint keyed by these internal port numbers.
export const QUESTDB_INTERNAL_HTTP_PORT = 9000;
export const QUESTDB_INTERNAL_ILP_PORT = 9009;
export const QUESTDB_INTERNAL_PG_PORT = 8812;

// Declared together so a managed QuestDB exposes every port a consumer
// (Signal K itself, or the companion Grafana plugin via
// resolveContainerAddress) might need to reach.
export const QUESTDB_ACCESSIBLE_PORTS = [
  QUESTDB_INTERNAL_HTTP_PORT,
  QUESTDB_INTERNAL_ILP_PORT,
  QUESTDB_INTERNAL_PG_PORT,
];

// Hostname a containerized Signal K uses to reach a port published on the
// host. Podman maps it natively; signalk-container injects the same mapping
// for Docker containers it manages. Used on the LAN-exposure path when SK is
// itself containerized (a published 0.0.0.0 port is reachable here, a
// loopback-only one is not — hence that path forces 0.0.0.0).
export const HOST_GATEWAY = "host.containers.internal";

export interface Endpoint {
  host: string;
  port: number;
}

/**
 * Parse a `host:port` string (as returned by resolveContainerAddress) into a
 * structured endpoint. Tolerates a bare host (no colon) by falling back to
 * `fallbackPort`, and bracketed IPv6 literals (`[::1]:9000`). Returns null for
 * empty/whitespace/malformed input so callers can fall back cleanly.
 */
export function splitHostPort(
  addr: string | null | undefined,
  fallbackPort: number,
): Endpoint | null {
  if (!addr) return null;
  const trimmed = addr.trim();
  if (!trimmed) return null;

  // Bracketed IPv6: [host]:port or [host]
  if (trimmed.startsWith("[")) {
    const close = trimmed.indexOf("]");
    if (close <= 1) return null;
    const host = trimmed.slice(1, close);
    const rest = trimmed.slice(close + 1);
    if (rest === "") return { host, port: fallbackPort };
    if (rest.startsWith(":")) {
      const port = Number(rest.slice(1));
      return Number.isInteger(port) && port > 0 ? { host, port } : null;
    }
    return null;
  }

  const lastColon = trimmed.lastIndexOf(":");
  // No colon, or a bare unbracketed IPv6 literal (multiple colons) — treat the
  // whole thing as the host and use the fallback port.
  if (lastColon === -1 || trimmed.indexOf(":") !== lastColon) {
    return { host: trimmed, port: fallbackPort };
  }
  const host = trimmed.slice(0, lastColon);
  const port = Number(trimmed.slice(lastColon + 1));
  if (!host || !Number.isInteger(port) || port <= 0) return null;
  return { host, port };
}

export interface ContainerAddressResolver {
  resolveContainerAddress?: (
    containerName: string,
    containerPort: number,
  ) => Promise<string | null>;
}

/**
 * Resolve the HTTP and ILP endpoints on the default (signalkAccessiblePorts)
 * path. Asks signalk-container for the reachable address of each internal
 * port and returns the parsed endpoints. Falls back to `fallbackHost` + the
 * internal port number when resolveContainerAddress is unavailable (older
 * signalk-container) or returns null/garbage — preserving the historical
 * loopback behaviour rather than aborting startup.
 */
export async function resolveManagedEndpoints(
  containers: ContainerAddressResolver,
  containerName: string,
  fallbackHost: string,
  debug?: (msg: string) => void,
): Promise<{ http: Endpoint; ilp: Endpoint }> {
  const resolve = async (internalPort: number): Promise<Endpoint> => {
    const fallback: Endpoint = { host: fallbackHost, port: internalPort };
    if (typeof containers.resolveContainerAddress !== "function") {
      return fallback;
    }
    try {
      const addr = await containers.resolveContainerAddress(
        containerName,
        internalPort,
      );
      return splitHostPort(addr, internalPort) ?? fallback;
    } catch (err) {
      debug?.(
        `resolveContainerAddress(${containerName}, ${internalPort}) threw, ` +
          `falling back to ${fallbackHost}:${internalPort}: ${String(err)}`,
      );
      return fallback;
    }
  };

  return {
    http: await resolve(QUESTDB_INTERNAL_HTTP_PORT),
    ilp: await resolve(QUESTDB_INTERNAL_ILP_PORT),
  };
}

/**
 * The narrowest shape resolveLanExposureHost needs from the container
 * manager: one optional method returning one field.
 *
 * Deliberately NOT the helper's ContainerManagerApi. Structural typing means
 * a real manager satisfies this, while a test can pass
 * `{ doctor: { selfDeployment: async () => ({ isContainerized: true }) } }` —
 * against the full type every fake would have to supply all twelve fields of
 * SelfDeploymentResult, which is noise rather than safety.
 *
 * selfDeployment lives on the diagnostics sub-API (containers.doctor), not at
 * the top level. Both the object and its method are optional so the plugin
 * degrades gracefully on an older signalk-container.
 */
export interface DeploymentResolver {
  doctor?: {
    selfDeployment?: () => Promise<{ isContainerized: boolean }>;
  };
}

/**
 * True when QuestDB answers its `/ping` liveness endpoint at host:port with
 * the documented empty 204. Requiring that exact status keeps an unrelated
 * service or proxy error page on a candidate host from winning the probe;
 * DNS failure, refused connection, timeout, and any other status are all
 * simply `false`.
 */
export async function probeQuestdbPing(
  host: string,
  port: number,
  timeoutMs = 3000,
): Promise<boolean> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);
  try {
    const response = await fetch(`http://${host}:${port}/ping`, {
      signal: controller.signal,
    });
    return response.status === 204;
  } catch {
    return false;
  } finally {
    clearTimeout(timer);
  }
}

// Per-candidate budget for one probe attempt, and the floor it never drops
// below even when the overall deadline is nearly spent — a too-small window
// would fail a perfectly reachable host and fall through to the wrong one.
const PROBE_TIMEOUT_MS = 3000;
const MIN_PROBE_TIMEOUT_MS = 1000;

export interface LanExposureProbeOptions {
  probe?: (host: string, port: number, timeoutMs?: number) => Promise<boolean>;
  // Keep retrying the candidate pair until this deadline before giving up on
  // probing; QuestDB may still be warming up when the first round runs.
  deadlineMs?: number;
  intervalMs?: number;
  sleep?: (ms: number) => Promise<void>;
  now?: () => number;
}

/**
 * The host the Signal K process must use to reach a QuestDB port published on
 * the host (LAN-exposure path).
 *
 * Bare-metal SK (or no doctor API to tell): the host loopback, as always.
 *
 * Containerized SK is resolved by PROBING, not inference, because the right
 * answer depends on the container's network mode, which `selfDeployment()`
 * doesn't expose (issue #67):
 *   - host-networked SK shares the host's namespace, so the published port is
 *     on its own loopback — and under Docker the `host.containers.internal`
 *     alias doesn't even resolve there (nothing injects it into a container
 *     we didn't create), which left the old inference-based resolution
 *     unhealthy forever;
 *   - bridge-networked SK has its own namespace, so loopback is wrong and the
 *     gateway alias (native on podman, injected by signalk-container on
 *     Docker) is right.
 * Loopback is probed first — a false positive would need something else
 * inside the SK container listening on QuestDB's exact host port. The pair is
 * retried until `deadlineMs` in case QuestDB is still starting; if nothing
 * ever answers we fall back to the gateway (the historical containerized
 * default), leaving the health check to report the real connectivity problem.
 */
export async function resolveLanExposureHost(
  containers: DeploymentResolver,
  httpPort: number,
  debug?: (msg: string) => void,
  options: LanExposureProbeOptions = {},
): Promise<string> {
  const selfDeployment = containers.doctor?.selfDeployment;
  if (typeof selfDeployment !== "function") return "127.0.0.1";

  let containerized: boolean;
  try {
    containerized = (await selfDeployment()).isContainerized;
  } catch (err) {
    debug?.(
      `doctor.selfDeployment threw, assuming bare-metal (127.0.0.1): ${String(err)}`,
    );
    return "127.0.0.1";
  }
  if (!containerized) return "127.0.0.1";

  const probe = options.probe ?? probeQuestdbPing;
  const deadlineMs = options.deadlineMs ?? 30_000;
  const intervalMs = options.intervalMs ?? 2000;
  const sleep =
    options.sleep ?? ((ms: number) => new Promise((r) => setTimeout(r, ms)));
  const now = options.now ?? Date.now;

  const deadline = now() + deadlineMs;
  // Each candidate's probe budget is clamped to the remaining deadline (with
  // a floor — see MIN_PROBE_TIMEOUT_MS), so a round that starts late can
  // overshoot the deadline by at most ~2x the floor rather than by two full
  // probe timeouts.
  const tryRound = async (): Promise<string | null> => {
    for (const host of ["127.0.0.1", HOST_GATEWAY]) {
      const budget = Math.max(
        Math.min(PROBE_TIMEOUT_MS, deadline - now()),
        MIN_PROBE_TIMEOUT_MS,
      );
      if (await probe(host, httpPort, budget)) {
        debug?.(`LAN-exposure host resolved by probe: ${host}:${httpPort}`);
        return host;
      }
    }
    return null;
  };

  // The first round runs unconditionally — probing is the whole point, even
  // with a zero deadline. Retry rounds never start after the deadline, and
  // the between-round sleep is clamped to the remaining budget.
  let winner = await tryRound();
  while (!winner && now() < deadline) {
    await sleep(Math.min(intervalMs, deadline - now()));
    if (now() >= deadline) break;
    winner = await tryRound();
  }
  if (winner) return winner;

  debug?.(
    `no LAN-exposure candidate answered /ping on port ${httpPort} within ` +
      `${deadlineMs}ms; falling back to ${HOST_GATEWAY}`,
  );
  return HOST_GATEWAY;
}

/**
 * Endpoints for the LAN-exposure path. QuestDB's host bindings are the
 * configured host ports; the host (loopback or gateway) was chosen by
 * `resolveLanExposureHost` from the deployment shape.
 */
export function lanExposureEndpoints(
  host: string,
  httpPort: number,
  ilpPort: number,
): { http: Endpoint; ilp: Endpoint } {
  return {
    http: { host, port: httpPort },
    ilp: { host, port: ilpPort },
  };
}
