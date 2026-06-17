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

export interface DeploymentResolver {
  // selfDeployment lives on the diagnostics sub-API (containers.doctor), not at
  // the top level. The whole `doctor` object and its method are optional so the
  // plugin degrades gracefully on older signalk-container.
  doctor?: {
    selfDeployment?: () => Promise<{ isContainerized: boolean }>;
  };
}

/**
 * The host a containerized Signal K must use to reach a port published on the
 * host. Returns `HOST_GATEWAY` when SK runs in a container, otherwise the host
 * loopback. `doctor.selfDeployment` is feature-detected and non-fatal — any
 * absence/throw degrades to loopback, the historical default.
 */
export async function resolveLanExposureHost(
  containers: DeploymentResolver,
  debug?: (msg: string) => void,
): Promise<string> {
  const selfDeployment = containers.doctor?.selfDeployment;
  if (typeof selfDeployment !== "function") return "127.0.0.1";
  try {
    const dep = await selfDeployment();
    return dep.isContainerized ? HOST_GATEWAY : "127.0.0.1";
  } catch (err) {
    debug?.(
      `doctor.selfDeployment threw, assuming bare-metal (127.0.0.1): ${String(err)}`,
    );
    return "127.0.0.1";
  }
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
