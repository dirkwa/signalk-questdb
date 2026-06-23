import { IRouter } from "express";
import { minimatch } from "minimatch";
import { ILPWriter } from "./ilp-writer";
import { QueryClient, isReadOnlySQL } from "./query-client";
import { Config, ConfigSchema } from "./config/schema";
import { createHistoryProviderV2 } from "./history-v2";
import { createHistoryProviderV1 } from "./history-v1";
import { startRetention } from "./retention";
import { buildFullExportWhere } from "./full-export-range";
import {
  QUESTDB_INTERNAL_HTTP_PORT,
  QUESTDB_INTERNAL_ILP_PORT,
  QUESTDB_INTERNAL_PG_PORT,
  QUESTDB_ACCESSIBLE_PORTS,
  resolveManagedEndpoints,
  resolveLanExposureHost,
  lanExposureEndpoints,
  type Endpoint,
} from "./questdb-endpoint";

interface App {
  debug: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
  setPluginStatus: (msg: string) => void;
  setPluginError: (msg: string) => void;
  selfContext: string;
  selfId: string;
  streambundle: {
    getBus: (path?: string) => {
      onValue: (cb: (delta: unknown) => void) => () => void;
    };
  };
  registerHistoryProvider: (provider: unknown) => void;
  registerHistoryApiProvider: (provider: unknown) => void;
  getDataDirPath: () => string;
  savePluginOptions: (config: unknown, cb: (err?: Error) => void) => void;
  [key: string]: unknown;
}

interface ContainerResourceLimits {
  cpus?: number | null;
  memory?: string | null;
}

interface ContainerConfig {
  image: string;
  tag: string;
  // Default (secure) connectivity path. signalk-container owns the networking
  // (port allocation, host binding, or attaching the container to Signal K's
  // own network) and exposes the resulting endpoint via
  // resolveContainerAddress(). Connectivity then works in every topology
  // (bare-metal SK, containerized SK on a user network, or default bridge)
  // without exposing QuestDB beyond loopback / the shared network. Mutually
  // exclusive with `ports`/`networkMode` — used when exposeToContainers is off.
  signalkAccessiblePorts?: number[];
  // Manual host port bindings ("9000/tcp" -> "0.0.0.0:9000"). Used for the
  // LAN-exposure path (exposeToContainers=true) so a separate machine or a
  // separate-Docker Grafana can reach QuestDB. Mutually exclusive with
  // signalkAccessiblePorts.
  ports?: Record<string, string>;
  networkMode?: string;
  volumes: Record<string, string>;
  env: Record<string, string>;
  restart?: string;
  resources?: ContainerResourceLimits;
  ulimits?: Record<string, number | { soft: number; hard: number }>;
  healthcheck?:
    | false
    | {
        test: string[];
        interval?: string;
        timeout?: string;
        startPeriod?: string;
        retries?: number;
      };
}

// Mirror of signalk-container's `UlimitClamp` event (signalk-container ≥
// 1.18.0). The plugin can't import the type — signalk-container is an optional
// peer reached via globalThis — so it is redeclared here.
interface UlimitClamp {
  ulimit: string;
  requested: number;
  granted: number;
  reason: string;
}

interface EnsureRunningOptions {
  onUlimitClamped?: (event: UlimitClamp) => void;
}

interface ContainerManagerApi {
  getRuntime: () => { runtime: string; version: string } | null;
  whenReady: () => Promise<void>;
  ensureRunning: (
    name: string,
    config: ContainerConfig,
    options?: EnsureRunningOptions,
  ) => Promise<void>;
  stop: (name: string) => Promise<void>;
  remove: (name: string) => Promise<void>;
  /**
   * Remove the managed container AND delete its bind-mount data at `hostPath`,
   * working around the rootless-Podman subuid-ownership trap (data written by
   * the container is owned by a subuid the Signal K user can't delete). Used by
   * the "Remove all data" action so a user can fully reset QuestDB — Signal K's
   * own plugin-uninstall can't delete this data. Optional: requires
   * signalk-container >= 1.19.0; the plugin degrades (reports unsupported) on
   * older versions.
   */
  removeManagedData?: (
    name: string,
    hostPath: string,
    options?: { ownerPluginId?: string },
  ) => Promise<void>;
  ensureNetwork: (name: string) => Promise<void>;
  /**
   * Attach an existing managed container to a user-defined network so other
   * containers on it can reach the container by its `sk-`-prefixed DNS name.
   * On the default path we use it so the companion signalk-grafana (which
   * joins `networkName` and resolves `sk-<name>`) still reaches QuestDB.
   * Optional so the plugin degrades gracefully on older signalk-container.
   */
  connectToNetwork?: (
    containerName: string,
    networkName: string,
  ) => Promise<void>;
  pullImage: (
    image: string,
    onProgress?: (msg: string) => void,
  ) => Promise<void>;
  /**
   * Diagnostics API. We only read `doctor.selfDeployment().isContainerized`:
   * on the LAN-exposure path it decides whether the Signal K process reaches
   * QuestDB's published port over the host loopback (bare-metal) or via the
   * `host.containers.internal` gateway (Signal K itself in a container).
   * Optional (the whole `doctor` object and its method) so the plugin degrades
   * gracefully on older signalk-container.
   */
  doctor?: {
    selfDeployment?: () => Promise<{ isContainerized: boolean }>;
  };
  /**
   * Resolve the `host:port` string the Signal K process should use to reach
   * `containerPort` on a managed container, for a port declared in that
   * container's `signalkAccessiblePorts`. signalk-container returns the right
   * endpoint for the current topology:
   *
   *   - bare-metal SK              → `127.0.0.1:<allocated host port>`
   *   - containerized, user net    → `sk-<name>:<containerPort>` (container DNS)
   *   - containerized, default net → `127.0.0.1:<containerPort>` (shared netns)
   *
   * Call after `ensureRunning()`. Returns `null` if the runtime is unavailable
   * or the port was never declared; throws if declared but `ensureRunning()`
   * has not run yet. Available in signalk-container 1.14.0+ — optional so the
   * plugin degrades gracefully (falls back to `questdbHost`) on older versions.
   */
  resolveContainerAddress?: (
    containerName: string,
    containerPort: number,
  ) => Promise<string | null>;
  /**
   * Translate a container-internal absolute path (e.g. the value
   * `app.getDataDirPath()` returns when SK is itself running in a
   * container) into the host-side source signalk-container should
   * pass to the runtime as a bind-mount source. Returns `null` on
   * bare-metal SK (no translation needed) or when no SK mount covers
   * the path; the caller falls back to the original path in that case.
   */
  resolveHostPath?: (
    absPath: string,
  ) => Promise<{ source: string; subPath: string } | null>;
}

// The managed QuestDB container's name. signalk-container prefixes it with
// `sk-` for the actual container and its DNS name (e.g. `sk-signalk-questdb`).
const QUESTDB_CONTAINER_NAME = "signalk-questdb";

// Tables exposed by /api/full-export. Hardcoded (not introspected via
// QuestDB's tables() function) because we DO know our schema — we
// created it. Single source of truth for both the listing endpoint and
// the per-table export's allowlist.
const FULL_EXPORT_TABLES = [
  "signalk",
  "signalk_str",
  "signalk_position",
] as const;
const FULL_EXPORT_TABLE_SET: ReadonlySet<string> = new Set(FULL_EXPORT_TABLES);

// Healthcheck for the QuestDB container. The `questdb/questdb` image ships no
// HEALTHCHECK of its own, so under Podman the container would otherwise sit in
// `starting` forever (Podman reports a probeless container as perpetually
// starting, never healthy). We give it an explicit probe — `curl` is present
// in the image — hitting QuestDB's purpose-built `/ping` liveness endpoint,
// which returns an empty `204` immediately. (Probing `/` instead makes curl
// hang on the web-console `301` redirect until it times out.) signalk-container
// emits this as `--health-*` run flags (see its `healthcheck` ContainerConfig
// field).
const QUESTDB_HEALTHCHECK = {
  test: [
    "CMD",
    "curl",
    "-f",
    `http://127.0.0.1:${QUESTDB_INTERNAL_HTTP_PORT}/ping`,
  ],
  interval: "30s",
  timeout: "5s",
  startPeriod: "15s",
  retries: 3,
};

// QuestDB recommends nofile=1048576; below it the engine logs an open-files
// warning and risks WAL corruption under heavy ingestion. A containerized
// process inherits this limit from the runtime, not the host's `fs.file-max`,
// so we pin it on the container. signalk-container clamps it down to what the
// host can actually grant (a rootless container cannot exceed the calling
// user's hard limit), so this is safe even where the host limit is lower.
const QUESTDB_ULIMITS = { nofile: 1048576 };

// How often to re-check that the owned tables still have the correct `ts`
// schema (and rebuild any ILP auto-created with the wrong shape). 60s is far
// faster than a human would notice "recording broke" yet negligible load.
const SCHEMA_HEAL_INTERVAL_MS = 60_000;

function buildResourceLimits(config: Config): ContainerResourceLimits {
  return {
    memory: config.questdbMemoryLimit?.trim() || null,
    cpus:
      typeof config.questdbCpuLimit === "number" && config.questdbCpuLimit > 0
        ? config.questdbCpuLimit
        : null,
  };
}

module.exports = (app: App) => {
  let writer: ILPWriter | null = null;
  let queryClient: QueryClient | null = null;
  let retentionTimer: NodeJS.Timeout | null = null;
  let schemaHealTimer: NodeJS.Timeout | null = null;
  let currentConfig: Config | null = null;
  // True when the `signalk` table exists with the wrong (ILP-auto-created)
  // schema — rows ingest but reads filtering on `ts` see nothing. Surfaced in
  // /api/status; the heal heartbeat clears it once the table is rebuilt.
  let schemaMismatch = false;
  // Last nofile-ulimit clamp reported by signalk-container, surfaced in
  // /api/status and the config panel so the operator can see the host limit
  // capped QuestDB's requested value. Null until a clamp happens (or on a
  // signalk-container older than 1.18.0, which doesn't emit the event).
  let ulimitClamp: UlimitClamp | null = null;

  // Record a clamp event so /api/status and the config-panel banner can
  // surface it. (The plugin status line is not used — it is driven by the
  // recording state and would immediately overwrite a warning set here.)
  // Wired into both ensureRunning call sites so an in-place update keeps the
  // warning current.
  const onUlimitClamped = (event: UlimitClamp): void => {
    ulimitClamp = event;
    app.debug(event.reason);
  };

  // Tables the plugin owns; each is rebuilt if ILP auto-created it with the
  // wrong designated-timestamp schema. (signalk_position uses `ts` too.)
  const OWNED_TABLES = ["signalk", "signalk_str", "signalk_position"];

  // Guard so a slow heal (DROP + recreate) on one heartbeat can't overlap the
  // next tick.
  let healing = false;

  // Detect and repair any owned table that ILP auto-created with the wrong
  // schema, updating the `schemaMismatch` flag that /api/status reports. Runs
  // at startup and on a heartbeat so a table dropped while the plugin is live
  // is rebuilt with the correct `ts` schema. Best-effort: introspection/heal
  // errors are logged, not thrown, so they never break the lifecycle.
  const healSchemaTables = async (): Promise<void> => {
    if (!queryClient || healing) return;
    healing = true;
    try {
      let mismatch = false;
      for (const table of OWNED_TABLES) {
        if (await queryClient.healSchema(table)) {
          app.debug(
            `Rebuilt ${table}: ILP had auto-created it with a wrong schema`,
          );
        }
        if (await queryClient.hasSchemaMismatch(table)) mismatch = true;
      }
      schemaMismatch = mismatch;
    } catch (err) {
      app.debug(
        `schema heal check failed: ${err instanceof Error ? err.message : String(err)}`,
      );
    } finally {
      healing = false;
    }
  };
  // The HTTP/ILP endpoints the Signal K process uses to reach QuestDB. In
  // managed mode signalk-container resolves these (loopback bare-metal,
  // container DNS when SK is containerized); in external mode they come from
  // config. The REST/export endpoints read `questdbEndpoints.http` so they
  // stay correct in every topology, not just when questdbHost is loopback.
  let questdbEndpoints: { http: Endpoint; ilp: Endpoint } | null = null;
  const unsubscribes: (() => void)[] = [];
  const throttleMap = new Map<string, number>();

  // The HTTP base URL for QuestDB's REST API (/exp, /exec). Used by the export
  // endpoints, which talk to QuestDB directly rather than through QueryClient.
  // Those callers all guard on `!queryClient` first, and `questdbEndpoints` is
  // set before `queryClient` in asyncStart and cleared after it in stop(), so
  // it is non-null whenever this runs. The config-derived branch is purely
  // defensive (never exercised in practice).
  function questdbHttpBaseUrl(): string {
    if (questdbEndpoints) {
      const { host, port } = questdbEndpoints.http;
      return `http://${host}:${port}`;
    }
    const host = currentConfig?.questdbHost ?? "127.0.0.1";
    const port = currentConfig?.questdbHttpPort ?? QUESTDB_INTERNAL_HTTP_PORT;
    return `http://${host}:${port}`;
  }

  /**
   * Compute the bind-mount source for QuestDB's /var/lib/questdb volume.
   *
   * `app.getDataDirPath()` returns the path from SK's own perspective. On
   * bare-metal that is the host path and the runtime can use it directly.
   * When SK runs inside a container the same string is the SK-container-
   * internal path, and the host's runtime daemon — which is on the host,
   * not inside SK — cannot resolve it. signalk-container 1.9.0+ exposes
   * `resolveHostPath()` to translate such paths back to the host source;
   * if it returns null (older signalk-container or no covering mount) we
   * fall back to the original path, preserving bare-metal behaviour.
   */
  async function resolveQuestdbVolumeSource(
    containers: ContainerManagerApi,
  ): Promise<string> {
    const dataPath = app.getDataDirPath();
    if (typeof containers.resolveHostPath !== "function") return dataPath;
    // signalk-container's resolveHostPath is documented as non-throwing, but
    // we consume it through a runtime cross-plugin API (cast through `any`),
    // so an unexpected throw from a future or older version must not abort
    // startup — fall back to the original path instead.
    try {
      const resolved = await containers.resolveHostPath(dataPath);
      return resolved?.source ?? dataPath;
    } catch (err) {
      app.debug("resolveHostPath threw, falling back to dataPath:", err);
      return dataPath;
    }
  }

  /**
   * Apply the chosen networking to a managed-QuestDB ContainerConfig and
   * return the endpoints the Signal K process should use to reach it. Two
   * paths, keyed on `exposeToContainers`:
   *
   *   - off (default): signalkAccessiblePorts — signalk-container owns the
   *     networking and resolveContainerAddress() yields the endpoint. Secure;
   *     QuestDB is not published beyond loopback / the shared network.
   *   - on: publish the configured host ports on 0.0.0.0 (for LAN / separate-
   *     Docker Grafana) on the shared `networkName`; the Signal K process
   *     reaches them on the host loopback (bare-metal) or via the
   *     host.containers.internal gateway (containerized SK).
   *
   * `endpoints` is resolved AFTER ensureRunning() on the signalkAccessiblePorts
   * path (the host port is allocated by then); on the LAN path it is already
   * known, so the returned resolver just echoes it.
   */
  async function applyQuestdbNetworking(
    config: Config,
    containers: ContainerManagerApi,
    name: string,
    containerConfig: ContainerConfig,
  ): Promise<() => Promise<{ http: Endpoint; ilp: Endpoint }>> {
    const httpPort = config.questdbHttpPort ?? QUESTDB_INTERNAL_HTTP_PORT;
    const ilpPort = config.questdbIlpPort ?? QUESTDB_INTERNAL_ILP_PORT;
    const pgPort = config.questdbPgPort ?? QUESTDB_INTERNAL_PG_PORT;
    const fallbackHost = config.questdbHost ?? "127.0.0.1";

    // signalkAccessiblePorts is the modern (1.14.0+) connectivity path. Without
    // it, signalk-container ignores the field and would publish no host port,
    // so on older versions we must keep the historical manual port bindings.
    const hasAccessiblePorts =
      typeof containers.resolveContainerAddress === "function";

    // The LAN-exposure path and the old-container fallback both publish ports
    // and attach to networkName; only the host the SK process uses differs.
    const publishOnHost = config.exposeToContainers || !hasAccessiblePorts;
    if (publishOnHost) {
      const bind = config.exposeToContainers ? "0.0.0.0" : "127.0.0.1";
      containerConfig.ports = {
        [`${QUESTDB_INTERNAL_HTTP_PORT}/tcp`]: `${bind}:${httpPort}`,
        [`${QUESTDB_INTERNAL_ILP_PORT}/tcp`]: `${bind}:${ilpPort}`,
        [`${QUESTDB_INTERNAL_PG_PORT}/tcp`]: `${bind}:${pgPort}`,
      };
      if (config.networkName) {
        await containers.ensureNetwork(config.networkName);
        containerConfig.networkMode = config.networkName;
      }
      // A 0.0.0.0-published port is reachable from a containerized SK via the
      // host gateway; a loopback-only one (or any bare-metal/old-container
      // case) is reached on 127.0.0.1.
      const skHost = config.exposeToContainers
        ? await resolveLanExposureHost(containers, (msg) => app.debug(msg))
        : "127.0.0.1";
      const endpoints = lanExposureEndpoints(skHost, httpPort, ilpPort);
      return async () => endpoints;
    }

    // Default path: signalk-container owns the networking. After the container
    // is up we additionally attach it to `networkName` so the companion
    // signalk-grafana (which joins that network and resolves QuestDB by its
    // `sk-`-prefixed DNS name) keeps working, then resolve the SK->QuestDB
    // endpoint from whatever address signalk-container reports.
    containerConfig.signalkAccessiblePorts = QUESTDB_ACCESSIBLE_PORTS;
    return async () => {
      if (
        config.networkName &&
        typeof containers.connectToNetwork === "function"
      ) {
        try {
          await containers.ensureNetwork(config.networkName);
          await containers.connectToNetwork(name, config.networkName);
        } catch (err) {
          // Non-fatal: SK->QuestDB does not depend on this network; only the
          // companion Grafana's DNS path does. Log and carry on.
          app.debug(
            `connectToNetwork(${name}, ${config.networkName}) failed: ${String(err)}`,
          );
        }
      }
      return resolveManagedEndpoints(containers, name, fallbackHost, (msg) =>
        app.debug(msg),
      );
    };
  }

  function shouldRecord(
    path: string,
    filter: { mode: string; paths: string[] },
  ): boolean {
    if (!filter.paths || filter.paths.length === 0) return true;

    const matches = filter.paths.some((pattern) => minimatch(path, pattern));
    return filter.mode === "exclude" ? !matches : matches;
  }

  function isThrottled(
    path: string,
    rates: Record<string, number>,
    defaultRate: number,
  ): boolean {
    const now = Date.now();

    // Check per-path overrides first
    for (const [pattern, minMs] of Object.entries(rates)) {
      if (minMs <= 0) continue;
      if (!minimatch(path, pattern)) continue;

      const lastWrite = throttleMap.get(path) ?? 0;
      if (now - lastWrite < minMs) return true;
      throttleMap.set(path, now);
      return false;
    }

    // Apply default rate
    if (defaultRate > 0) {
      const lastWrite = throttleMap.get(path) ?? 0;
      if (now - lastWrite < defaultRate) return true;
      throttleMap.set(path, now);
    }

    return false;
  }

  async function asyncStart(config: Config) {
    currentConfig = config;
    // External mode: the configured host/ports are authoritative. Managed mode
    // overwrites this below with the endpoints signalk-container resolves.
    questdbEndpoints = {
      http: {
        host: config.questdbHost ?? "127.0.0.1",
        port: config.questdbHttpPort ?? QUESTDB_INTERNAL_HTTP_PORT,
      },
      ilp: {
        host: config.questdbHost ?? "127.0.0.1",
        port: config.questdbIlpPort ?? QUESTDB_INTERNAL_ILP_PORT,
      },
    };

    if (config.managedContainer !== false) {
      const containers = (globalThis as any).__signalk_containerManager as
        | ContainerManagerApi
        | undefined;

      if (!containers) {
        app.debug("containerManager not found");
        app.setPluginError(
          "signalk-container plugin required for managed mode. Install it or set managedContainer=false.",
        );
        return;
      }

      app.setPluginStatus("Waiting for container runtime detection...");
      await containers.whenReady();

      if (!containers.getRuntime()) {
        app.debug("container runtime not detected");
        app.setPluginError(
          "No container runtime detected. Check signalk-container plugin.",
        );
        return;
      }

      void (async () => {
        try {
          const fs = await import("fs/promises");
          await fs.unlink(`${app.getDataDirPath()}.container-hash`);
        } catch {
          /* never existed or already cleaned up */
        }
      })();

      app.debug("container runtime ready, starting QuestDB");
      try {
        const containerEnv: Record<string, string> = {
          QDB_TELEMETRY_ENABLED: "false",
          QDB_HTTP_ENABLED: "true",
          QDB_LINE_TCP_ENABLED: "true",
          // Reduce CPU usage on low-power devices (Pi, Cerbo)
          QDB_CAIRO_WAL_APPLY_WORKER_COUNT: "1",
          QDB_SHARED_WORKER_COUNT: "1",
          QDB_LINE_TCP_WRITER_WORKER_COUNT: "1",
          QDB_CAIRO_O3_COLUMN_MEMORY_SIZE: "262144",
          ...(config.compression && config.compression !== "none"
            ? {
                QDB_CAIRO_WAL_SEGMENT_COMPRESSION_CODEC:
                  config.compression === "zstd" ? "ZSTD" : "LZ4",
                ...(config.compression === "zstd" && config.compressionLevel
                  ? {
                      QDB_CAIRO_WAL_SEGMENT_COMPRESSION_LEVEL: String(
                        config.compressionLevel,
                      ),
                    }
                  : {}),
              }
            : {}),
        };

        const volumeSource = await resolveQuestdbVolumeSource(containers);
        const containerConfig: ContainerConfig = {
          image: "questdb/questdb",
          tag: config.questdbVersion ?? "latest",
          volumes: {
            "/var/lib/questdb": volumeSource,
          },
          env: containerEnv,
          restart: "unless-stopped",
          resources: buildResourceLimits(config),
          ulimits: QUESTDB_ULIMITS,
          healthcheck: QUESTDB_HEALTHCHECK,
        };
        const resolveEndpoints = await applyQuestdbNetworking(
          config,
          containers,
          QUESTDB_CONTAINER_NAME,
          containerConfig,
        );

        app.setPluginStatus("Starting QuestDB container...");
        // Clear any prior clamp so a run that no longer clamps (e.g. the host
        // limit was raised) doesn't leave a stale warning; onUlimitClamped
        // re-sets it if this run clamps again.
        ulimitClamp = null;
        await containers.ensureRunning(
          QUESTDB_CONTAINER_NAME,
          containerConfig,
          {
            onUlimitClamped,
          },
        );
        app.debug("QuestDB container ready");

        questdbEndpoints = await resolveEndpoints();
      } catch (err) {
        app.debug("ensureRunning failed:", err);
        app.setPluginError(
          `Failed to start QuestDB container: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
    }

    const { host: httpHost, port: httpPort } = questdbEndpoints.http;
    const { host: ilpHost, port: ilpPort } = questdbEndpoints.ilp;

    app.debug("connecting to QuestDB at %s:%d", httpHost, httpPort);
    queryClient = new QueryClient(httpHost, httpPort);

    app.setPluginStatus("Waiting for QuestDB to become ready...");
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      if (await queryClient.isHealthy()) break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    if (!(await queryClient.isHealthy())) {
      app.setPluginError(`QuestDB not responding at ${httpHost}:${httpPort}`);
      return;
    }

    app.setPluginStatus("Creating tables...");
    await queryClient.ensureTables();
    // A prior crash/drop may have left an ILP-auto-created `signalk` table with
    // the wrong (`timestamp`, not `ts`) schema; heal it before the writer can
    // ingest into the broken shape.
    await healSchemaTables();

    writer = new ILPWriter(ilpHost, ilpPort, (msg) => app.debug(msg), {
      // Surface a flapping ILP connection instead of leaving the status line
      // stuck on a cheerful "Recording" while every sample is dropped.
      onUnhealthy: (msg) => app.setPluginError(msg),
      onHealthy: () =>
        app.setPluginStatus(`Recording to QuestDB at ${ilpHost}:${ilpPort}`),
    });
    await writer.connect();

    const v2Provider = createHistoryProviderV2(queryClient, app.selfContext);
    app.registerHistoryApiProvider(v2Provider);

    // Set ourselves as default history provider (other plugins like Kip may
    // register first depending on load order)
    setTimeout(async () => {
      try {
        const port = process.env.PORT || 3000;
        await fetch(
          `http://127.0.0.1:${port}/signalk/v2/api/history/_providers/_default/signalk-questdb`,
          { method: "POST", signal: AbortSignal.timeout(5000) },
        );
        app.debug("set as default history provider");
      } catch {
        app.debug("could not set as default history provider");
      }
    }, 5000);

    const v1Provider = createHistoryProviderV1(
      queryClient,
      app.selfContext,
      (msg) => app.debug(msg),
    );
    app.registerHistoryProvider(v1Provider);

    const bus = app.streambundle.getBus();
    const unsub = bus.onValue((delta: any) => {
      if (!writer) return;
      const { path, value, context, timestamp } = delta;
      if (!path || value === undefined || value === null) return;

      const isSelf = context === app.selfContext;
      if (isSelf && !config.recordSelf) return;
      if (!isSelf && !config.recordOthers) return;

      if (!shouldRecord(path, config.pathFilter)) return;
      if (
        isThrottled(
          path,
          config.samplingRates,
          config.defaultSamplingRate ?? 2000,
        )
      )
        return;

      const ts = timestamp ? new Date(timestamp) : new Date();
      const ctx = isSelf ? "self" : context;

      if (typeof value === "number") {
        writer.write(path, ctx, value, ts);
      } else if (typeof value === "string") {
        writer.writeString(path, ctx, value, ts);
      } else if (
        value &&
        typeof value === "object" &&
        "latitude" in value &&
        "longitude" in value
      ) {
        writer.writePosition(path, ctx, value, ts);
      }
    });
    unsubscribes.push(unsub);

    if (config.retentionDays && config.retentionDays > 0) {
      retentionTimer = startRetention(
        queryClient,
        config.retentionDays,
        (msg) => app.debug(msg),
      );
    }

    // Heartbeat: catch and repair a table that gets dropped (e.g. a manual
    // WAL recovery) and re-auto-created by ILP with the wrong schema, so
    // recording silently breaking heals itself instead of needing a restart.
    schemaHealTimer = setInterval(() => {
      void healSchemaTables();
    }, SCHEMA_HEAL_INTERVAL_MS);

    app.setPluginStatus(`Recording to QuestDB at ${ilpHost}:${ilpPort}`);
  }

  const plugin = {
    id: "signalk-questdb",
    name: "QuestDB History",

    schema: ConfigSchema,

    start(config: Config) {
      // Server does not await start(), so run async init in a
      // self-contained promise that handles its own errors.

      asyncStart(config).catch((err) => {
        app.setPluginError(
          `Startup failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      });
    },

    async stop() {
      for (const unsub of unsubscribes) {
        try {
          unsub();
        } catch {
          /* ignore */
        }
      }
      unsubscribes.length = 0;

      if (retentionTimer) {
        clearInterval(retentionTimer);
        retentionTimer = null;
      }

      if (schemaHealTimer) {
        clearInterval(schemaHealTimer);
        schemaHealTimer = null;
      }
      schemaMismatch = false;

      // Clear the clamp warning so it doesn't survive into the next start
      // (e.g. a switch to external/unmanaged mode that never calls ensureRunning).
      ulimitClamp = null;

      if (writer) {
        await writer.disconnect();
        writer = null;
      }

      throttleMap.clear();
      queryClient = null;
      questdbEndpoints = null;

      // Stop the managed container when plugin is disabled
      if (currentConfig?.managedContainer !== false) {
        const containers = (globalThis as any).__signalk_containerManager as
          | ContainerManagerApi
          | undefined;
        if (containers) {
          try {
            await containers.stop(QUESTDB_CONTAINER_NAME);
          } catch {
            // container may already be stopped
          }
        }
      }

      currentConfig = null;
    },

    registerWithRouter(router: IRouter) {
      router.get("/api/status", async (_req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ status: "not_running" });
            return;
          }

          const healthy = await queryClient.isHealthy();
          if (!healthy) {
            res.status(503).json({ status: "unhealthy" });
            return;
          }

          let totalRows = 0;
          let activePathsToday = 0;
          try {
            const countResult = await queryClient.exec(
              "SELECT count() as cnt FROM signalk",
            );
            totalRows =
              countResult.dataset.length > 0
                ? (countResult.dataset[0][0] as number)
                : 0;
            const pathResult = await queryClient.exec(
              "SELECT count(distinct path) as cnt FROM signalk WHERE ts > dateadd('d', -1, now())",
            );
            activePathsToday =
              pathResult.dataset.length > 0
                ? (pathResult.dataset[0][0] as number)
                : 0;
          } catch {
            // tables may not exist yet during startup
          }

          // A suspended WAL means rows are arriving over ILP but never commit,
          // so totalRows/activePathsToday quietly flatline while the status line
          // still reads "running". Surface it explicitly so the panel can warn
          // instead of looking healthy. `txnLag` = sequencer ahead of writer =
          // the backlog that will never drain until the WAL is resumed.
          let suspendedTables: {
            name: string;
            writerTxn: number;
            sequencerTxn: number;
            txnLag: number;
          }[] = [];
          try {
            const walResult = await queryClient.exec(
              "SELECT name, writerTxn, sequencerTxn FROM wal_tables() WHERE suspended = true",
            );
            suspendedTables = queryClient.toObjects(walResult).map((row) => {
              const writerTxn = Number(row.writerTxn ?? 0);
              const sequencerTxn = Number(row.sequencerTxn ?? 0);
              return {
                name: String(row.name),
                writerTxn,
                sequencerTxn,
                txnLag: Math.max(0, sequencerTxn - writerTxn),
              };
            });
          } catch {
            // wal_tables() is unavailable on non-WAL/older QuestDB, or the
            // tables don't exist yet during startup — treat as "not suspended".
          }

          res.json({
            status: "running",
            totalRows,
            activePathsToday,
            walSuspended: suspendedTables.length > 0,
            suspendedTables,
            schemaMismatch,
            ulimitClamp,
            endpoint: questdbEndpoints
              ? `${questdbEndpoints.http.host}:${questdbEndpoints.http.port}`
              : null,
          });
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.get("/api/query", async (req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ error: "QuestDB not connected" });
            return;
          }
          const sql = req.query.sql as string;
          if (!sql) {
            res.status(400).json({ error: "Missing sql parameter" });
            return;
          }
          if (!isReadOnlySQL(sql)) {
            res.status(403).json({ error: "Only read-only queries allowed" });
            return;
          }
          const result = await queryClient.execSafe(sql);
          res.json(result);
        } catch (err) {
          res.status(400).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.get("/api/paths", async (_req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ error: "QuestDB not connected" });
            return;
          }
          const result = await queryClient.exec(
            `SELECT path, count() as rows, min(ts) as first_seen, max(ts) as last_seen
             FROM signalk
             WHERE context = 'self'
             GROUP BY path
             ORDER BY path`,
          );
          res.json(queryClient.toObjects(result));
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.get("/api/versions", async (_req, res) => {
        try {
          const ghRes = await fetch(
            "https://api.github.com/repos/questdb/questdb/releases?per_page=10",
            {
              headers: { Accept: "application/vnd.github+json" },
              signal: AbortSignal.timeout(10000),
            },
          );
          if (!ghRes.ok) {
            res.status(502).json({ error: "Failed to fetch releases" });
            return;
          }
          const releases = (await ghRes.json()) as {
            tag_name: string;
            prerelease: boolean;
            draft: boolean;
          }[];
          const versions = releases
            .filter((r) => !r.draft)
            .map((r) => ({
              tag: r.tag_name,
              prerelease: r.prerelease,
            }));
          res.json(versions);
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.get("/api/update/check", async (_req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ error: "QuestDB not connected" });
            return;
          }

          // Get running QuestDB version via SQL
          const buildResult = await queryClient.exec("SELECT build()");
          const buildStr =
            buildResult.dataset.length > 0
              ? (buildResult.dataset[0][0] as string)
              : "";
          const versionMatch = buildStr.match(/QuestDB\s+([\d.]+)/);
          const currentVersion = versionMatch ? versionMatch[1] : "unknown";

          // Get latest stable release from GitHub
          const ghRes = await fetch(
            "https://api.github.com/repos/questdb/questdb/releases?per_page=5",
            {
              headers: { Accept: "application/vnd.github+json" },
              signal: AbortSignal.timeout(10000),
            },
          );
          let latestVersion = "unknown";
          if (ghRes.ok) {
            const releases = (await ghRes.json()) as {
              tag_name: string;
              prerelease: boolean;
              draft: boolean;
            }[];
            const stable = releases.find((r) => !r.draft && !r.prerelease);
            if (stable) latestVersion = stable.tag_name;
          }

          const semverGreater = (a: string, b: string): boolean => {
            const pa = a.split(".").map(Number);
            const pb = b.split(".").map(Number);
            for (let i = 0; i < Math.max(pa.length, pb.length); i++) {
              const va = pa[i] ?? 0;
              const vb = pb[i] ?? 0;
              if (vb > va) return true;
              if (vb < va) return false;
            }
            return false;
          };
          const updateAvailable =
            currentVersion !== "unknown" &&
            latestVersion !== "unknown" &&
            semverGreater(currentVersion, latestVersion);

          res.json({ currentVersion, latestVersion, updateAvailable });
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.post("/api/update/apply", async (_req, res) => {
        try {
          const containers = (globalThis as any).__signalk_containerManager as
            | ContainerManagerApi
            | undefined;
          if (!containers || !containers.getRuntime()) {
            res.status(503).json({ error: "Container manager not available" });
            return;
          }

          // Get latest stable version from GitHub
          const ghRes = await fetch(
            "https://api.github.com/repos/questdb/questdb/releases?per_page=5",
            {
              headers: { Accept: "application/vnd.github+json" },
              signal: AbortSignal.timeout(10000),
            },
          );
          if (!ghRes.ok) {
            res.status(502).json({ error: "Failed to fetch releases" });
            return;
          }
          const releases = (await ghRes.json()) as {
            tag_name: string;
            prerelease: boolean;
            draft: boolean;
          }[];
          const stable = releases.find((r) => !r.draft && !r.prerelease);
          if (!stable) {
            res.status(404).json({ error: "No stable release found" });
            return;
          }
          const newTag = stable.tag_name;

          app.setPluginStatus(`Pulling QuestDB ${newTag}...`);
          await containers.pullImage(`questdb/questdb:${newTag}`);

          if (currentConfig) {
            currentConfig.questdbVersion = newTag;
            await new Promise<void>((resolve, reject) => {
              app.savePluginOptions({ ...currentConfig! }, (err) => {
                if (err) reject(err);
                else resolve();
              });
            });
          }

          const updateVolumeSource =
            await resolveQuestdbVolumeSource(containers);
          app.setPluginStatus(`Starting QuestDB ${newTag}...`);
          const updateConfig: ContainerConfig = {
            image: "questdb/questdb",
            tag: newTag,
            volumes: {
              "/var/lib/questdb": updateVolumeSource,
            },
            env: {
              QDB_TELEMETRY_ENABLED: "false",
              QDB_HTTP_ENABLED: "true",
              QDB_LINE_TCP_ENABLED: "true",
              ...(currentConfig?.compression &&
              currentConfig.compression !== "none"
                ? {
                    QDB_CAIRO_WAL_SEGMENT_COMPRESSION_CODEC:
                      currentConfig.compression === "zstd" ? "ZSTD" : "LZ4",
                    ...(currentConfig.compression === "zstd" &&
                    currentConfig.compressionLevel
                      ? {
                          QDB_CAIRO_WAL_SEGMENT_COMPRESSION_LEVEL: String(
                            currentConfig.compressionLevel,
                          ),
                        }
                      : {}),
                  }
                : {}),
            },
            restart: "unless-stopped",
            resources: currentConfig
              ? buildResourceLimits(currentConfig)
              : undefined,
            ulimits: QUESTDB_ULIMITS,
            healthcheck: QUESTDB_HEALTHCHECK,
          };
          const resolveUpdateEndpoints = await applyQuestdbNetworking(
            currentConfig ?? ({} as Config),
            containers,
            QUESTDB_CONTAINER_NAME,
            updateConfig,
          );
          // Clear any prior clamp before re-running so a no-longer-clamping
          // update doesn't leave a stale warning.
          ulimitClamp = null;
          await containers.ensureRunning(QUESTDB_CONTAINER_NAME, updateConfig, {
            onUlimitClamped,
          });

          // Re-resolve so the export endpoints and status line reflect the
          // current endpoint. The version bump keeps the same container name
          // and networking, so the endpoint is stable — the existing
          // QueryClient/ILPWriter stay valid (and the registered history
          // providers keep their reference).
          questdbEndpoints = await resolveUpdateEndpoints();
          const { host: ilpHost, port: ilpPort } = questdbEndpoints.ilp;

          // Wait for the recreated container to answer, then reconnect ILP.
          if (queryClient) {
            const deadline = Date.now() + 30000;
            while (Date.now() < deadline) {
              if (await queryClient.isHealthy()) break;
              await new Promise((resolve) => setTimeout(resolve, 500));
            }
          }
          if (writer) {
            try {
              await writer.disconnect();
            } catch {
              /* ignore */
            }
            await writer.connect();
          }

          app.setPluginStatus(
            `Recording to QuestDB ${newTag} at ${ilpHost}:${ilpPort}`,
          );

          res.json({
            status: "updated",
            newVersion: newTag,
            message: `Updated to QuestDB ${newTag}. Container running.`,
          });
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      // Remove the QuestDB container AND delete all its data. Exists because
      // Signal K's own plugin-uninstall can't delete the data dir on rootless
      // Podman (the container writes files as a subuid the SK user can't
      // remove); removeManagedData wipes them from inside the userns.
      router.post("/api/purge-data", async (_req, res) => {
        try {
          const containers = (globalThis as any).__signalk_containerManager as
            | ContainerManagerApi
            | undefined;
          if (!containers || !containers.getRuntime()) {
            res.status(503).json({ error: "Container manager not available" });
            return;
          }
          if (!containers.removeManagedData) {
            res.status(501).json({
              error:
                "Data removal requires signalk-container 1.19.0 or newer. Update it, or delete the QuestDB data directory manually.",
            });
            return;
          }
          if (currentConfig?.managedContainer === false) {
            res.status(400).json({
              error:
                "QuestDB is not managed by this plugin (external mode); nothing to remove.",
            });
            return;
          }

          // Stop all activity against the container before it and its data go
          // away — otherwise the retention timer keeps issuing DROP PARTITION
          // against a removed container and the writer keeps trying to connect.
          if (schemaHealTimer) {
            clearInterval(schemaHealTimer);
            schemaHealTimer = null;
          }
          if (retentionTimer) {
            clearInterval(retentionTimer);
            retentionTimer = null;
          }
          if (writer) {
            try {
              await writer.disconnect();
            } catch {
              /* ignore */
            }
            writer = null;
          }
          queryClient = null;

          const hostPath = await resolveQuestdbVolumeSource(containers);
          app.setPluginStatus("Removing QuestDB container and data...");
          await containers.removeManagedData(QUESTDB_CONTAINER_NAME, hostPath, {
            ownerPluginId: "signalk-questdb",
          });

          app.setPluginStatus(
            "QuestDB data removed. Disable and re-enable the plugin to start fresh.",
          );
          res.json({
            status: "removed",
            message:
              "QuestDB container and all data removed. Re-enable the plugin to start a fresh database.",
          });
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.get("/api/migration/detect", async (req, res) => {
        const baseUrl = (req.query.url as string) || "http://localhost:8086";

        try {
          const urlObj = new URL(baseUrl);
          const host = urlObj.hostname;
          const isLocal =
            host === "localhost" || host === "127.0.0.1" || host === "::1";
          const isPrivate = /^(10\.|172\.(1[6-9]|2\d|3[01])\.|192\.168\.)/.test(
            host,
          );
          if (!isLocal && !isPrivate) {
            res.status(400).json({
              error: "Only localhost and private network URLs allowed",
            });
            return;
          }
        } catch {
          res.status(400).json({ error: "Invalid URL" });
          return;
        }

        const sources: {
          type: string;
          url: string;
          status: string;
          version?: string;
        }[] = [];

        // Detect InfluxDB 1.x
        try {
          const r = await fetch(`${baseUrl}/ping`, {
            method: "HEAD",
            signal: AbortSignal.timeout(3000),
          });
          if (r.status === 204) {
            sources.push({
              type: "influxdb1",
              url: baseUrl,
              status: "found",
              version: r.headers.get("X-Influxdb-Version") || "unknown",
            });
          }
        } catch {
          // not running
        }

        // Detect InfluxDB 2.x
        try {
          const r = await fetch(`${baseUrl}/health`, {
            signal: AbortSignal.timeout(3000),
          });
          if (r.ok) {
            const data = (await r.json()) as {
              status?: string;
              version?: string;
            };
            if (data.status === "pass") {
              sources.push({
                type: "influxdb2",
                url: baseUrl,
                status: "found",
                version: data.version || "unknown",
              });
            }
          }
        } catch {
          // not running
        }

        res.json({ sources });
      });

      router.get("/api/export", async (req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ error: "QuestDB not connected" });
            return;
          }
          const from = req.query.from as string;
          const to = req.query.to as string;
          const format = (req.query.format as string) || "csv";
          if (!from || !to) {
            res.status(400).json({ error: "Missing from/to parameters" });
            return;
          }

          const fromDate = new Date(from);
          const toDate = new Date(to);
          if (isNaN(fromDate.getTime()) || isNaN(toDate.getTime())) {
            res
              .status(400)
              .json({ error: "Invalid date format for from/to parameters" });
            return;
          }
          const safeFrom = fromDate.toISOString();
          const safeTo = toDate.toISOString();
          const sql = `SELECT ts, path, context, value FROM signalk WHERE ts >= '${safeFrom}' AND ts <= '${safeTo}' ORDER BY ts`;
          const dateSlug = safeFrom.slice(0, 10);

          const expUrl = new URL("/exp", questdbHttpBaseUrl());
          expUrl.searchParams.set("query", sql);

          if (format === "parquet") {
            expUrl.searchParams.set("fmt", "parquet");
            const codec = currentConfig?.compression ?? "lz4";
            if (codec !== "none") {
              expUrl.searchParams.set(
                "compression_codec",
                codec === "lz4" ? "LZ4_RAW" : "ZSTD",
              );
              if (codec === "zstd") {
                expUrl.searchParams.set(
                  "compression_level",
                  String(currentConfig?.compressionLevel ?? 3),
                );
              }
            }

            const qdbRes = await fetch(expUrl.toString(), {
              signal: AbortSignal.timeout(300000),
            });
            if (!qdbRes.ok || !qdbRes.body) {
              const body = await qdbRes.text().catch(() => "");
              res.status(502).json({ error: `QuestDB export failed: ${body}` });
              return;
            }

            res.setHeader("Content-Type", "application/vnd.apache.parquet");
            res.setHeader(
              "Content-Disposition",
              `attachment; filename="signalk-export-${dateSlug}.parquet"`,
            );
            // Stream the response through
            const reader = qdbRes.body.getReader();
            try {
              while (true) {
                const { done, value } = await reader.read();
                if (done) break;
                if (!res.write(value)) {
                  await new Promise((resolve) => res.once("drain", resolve));
                }
              }
              res.end();
            } catch (streamErr) {
              app.debug("export stream error:", streamErr);
              res.end();
            }
          } else {
            const csv = await queryClient.execCsv(sql);
            res.setHeader("Content-Type", "text/csv");
            res.setHeader(
              "Content-Disposition",
              `attachment; filename="signalk-export-${dateSlug}.csv"`,
            );
            res.send(csv);
          }
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      // List the tables a backup-style "full export" caller should pull.
      // The allowlist (FULL_EXPORT_TABLES) is module-level so both this
      // listing and the per-table export below stay in sync.
      router.get("/api/full-export/tables", (_req, res) => {
        if (!queryClient) {
          res.status(503).json({ error: "QuestDB not connected" });
          return;
        }
        res.json({ tables: FULL_EXPORT_TABLES });
      });

      // Full-content export: stream EVERY row of one table as Parquet.
      // Used by signalk-backup to capture history into snapshots. Distinct
      // from /api/export, which is range-bounded (from/to required) and
      // hardwired to the `signalk` value table.
      router.get("/api/full-export/:table", async (req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ error: "QuestDB not connected" });
            return;
          }
          const table = req.params.table;
          if (!FULL_EXPORT_TABLE_SET.has(table)) {
            res.status(404).json({ error: `Unknown table: ${table}` });
            return;
          }

          // Half-open [from, to) range, both required together. Lets the
          // backup plugin slice the table into kopia-dedup-friendly weekly
          // shards. Omitting both keeps the full-table behavior.
          // Reject repeated params (`?from=A&from=B` → string[]) — silently
          // downgrading to full-export hides the bug from the caller.
          const rawFrom = req.query.from;
          const rawTo = req.query.to;
          if (
            (rawFrom !== undefined && typeof rawFrom !== "string") ||
            (rawTo !== undefined && typeof rawTo !== "string")
          ) {
            res
              .status(400)
              .json({ error: "from and to must each be a single value" });
            return;
          }
          const rangeResult = buildFullExportWhere(rawFrom, rawTo);
          if (!rangeResult.ok) {
            res.status(400).json({ error: rangeResult.error });
            return;
          }

          // No ORDER BY — QuestDB rows are already returned in designated-
          // timestamp order, and adding ORDER BY forces a sort over the full
          // table that's slow on the Pi for the wide signalk table.
          const sql = `SELECT * FROM ${table}${rangeResult.where}`;

          const expUrl = new URL("/exp", questdbHttpBaseUrl());
          expUrl.searchParams.set("query", sql);
          expUrl.searchParams.set("fmt", "parquet");
          const codec = currentConfig?.compression ?? "lz4";
          if (codec !== "none") {
            expUrl.searchParams.set(
              "compression_codec",
              codec === "lz4" ? "LZ4_RAW" : "ZSTD",
            );
            if (codec === "zstd") {
              expUrl.searchParams.set(
                "compression_level",
                String(currentConfig?.compressionLevel ?? 3),
              );
            }
          }

          // Manual AbortController so we can cancel the upstream fetch
          // when EITHER (a) the 10-min cap fires for runaway queries, or
          // (b) the downstream client (the backup plugin) disconnects
          // mid-stream. Without (b), QuestDB keeps streaming bytes into
          // a closed socket until the timeout — wasteful on a Pi.
          const controller = new AbortController();
          const timeoutId = setTimeout(() => {
            controller.abort();
          }, 600_000);
          const onClientClose = () => {
            controller.abort();
          };
          res.once("close", onClientClose);

          // Pre-stream phase: fetch + status check. Any failure here
          // ends in a 4xx/5xx JSON response, no headers committed to
          // the body yet.
          let qdbRes: Response;
          try {
            qdbRes = await fetch(expUrl.toString(), {
              signal: controller.signal,
            });
          } catch (fetchErr) {
            clearTimeout(timeoutId);
            res.removeListener("close", onClientClose);
            const msg =
              fetchErr instanceof Error ? fetchErr.message : String(fetchErr);
            if (!res.headersSent) {
              res.status(502).json({ error: `QuestDB unreachable: ${msg}` });
            }
            return;
          }
          if (!qdbRes.ok || !qdbRes.body) {
            clearTimeout(timeoutId);
            res.removeListener("close", onClientClose);
            const body = await qdbRes.text().catch(() => "");
            res.status(502).json({
              error: `QuestDB export failed: ${body}`,
            });
            return;
          }

          // Streaming phase: headers are committed at the first write,
          // so we can no longer switch to a JSON error. Best we can do
          // on stream failure is end the response and let the client
          // notice the truncation.
          res.setHeader("Content-Type", "application/vnd.apache.parquet");
          res.setHeader(
            "Content-Disposition",
            `attachment; filename="${table}.parquet"`,
          );

          const reader = qdbRes.body.getReader();
          try {
            while (true) {
              const { done, value } = await reader.read();
              if (done) break;
              if (!res.write(value)) {
                await new Promise((resolve) => res.once("drain", resolve));
              }
            }
          } catch (streamErr) {
            app.debug("full-export stream error:", streamErr);
          } finally {
            clearTimeout(timeoutId);
            res.removeListener("close", onClientClose);
            // Cancel the upstream reader so QuestDB's connection is
            // closed promptly. cancel() throws if the stream already
            // ended cleanly — that's fine, swallow it.
            try {
              await reader.cancel();
            } catch {
              // already finished
            }
            if (!res.writableEnded) {
              res.end();
            }
          }
        } catch (err) {
          // Pre-fetch errors only (URL construction etc.) — the inner
          // streaming block has its own finally for cleanup.
          if (!res.headersSent) {
            res.status(500).json({
              error: err instanceof Error ? err.message : "Unknown error",
            });
          }
        }
      });
    },
  };

  return plugin;
};
