import { IRouter } from "express";
import { waitForContainerManager } from "signalk-container-helper";
import type {
  ContainerConfig,
  ContainerManagerApi,
  ContainerResourceLimits,
  UlimitClamp,
} from "signalk-container-helper";
import { ILPWriter } from "./ilp-writer.js";
import { QueryClient, isReadOnlySQL } from "./query-client.js";
import type { QuestDBResult } from "./query-client.js";
import { Config, ConfigSchema, normalizeConfig } from "./config/schema.js";
import {
  extractVesselName,
  flattenObjectValue,
  routeDeltaValue,
  UnstorableTracker,
} from "./delta-routing.js";
import { createHistoryProviderV2 } from "./history-v2.js";
import { createHistoryProviderV1 } from "./history-v1.js";
import { startRetention } from "./retention.js";
import { RESTORE_SOURCE, restoreFromHistory } from "./restore.js";
import { createConsoleProxy } from "./console-proxy.js";
import { buildFullExportWhere } from "./full-export-range.js";
import { detectInflux, validateInfluxUrl } from "./influx-detect.js";
import {
  WalMonitor,
  buildPendingSegmentsSQL,
  computeSkipPlan,
  extractApplyError,
  isPartitionOpenFailure,
  skipPlansEqual,
  type PendingSegment,
  type SuspendedTable,
} from "./wal-monitor.js";
import {
  QUESTDB_INTERNAL_HTTP_PORT,
  QUESTDB_INTERNAL_ILP_PORT,
  QUESTDB_INTERNAL_PG_PORT,
  QUESTDB_ACCESSIBLE_PORTS,
  resolveManagedEndpoints,
  resolveLanExposureHost,
  lanExposureEndpoints,
  type Endpoint,
} from "./questdb-endpoint.js";
import { nofileClampSatisfied, readMaxMapCount } from "./host-limits.js";
import type {
  DbStatus,
  MigrationDetectResponse,
  QuestdbVersion,
  RestoreStatus,
  ResumeWalResponse,
  SkipWalResponse,
  UpdateApplyResponse,
  UpdateInfo,
  WalDiagnosis,
  WalDiagnosisTable,
} from "./api-contract.js";
import { buildContainerEnv } from "./container-env.js";
import { PathMatcher, RateMatcher, Throttle } from "./path-matcher.js";

interface App {
  debug: (...args: unknown[]) => void;
  error: (...args: unknown[]) => void;
  setPluginStatus: (msg: string) => void;
  setPluginError: (msg: string) => void;
  handleMessage: (pluginId: string, delta: unknown) => void;
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

// signalk-container's API, imported rather than mirrored. The 156-line
// hand-written copy that used to live here drifted by construction: it was
// only ever updated when this plugin happened to need a new member, so
// getContainerNofile sat unmirrored from 1.25.3 until a probe needed it.
// signalk-container-helper carries the same types and pins them against
// signalk-container/types in CI, so a drift fails there instead of here.

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
// Caveat: rootless podman < 5.5.0 drops this request over the compat API
// (containers/podman#25881) and the container inherits the podman service's
// limits instead — still safe, and the /api/status live probe reports what
// the container actually got either way.
const QUESTDB_ULIMITS = { nofile: 1048576 };

// How often to re-check that the owned tables still have the correct `ts`
// schema (and rebuild any ILP auto-created with the wrong shape). 60s is far
// faster than a human would notice "recording broke" yet negligible load.
const SCHEMA_HEAL_INTERVAL_MS = 60_000;

// How long the guided-skip endpoint waits after a RESUME WAL before
// re-reading wal_tables(). An apply attempt against unreadable segment data
// fails within milliseconds (observed ~3ms in the field), so a few seconds
// cleanly separates "re-suspended at the same txn" from "healthily replaying".
const SKIP_RECHECK_DELAY_MS = 3_000;

// Deadline for the engine-log fetch inside /api/wal-diagnosis. The container
// API exposes no abort signal, so this bounds how long the diagnosis waits —
// a runtime that stalls streaming logs must degrade the response (applyError
// null), not hang it.
const ENGINE_LOG_TIMEOUT_MS = 10_000;

// Deadline for the live nofile probe inside /api/status. Same rationale as
// ENGINE_LOG_TIMEOUT_MS, but tighter: the panel polls status every few
// seconds, so a stalled probe must never back requests up behind it — on
// timeout the clamp advisory is simply kept for this poll.
const NOFILE_PROBE_TIMEOUT_MS = 3_000;

function buildResourceLimits(config: Config): ContainerResourceLimits {
  return {
    memory: config.questdbMemoryLimit?.trim() || null,
    cpus:
      typeof config.questdbCpuLimit === "number" && config.questdbCpuLimit > 0
        ? config.questdbCpuLimit
        : null,
  };
}

export default (app: App) => {
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
  // Whether this QuestDB's wal_tables() exposes the errorTag/errorMessage
  // columns (present on current builds, absent on an older pinned version).
  // Probed once via the richer status query and cached so an old build does
  // not throw-and-fall-back on every /api/status call. Null = not yet probed.
  let walTablesHasErrorColumns: boolean | null = null;
  // Watches for WAL suspensions, auto-applies the lossless remedy once per
  // stall point, and raises/clears the Signal K notification. Lives from a
  // completed start to the next stop/purge.
  let walMonitor: WalMonitor | null = null;
  // Serializes the lifecycle operations that create or destroy QuestDB
  // resources — asyncStart, /api/update/apply, and /api/purge-data — so they
  // can never interleave. Without this, a purge (or update) issued while a
  // multi-second asyncStart is awaiting whenReady()/ensureRunning()/connect()
  // could tear down resources the start is about to (re)create, or the start
  // could resurrect a just-purged container. A simple promise-chain mutex:
  // each call waits for the previous to settle, then runs.
  let lifecycleChain: Promise<unknown> = Promise.resolve();
  // Bumped by stop() and purge before they tear down. Lifecycle work captures
  // the value when it is ENQUEUED and re-checks it when it actually runs (and
  // after its long awaits): startAbort can only cancel the start that is
  // currently executing, so without this a start/update still waiting behind
  // the chain — or one parked in a long await — would run to completion after
  // teardown and resurrect the resources that were just torn down.
  let lifecycleGeneration = 0;
  // Bumped by /api/update/apply when it recreates the QuestDB container.
  // stop/purge bump lifecycleGeneration, but an update deliberately does
  // not (its own generation check detects stop preemption) — so a request
  // that must not span a container swap (the WAL skip's final guard) needs
  // this separate epoch to notice one.
  let containerEpoch = 0;
  // True only between a FULLY completed start (providers, stream
  // subscription, and timers all registered) and the next stop/purge.
  // queryClient/writer are not usable as a running sentinel: they are
  // created partway through startup, so a start that fails after that point
  // (health-wait exhausted, ensureTables/connect threw) leaves them non-null
  // on a plugin that never came up.
  let pluginRunning = false;

  // Summary of the startup restore, appended to the plugin status so the
  // result is visible on the plugin card. Without this the only report was an
  // app.debug() line, invisible unless DEBUG is set — so a user had no way to
  // tell whether a feature whose entire point is "did it repopulate?" had run.
  let restoreSummary: string | null = null;
  // Structured form of the same result, served on /api/status. The status
  // string above is kept because it is correct and will start working if
  // Signal K's plugin.id/plugin.name status mismatch is ever fixed, but the
  // config panel reads THIS — it is the only path that is visible today.
  let restoreStatus: RestoreStatus | null = null;

  const recordingStatus = (host: string, port: number): string =>
    `Recording to QuestDB at ${host}:${port}` +
    (restoreSummary ? ` — ${restoreSummary}` : "");

  // True while a writer/WAL failure is on the status line. Signal K keeps ONE
  // status entry per plugin, so publishing "Recording…" over an active error
  // hides it — and the restore result, which lands seconds after startup on a
  // timer of its own, is exactly the kind of late writer that would do it.
  let pluginErrorActive = false;
  const publishError = (msg: string) => {
    pluginErrorActive = true;
    app.setPluginError(msg);
  };
  // Republish the recording line, but never at the cost of an active error.
  const publishRecordingStatus = (host: string, port: number) => {
    if (pluginErrorActive) return;
    app.setPluginStatus(recordingStatus(host, port));
  };

  // Contexts seen live while a startup restore is in flight. A stored fix
  // must never overwrite a vessel that has already transmitted since boot —
  // that would move it backwards on the chart. Populated by the recorder
  // subscription and cleared as soon as the restore settles, so this stays a
  // startup-window set rather than an unbounded one.
  const liveContexts = new Set<string>();
  let trackLiveContexts = false;

  const withLifecycleLock = <T>(fn: () => Promise<T>): Promise<T> => {
    const run = lifecycleChain.then(fn, fn);
    // Keep the chain alive regardless of this op's outcome.
    lifecycleChain = run.then(
      () => undefined,
      () => undefined,
    );
    return run;
  };

  // AbortController for the start currently in flight (null when no start is
  // running). /api/purge-data — the recovery action — aborts it so a wedged
  // start releases the lock instead of pinning purge behind it forever.
  // runStart checks this signal after each long/external await and bails
  // without creating resources, so an aborted start can't resurrect a
  // just-purged container.
  let startAbort: AbortController | null = null;

  // How long a start waits for signalk-container to publish its API and for
  // runtime detection to settle. Generous because both are one-off startup
  // costs on a cold boot — signalk-container may still be pulling its own
  // image — and the alternative to waiting is a spurious "plugin required"
  // error on a system that has it. A purge aborts the wait, so a wedged start
  // does not hold the lock for this long.
  const CONTAINER_MANAGER_TIMEOUT_MS = 120000;

  // How long purge waits to acquire the lifecycle lock before proceeding
  // anyway. After this it forces its teardown through even if a hung start
  // never released the lock (see PURGE_LOCK_TIMEOUT_MS use below).
  const PURGE_LOCK_TIMEOUT_MS = 30000;

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
        if (await queryClient.healSchema(table, (msg) => app.error(msg))) {
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
  const throttle = new Throttle();
  // Last vessel name written per context. Names repeat with every AIS
  // static report (~6 min); a row is only worth writing when the name
  // actually changes — replay works off the last-known value, not a
  // window, so repeats add nothing. Valid only while the writer has
  // dropped nothing since: an enqueued name can be discarded when the
  // disconnected buffer overflows, and unlike data paths a deduplicated
  // name would never be retried. `nameDedupeDropMark` remembers the
  // writer's drop counter at the last write; when it advances, the whole
  // map is invalidated and names re-establish from the next AIS cycle.
  const lastNameByContext = new Map<string, string>();
  let nameDedupeDropMark = 0;

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

  // Built once; it resolves the upstream lazily on every request so a
  // container restart (which can change the resolved address) is picked up
  // without re-registering the route.
  //
  // Gated on config rather than on `questdbEndpoints`: that is only populated
  // on the managed-container path, so keying off it made the console return
  // 503 forever in external mode, where the host/port come from config and
  // questdbHttpBaseUrl() already falls back to them. Null only before any
  // config has loaded, which is the one moment there is nothing to point at.
  const consoleProxy = createConsoleProxy({
    baseUrl: () => (currentConfig ? questdbHttpBaseUrl() : null),
    debug: (msg) => app.debug(msg),
    mountPath: "/plugins/signalk-questdb/console",
  });

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
   *     reaches them on the host loopback (bare-metal) or — containerized —
   *     on whichever of loopback/host.containers.internal actually answers
   *     (probed, since the right one depends on the SK container's network
   *     mode; see resolveLanExposureHost).
   *
   * `endpoints` is resolved AFTER ensureRunning() on both paths: the
   * signalkAccessiblePorts path needs the allocated host port, and the LAN
   * path's probe needs a running QuestDB. The returned resolver defers the
   * work into the closure (memoized on the LAN path) for exactly that reason.
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
      // ensureNetwork is feature-detected: the helper types every
      // version-gated member as optional so a plugin degrades on an older
      // signalk-container rather than throwing. The previous hand-written
      // mirror declared it required, which hid the need for this guard —
      // and the connectToNetwork call further down was already guarding, so
      // the two disagreed about the same API.
      //
      // networkMode is set REGARDLESS. Creating the network is the optional
      // step; joining it is what the companion Grafana depends on, and on a
      // signalk-container without ensureNetwork the network the operator
      // named almost certainly exists already. Gating the assignment on the
      // probe would silently drop the container off that network.
      if (config.networkName) {
        if (typeof containers.ensureNetwork === "function") {
          await containers.ensureNetwork(config.networkName);
        }
        containerConfig.networkMode = config.networkName;
      }
      // A 0.0.0.0-published port is reached on 127.0.0.1 or via the host
      // gateway depending on how a containerized SK is networked — resolved
      // by probing, which only works once QuestDB is up. The resolver runs
      // after ensureRunning() on both call sites, so defer the probe into
      // the closure (and memoize: the endpoint can't change while the
      // container config that produced it is live).
      let endpoints: { http: Endpoint; ilp: Endpoint } | null = null;
      return async () => {
        if (!endpoints) {
          const skHost = config.exposeToContainers
            ? await resolveLanExposureHost(containers, httpPort, (msg) =>
                app.debug(msg),
              )
            : "127.0.0.1";
          endpoints = lanExposureEndpoints(skHost, httpPort, ilpPort);
        }
        return endpoints;
      };
    }

    // Default path: signalk-container owns the networking. After the container
    // is up we additionally attach it to `networkName` so the companion
    // signalk-grafana (which joins that network and resolves QuestDB by its
    // `sk-`-prefixed DNS name) keeps working, then resolve the SK->QuestDB
    // endpoint from whatever address signalk-container reports.
    containerConfig.signalkAccessiblePorts = QUESTDB_ACCESSIBLE_PORTS;
    return async () => {
      // The two are feature-detected SEPARATELY. Requiring both would skip
      // attachment entirely on a signalk-container that has connectToNetwork
      // but not ensureNetwork -- and there the network almost certainly
      // already exists, so attaching still works. Creating it is the
      // optional step; joining it is the one Grafana depends on.
      if (
        config.networkName &&
        typeof containers.connectToNetwork === "function"
      ) {
        try {
          if (typeof containers.ensureNetwork === "function") {
            await containers.ensureNetwork(config.networkName);
          }
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

  // Compiled once per config (see runStart), never per delta — recompiling
  // these globs on every incoming value pinned a real vessel's event loop at
  // 100% CPU with an 89-pattern filter list.
  let pathFilterMatcher: PathMatcher | null = null;
  let rateMatcher: RateMatcher | null = null;

  // Paths carrying a value with no representation in any table: arrays, and
  // objects nested below the one level `flattenObjectValue` descends. These
  // used to vanish without any trace at all — the whole reason
  // navigation.attitude went unnoticed until a user asked (issue #128) — so
  // record them and surface the count on /api/status.
  //
  // See UnstorableTracker: capped, and deliberately silent on the per-delta
  // path. The diagnostic surfaces on /api/status, which is where a user will
  // actually see it — this whole class of loss went unnoticed precisely
  // because it was only ever visible in a log nobody had enabled (issue #128).
  const unstorable = new UnstorableTracker();
  const noteUnstorable = (path: string) => unstorable.note(path);

  // Takes only the mode: the patterns themselves live in pathFilterMatcher,
  // compiled from this same config at start. Accepting a `paths` array here
  // would imply it is consulted when it is not.
  function shouldRecord(path: string, mode: string): boolean {
    const matcher = pathFilterMatcher;
    if (!matcher || matcher.isEmpty) return true;

    const matches = matcher.matches(path);
    return mode === "exclude" ? !matches : matches;
  }

  function isThrottled(
    path: string,
    context: string,
    defaultRate: number,
  ): boolean {
    // Per-path override wins when one matches; otherwise the default rate.
    const overrideRate = rateMatcher?.rateFor(path) ?? null;
    return throttle.shouldDrop(
      path,
      context,
      overrideRate ?? defaultRate,
      Date.now(),
    );
  }

  const WAL_NOTIFICATION_PATH = "notifications.signalk-questdb.walSuspended";
  // Dedupe key of the last emitted notification. The monitor reports every
  // check cycle; the delta only goes out when the key changes, so clients
  // aren't spammed with a fresh alert every minute. The key must be built
  // from STABLE facts (state, table@stall-point, outcome) — the message
  // itself contains txnLag, which grows every cycle while ingestion
  // continues and would defeat the dedupe.
  let lastWalNotification: string | null = null;

  function emitWalNotification(
    state: "alert" | "normal",
    message: string,
    key: string,
  ): void {
    if (key === lastWalNotification) return;
    lastWalNotification = key;
    app.handleMessage("signalk-questdb", {
      updates: [
        {
          values: [
            {
              path: WAL_NOTIFICATION_PATH,
              value: {
                state,
                method: ["visual"],
                message,
                timestamp: new Date().toISOString(),
              },
            },
          ],
        },
      ],
    });
  }

  // A latched alert must not outlive the monitoring that backs it: after
  // stop/purge nobody would ever clear it, and a permanent stale alarm
  // teaches operators to ignore the path. Downgrade honestly (monitoring
  // stopped ≠ recovered) and reset the dedupe so the next start re-alerts
  // if the suspension is still there.
  function clearWalAlertOnTeardown(): void {
    if (lastWalNotification?.startsWith("alert:")) {
      emitWalNotification(
        "normal",
        "QuestDB WAL monitoring stopped (plugin stopped or data removed); " +
          "suspension state is no longer tracked.",
        "normal:teardown",
      );
    }
    lastWalNotification = null;
  }

  // wal_tables() probe shared by /api/status, the WAL monitor, and the
  // resume endpoints. errorTag/errorMessage carry QuestDB's reason for a
  // suspension when it has one; the columns are absent on an older pinned
  // QuestDB, so probe for them once (the richer query throws if missing),
  // cache the result in `walTablesHasErrorColumns`, and thereafter run the
  // query the build actually supports. Detection never regresses; only the
  // diagnostic columns are dropped on old builds. Throws on query failure —
  // callers decide whether that means "treat as not suspended" (status) or
  // "skip this cycle" (monitor).
  const RICH_WAL_QUERY =
    "SELECT name, writerTxn, sequencerTxn, errorTag, errorMessage FROM wal_tables() WHERE suspended = true";
  const BASIC_WAL_QUERY =
    "SELECT name, writerTxn, sequencerTxn FROM wal_tables() WHERE suspended = true";

  async function listSuspendedTables(
    client: QueryClient,
  ): Promise<SuspendedTable[]> {
    let walResult: QuestDBResult;
    if (walTablesHasErrorColumns === null) {
      try {
        walResult = await client.exec(RICH_WAL_QUERY);
        walTablesHasErrorColumns = true;
      } catch (err) {
        walResult = await client.exec(BASIC_WAL_QUERY);
        // Only a definitive rejection (a 4xx — unknown column on an older
        // QuestDB) proves the columns are missing. A transient failure
        // (timeout, engine mid-restart) that happens to spare the cheaper
        // basic query must not permanently disable the diagnostic columns —
        // leave the flag unprobed so the next call retries the richer query.
        const msg = err instanceof Error ? err.message : String(err);
        if (/^QuestDB query failed \(4\d\d\)/.test(msg)) {
          walTablesHasErrorColumns = false;
        }
      }
    } else {
      walResult = await client.exec(
        walTablesHasErrorColumns ? RICH_WAL_QUERY : BASIC_WAL_QUERY,
      );
    }
    return client.toObjects(walResult).map((row) => {
      const writerTxn = Number(row.writerTxn ?? 0);
      const sequencerTxn = Number(row.sequencerTxn ?? 0);
      return {
        name: String(row.name),
        writerTxn,
        sequencerTxn,
        txnLag: Math.max(0, sequencerTxn - writerTxn),
        errorTag: String(row.errorTag ?? ""),
        errorMessage: String(row.errorMessage ?? ""),
      };
    });
  }

  async function pendingSegments(
    client: QueryClient,
    table: SuspendedTable,
  ): Promise<PendingSegment[]> {
    const result = await client.exec(
      buildPendingSegmentsSQL(table.name, table.writerTxn),
      15_000,
    );
    return client.toObjects(result).map((row) => ({
      walId: Number(row.walId ?? 0),
      segmentId: Number(row.segmentId ?? 0),
      txns: Number(row.txns ?? 0),
      minTxn: Number(row.minTxn ?? 0),
      maxTxn: Number(row.maxTxn ?? 0),
      minTimestamp: String(row.minTimestamp ?? ""),
      maxTimestamp: String(row.maxTimestamp ?? ""),
    }));
  }

  // Engine log lines for the apply-failure scrape; extractApplyError then
  // runs per suspended table over the one fetch. Best effort: external mode,
  // an older signalk-container without getLogs, or a fetch failure all
  // degrade to null (the diagnosis then shows only what wal_tables()
  // reports).
  async function fetchEngineLogLines(): Promise<string[] | null> {
    if (currentConfig?.managedContainer === false) return null;
    const containers = (globalThis as any).__signalk_containerManager as
      ContainerManagerApi | undefined;
    if (!containers?.getLogs) return null;
    let timeoutTimer: NodeJS.Timeout | undefined;
    try {
      return await Promise.race([
        // The trailing catch keeps a fetch that fails AFTER the timeout won
        // the race from surfacing as an unhandled rejection.
        containers
          .getLogs(QUESTDB_CONTAINER_NAME, { tail: 3000 })
          .catch(() => null),
        new Promise<null>((resolve) => {
          timeoutTimer = setTimeout(() => resolve(null), ENGINE_LOG_TIMEOUT_MS);
        }),
      ]);
    } catch {
      return null;
    } finally {
      if (timeoutTimer) clearTimeout(timeoutTimer);
    }
  }

  // Runs the actual startup. Always invoked through the lifecycle lock (see
  // asyncStart) so a purge/update can't interleave with it.
  //
  // Abortable: a fresh AbortController is published in `startAbort` at entry so
  // /api/purge-data can preempt a slow start. The container API mirror exposes
  // no AbortSignal, so we can't cancel an in-flight whenReady()/ensureRunning()
  // — instead we check `signal.aborted` after each long/external await and bail
  // before registering any resources, so an aborted (preempted) start never
  // resurrects what purge is about to remove. (Purge's own bounded lock-acquire
  // covers the case where such a call truly never settles.)
  async function runStart(config: Config) {
    const abort = new AbortController();
    startAbort = abort;
    const { signal } = abort;
    try {
      await runStartInner(config, signal);
    } finally {
      // Only clear if we're still the in-flight start; a later start may have
      // already replaced us.
      if (startAbort === abort) startAbort = null;
    }
  }

  async function runStartInner(config: Config, signal: AbortSignal) {
    // Not running until THIS start completes — a restart that reuses the
    // slot must not leave a stale true from the previous run.
    pluginRunning = false;
    // Older saved configs miss keys added since (pathFilter, samplingRates);
    // the per-delta pipeline dereferences them per delta, so normalize once
    // at the boundary.
    config = normalizeConfig(config);
    currentConfig = config;
    // Compile the filter/throttle globs here, alongside the config they come
    // from, so the per-delta path only does lookups. Rebuilt on every start,
    // which is also how a config change takes effect (the server stops and
    // restarts the plugin).
    pathFilterMatcher = new PathMatcher(config.pathFilter?.paths ?? []);
    rateMatcher = new RateMatcher(config.samplingRates ?? {});
    // A start may connect to a different QuestDB than the previous run —
    // notably external mode repointed at a new host/build — so drop any cached
    // wal_tables() capability flag and let /api/status re-probe. The
    // update/apply path resets it too, for the in-place recreate that does not
    // re-enter this function.
    walTablesHasErrorColumns = null;
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
      // Two phases in one call: poll for the manager global, then wait for
      // runtime detection to settle. The previous code read the global ONCE,
      // synchronously — which worked only because plugins start in
      // alphabetical order and "signalk-questdb" sorts after
      // "signalk-container". Any rename, or a slower container start, and the
      // plugin reported "signalk-container required" on a system that has it.
      //
      // The signal is threaded through so a purge preempting a start stops
      // the poll immediately rather than waiting out the full budget.
      app.setPluginStatus("Waiting for signalk-container plugin...");
      const { manager: containers, runtime } = await waitForContainerManager({
        timeoutMs: CONTAINER_MANAGER_TIMEOUT_MS,
        signal,
        onWaiting: (phase) => {
          if (phase === "runtime")
            app.setPluginStatus("Waiting for container runtime detection...");
        },
      });
      // Purge may have preempted us while the wait was pending; bail before
      // touching the runtime so we don't recreate what purge is removing.
      if (signal.aborted) return;

      if (!containers) {
        app.debug("containerManager not found");
        publishError(
          "signalk-container plugin required for managed mode. Install it or set managedContainer=false.",
        );
        return;
      }

      if (!runtime) {
        app.debug("container runtime not detected");
        publishError(
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
        const containerEnv = buildContainerEnv(config);

        const volumeSource = await resolveQuestdbVolumeSource(containers);
        if (signal.aborted) return;
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
        if (signal.aborted) return;

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
        // ensureRunning() just (re)created the container. If purge preempted us
        // while it ran, return now so we don't proceed to resolve endpoints and
        // wire up a writer against a container purge is about to remove.
        if (signal.aborted) return;
        app.debug("QuestDB container ready");

        // The LAN path probes for the reachable host in here (up to its
        // retry deadline), so this await is long enough for a stop/purge to
        // preempt us — re-check before committing the result.
        const resolved = await resolveEndpoints();
        if (signal.aborted) return;
        questdbEndpoints = resolved;
      } catch (err) {
        app.debug("ensureRunning failed:", err);
        publishError(
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
      if (signal.aborted) {
        queryClient = null;
        return;
      }
      if (await queryClient.isHealthy()) break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    if (signal.aborted) {
      queryClient = null;
      return;
    }

    if (!(await queryClient.isHealthy())) {
      publishError(`QuestDB not responding at ${httpHost}:${httpPort}`);
      return;
    }

    app.setPluginStatus("Creating tables...");
    // A migration that degrades (dedup off on an external table) is worth a
    // line in the server log, but must never abort startup — see ensureTables.
    await queryClient.ensureTables((msg) => app.error(msg));
    // A prior crash/drop may have left an ILP-auto-created `signalk` table with
    // the wrong (`timestamp`, not `ts`) schema; heal it before the writer can
    // ingest into the broken shape.
    await healSchemaTables();

    writer = new ILPWriter(ilpHost, ilpPort, (msg) => app.debug(msg), {
      // Surface a flapping ILP connection instead of leaving the status line
      // stuck on a cheerful "Recording" while every sample is dropped.
      onUnhealthy: (msg) => publishError(msg),
      onHealthy: () => {
        // The writer recovered: the error is no longer current, so the
        // recording line may take the status entry back.
        pluginErrorActive = false;
        app.setPluginStatus(recordingStatus(ilpHost, ilpPort));
      },
      flushIntervalMs: config.ilpFlushIntervalMs,
    });
    await writer.connect();

    // Last abort checkpoint before we register providers, the stream
    // subscription, and the retention/heal timers — none of which is wired up
    // yet, so tearing down here is just nulling the writer/client we created.
    if (signal.aborted) {
      try {
        await writer.disconnect();
      } catch {
        /* ignore */
      }
      writer = null;
      queryClient = null;
      return;
    }

    const v2Provider = createHistoryProviderV2(queryClient, app.selfContext);
    app.registerHistoryApiProvider(v2Provider);

    const v1Provider = createHistoryProviderV1(
      queryClient,
      app.selfContext,
      (msg) => app.debug(msg),
    );
    app.registerHistoryProvider(v1Provider);

    // Arm live-context tracking before the first delta can arrive, so the
    // restore sees every vessel that transmitted during startup.
    liveContexts.clear();
    trackLiveContexts = config.restoreOnStart;

    const bus = app.streambundle.getBus();
    const unsub = bus.onValue((delta: any) => {
      if (!writer) return;
      const { path, value, context } = delta;

      // Values this plugin replayed at startup come straight back around
      // through the streambundle. Recording them would re-stamp historical
      // fixes with the current receive time — inventing present-tense
      // positions for vessels that may be long gone — and marking their
      // context live would defeat the guard that stops a restore from
      // overwriting a vessel that actually transmitted.
      if (delta.$source === RESTORE_SOURCE) return;

      // The delta's sourceRef, stored as the rows' `source` column so
      // interleaved multi-source streams (two GPS receivers) can be told
      // apart afterwards and filtered via the History API's `path|sourceRef`
      // syntax. Undefined when the delta carries none — the column stays
      // null, same as rows written before it existed.
      const source =
        typeof delta.$source === "string" ? delta.$source : undefined;

      // Note the context BEFORE any early return or filter: identity-only
      // deltas return below, and path filters and throttles decide what gets
      // STORED — but any delta at all proves the vessel is transmitting now,
      // which is what makes its stored position obsolete for the restore.
      if (trackLiveContexts) {
        liveContexts.add(context === app.selfContext ? "self" : context);
      }

      // Static vessel identity (issue #91): names arrive as empty-path
      // object deltas, which the path guard below would drop. Stored as
      // path "name" in the string table; history-v1 replays them in the
      // original empty-path shape Freeboard reads. Deliberately bypasses
      // the path filter and throttle: this is identity, not a data
      // stream — an include-mode filter would silently disable it, and
      // the shared throttle would drop most of an AIS fleet's initial
      // burst. The per-context change-dedupe below bounds the volume
      // instead.
      const vesselName = extractVesselName(path, value);
      if (vesselName !== null) {
        const nameIsSelf = context === app.selfContext;
        if (nameIsSelf ? config.recordSelf : config.recordOthers) {
          if (writer.droppedLineCount !== nameDedupeDropMark) {
            nameDedupeDropMark = writer.droppedLineCount;
            lastNameByContext.clear();
          }
          const nameCtx = nameIsSelf ? "self" : context;
          // Context-qualified throttle key: identity writes respect the
          // sampling rate per VESSEL — a shared key would let one AIS
          // target's name suppress every other's during the initial
          // fleet burst. The change-dedupe alone can't bound the volume:
          // two receivers disagreeing on a target's static data would
          // flap the name on every alternation.
          if (
            lastNameByContext.get(nameCtx) !== vesselName &&
            !isThrottled("name", nameCtx, config.defaultSamplingRate ?? 2000)
          ) {
            lastNameByContext.set(nameCtx, vesselName);
            // Tagged "identity" so replay can tell these synthetic rows
            // from a data path literally named "name".
            writer.writeString(
              "name",
              nameCtx,
              vesselName,
              undefined,
              "identity",
              source,
            );
          }
        }
        return;
      }

      if (!path || value === undefined || value === null) return;

      const isSelf = context === app.selfContext;
      if (isSelf && !config.recordSelf) return;
      if (!isSelf && !config.recordOthers) return;

      // Routed once, before the gates, because object values are gated on
      // their leaf paths instead — see the "flatten" branch below. Applying
      // the parent's gates to those would be wrong twice over: an
      // include-filter listing only `navigation.attitude.roll` would drop the
      // parent before any leaf was seen, and the parent would consume a
      // throttle slot the leaves then have to wait out again, halving the
      // effective sampling rate.
      const route = routeDeltaValue(path, value);

      // Recorded BEFORE the gates. A value nothing can store is worth
      // reporting whether or not the user also filters or throttles that
      // path — and gating it first would hide exactly the case this exists to
      // surface, since a throttled array would return here and never be
      // counted.
      if (route === null) {
        noteUnstorable(path);
        return;
      }

      if (route !== "flatten") {
        if (!shouldRecord(path, config.pathFilter.mode)) return;
        // Throttled per path AND context: the sampling rate bounds each
        // vessel's stream, not the fleet's (issue #93). The stored context
        // ("self" vs raw) is the key, matching what the rows carry. The key
        // deliberately does NOT include source: the sampling rate bounds the
        // per-path row volume, and keying per source would multiply it by the
        // number of receivers. With two sources alternating, each window
        // keeps whichever arrived first — a source-filtered read then sees
        // roughly every other sample, which the rate already allows for.
        if (
          isThrottled(
            path,
            isSelf ? "self" : context,
            config.defaultSamplingRate ?? 2000,
          )
        )
          return;
      }

      // Rows are stamped with the server receive time, deliberately NOT the
      // delta's own timestamp: a boat is a set of independent clocks (GPS
      // time, RTC-less devices, gateway latencies), and storing per-source
      // timestamps makes commits land out of order. QuestDB then rewrites
      // partition tails on every merge — observed in the field as >3000x
      // write amplification (physical rows rewritten per row inserted),
      // grinding SD cards and, before batching, stalling the WAL outright.
      // The writer assigns the actual timestamp (strictly monotonic to the
      // microsecond, so same-millisecond writes don't collide on the dedup
      // key), so every commit is a pure append. A device with a broken clock
      // even gets *more* accurate history. Re-sent ILP batches keep their
      // original stamps (baked at write() time), so replay-idempotency holds.
      const ctx = isSelf ? "self" : context;

      if (route === "number") {
        writer.write(path, ctx, value as number, undefined, source);
      } else if (route === "string") {
        writer.writeString(
          path,
          ctx,
          value as string,
          undefined,
          undefined,
          source,
        );
      } else if (route === "boolean") {
        // Tagged, so a replayed boolean stays a boolean instead of becoming
        // the text "true" — indistinguishable from a path whose value really
        // is that word.
        writer.writeString(
          path,
          ctx,
          value ? "true" : "false",
          undefined,
          "boolean",
          source,
        );
      } else if (route === "position") {
        writer.writePosition(
          ctx,
          value as { latitude: number; longitude: number },
          undefined,
          source,
        );
      } else if (route === "flatten") {
        // Object values are recorded as their scalar leaves (issue #128).
        const { leaves, skipped } = flattenObjectValue(path, value);
        // Only the leaves are reported, never the parent as well. Each
        // skipped leaf is already named here, so also noting the parent
        // double-counts one problem and — worse — burns a second slot against
        // the cap, hitting it sooner and hiding genuinely distinct paths.
        //
        // An object with NO storable leaf still gets reported: `skipped` then
        // holds every key, so the drop is visible through them. The only
        // silent case left is an empty object, which carries no data to lose.
        for (const leafPath of skipped) noteUnstorable(leafPath);
        for (const leaf of leaves) {
          // Filtered and throttled on the LEAF path, not the parent: the
          // leaves are what the history API exposes, so excluding
          // `navigation.attitude.yaw` has to actually exclude it. Throttling
          // per leaf also keeps one object's sampling budget from being
          // consumed by whichever key happened to be written first.
          if (!shouldRecord(leaf.path, config.pathFilter.mode)) continue;
          if (isThrottled(leaf.path, ctx, config.defaultSamplingRate ?? 2000))
            continue;
          if (typeof leaf.value === "number") {
            writer.write(leaf.path, ctx, leaf.value, undefined, source);
          } else if (typeof leaf.value === "boolean") {
            writer.writeString(
              leaf.path,
              ctx,
              leaf.value ? "true" : "false",
              undefined,
              "boolean",
              source,
            );
          } else {
            writer.writeString(
              leaf.path,
              ctx,
              leaf.value,
              undefined,
              undefined,
              source,
            );
          }
        }
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

    // Watch for WAL suspensions. Detection alone is not enough — a suspended
    // table keeps accepting ILP writes while committing nothing, and the only
    // built-in recovery QuestDB itself attempts is at engine start. The
    // monitor closes that gap: loud persistent logging, a Signal K alert
    // (recording silently stalling for weeks is exactly what notifications
    // exist for), and one automatic lossless resume per stall point.
    const client = queryClient;
    walMonitor = new WalMonitor({
      // Owned tables only: in external mode the plugin shares a QuestDB with
      // whatever else the operator runs there, and auto-resuming (or
      // alerting on) someone else's suspended table is not this plugin's
      // call to make.
      listSuspended: async () =>
        (await listSuspendedTables(client)).filter((t) =>
          FULL_EXPORT_TABLE_SET.has(t.name),
        ),
      resumeTable: (name) =>
        client
          .exec(`ALTER TABLE "${name.replace(/"/g, '""')}" RESUME WAL`, 10_000)
          .then(() => undefined),
      onSuspended: (tables, anyAutoResumeFailed) => {
        const summary = tables
          .map((t) => `${t.name} (${t.txnLag} txns behind)`)
          .join(", ");
        publishError(`QuestDB WAL suspended: ${summary}`);
        const key =
          "alert:" +
          tables
            .map((t) => `${t.name}@${t.writerTxn}`)
            .sort()
            .join(",") +
          `:${anyAutoResumeFailed}`;
        emitWalNotification(
          "alert",
          anyAutoResumeFailed
            ? `QuestDB history recording is stalled: ${summary}. Automatic ` +
                `resume failed — the data at the stall point is likely ` +
                `unreadable. Open the QuestDB History plugin panel to repair.`
            : `QuestDB history recording is stalled: ${summary}. Automatic ` +
                `recovery is being attempted.`,
          key,
        );
      },
      onResolved: () => {
        emitWalNotification(
          "normal",
          "QuestDB history recording has recovered.",
          "normal:recovered",
        );
        app.setPluginStatus(recordingStatus(ilpHost, ilpPort));
      },
      debug: (msg) => app.debug(msg),
      error: (msg) => app.error(msg),
    });
    walMonitor.start();

    // vm.max_map_count is kernel-global (not namespaced), so in managed mode
    // this local probe reflects the limit the QuestDB container actually runs
    // under. QuestDB memory-maps every partition column file and WAL segment;
    // on a grown database the stock 65530 exhausts and mmap fails with
    // out-of-memory errors — failed queries and suspended WAL apply while
    // plenty of RAM is free. Warn once here; /api/status re-reads it live so
    // the panel banner clears as soon as the operator raises the sysctl.
    if (config.managedContainer !== false) {
      const mapCount = await readMaxMapCount();
      if (mapCount?.tooLow) {
        app.error(
          `Host vm.max_map_count is ${mapCount.current}; QuestDB recommends ` +
            `${mapCount.recommended}. A grown database can exhaust this and ` +
            `fail queries or suspend recording. Fix in a host shell (not ` +
            `inside the container — the limit is kernel-global): ` +
            `echo 'vm.max_map_count=${mapCount.recommended}' | sudo tee ` +
            `/etc/sysctl.d/99-questdb.conf && sudo sysctl --system`,
        );
      }
    }

    app.setPluginStatus(recordingStatus(ilpHost, ilpPort));
    // Everything is registered — only now does the plugin count as running.
    // Any earlier return/throw leaves the flag false, so a half-started
    // plugin rejects lifecycle-dependent requests like /api/update/apply.
    pluginRunning = true;

    // Repopulate the model from history — only now, because the guard below
    // reads pluginRunning and everything before this point can still throw
    // and abandon the start.
    //
    // The recorder is already subscribed, so live deltas keep arriving while
    // the (slow, whole-table) restore query runs. Those are newer by
    // definition: replaying a stored fix over a vessel that just transmitted
    // would move it BACKWARDS on the chart. liveContexts records what the
    // recorder saw and the restore skips those contexts entirely.
    if (config.restoreOnStart) {
      const restoreGeneration = lifecycleGeneration;
      void restoreFromHistory(
        {
          queryClient,
          handleMessage: (delta) => {
            // A restore outliving its generation would inject data into a
            // model the operator just stopped recording into.
            if (!pluginRunning || lifecycleGeneration !== restoreGeneration) {
              return;
            }
            app.handleMessage("signalk-questdb", delta);
          },
          selfContext: app.selfContext,
          debug: (msg) => app.debug(msg),
          hasLiveData: (context) => liveContexts.has(context),
        },
        {
          maxAgeMs: config.restoreMaxAgeMinutes * 60_000,
          restoreSelf: config.recordSelf,
          restoreOthers: config.recordOthers,
        },
      )
        .then((result) => {
          if (lifecycleGeneration !== restoreGeneration) return;
          restoreSummary =
            result.contexts > 0
              ? `restored ${result.contexts} vessel${result.contexts === 1 ? "" : "s"} at startup`
              : "no vessels to restore at startup";
          restoreStatus = {
            contexts: result.contexts,
            skippedLive: result.skippedLive,
            failed: false,
          };
          // Re-publish so the summary lands on the card even when the status
          // was already set before the (slow) restore query finished.
          if (questdbEndpoints) {
            const { host, port } = questdbEndpoints.ilp;
            publishRecordingStatus(host, port);
          }
        })
        .catch((err: unknown) => {
          // A failed restore is a cosmetic loss — targets still arrive live —
          // so it must never take the plugin down with it. Surfaced on the
          // card rather than only in debug: silence is indistinguishable from
          // "restore is off".
          const message = err instanceof Error ? err.message : String(err);
          app.debug(`restore failed: ${message}`);
          if (lifecycleGeneration !== restoreGeneration) return;
          restoreSummary = "startup restore failed";
          restoreStatus = { contexts: 0, skippedLive: 0, failed: true };
          if (questdbEndpoints) {
            const { host, port } = questdbEndpoints.ilp;
            publishRecordingStatus(host, port);
          }
        })
        .finally(() => {
          // Only needed to shield the restore; tracking every context for the
          // life of the plugin would be an unbounded set. Guarded on the
          // generation so a slow restore settling after a restart does not
          // disarm the tracking the NEW start just armed.
          if (lifecycleGeneration !== restoreGeneration) return;
          liveContexts.clear();
          trackLiveContexts = false;
        });
    }
  }

  // Public entry: serialize startup behind the lifecycle lock so a purge or
  // update can't interleave with it. The generation is captured at enqueue: a
  // stop()/purge landing while this start is still queued invalidates it
  // before it begins — the window startAbort cannot cover, since the abort
  // controller only exists once runStart() is executing.
  function asyncStart(config: Config): Promise<void> {
    const generation = lifecycleGeneration;
    return withLifecycleLock(async () => {
      if (generation !== lifecycleGeneration) {
        app.debug("skipping queued start: plugin stopped while it waited");
        return;
      }
      await runStart(config);
    });
  }

  const plugin = {
    id: "signalk-questdb",
    name: "QuestDB History",

    schema: ConfigSchema,

    start(config: Config) {
      // Server does not await start(), so run async init in a
      // self-contained promise that handles its own errors.

      asyncStart(config).catch((err) => {
        publishError(
          `Startup failed: ${err instanceof Error ? err.message : String(err)}`,
        );
      });
    },

    async stop() {
      // Preempt lifecycle work: bump the generation so anything still queued
      // behind the lifecycle chain (or parked in a long await that re-checks
      // it) bails, and abort the currently-executing start so its post-await
      // signal checks return before registering resources. Together these
      // keep a slow start/update (container pull, LAN-host probe) from
      // resurrecting a writer/client after this teardown ran.
      lifecycleGeneration++;
      startAbort?.abort();
      pluginRunning = false;

      // A start aborted before its restore ran would otherwise leave tracking
      // armed, accumulating contexts for a restore that never happens.
      trackLiveContexts = false;
      liveContexts.clear();
      // Stale across a restart otherwise: the card would still claim a
      // restore count from the previous run.
      restoreSummary = null;
      restoreStatus = null;
      // Same reason: a path that is unstorable under this config may not be
      // under the next, so the count must not carry across a restart.
      unstorable.clear();
      pluginErrorActive = false;

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
      if (walMonitor) {
        walMonitor.stop();
        walMonitor = null;
      }
      clearWalAlertOnTeardown();
      schemaMismatch = false;

      // Clear the clamp warning so it doesn't survive into the next start
      // (e.g. a switch to external/unmanaged mode that never calls ensureRunning).
      ulimitClamp = null;

      if (writer) {
        await writer.disconnect();
        writer = null;
      }

      throttle.clear();
      lastNameByContext.clear();
      // Drop the compiled globs with the config they belong to, so a restart
      // with different patterns cannot match against the previous run's.
      pathFilterMatcher = null;
      rateMatcher = null;
      queryClient = null;
      questdbEndpoints = null;

      // Stop the managed container when plugin is disabled
      if (currentConfig?.managedContainer !== false) {
        const containers = (globalThis as any).__signalk_containerManager as
          ContainerManagerApi | undefined;
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
      // QuestDB's own console, proxied so the Signal K admin UI can embed it.
      //
      // Registered DIRECTLY on the router and NOT through router.access(...):
      // plugin routes default to admin-only in signalk-server, and only
      // .access() downgrades them. The console is a full SQL client that can
      // drop tables, so downgrading it would hand a non-admin the ability to
      // delete the vessel's recorded history. Leave this as it is.
      //
      // The enableConsole check lives INSIDE the handler, not around the
      // mount: registerWithRouter runs at plugin registration, before start()
      // has loaded any configuration, so `currentConfig` is still null here
      // and gating the mount would ignore a saved opt-out entirely. Checking
      // per-request also means toggling the option takes effect on plugin
      // restart without the route going stale.
      router.use("/console", (req, res, next) => {
        if (currentConfig?.enableConsole === false) {
          next();
          return;
        }
        consoleProxy(req, res);
      });

      router.get("/api/status", async (_req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ status: "not_running" } satisfies DbStatus);
            return;
          }

          // Probe the host limit BEFORE the health gate: mmap exhaustion is
          // one of the ways QuestDB becomes unhealthy in the first place, so
          // the remediation hint must survive into the unhealthy response
          // instead of disappearing exactly when it matters. Live /proc read
          // (cheap) rather than a cached startup probe, so the panel warning
          // clears on the next poll once the operator applies the sysctl —
          // raising it takes effect immediately, no container restart needed.
          // Managed mode only: an external QuestDB may run on another machine
          // whose kernel this probe cannot see.
          const hostMaxMapCount =
            currentConfig?.managedContainer !== false
              ? await readMaxMapCount()
              : null;

          const healthy = await queryClient.isHealthy();
          if (!healthy) {
            res.status(503).json({
              status: "unhealthy",
              hostMaxMapCount,
            } satisfies DbStatus);
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
          // `autoResume` is the monitor's verdict for the current stall point:
          // "pending" while the automatic lossless resume may still succeed,
          // "failed" once replay re-hit the same failure — the panel then
          // offers the guided skip instead of just the resume button.
          let suspendedTables: (SuspendedTable & {
            autoResume: string | null;
          })[] = [];
          try {
            suspendedTables = (await listSuspendedTables(queryClient)).map(
              (t) => ({
                ...t,
                autoResume: walMonitor?.outcomeFor(t.name) ?? null,
              }),
            );
          } catch {
            // wal_tables() itself is unavailable on non-WAL/older QuestDB, or
            // the tables don't exist yet during startup — treat as "not
            // suspended".
          }

          // The clamp advisory is a snapshot from the moment the container
          // was created; the container may since have been recreated with the
          // full limit (signalk-container's regrant on a raised host ceiling,
          // an update, an out-of-band operator fix). While the advisory
          // stands, verify it against the live limit on each poll and clear
          // it once satisfied — the same self-healing the hostMaxMapCount
          // probe above has. Requires signalk-container >= 1.25.3; without
          // the probe the advisory clears on the next plugin start.
          if (ulimitClamp && currentConfig?.managedContainer !== false) {
            const containers = (globalThis as any)
              .__signalk_containerManager as ContainerManagerApi | undefined;
            if (containers?.getContainerNofile) {
              // Capture what the probe is verifying: if an update recreates
              // the container while the probe is in flight (containerEpoch
              // bump) or the recreate records a NEW clamp, a stale probe
              // result must not clear it. Bounded like fetchEngineLogLines —
              // the container API mirror exposes no abort signal, so a
              // stalled runtime must degrade the response (advisory kept),
              // not hang the panel's status poll.
              const observedClamp = ulimitClamp;
              const observedEpoch = containerEpoch;
              let probeTimer: NodeJS.Timeout | undefined;
              try {
                const live = await Promise.race([
                  // The trailing catch keeps a probe that fails AFTER the
                  // timeout won the race from surfacing as an unhandled
                  // rejection.
                  containers
                    .getContainerNofile(QUESTDB_CONTAINER_NAME)
                    .catch(() => null),
                  new Promise<null>((resolve) => {
                    probeTimer = setTimeout(
                      () => resolve(null),
                      NOFILE_PROBE_TIMEOUT_MS,
                    );
                  }),
                ]);
                if (
                  nofileClampSatisfied(observedClamp.requested, live) &&
                  ulimitClamp === observedClamp &&
                  containerEpoch === observedEpoch
                ) {
                  ulimitClamp = null;
                }
              } catch {
                // Probe failure — keep the advisory rather than guess.
              } finally {
                if (probeTimer) clearTimeout(probeTimer);
              }
            }
          }

          res.json({
            status: "running",
            totalRows,
            activePathsToday,
            walSuspended: suspendedTables.length > 0,
            suspendedTables,
            schemaMismatch,
            ulimitClamp,
            hostMaxMapCount,
            endpoint: questdbEndpoints
              ? `${questdbEndpoints.http.host}:${questdbEndpoints.http.port}`
              : null,
            restore: restoreStatus,
            // Omitted entirely when nothing was dropped, so the panel can
            // treat presence as "there is something to report".
            unstorable:
              unstorable.size > 0
                ? {
                    paths: unstorable.size,
                    truncated: unstorable.truncated,
                    // A handful is enough to identify the shape; the tracked
                    // set is capped but still larger than a status body wants.
                    examples: unstorable.examples(5),
                  }
                : null,
          } satisfies DbStatus);
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
            })) satisfies QuestdbVersion[];
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

          res.json({
            currentVersion,
            latestVersion,
            updateAvailable,
          } satisfies UpdateInfo);
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
          });
        }
      });

      router.post("/api/update/apply", async (_req, res) => {
        try {
          // Captured at route entry, before ANY await: a stop() that lands
          // during the release fetch below must already invalidate this
          // update, not just one that lands after the lock is acquired.
          const updateGeneration = lifecycleGeneration;
          const containers = (globalThis as any).__signalk_containerManager as
            ContainerManagerApi | undefined;
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

          // Run the mutating update under the lifecycle lock so a concurrent
          // purge/start can't interleave with the pull + recreate + reconnect.
          // The route-entry generation is re-checked at lock entry and after
          // the long awaits: a stop() during the release fetch, the pull, or
          // the LAN-host probe must not let this resume, reassign endpoints,
          // and report success for a container the teardown already dealt
          // with.
          const ilp = await withLifecycleLock(async () => {
            const assertNotStopped = () => {
              if (updateGeneration !== lifecycleGeneration)
                throw new Error("plugin stopped while the update was running");
            };
            assertNotStopped();
            // A generation match only proves no stop() happened since route
            // entry — it can't tell that the plugin was already stopped (or
            // never fully started) when the request arrived. Gate on the
            // completed-start sentinel before pulling or recreating anything.
            if (!pluginRunning) throw new Error("plugin is not running");
            app.setPluginStatus(`Pulling QuestDB ${newTag}...`);
            await containers.pullImage(`questdb/questdb:${newTag}`);
            assertNotStopped();

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
              env: buildContainerEnv(currentConfig ?? {}),
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
            // update doesn't leave a stale warning. The recreate must not
            // happen after a stop/purge teardown, so gate it on the
            // generation one more time.
            assertNotStopped();
            ulimitClamp = null;
            containerEpoch++;
            await containers.ensureRunning(
              QUESTDB_CONTAINER_NAME,
              updateConfig,
              {
                onUlimitClamped,
              },
            );
            assertNotStopped();

            // The QuestDB version just changed, so the cached wal_tables()
            // column shape may no longer match (the errorTag/errorMessage
            // columns could appear on an upgrade or disappear on a downgrade).
            // Force the next /api/status to re-probe instead of trusting a
            // stale capability flag.
            walTablesHasErrorColumns = null;

            // Re-resolve so the export endpoints and status line reflect the
            // current endpoint. The version bump keeps the same container name
            // and networking, so the endpoint is stable — the existing
            // QueryClient/ILPWriter stay valid (and the registered history
            // providers keep their reference). On the LAN path this await
            // probes for the reachable host, so it's long enough for a stop()
            // to land mid-flight — re-check before touching plugin state.
            const updatedEndpoints = await resolveUpdateEndpoints();
            assertNotStopped();
            questdbEndpoints = updatedEndpoints;
            const { host: ilpHost, port: ilpPort } = questdbEndpoints.ilp;

            // Wait for the recreated container to answer, then reconnect ILP.
            // Locals are captured because stop() nulls the module bindings:
            // reading them again mid-loop would throw, and the generation
            // checks after every await are what keep a concurrent teardown
            // from being resurrected by the reconnect.
            const client = queryClient;
            if (client) {
              const deadline = Date.now() + 30000;
              while (Date.now() < deadline) {
                assertNotStopped();
                if (await client.isHealthy()) break;
                await new Promise((resolve) => setTimeout(resolve, 500));
              }
            }
            assertNotStopped();
            const ilpWriter = writer;
            if (ilpWriter) {
              try {
                await ilpWriter.disconnect();
              } catch {
                /* ignore */
              }
              assertNotStopped();
              await ilpWriter.connect();
              // A stop() that landed during connect() has already nulled
              // `writer`, so nothing would ever disconnect this fresh
              // connection — close it ourselves before failing the update.
              if (updateGeneration !== lifecycleGeneration) {
                try {
                  await ilpWriter.disconnect();
                } catch {
                  /* ignore */
                }
                assertNotStopped();
              }
            }
            return { ilpHost, ilpPort };
          });

          app.setPluginStatus(
            `Recording to QuestDB ${newTag} at ${ilp.ilpHost}:${ilp.ilpPort}`,
          );

          res.json({
            status: "updated",
            newVersion: newTag,
            message: `Updated to QuestDB ${newTag}. Container running.`,
          } satisfies UpdateApplyResponse);
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
      // Lossless recovery for a suspended WAL: one plain `RESUME WAL` per
      // suspended plugin table. Deliberately a SINGLE attempt each, no
      // retries — QuestDB's own HTTP console parks a busy ALTER and retries
      // it server-side once per second indefinitely, and several console
      // tabs doing that produced a self-sustaining retry storm in the field.
      // A busy/failed table is reported to the caller instead. `ALTER` is
      // DDL, so this cannot go through /api/query (isReadOnlySQL blocks it).
      //
      // No lifecycle lock: the resumes mutate no plugin-side state and are
      // idempotent, so racing a concurrent start/update/purge at worst
      // yields a per-table error the caller sees — while taking the lock
      // could park this request behind a wedged start forever.
      router.post("/api/resume-wal", async (_req, res) => {
        try {
          const client = queryClient;
          if (!client) {
            res
              .status(503)
              .json({ error: "QuestDB connection not initialized" });
            return;
          }
          const suspended = (await listSuspendedTables(client)).filter((row) =>
            FULL_EXPORT_TABLE_SET.has(row.name),
          );

          const results: {
            table: string;
            ok: boolean;
            error?: string;
            writerTxn: number;
            sequencerTxn: number;
          }[] = [];
          for (const row of suspended) {
            const table = row.name;
            const base = {
              table,
              writerTxn: row.writerTxn,
              sequencerTxn: row.sequencerTxn,
            };
            try {
              // Plain RESUME WAL replays from the next unapplied txn — it
              // skips nothing, so no data is lost. 10s deadline: a busy
              // table parks the request server-side (see above), and one
              // bounded attempt that reports "busy" beats waiting.
              await client.exec(
                `ALTER TABLE "${table.replace(/"/g, '""')}" RESUME WAL`,
                10_000,
              );
              results.push({ ...base, ok: true });
            } catch (err) {
              const msg = err instanceof Error ? err.message : String(err);
              results.push({
                ...base,
                ok: false,
                error: /abort|timeout/i.test(msg)
                  ? "table busy — the writer is held (likely by the WAL apply job or a stuck operation); if this persists, restart the QuestDB container"
                  : msg,
              });
            }
          }
          res.json({
            results,
            resumed: results.filter((r) => r.ok).length,
            message:
              results.length === 0
                ? "No suspended tables found"
                : `Resumed ${results.filter((r) => r.ok).length} of ${results.length} suspended table(s)`,
          } satisfies ResumeWalResponse);
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      // Per-suspended-table repair diagnosis: the pending-segment map, the
      // minimal bounded skip that would get past the stuck segment (with the
      // exact loss it implies), and the apply job's real failure reason
      // scraped from the engine log. The panel calls this when a suspension's
      // automatic resume has failed, to present the skip decision with its
      // cost quantified instead of a blind "RESUME WAL FROM TXN" incantation.
      router.get("/api/wal-diagnosis", async (_req, res) => {
        try {
          const client = queryClient;
          if (!client) {
            res
              .status(503)
              .json({ error: "QuestDB connection not initialized" });
            return;
          }
          const suspended = (await listSuspendedTables(client)).filter((row) =>
            FULL_EXPORT_TABLE_SET.has(row.name),
          );
          const logLines =
            suspended.length > 0 ? await fetchEngineLogLines() : null;
          // Annotated so every push is checked against the element shape;
          // `satisfies WalDiagnosis` on the response alone would only check
          // the wrapper.
          const tables: WalDiagnosisTable[] = [];
          for (const table of suspended) {
            let segments: PendingSegment[] = [];
            let segmentError: string | null = null;
            try {
              segments = await pendingSegments(client, table);
            } catch (err) {
              // wal_transactions() may be unavailable on an older QuestDB —
              // report the rest of the diagnosis without a skip plan.
              segmentError = err instanceof Error ? err.message : String(err);
            }
            const applyError = logLines
              ? extractApplyError(logLines, table.name)
              : null;
            // A torn applied partition fails at open, before any transaction
            // is read, so no RESUME WAL target can get past it. Withhold the
            // skip plan entirely rather than let the panel offer a repair
            // that destroys the backlog and still leaves the table suspended.
            const partitionOpenFailure = isPartitionOpenFailure(applyError);
            tables.push({
              ...table,
              autoResume: walMonitor?.outcomeFor(table.name) ?? null,
              // Commit-time of the first unapplied txn = when apply froze.
              suspendedSince:
                segments.length > 0 ? segments[0].minTimestamp : null,
              pendingSegments: segments.length,
              skipPlan: partitionOpenFailure ? null : computeSkipPlan(segments),
              partitionOpenFailure,
              segmentError,
              applyError,
            });
          }
          res.json({ tables } satisfies WalDiagnosis);
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      // Bounded lossy skip past an unreadable WAL segment. This is the ONLY
      // write path that can lose data, so it is deliberately narrow:
      //   - the table must be one of ours and currently suspended;
      //   - a lossless resume is attempted first and re-checked — if it
      //     sticks, nothing is skipped;
      //   - the skip target is computed server-side from the live segment
      //     map (never taken from the client), so it can only ever be the
      //     minimal next-segment boundary.
      // The client sends the plan it displayed only so a stale UI can be
      // rejected instead of skipping more than the operator agreed to.
      router.post("/api/resume-wal/skip", async (req, res) => {
        try {
          const client = queryClient;
          if (!client) {
            res
              .status(503)
              .json({ error: "QuestDB connection not initialized" });
            return;
          }
          const table = String(req.body?.table ?? "");
          if (!FULL_EXPORT_TABLE_SET.has(table)) {
            res.status(400).json({ error: `Unknown table: ${table}` });
            return;
          }
          const quoted = `"${table.replace(/"/g, '""')}"`;
          // Captured at entry: stop/purge bump the generation, an update
          // recreate bumps the container epoch — the destructive statement
          // below must never run against whatever lifecycle replaced the
          // state this request was validated on.
          const generation = lifecycleGeneration;
          const epoch = containerEpoch;
          const recheck = async () =>
            (await listSuspendedTables(client)).find((t) => t.name === table);

          const before = await recheck();
          if (!before) {
            res.json({
              skipped: false,
              healed: true,
              message: `${table} is not suspended — nothing to skip.`,
            } satisfies SkipWalResponse);
            return;
          }

          // Safety ladder step 1: the lossless path. If the suspension cause
          // was transient after all, this recovers everything and the lossy
          // step below never runs.
          await client.exec(`ALTER TABLE ${quoted} RESUME WAL`, 10_000);
          await new Promise((resolve) =>
            setTimeout(resolve, SKIP_RECHECK_DELAY_MS),
          );
          const afterResume = await recheck();
          if (!afterResume) {
            res.json({
              skipped: false,
              healed: true,
              message: `Lossless resume succeeded on ${table} — no data was skipped. The backlog is now replaying.`,
            } satisfies SkipWalResponse);
            return;
          }
          if (afterResume.writerTxn !== before.writerTxn) {
            res.json({
              skipped: false,
              healed: false,
              progressed: true,
              message: `The writer advanced on ${table} before re-suspending — the situation changed. Re-run the diagnosis.`,
            } satisfies SkipWalResponse);
            return;
          }

          // Step 2: replay re-froze at the same transaction. Everything the
          // destructive statement depends on is validated from the state
          // read CLOSEST to execution: re-read the suspension state, require
          // the writer still frozen at the same txn and the lifecycle
          // untouched by stop/update/purge, recompute the plan from that
          // state, and require it to match the operator's confirmation in
          // every field. Only that final validated plan is executed.
          const preExec = await recheck();
          if (
            generation !== lifecycleGeneration ||
            epoch !== containerEpoch ||
            !preExec ||
            preExec.writerTxn !== before.writerTxn
          ) {
            res.status(409).json({
              error:
                "The table's state changed while confirming the skip — nothing was skipped. Re-run the diagnosis.",
            });
            return;
          }
          // The skip is pointless AND destructive when the writer is dying at
          // partition open (torn `_txn` vs column data after a power cut): it
          // drops the pending backlog and the table stays suspended. Re-scrape
          // the engine log here, not from the client's stale diagnosis.
          const preExecLog = await fetchEngineLogLines();
          if (
            preExecLog &&
            isPartitionOpenFailure(extractApplyError(preExecLog, table))
          ) {
            res.status(409).json({
              error:
                `${table} is failing while opening its partition data, not while reading a WAL segment. ` +
                "A skip cannot repair this — the writer never reaches the transactions it would skip — " +
                "and it would drop the pending backlog for nothing. Nothing was skipped. This is a torn " +
                "commit record (power loss under QuestDB's non-durable default), which needs manual repair.",
            });
            return;
          }
          const plan = computeSkipPlan(await pendingSegments(client, preExec));
          if (!plan) {
            res.status(500).json({
              error: `Cannot compute a skip plan for ${table} — no pending transactions found.`,
            });
            return;
          }
          if (!skipPlansEqual(plan, req.body?.confirmPlan)) {
            res.status(409).json({
              error:
                "The skip plan changed since the diagnosis was displayed. Re-run the diagnosis and confirm again.",
              skipPlan: plan,
            });
            return;
          }

          // Last state read before the destructive statement — only
          // synchronous code between this validation and the ALTER, so no
          // await window remains in which the writer could move or the
          // lifecycle could be swapped. (The two HTTP calls are still
          // distinct reads; that residue is irreducible client-side.)
          const finalState = await recheck();
          if (
            generation !== lifecycleGeneration ||
            epoch !== containerEpoch ||
            !finalState ||
            finalState.writerTxn !== before.writerTxn
          ) {
            res.status(409).json({
              error:
                "The table's state changed while confirming the skip — nothing was skipped. Re-run the diagnosis.",
              skipPlan: plan,
            });
            return;
          }

          app.error(
            `Skipping ${plan.skippedTxns} unreadable WAL txn(s) on ${table} ` +
              `(walId=${plan.walId} segmentId=${plan.segmentId}, ` +
              `${plan.skipWindowStart} → ${plan.skipWindowEnd}) — ` +
              `RESUME WAL FROM TXN ${plan.skipToTxn}, requested via panel`,
          );
          await client.exec(
            `ALTER TABLE ${quoted} RESUME WAL FROM TXN ${plan.skipToTxn}`,
            10_000,
          );
          await new Promise((resolve) =>
            setTimeout(resolve, SKIP_RECHECK_DELAY_MS),
          );
          const afterSkip = await recheck();
          res.json({
            skipped: true,
            skipPlan: plan,
            stillSuspended: Boolean(afterSkip),
            message: afterSkip
              ? `Skipped ${plan.skippedTxns} txn(s) on ${table}, but the table re-suspended at txn ${afterSkip.writerTxn + 1} — another segment appears unreadable. Re-run the diagnosis to skip it too.`
              : `Skipped ${plan.skippedTxns} txn(s) on ${table} (${plan.skipWindowStart} → ${plan.skipWindowEnd}). The remaining backlog is replaying.`,
          } satisfies SkipWalResponse);
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : String(err),
          });
        }
      });

      router.post("/api/purge-data", async (_req, res) => {
        try {
          // External mode is a config fact independent of the runtime, so
          // answer it first — an external-mode install without signalk-container
          // should get the clear 400, not a 503 about a missing container
          // manager it doesn't need.
          if (currentConfig?.managedContainer === false) {
            res.status(400).json({
              error:
                "QuestDB is not managed by this plugin (external mode); nothing to remove.",
            });
            return;
          }
          const containers = (globalThis as any).__signalk_containerManager as
            ContainerManagerApi | undefined;
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

          // The actual teardown + data removal. Normally runs under the
          // lifecycle lock; the timeout fallback below may run it unlocked.
          // `teardownStarted` makes it run at most once: if the timeout
          // fallback runs the teardown while a wedged start still holds the
          // lock, the locked attempt becomes a no-op once the start finally
          // releases it (so removeManagedData isn't invoked twice).
          let teardownStarted = false;
          const teardown = async () => {
            if (teardownStarted) return;
            teardownStarted = true;
            pluginRunning = false;
            // Stop all activity against the container before it and its data go
            // away — otherwise the retention timer keeps issuing DROP PARTITION
            // against a removed container and the writer keeps trying to connect.
            if (schemaHealTimer) {
              clearInterval(schemaHealTimer);
              schemaHealTimer = null;
            }
            if (walMonitor) {
              walMonitor.stop();
              walMonitor = null;
            }
            clearWalAlertOnTeardown();
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
            await containers.removeManagedData!(
              QUESTDB_CONTAINER_NAME,
              hostPath,
              { ownerPluginId: "signalk-questdb" },
            );
          };

          // Purge is the RECOVERY action, so it must make progress even when a
          // start is wedged on a never-settling container call. Step 1: signal
          // any in-flight start to bail (its post-await checks return without
          // creating resources) and bump the generation so a start/update
          // still QUEUED behind the chain bails at entry instead of running
          // after the teardown. Step 2: try to acquire the lifecycle lock so
          // we still serialize against a start/update that IS progressing —
          // but only for a bounded time. If the lock isn't free within
          // PURGE_LOCK_TIMEOUT_MS (a truly hung start that ignored the abort
          // and never released it), run the teardown anyway. Interleaving risk
          // is minimal because we already aborted the start, so any start that
          // later wakes up returns early instead of recreating the container.
          lifecycleGeneration++;
          startAbort?.abort();

          let lockedTeardownSettled = false;
          let lockedTeardownError: unknown;
          const lockedTeardown = withLifecycleLock(teardown).then(
            () => {
              lockedTeardownSettled = true;
            },
            (err) => {
              // Capture rather than rethrow: in the timeout branch nothing
              // awaits this promise, so a rethrow would surface as an
              // unhandled rejection. The locked branch re-throws it below.
              lockedTeardownSettled = true;
              lockedTeardownError = err;
            },
          );
          let timeoutTimer: NodeJS.Timeout | undefined;
          const timeout = new Promise<"timeout">((resolve) => {
            timeoutTimer = setTimeout(
              () => resolve("timeout"),
              PURGE_LOCK_TIMEOUT_MS,
            );
          });
          const outcome = await Promise.race([
            lockedTeardown.then(() => "locked" as const),
            timeout,
          ]);
          if (timeoutTimer) clearTimeout(timeoutTimer);
          if (outcome === "locked") {
            // The lock-held teardown completed (or failed). Propagate its error
            // to the outer catch so the client sees the real failure.
            if (lockedTeardownError) throw lockedTeardownError;
          } else if (!lockedTeardownSettled) {
            app.debug(
              "purge: lifecycle lock not acquired within timeout; a start " +
                "appears wedged. Proceeding with teardown to make progress.",
            );
            await teardown();
            // The wedged start still owns the old chain (its promise never
            // settled), so reset the gate to a fresh resolved promise —
            // otherwise every future lifecycle op (e.g. the asyncStart when the
            // operator re-enables the plugin) would queue behind the dead chain
            // forever. The abandoned start's own resources are already aborted.
            lifecycleChain = Promise.resolve();
            startAbort = null;
          }

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
        // `?url=a&url=b` arrives as an ARRAY, not a string — the cast this
        // replaces would have handed a non-string to validateInfluxUrl and
        // thrown inside the handler rather than answering 400.
        const requestedUrl = req.query.url;
        if (requestedUrl !== undefined && typeof requestedUrl !== "string") {
          res.status(400).json({
            error: "url must be a single value",
            sources: [],
          } satisfies MigrationDetectResponse);
          return;
        }
        const baseUrl = validateInfluxUrl(
          requestedUrl || "http://localhost:8086",
        );
        if (!baseUrl) {
          res.status(400).json({
            error: "Only localhost and private network http(s) URLs allowed",
            sources: [],
          } satisfies MigrationDetectResponse);
          return;
        }
        try {
          const sources = await detectInflux(baseUrl);
          res.json({ sources } satisfies MigrationDetectResponse);
        } catch (err) {
          res.status(500).json({
            error: err instanceof Error ? err.message : "Unknown error",
            sources: [],
          } satisfies MigrationDetectResponse);
        }
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
