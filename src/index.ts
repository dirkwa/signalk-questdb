import { readFileSync, unlinkSync, writeFileSync } from "fs";
import { IRouter } from "express";
import { minimatch } from "minimatch";
import { ILPWriter } from "./ilp-writer";
import { QueryClient, isReadOnlySQL } from "./query-client";
import { Config, ConfigSchema } from "./config/schema";
import { createHistoryProviderV2 } from "./history-v2";
import { createHistoryProviderV1 } from "./history-v1";
import { startRetention } from "./retention";

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

interface ContainerManagerApi {
  getRuntime: () => { runtime: string; version: string } | null;
  ensureRunning: (
    name: string,
    config: unknown,
    options?: unknown,
  ) => Promise<void>;
  stop: (name: string) => Promise<void>;
  remove: (name: string) => Promise<void>;
  getState: (name: string) => Promise<string>;
  ensureNetwork: (name: string) => Promise<void>;
  pullImage: (
    image: string,
    onProgress?: (msg: string) => void,
  ) => Promise<void>;
}

module.exports = (app: App) => {
  let writer: ILPWriter | null = null;
  let queryClient: QueryClient | null = null;
  let retentionTimer: NodeJS.Timeout | null = null;
  let currentConfig: Config | null = null;
  const unsubscribes: (() => void)[] = [];
  const throttleMap = new Map<string, number>();

  function shouldRecord(
    path: string,
    filter: { mode: string; paths: string[] },
  ): boolean {
    if (!filter.paths || filter.paths.length === 0) return true;

    const matches = filter.paths.some((pattern) => minimatch(path, pattern));
    return filter.mode === "exclude" ? !matches : matches;
  }

  function isThrottled(path: string, rates: Record<string, number>): boolean {
    const now = Date.now();
    for (const [pattern, minMs] of Object.entries(rates)) {
      if (minMs <= 0) continue;
      if (!minimatch(path, pattern)) continue;

      const lastWrite = throttleMap.get(path) ?? 0;
      if (now - lastWrite < minMs) return true;
      throttleMap.set(path, now);
      return false;
    }
    return false;
  }

  async function asyncStart(config: Config) {
    currentConfig = config;
    const host = config.questdbHost ?? "127.0.0.1";
    const ilpPort = config.questdbIlpPort ?? 9009;
    const httpPort = config.questdbHttpPort ?? 9000;

    if (config.managedContainer !== false) {
      let containers: ContainerManagerApi | undefined;
      const waitDeadline = Date.now() + 30000;
      while (Date.now() < waitDeadline) {
        containers = (globalThis as any).__signalk_containerManager as
          | ContainerManagerApi
          | undefined;
        if (containers && containers.getRuntime()) break;
        app.setPluginStatus("Waiting for container runtime detection...");
        await new Promise((resolve) => setTimeout(resolve, 1000));
      }

      if (!containers) {
        app.debug("containerManager not found after timeout");
        app.setPluginError(
          "signalk-container plugin required for managed mode. Install it or set managedContainer=false.",
        );
        return;
      }

      if (!containers.getRuntime()) {
        app.debug("container runtime not detected after timeout");
        app.setPluginError(
          "No container runtime detected. Check signalk-container plugin.",
        );
        return;
      }

      app.debug("container runtime ready, starting QuestDB");
      try {
        const containerEnv: Record<string, string> = {
          QDB_TELEMETRY_ENABLED: "false",
          QDB_HTTP_ENABLED: "true",
          QDB_LINE_TCP_ENABLED: "true",
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

        const bind = config.exposeToContainers ? "0.0.0.0" : "127.0.0.1";
        const containerConfig: Record<string, unknown> = {
          image: "questdb/questdb",
          tag: config.questdbVersion ?? "latest",
          ports: {
            "9000/tcp": `${bind}:${httpPort}`,
            "9009/tcp": `${bind}:${ilpPort}`,
            "8812/tcp": `${bind}:${config.questdbPgPort ?? 8812}`,
          },
          volumes: {
            "/var/lib/questdb": app.getDataDirPath(),
          },
          env: containerEnv,
          restart: "unless-stopped",
        };

        if (config.networkName) {
          await containers.ensureNetwork(config.networkName);
          containerConfig.networkMode = config.networkName;
        }

        // Check if container needs recreation (config/version/compression changed).
        // Compare against stored hash from last successful start.
        const configHash = JSON.stringify({
          tag: containerConfig.tag,
          ports: containerConfig.ports,
          env: containerConfig.env,
          networkMode: containerConfig.networkMode,
        });
        // Store hash next to plugin config JSON, not in the QuestDB data volume
        const hashFile = `${app.getDataDirPath()}.container-hash`;
        let lastHash = "";
        try {
          lastHash = readFileSync(hashFile, "utf8");
        } catch {
          // first run
        }

        const state = await containers.getState("signalk-questdb");
        if (state !== "missing" && configHash !== lastHash) {
          app.setPluginStatus(
            "Recreating QuestDB container (config changed)...",
          );
          await containers.remove("signalk-questdb");
        }

        app.setPluginStatus("Starting QuestDB container...");
        await containers.ensureRunning("signalk-questdb", containerConfig);

        // Store hash for next start comparison
        writeFileSync(hashFile, configHash);
        app.debug("QuestDB container ready");
      } catch (err) {
        app.debug("ensureRunning failed:", err);
        app.setPluginError(
          `Failed to start QuestDB container: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }
    }

    app.debug("connecting to QuestDB at %s:%d", host, httpPort);
    queryClient = new QueryClient(host, httpPort);

    app.setPluginStatus("Waiting for QuestDB to become ready...");
    const deadline = Date.now() + 30000;
    while (Date.now() < deadline) {
      if (await queryClient.isHealthy()) break;
      await new Promise((resolve) => setTimeout(resolve, 500));
    }

    if (!(await queryClient.isHealthy())) {
      app.setPluginError(`QuestDB not responding at ${host}:${httpPort}`);
      return;
    }

    app.setPluginStatus("Creating tables...");
    await queryClient.ensureTables();

    writer = new ILPWriter(host, ilpPort, (msg) => app.debug(msg));
    await writer.connect();

    const v2Provider = createHistoryProviderV2(queryClient);
    app.registerHistoryApiProvider(v2Provider);

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
      if (isThrottled(path, config.samplingRates)) return;

      const ts = timestamp ? new Date(timestamp) : new Date();
      const ctx = isSelf ? "self" : context;

      if (typeof value === "number") {
        writer.write(path, ctx, value, ts);
      } else if (typeof value === "string") {
        writer.writeString(path, ctx, value, ts);
      } else if (value && typeof value === "object" && "latitude" in value) {
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

    app.setPluginStatus(`Recording to QuestDB at ${host}:${ilpPort}`);
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

      if (writer) {
        await writer.disconnect();
        writer = null;
      }

      throttleMap.clear();
      queryClient = null;

      // Stop the managed container when plugin is disabled
      if (currentConfig?.managedContainer !== false) {
        const containers = (globalThis as any).__signalk_containerManager as
          | ContainerManagerApi
          | undefined;
        if (containers) {
          try {
            await containers.stop("signalk-questdb");
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

          res.json({
            status: "running",
            totalRows,
            activePathsToday,
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

          // Stop and remove old container
          app.setPluginStatus("Replacing container...");
          await containers.remove("signalk-questdb");

          // Update config and persist
          if (currentConfig) {
            currentConfig.questdbVersion = newTag;
            await new Promise<void>((resolve, reject) => {
              app.savePluginOptions({ ...currentConfig! }, (err) => {
                if (err) reject(err);
                else resolve();
              });
            });
          }

          // Clear hash and recreate container with new image
          const hashFile = `${app.getDataDirPath()}.container-hash`;
          try {
            unlinkSync(hashFile);
          } catch {
            // doesn't exist
          }

          const host = currentConfig?.questdbHost ?? "127.0.0.1";
          const httpPort = currentConfig?.questdbHttpPort ?? 9000;
          const ilpPort = currentConfig?.questdbIlpPort ?? 9009;

          const updateBind = currentConfig?.exposeToContainers
            ? "0.0.0.0"
            : "127.0.0.1";
          app.setPluginStatus(`Starting QuestDB ${newTag}...`);
          await containers.ensureRunning("signalk-questdb", {
            image: "questdb/questdb",
            tag: newTag,
            ports: {
              "9000/tcp": `${updateBind}:${httpPort}`,
              "9009/tcp": `${updateBind}:${ilpPort}`,
              "8812/tcp": `${updateBind}:${currentConfig?.questdbPgPort ?? 8812}`,
            },
            volumes: {
              "/var/lib/questdb": app.getDataDirPath(),
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
          });

          // Write new hash
          const newHash = JSON.stringify({
            tag: newTag,
            ports: {
              "9000/tcp": `${updateBind}:${httpPort}`,
              "9009/tcp": `${updateBind}:${ilpPort}`,
              "8812/tcp": `${updateBind}:${currentConfig?.questdbPgPort ?? 8812}`,
            },
            env: {
              QDB_TELEMETRY_ENABLED: "false",
              QDB_HTTP_ENABLED: "true",
              QDB_LINE_TCP_ENABLED: "true",
            },
          });
          writeFileSync(hashFile, newHash);

          // Reconnect ILP and query client
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
            `Recording to QuestDB ${newTag} at ${host}:${ilpPort}`,
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

          const host = currentConfig?.questdbHost ?? "127.0.0.1";
          const httpPort = currentConfig?.questdbHttpPort ?? 9000;
          const expUrl = new URL("/exp", `http://${host}:${httpPort}`);
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
    },
  };

  return plugin;
};
