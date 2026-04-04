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
  setPluginStatus: (id: string, msg: string) => void;
  setPluginError: (id: string, msg: string) => void;
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
  [key: string]: unknown;
}

interface ContainerManagerApi {
  ensureRunning: (
    name: string,
    config: unknown,
    options?: unknown,
  ) => Promise<void>;
  getState: (name: string) => Promise<string>;
}

module.exports = (app: App) => {
  let writer: ILPWriter | null = null;
  let queryClient: QueryClient | null = null;
  let retentionTimer: NodeJS.Timeout | null = null;
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

  const plugin = {
    id: "signalk-questdb",
    name: "QuestDB History",

    schema: ConfigSchema,

    async start(config: Config) {
      const host = config.questdbHost ?? "127.0.0.1";
      const ilpPort = config.questdbIlpPort ?? 9009;
      const httpPort = config.questdbHttpPort ?? 9000;

      if (config.managedContainer !== false) {
        const containers = (app as any).containerManager as
          | ContainerManagerApi
          | undefined;
        if (!containers) {
          app.setPluginError(
            plugin.id,
            "signalk-container plugin required for managed mode. Install it or set managedContainer=false.",
          );
          return;
        }

        try {
          await containers.ensureRunning("signalk-questdb", {
            image: "questdb/questdb",
            tag: config.questdbVersion ?? "9.2.0",
            ports: {
              "9000/tcp": `127.0.0.1:${httpPort}`,
              "9009/tcp": `127.0.0.1:${ilpPort}`,
              "8812/tcp": `127.0.0.1:${config.questdbPgPort ?? 8812}`,
            },
            volumes: {
              "/var/lib/questdb": app.getDataDirPath(),
            },
            env: {
              QDB_TELEMETRY_ENABLED: "false",
              QDB_HTTP_ENABLED: "true",
              QDB_LINE_TCP_ENABLED: "true",
            },
            restart: "unless-stopped",
          });
        } catch (err) {
          app.setPluginError(
            plugin.id,
            `Failed to start QuestDB container: ${err instanceof Error ? err.message : String(err)}`,
          );
          return;
        }
      }

      queryClient = new QueryClient(host, httpPort);

      const deadline = Date.now() + 30000;
      while (Date.now() < deadline) {
        if (await queryClient.isHealthy()) break;
        await new Promise((resolve) => setTimeout(resolve, 500));
      }

      if (!(await queryClient.isHealthy())) {
        app.setPluginError(
          plugin.id,
          `QuestDB not responding at ${host}:${httpPort}`,
        );
        return;
      }

      try {
        await queryClient.ensureTables();
      } catch (err) {
        app.setPluginError(
          plugin.id,
          `Failed to create tables: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }

      writer = new ILPWriter(host, ilpPort, (msg) => app.debug(msg));
      try {
        await writer.connect();
      } catch (err) {
        app.setPluginError(
          plugin.id,
          `ILP connect failed: ${err instanceof Error ? err.message : String(err)}`,
        );
        return;
      }

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

      app.setPluginStatus(
        plugin.id,
        `Recording to QuestDB at ${host}:${ilpPort}`,
      );
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

          const countResult = await queryClient.exec(
            "SELECT count() as cnt FROM signalk",
          );
          const pathResult = await queryClient.exec(
            "SELECT count(distinct path) as cnt FROM signalk WHERE ts > dateadd('d', -1, now())",
          );

          res.json({
            status: "running",
            totalRows:
              countResult.dataset.length > 0 ? countResult.dataset[0][0] : 0,
            activePathsToday:
              pathResult.dataset.length > 0 ? pathResult.dataset[0][0] : 0,
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

      router.get("/api/export", async (req, res) => {
        try {
          if (!queryClient) {
            res.status(503).json({ error: "QuestDB not connected" });
            return;
          }
          const from = req.query.from as string;
          const to = req.query.to as string;
          if (!from || !to) {
            res.status(400).json({ error: "Missing from/to parameters" });
            return;
          }

          const safeFrom = new Date(from).toISOString();
          const safeTo = new Date(to).toISOString();
          const sql = `SELECT ts, path, context, value FROM signalk WHERE ts >= '${safeFrom}' AND ts <= '${safeTo}' ORDER BY ts`;

          const csv = await queryClient.execCsv(sql);
          res.setHeader("Content-Type", "text/csv");
          res.setHeader(
            "Content-Disposition",
            `attachment; filename="signalk-export-${safeFrom.slice(0, 10)}.csv"`,
          );
          res.send(csv);
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
