import { Type, Static } from "@sinclair/typebox";

export const ConfigSchema = Type.Object({
  questdbHost: Type.String({
    default: "127.0.0.1",
    title: "QuestDB host",
    description:
      "Only used when managed container is off (external QuestDB). In managed mode the address is resolved automatically.",
  }),
  questdbIlpPort: Type.Number({
    default: 9009,
    title: "ILP port (writes)",
    description:
      "External mode, or the host binding when 'Bind to 0.0.0.0' is on. Ignored otherwise (signalk-container allocates the port).",
  }),
  questdbHttpPort: Type.Number({
    default: 9000,
    title: "HTTP port (queries)",
    description:
      "External mode, or the host binding when 'Bind to 0.0.0.0' is on. Ignored otherwise (signalk-container allocates the port).",
  }),
  questdbPgPort: Type.Number({
    default: 8812,
    title: "PostgreSQL wire port",
    description:
      "Host binding for Grafana/psql when 'Bind to 0.0.0.0' is on. Ignored otherwise (signalk-container allocates the port).",
  }),
  questdbVersion: Type.String({
    default: "latest",
    title: "QuestDB image version",
  }),

  managedContainer: Type.Boolean({
    default: true,
    title: "Manage QuestDB container via signalk-container",
    description: "Disable to connect to an external QuestDB instance",
  }),

  questdbMemoryLimit: Type.String({
    default: "768m",
    title: "QuestDB memory limit",
    description:
      "Hard cgroup memory cap (e.g. 512m, 1g, 2g). Empty = unlimited. The JVM auto-sizes its heap to a fraction of this, so capping here bounds total footprint including off-heap.",
  }),

  questdbCpuLimit: Type.Number({
    default: 1.5,
    title: "QuestDB CPU limit (cores)",
    description:
      "Max CPU cores QuestDB can use (fractional, e.g. 1.5). 0 = unlimited.",
  }),

  pathFilter: Type.Object({
    mode: Type.Union([Type.Literal("exclude"), Type.Literal("include")], {
      default: "exclude",
      title: "Filter mode",
    }),
    paths: Type.Array(Type.String(), {
      default: [],
      title: "Path patterns (glob supported)",
      description: 'e.g. "notifications.*", "environment.wind.*"',
    }),
  }),

  defaultSamplingRate: Type.Number({
    default: 2000,
    title: "Default sampling rate (ms)",
    description:
      "Minimum ms between writes for any path (0 = write every update). 2000ms is a sensible default for Pi/Cerbo-class hardware; lower it per-path via samplingRates when you need finer resolution.",
  }),

  samplingRates: Type.Record(Type.String(), Type.Number(), {
    default: {},
    title: "Per-path sampling rates (ms)",
    description:
      'Override default rate for specific paths. e.g. { "environment.wind.*": 200, "tanks.*": 10000 }',
  }),

  recordSelf: Type.Boolean({
    default: true,
    title: "Record own vessel",
  }),
  recordOthers: Type.Boolean({
    default: false,
    title: "Record AIS targets",
  }),

  retentionDays: Type.Number({
    default: 0,
    title: "Retention (days, 0 = keep forever)",
  }),

  networkName: Type.String({
    default: "sk-network",
    title: "Container network",
    description:
      "Shared Podman/Docker network for QuestDB. Only applied when 'Bind to 0.0.0.0' is on (so a separate-Docker Grafana can reach QuestDB by DNS).",
  }),

  exposeToContainers: Type.Boolean({
    default: false,
    title: "Bind to 0.0.0.0",
    description:
      "Publish QuestDB's ports on all interfaces so another machine or a separate-Docker Grafana can reach them. Off (default) keeps QuestDB private — Signal K still reaches it automatically. Caution! Enabling exposes your data to the network.",
  }),

  compression: Type.Union(
    [Type.Literal("none"), Type.Literal("lz4"), Type.Literal("zstd")],
    {
      default: "lz4",
      title: "Compression codec",
      description:
        "Used for QuestDB on-disk WAL segments and Parquet exports (lz4 = fast, zstd = smaller)",
    },
  ),
  compressionLevel: Type.Number({
    default: 3,
    title: "Compression level",
    description: "zstd: 1-22 (default 3), lz4: ignored",
  }),
});

export type Config = Static<typeof ConfigSchema>;
