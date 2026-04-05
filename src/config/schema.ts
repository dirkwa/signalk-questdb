import { Type, Static } from "@sinclair/typebox";

export const ConfigSchema = Type.Object({
  questdbHost: Type.String({
    default: "127.0.0.1",
    title: "QuestDB host",
  }),
  questdbIlpPort: Type.Number({
    default: 9009,
    title: "ILP port (writes)",
  }),
  questdbHttpPort: Type.Number({
    default: 9000,
    title: "HTTP port (queries)",
  }),
  questdbPgPort: Type.Number({
    default: 8812,
    title: "PostgreSQL wire port",
  }),
  questdbVersion: Type.String({
    default: "9.2.0",
    title: "QuestDB image version",
  }),

  managedContainer: Type.Boolean({
    default: true,
    title: "Manage QuestDB container via signalk-container",
    description: "Disable to connect to an external QuestDB instance",
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

  samplingRates: Type.Record(Type.String(), Type.Number(), {
    default: {},
    title: "Per-path sampling rates (ms)",
    description:
      'Minimum ms between writes per path. e.g. { "environment.wind.*": 1000 }',
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

  exposeToContainers: Type.Boolean({
    default: false,
    title: "Expose ports to other containers",
    description:
      "Bind to 0.0.0.0 instead of 127.0.0.1 so Grafana or other containers running in Docker/Podman can connect",
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
