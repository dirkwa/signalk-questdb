# signalk-questdb

QuestDB history provider for Signal K -- a drop-in replacement for signalk-to-influxdb and signalk-to-influxdb2.

Stores all vessel data in QuestDB running as a managed container (via [signalk-container](https://github.com/dirkwa/signalk-container)). Implements both the modern v2 History API and the legacy v1 playback API.

## Features

- **Automatic container management** -- QuestDB runs in Podman/Docker, managed by signalk-container
- **ILP ingestion** -- writes via InfluxDB Line Protocol over raw TCP (no client library needed)
- **v2 History API** -- `getValues`, `getPaths`, `getContexts` with all aggregate methods
- **v1 Legacy API** -- `hasAnyData`, `streamHistory`, `getHistory` for WebSocket playback
- **Path filtering** -- include/exclude paths with glob patterns
- **Sampling rates** -- per-path throttling to control write volume
- **Retention policy** -- automatic partition drop after N days
- **AIS recording** -- optionally record other vessels
- **Position tracking** -- separate optimized table for lat/lon
- **On-disk compression** -- LZ4 (fast) or ZSTD (smaller) via QuestDB WAL segment compression
- **Parquet export** -- native QuestDB Parquet export with configurable compression
- **CSV export** -- download historical data via REST endpoint
- **InfluxDB migration** -- auto-detect InfluxDB 1.x/2.x on localhost or remote URL
- **One-click updates** -- check for new QuestDB releases and update from the config panel
- **Config panel** -- status dashboard with row counts, version picker, update check, collapsible compression/migration/export sections
- **SQL injection protection** -- strict input validation on all query endpoints
- **Container lifecycle** -- container stops when plugin is disabled, starts on enable

## Config Panel

The plugin embeds a React config panel in the Signal K Admin UI showing:

- **QuestDB Status** -- running/not running indicator, total rows, active paths today
- **Update check** -- compares running version against latest GitHub release, one-click update
- **Image Version** -- dropdown with latest, pre-releases, and last 3 stable releases
- **Connection** -- managed container toggle, host/ports, PostgreSQL port for Grafana
- **Recording** -- record self, record AIS targets, retention days
- **Path filtering** (collapsible) -- exclude or include-only paths with glob patterns (e.g. exclude `navigation.position`)
- **Compression** (collapsible) -- LZ4/ZSTD codec selection for on-disk storage
- **InfluxDB Migration** (collapsible) -- auto-detect with manual URL for remote instances
- **Data Export** (collapsible) -- date range picker, Parquet/CSV format, download button
- **Danger zone** (collapsible) -- "Remove container & all data" to fully reset QuestDB (deletes data Signal K's plugin-uninstall can't, on rootless Podman)

## QuestDB Schema

Three tables, all with WAL mode, daily partitioning, and deduplication:

| Table              | Purpose        | Columns                                                          |
| ------------------ | -------------- | ---------------------------------------------------------------- |
| `signalk`          | Numeric values | `ts`, `path` (SYMBOL), `context` (SYMBOL), `value` (DOUBLE)      |
| `signalk_str`      | String values  | `ts`, `path` (SYMBOL), `context` (SYMBOL), `value_str` (VARCHAR) |
| `signalk_position` | Positions      | `ts`, `context` (SYMBOL), `lat` (DOUBLE), `lon` (DOUBLE)         |

## History API

### v2 (REST -- `/signalk/v2/api/history/`)

Registered via `app.registerHistoryApiProvider()`. Supports all aggregate methods:

| Method    | QuestDB mapping                        |
| --------- | -------------------------------------- |
| `average` | `avg(value)`                           |
| `min`     | `min(value)`                           |
| `max`     | `max(value)`                           |
| `first`   | `first(value)`                         |
| `last`    | `last(value)`                          |
| `mid`     | `(min + max) / 2`                      |
| `sma`     | Client-side N-sample moving average    |
| `ema`     | Client-side exponential moving average |

Query example:

```
GET /signalk/v2/api/history/values?paths=navigation.speedOverGround&duration=PT1H&resolution=60
```

### v1 (WebSocket playback)

Registered via `app.registerHistoryProvider()`. Supports playback at configurable speed multipliers using chunked reads from QuestDB.

## REST Endpoints

All mounted at `/plugins/signalk-questdb/api/`:

| Method | Path                                     | Description                                                                              |
| ------ | ---------------------------------------- | ---------------------------------------------------------------------------------------- |
| GET    | `/status`                                | QuestDB health, row counts, active paths                                                 |
| GET    | `/query?sql=...`                         | Read-only SQL proxy (DDL/DML blocked)                                                    |
| GET    | `/paths`                                 | All recorded paths with row counts and time range                                        |
| GET    | `/versions`                              | QuestDB releases from GitHub (for version picker)                                        |
| GET    | `/update/check`                          | Compare running version against latest release                                           |
| POST   | `/update/apply`                          | Pull latest image, recreate container, reconnect                                         |
| POST   | `/purge-data`                            | Remove the QuestDB container and delete all its data (rootless-Podman-safe)              |
| GET    | `/migration/detect`                      | Auto-detect InfluxDB (supports `?url=` for remote)                                       |
| GET    | `/export?from=...&to=...&format=parquet` | Parquet or CSV export of the `signalk` numeric table (date range required)               |
| GET    | `/full-export/tables`                    | List tables exposed by the per-table full-export route                                   |
| GET    | `/full-export/:table?from=...&to=...`    | Stream a table as Parquet. Optional half-open `[from, to)` range for slicing into shards |

### `/full-export/:table` (since 0.4.0)

Designed for snapshot/backup tooling that needs the full table content but
wants to slice it into kopia-dedup-friendly shards. Allowed tables:
`signalk`, `signalk_str`, `signalk_position`.

- Both `from` and `to` are **optional but must be set together**: omit both for a full-table export, or pass both as ISO 8601 timestamps for a windowed export. Half-open `[from, to)` interval — no row appears in two adjacent windows.
- Repeated query params (`?from=A&from=B`) and empty strings (`?from=`) are rejected with HTTP 400 — silently downgrading to a full-table export would hide bugs in the caller.
- Output format and compression follow the plugin's `compression` config (LZ4_RAW or ZSTD), same as `/export`.

## Configuration

| Setting            | Default      | Description                                                                         |
| ------------------ | ------------ | ----------------------------------------------------------------------------------- |
| QuestDB version    | `latest`     | Docker image tag (dropdown shows stable + pre-releases)                             |
| Managed container  | `true`       | Let signalk-container manage QuestDB, or connect to external                        |
| QuestDB host       | `127.0.0.1`  | External QuestDB host (only used when managed=false)                                |
| HTTP port          | `9000`       | External mode, or the host binding when "Bind to 0.0.0.0" is on                     |
| ILP port           | `9009`       | External mode, or the host binding when "Bind to 0.0.0.0" is on                     |
| PostgreSQL port    | `8812`       | Host binding for Grafana/psql when "Bind to 0.0.0.0" is on                          |
| Sampling rate (ms) | `2000`       | Default min ms between writes per path (0 = every update)                           |
| Memory limit       | `768m`       | Hard cgroup cap on QuestDB container RAM (empty = unlimited)                        |
| CPU limit (cores)  | `1.5`        | Max CPU cores QuestDB can use (0 = unlimited)                                       |
| Record own vessel  | `true`       | Record self context                                                                 |
| Record AIS targets | `false`      | Record other vessels                                                                |
| Retention (days)   | `0`          | Auto-delete old partitions (0 = keep forever)                                       |
| Path filter mode   | `exclude`    | `exclude` matching paths, or `include` only matching paths                          |
| Path filter paths  | _(empty)_    | Glob patterns, one per line (e.g. `navigation.position`); empty = record everything |
| Compression codec  | `lz4`        | On-disk WAL compression: `none`, `lz4`, or `zstd`                                   |
| Compression level  | `3`          | ZSTD level 1-22 (only when codec is zstd)                                           |
| Container network  | `sk-network` | Shared network for QuestDB (only applied when binding to 0.0.0.0)                   |
| Bind to 0.0.0.0    | `false`      | Expose QuestDB's ports on the LAN (see Connectivity below)                          |

## Connectivity

In **managed mode** the plugin no longer needs `QuestDB host` to be correct for
your deployment — signalk-container resolves the right address automatically,
whether Signal K runs on bare metal or is itself containerized:

- **Bind to 0.0.0.0 = off (default).** QuestDB stays private. signalk-container
  binds its ports to the host loopback (bare-metal Signal K) or attaches QuestDB
  to Signal K's own container network (containerized Signal K), and the plugin
  connects to whatever address it reports back. Nothing is exposed to the
  network. QuestDB is also attached to the shared `Container network` so the
  companion [signalk-grafana](https://github.com/dirkwa/signalk-grafana) plugin
  still reaches it by container DNS. This is the recommended setup and fixes
  connectivity for Signal K in a container.
- **Bind to 0.0.0.0 = on.** QuestDB's HTTP/ILP/PostgreSQL ports are published on
  all interfaces using the configured port numbers, on the shared
  `Container network`. Enable this only to reach QuestDB from another machine or
  from a Grafana running in a separate Docker instance. When Signal K itself is
  containerized it reaches the published ports via `host.containers.internal`.

In **external mode** (`Managed container` off) the plugin connects to the
QuestDB you point it at via `QuestDB host` + the HTTP/ILP ports.

## QuestDB Web Console

QuestDB ships a web console (SQL editor + import UI) on its HTTP port. On the
Signal K host it is at:

```
http://localhost:9000
```

By default ("Bind to 0.0.0.0" off) that port is bound to loopback only, so it
is **not** reachable from another machine. To open the console from your
laptop, either tunnel over SSH:

```
ssh -L 9000:127.0.0.1:9000 <user>@<signalk-host>
```

then browse to `http://localhost:9000`, or enable **"Bind to 0.0.0.0"** in the
plugin config and use `http://<signalk-host-ip>:9000` (this exposes QuestDB to
your network — see the warning under Grafana Integration).

## Performance (Pi / Low-Power Devices)

The plugin is optimized for Raspberry Pi and similar low-power devices:

- **Default sampling rate** of 2000ms limits each path to 1 write per 2 seconds, keeping write volume modest on busy NMEA 2000 buses
- **Resource caps** of 768 MB RAM and 1.5 CPU cores (cgroup limits via signalk-container) keep QuestDB from squeezing co-resident containers like Grafana, mayara, or signalk-backup. The JVM auto-sizes its heap to a fraction of the memory cap, so total footprint (heap + off-heap) is bounded
- **QuestDB worker threads** reduced to 1 each (WAL, shared, ILP) to minimize CPU usage
- **ILP batching** at 500ms intervals with 1000-row batches reduces TCP overhead
- Per-path overrides allow faster rates for critical paths (e.g. `{ "environment.wind.*": 200 }`) while keeping slow-changing paths throttled
- Set the memory limit to empty or CPU limit to `0` to disable the cap entirely on roomier hosts

## History API Provider

QuestDB automatically registers as the **default** Signal K v2 History API provider. Any app or Grafana plugin that queries `/signalk/v2/api/history/` uses QuestDB.

## Data Storage

QuestDB data is stored at `~/.signalk/plugin-config-data/signalk-questdb/` on the host, mounted into the container at `/var/lib/questdb`. Data survives container restarts, image upgrades, and plugin disable/enable cycles.

## Grafana Integration

Connect Grafana to QuestDB via the PostgreSQL data source (user `admin`,
password `quest`, database `qdb`).

The companion [signalk-grafana](https://github.com/dirkwa/signalk-grafana)
plugin wires this up for you: it runs Grafana as a managed container and
reaches QuestDB by its container DNS name on the shared `sk-network`, so no
host port needs to be exposed.

For a **self-hosted Grafana on the host or in Podman**, point it at
`localhost:<HTTP/PostgreSQL port>` — but note this only works when **"Bind to
0.0.0.0"** is enabled (otherwise QuestDB is not published on a host port).

For Grafana in a **separate Docker instance**, enable **"Bind to 0.0.0.0"** and
use your machine's LAN IP (e.g. `192.168.0.122:8812`) as the host in Grafana.

**Warning:** Binding to 0.0.0.0 exposes QuestDB's ports to your entire network. Only enable this if necessary, and ensure your firewall is configured appropriately.

Example query:

```sql
SELECT ts AS time, avg(value) AS sog
FROM signalk
WHERE path = 'navigation.speedOverGround'
  AND context = 'self'
  AND ts BETWEEN $__timeFrom() AND $__timeTo()
SAMPLE BY $__interval
```

## Requirements

- Node.js >= 22
- [signalk-container](https://github.com/dirkwa/signalk-container) >= 1.14.0 plugin (for managed mode; older versions still work but fall back to loopback connectivity)
- Signal K server

## License

MIT
