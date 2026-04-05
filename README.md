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
- **Compression** (collapsible) -- LZ4/ZSTD codec selection for on-disk storage
- **InfluxDB Migration** (collapsible) -- auto-detect with manual URL for remote instances
- **Data Export** (collapsible) -- date range picker, Parquet/CSV format, download button

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

| Method | Path                                     | Description                                        |
| ------ | ---------------------------------------- | -------------------------------------------------- |
| GET    | `/status`                                | QuestDB health, row counts, active paths           |
| GET    | `/query?sql=...`                         | Read-only SQL proxy (DDL/DML blocked)              |
| GET    | `/paths`                                 | All recorded paths with row counts and time range  |
| GET    | `/versions`                              | QuestDB releases from GitHub (for version picker)  |
| GET    | `/update/check`                          | Compare running version against latest release     |
| POST   | `/update/apply`                          | Pull latest image, recreate container, reconnect   |
| GET    | `/migration/detect`                      | Auto-detect InfluxDB (supports `?url=` for remote) |
| GET    | `/export?from=...&to=...&format=parquet` | Parquet or CSV export                              |

## Configuration

| Setting            | Default     | Description                                                   |
| ------------------ | ----------- | ------------------------------------------------------------- |
| QuestDB version    | `latest`    | Docker image tag (dropdown shows stable + pre-releases)       |
| Managed container  | `true`      | Let signalk-container manage QuestDB, or connect to external  |
| QuestDB host       | `127.0.0.1` | Host (only used when managed=false)                           |
| HTTP port          | `9000`      | QuestDB REST API port                                         |
| ILP port           | `9009`      | InfluxDB Line Protocol write port                             |
| PostgreSQL port    | `8812`      | For Grafana connections                                       |
| Record own vessel  | `true`      | Record self context                                           |
| Record AIS targets | `false`     | Record other vessels                                          |
| Retention (days)   | `0`         | Auto-delete old partitions (0 = keep forever)                 |
| Compression codec  | `lz4`       | On-disk WAL compression: `none`, `lz4`, or `zstd`             |
| Compression level  | `3`         | ZSTD level 1-22 (only when codec is zstd)                     |
| Bind to 0.0.0.0    | `false`     | Bind ports to all interfaces instead of localhost (see below) |

## Data Storage

QuestDB data is stored at `~/.signalk/plugin-config-data/signalk-questdb/` on the host, mounted into the container at `/var/lib/questdb`. Data survives container restarts, image upgrades, and plugin disable/enable cycles.

## Grafana Integration

Connect Grafana to QuestDB via the PostgreSQL data source:

- Host: `localhost:8812`
- User: `admin`
- Password: `quest`
- Database: `qdb`

If Grafana runs on the host or in Podman, `localhost:8812` works out of the box.

If Grafana runs in a **separate Docker** container, it cannot reach the host's localhost. In that case, enable **"Bind to 0.0.0.0"** in the QuestDB plugin config and use your machine's LAN IP (e.g. `192.168.0.122:8812`) as the host in Grafana.

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
- [signalk-container](https://github.com/dirkwa/signalk-container) plugin (for managed mode)
- Signal K server

## License

MIT
