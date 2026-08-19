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
- **InfluxDB migration** -- detect InfluxDB 1.x/2.x, browse its buckets and measurements, and import history into QuestDB with original timestamps
- **One-click updates** -- check for new QuestDB releases and update from the config panel
- **Console webapp** -- QuestDB's own SQL console embedded in the Signal K admin UI (admin only)
- **Config panel** -- status dashboard with row counts, version picker, update check, collapsible compression/migration/export sections
- **SQL injection protection** -- strict input validation on all query endpoints
- **Container lifecycle** -- container stops when plugin is disabled, starts on enable

## Config Panel

The plugin embeds a React config panel in the Signal K Admin UI showing:

- **QuestDB Status** -- running/not running indicator, total rows, active paths today
- **Update check** -- compares running version against latest GitHub release, one-click update
- **Image Version** -- dropdown with latest, pre-releases, and last 3 stable releases
- **Connection** -- managed container toggle, host/ports, PostgreSQL port for Grafana
- **Recording** -- record self, record AIS targets, startup restore, console webapp, retention days
- **Path filtering** (collapsible) -- exclude or include-only paths with glob patterns
- **Compression** (collapsible) -- LZ4/ZSTD codec selection for on-disk storage
- **InfluxDB Migration** (collapsible) -- detect or enter a URL, pick a bucket/database and time range, then run the import with live progress
- **Data Export** (collapsible) -- date range picker, Parquet/CSV format, download button
- **Danger zone** (collapsible) -- "Remove container & all data" to fully reset QuestDB (deletes data Signal K's plugin-uninstall can't, on rootless Podman)

## QuestDB Schema

Three tables, all with WAL mode, daily partitioning, and deduplication:

| Table              | Purpose        | Columns                                                                                                    |
| ------------------ | -------------- | ---------------------------------------------------------------------------------------------------------- |
| `signalk`          | Numeric values | `ts`, `path` (SYMBOL), `context` (SYMBOL), `source` (SYMBOL), `value` (DOUBLE)                             |
| `signalk_str`      | String values  | `ts`, `path` (SYMBOL), `context` (SYMBOL), `source` (SYMBOL), `value_str` (VARCHAR), `value_kind` (SYMBOL) |
| `signalk_position` | Positions      | `ts`, `context` (SYMBOL), `source` (SYMBOL), `lat` (DOUBLE), `lon` (DOUBLE)                                |

`ts` is the **server receive time**, not the timestamp a source claims. Marine
sources carry independent clocks, and storing their timestamps makes commits
land out of order — QuestDB then rewrites partition tails on every merge
(observed as >3000x write amplification). Receive time keeps ingestion
append-only; the millisecond difference is far below the sampling resolution,
and a device with a broken clock gets more accurate history, not less.

`source` is the delta's sourceRef — which receiver produced the row. Two GPS
units feeding the same server interleave in storage, and without the column a
track drawn from history zigzags between them. Rows recorded before the column
existed have `source` null; they replay unattributed and cannot be filtered.

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

Append `|<sourceRef>` to a path to read one source's rows only (server 2.29+,
[signalk-server#2737](https://github.com/SignalK/signalk-server/pull/2737)).
The same path may appear once per source, giving one column per receiver:

```
GET /signalk/v2/api/history/values?paths=navigation.position|gps.main,navigation.position|gps.backup&duration=PT1H
```

Without a sourceRef a path returns all sources mixed, as before.

### v1 (WebSocket playback)

Registered via `app.registerHistoryProvider()`. Supports playback at configurable speed multipliers using chunked reads from QuestDB. Replayed updates carry the recorded sourceRef as `$source`, one update per source, so consumers see the same attribution the live stream had.

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
| GET    | `/migration/detect`                      | Detect InfluxDB (supports `?url=` for remote)                                            |
| POST   | `/migration/buckets`                     | List buckets (2.x) or databases (1.x); credentials in the body, never the query string   |
| POST   | `/migration/measurements`                | List measurements and their field keys in a bucket/database                              |
| POST   | `/migration/start`                       | Start an import; returns immediately, progress via `/migration/status`                   |
| GET    | `/migration/status`                      | Progress and state of the current/last import                                            |
| POST   | `/migration/cancel`                      | Cancel the running import                                                                |
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

## Migrating from InfluxDB

The **InfluxDB Migration** section of the config panel copies history out of an
existing InfluxDB into QuestDB. It supports both InfluxDB 1.x (InfluxQL) and
2.x (Flux).

1. **Detect** finds an InfluxDB on `localhost:8086`, or enter a URL for a
   remote one (loopback and private-network addresses only).
2. Select the detected instance, supply credentials — an API token and
   organisation for 2.x, or username/password for 1.x if authentication is
   enabled — and list its buckets/databases. Credentials are used for the
   import only, are never written to the plugin's settings, and travel in the
   request body rather than the query string (Signal K logs full request URLs).
3. Pick a bucket/database and a time range, then **Start import**. Progress is
   polled while it runs and the import can be cancelled at any point.

How the data maps:

- A measurement with the conventional `value` field becomes the Signal K path
  of the same name; a measurement with several named fields becomes
  `measurement.field` paths, so two fields cannot overwrite each other.
- Numbers go to `signalk`, strings and booleans to `signalk_str` (booleans
  tagged `value_kind=boolean`), and `latitude`/`longitude` field pairs are
  recombined into `signalk_position`.
- Rows keep their **original nanosecond timestamps**, so imported history sorts
  and aggregates alongside live data.
- Every imported row is tagged `source=influxdb-import`, which makes it
  distinguishable from live recording — and because the tables deduplicate on
  `(ts, path, context, source)`, **re-running the same range overwrites rather
  than duplicating**. An interrupted import can simply be run again.

Anything that cannot be mapped (a gap, an unsupported value type, a latitude
with no matching longitude) is counted in the run's `skipped` total rather than
being dropped silently.

## Configuration

| Setting                    | Default      | Description                                                                                           |
| -------------------------- | ------------ | ----------------------------------------------------------------------------------------------------- |
| QuestDB version            | `latest`     | Docker image tag (dropdown shows stable + pre-releases)                                               |
| Managed container          | `true`       | Let signalk-container manage QuestDB, or connect to external                                          |
| QuestDB host               | `127.0.0.1`  | External QuestDB host (only used when managed=false)                                                  |
| HTTP port                  | `9000`       | External mode, or the host binding when "Bind to 0.0.0.0" is on                                       |
| ILP port                   | `9009`       | External mode, or the host binding when "Bind to 0.0.0.0" is on                                       |
| PostgreSQL port            | `8812`       | Host binding for Grafana/psql when "Bind to 0.0.0.0" is on                                            |
| Sampling rate (ms)         | `2000`       | Default min ms between writes per path (0 = every update)                                             |
| Write batch interval (ms)  | `5000`       | How often buffered samples are committed — one WAL transaction per table per commit (see Performance) |
| Memory limit               | `768m`       | Hard cgroup cap on QuestDB container RAM (empty = unlimited)                                          |
| CPU limit (cores)          | `1.5`        | Max CPU cores QuestDB can use (0 = unlimited)                                                         |
| Record own vessel          | `true`       | Record self context                                                                                   |
| Record AIS targets         | `true`       | Record other vessels                                                                                  |
| Restore vessels on startup | `false`      | Replay each vessel's last recorded position after a restart (see Startup restore)                     |
| Restore max age (minutes)  | `9`          | Only replay values recorded within this window                                                        |
| QuestDB console webapp     | `true`       | Serve QuestDB's console in the Signal K admin UI, admin only (see Console webapp)                     |
| Retention (days)           | `0`          | Auto-delete old partitions (0 = keep forever)                                                         |
| Path filter mode           | `exclude`    | `exclude` matching paths, or `include` only matching paths                                            |
| Path filter paths          | _(empty)_    | Glob patterns, one per line (e.g. `notifications.*`); empty = record everything, which is the default |
| Compression codec          | `lz4`        | On-disk WAL compression: `none`, `lz4`, or `zstd`                                                     |
| Compression level          | `3`          | ZSTD level 1-22 (only when codec is zstd)                                                             |
| Container network          | `sk-network` | Shared network for QuestDB (only applied when binding to 0.0.0.0)                                     |
| Bind to 0.0.0.0            | `false`      | Expose QuestDB's ports on the LAN (see Connectivity below)                                            |

## Console webapp

QuestDB ships its own web console — a full SQL workbench with schema browsing,
query history and CSV import. It normally lives on QuestDB's HTTP port (9000),
which is bound to loopback unless you turn on **Bind to 0.0.0.0**, so from any
other device on the boat it is effectively unreachable.

With **QuestDB console webapp** enabled (the default) the plugin appears in the
Signal K webapp list and serves that console inside the admin UI, with Signal
K's own navigation panel still in place.

**It is admin only.** The console is proxied at
`/plugins/signalk-questdb/console/`, and Signal K gives plugin routes an
admin-only default — the plugin deliberately does not relax that. A non-admin
who opens the webapp sees the page frame and an authorization error, never the
console.

**It can modify data.** Unlike the read-only `/query` endpoint, which rejects
anything that is not a `SELECT`, the console is a real SQL client: it can drop
tables and delete recorded history. That is the point of having it, and it is
what an administrator would use it for — but it is worth knowing before handing
someone an admin account.

If you would rather not serve it at all, switch the option off and the route is
not registered.

> Note this is _safer_ than the alternative it replaces. Reaching the console
> from another device previously meant enabling **Bind to 0.0.0.0**, which
> publishes an unauthenticated SQL endpoint on your network. The webapp route
> requires a Signal K admin session.

## Startup restore

Signal K's data model lives in memory only. After a server restart every vessel
is gone until it transmits again — for AIS that means roughly 30 seconds for a
Class B target, up to 3 minutes for a Class A at anchor, and about 6 minutes
before names arrive. The chart fills in gradually instead of showing the traffic
that was there a moment ago.

With **Restore vessels on startup** enabled, the plugin replays each vessel's
last recorded position (plus course, speed, heading and identity — name,
dimensions and AIS ship type) from QuestDB as soon as it connects, so the chart
is populated immediately, with each target drawn in its ship-type colour rather
than the default.

**A restored position is where a vessel _was_, not where it is now.** Nothing is
dead-reckoned forward. The plugin presents these values as the history they are
rather than passing them off as live: each delta carries its original recorded
timestamp, so a chart plotter ages it out under the same staleness rules it
applies to any other target, and the deltas are tagged
`$source: signalk-questdb.restore`.

Values outside the **Restore max age** window are not replayed at all. The
9-minute default matches Freeboard's AIS expiry, so a restored target is one that
would still have been on the chart had the server never stopped. Raising it puts
progressively staler positions in front of you — treat the window as a
collision-avoidance setting, not a convenience one.

Only navigation and identity paths are replayed, so stale tank levels and engine
temperatures are not resurrected as though they were current readings. A vessel
with no position in the window is skipped, as is one that has already transmitted
since startup. Restore honours the recording toggles: with **Record AIS targets**
off, no AIS target is restored.

The config panel reports the outcome once startup finishes: **Vessels Restored** with the count, or **Restore failed** if the replay could not run. Neither appears when the option is off.

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

## Durability on power loss

The managed container runs QuestDB with `cairo.commit.mode=sync`.

QuestDB's default is `nosync`: nothing on the ingest path is ever fsynced, and
durability is left to the OS page cache. On a power cut an arbitrary subset of
recent writes survives — including the case where a commit record reaches disk
while the row data it describes does not. QuestDB then fails while _opening_
that table's partition, which suspends the table permanently: neither a
lossless `RESUME WAL` nor a bounded `RESUME WAL FROM TXN` skip can get past it,
because the failure happens before any transaction is read. Repair means
hand-patching the commit record.

With `sync`, partition columns are fsynced before the commit record is written
and WAL segments are fsynced on commit, so the stored state can never claim
data that isn't durable. The cost is a few fsyncs per commit interval — writes
are already batched (see `ilpFlushIntervalMs`), so at boat data rates it is
negligible against losing a table.

**Running an external QuestDB?** Set this yourself in `server.conf`:

```
cairo.commit.mode=sync
```

## QuestDB Web Console

QuestDB ships a web console (SQL editor + import UI) on its HTTP port. See the
**[Web Console user guide](doc/web-console.md)** for an end-user walkthrough
with ready-to-paste sample queries (speed in knots, temperatures in °C,
distance per day, track export, and more). On the Signal K host it is at:

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

### The "Small transactions — consider batching" alert

The console's Monitoring view flags any table whose 90th-percentile WAL
transaction stays under QuestDB's recommended batch size (100 rows).
`signalk_position` triggers this **structurally**: the plugin commits its
write buffer every "Write batch interval", each commit is one WAL transaction
per table, and the position table holds exactly one path — so its share of
every commit is just the handful of GPS fixes since the last one. The other
tables spread hundreds of paths across each transaction and are rarely
flagged.

At vessel data rates this alert is cosmetic. The numbers that actually matter
are on the same page: **Write Amplification** near 1x, **Pending Rows** 0, and
**Transaction Lag** 0 mean the WAL apply is perfectly healthy — QuestDB merges
many small transactions per apply cycle anyway.

If you want bigger transactions regardless, raise **"Write batch interval"**
in the plugin config. Clearing the alert for the position table needs roughly
100 fixes per commit — a batch window in the minutes, not seconds (the exact
numbers depend on your sampling rate; the config panel states the current
bounds). The trade-off is honest: up to one batch interval of buffered data is
lost on a hard crash (a clean Signal K shutdown flushes first), and live-ish
history queries see new data that much later.

## Performance (Pi / Low-Power Devices)

The plugin is optimized for Raspberry Pi and similar low-power devices:

- **Default sampling rate** of 2000ms limits each path to 1 write per 2 seconds, keeping write volume modest on busy NMEA 2000 buses
- **Resource caps** of 768 MB RAM and 1.5 CPU cores (cgroup limits via signalk-container) keep QuestDB from squeezing co-resident containers like Grafana, mayara, or signalk-backup. The JVM auto-sizes its heap to a fraction of the memory cap, so total footprint (heap + off-heap) is bounded
- **QuestDB worker threads** reduced to 1 each (WAL, shared, ILP) to minimize CPU usage
- **ILP batching** commits every 5s (configurable via "Write batch interval"; a large backlog of buffered rows commits early). Each commit is one WAL transaction per table, and with deduplicated tables the apply cost of a transaction grows with partition size — frequent tiny commits eventually outpace what a Pi can apply and recording stalls. Bigger batches keep the WAL healthy; the cost is that at most one interval of buffered samples is lost on a hard crash
- Per-path overrides allow faster rates for critical paths (e.g. `{ "environment.wind.*": 200 }`) while keeping slow-changing paths throttled
- Set the memory limit to empty or CPU limit to `0` to disable the cap entirely on roomier hosts

## Troubleshooting

### 'Module "signalk-questdb" is not available' right after install or update

Right after installing or updating the plugin, the admin UI can show
`Module "signalk-questdb" is not available. Make sure the webapp is installed.`
It is installed — the browser tab is still running the admin UI it loaded
before the update, which looks for the panel bundle it knew then. A hard
refresh of the tab (Shift-Reload) fixes it.

### "QuestDB keeps dropping the write connection — the container may be unhealthy or out of memory"

This status appears when the plugin's ILP writer connects to QuestDB, gets
dropped within a few seconds, and retries — repeatedly. It is QuestDB itself
being unhealthy (typically OOM-killed and restart-looping), not a problem with
the plugin or with a single table. The most common cause on a Raspberry Pi or
other low-RAM host is that QuestDB is hitting a memory ceiling. There are two
variants, and the cgroup check below tells them apart.

**1. Is the cgroup `memory` controller delegated?**

```bash
cat /sys/fs/cgroup/cgroup.controllers
#   ...memory...   present  -> memory delegation is available, so the cap can be enforced (see B)
#   memory ABSENT           -> the cap is silently dropped (see A)
```

When Signal K's containers run under rootless Podman, the kernel only enforces
a resource limit whose cgroup controller has been delegated to the user
session. Many distributions delegate `cpu`, `cpuset`, `io`, and `pids` by
default but **not** `memory`. signalk-container drops a limit whose controller
is missing rather than failing the container, so a configured cap can silently
have no effect.

**A. `memory` not delegated — the cap is silently dropped.** QuestDB grows
without bound and the host kernel's OOM killer eventually kills it under
whole-system memory pressure; it restarts and the cycle repeats. Enable memory
delegation on the host (one-time, needs sudo), then **recreate** the QuestDB
container (a restart is not enough — the limit is set at create time):

```bash
sudo mkdir -p /etc/systemd/system/user@.service.d
sudo tee /etc/systemd/system/user@.service.d/delegate.conf >/dev/null <<'EOF'
[Service]
Delegate=cpu cpuset io memory pids
EOF
sudo systemctl daemon-reload
```

`daemon-reload` reloads the drop-in file but does not re-apply `Delegate=` to
the already-running user manager, so the new delegation only takes effect after
the **user session restarts**. On a headless box a reboot is simplest (and is
required anyway if you edit `cmdline.txt` below); otherwise restart the user
manager with `sudo systemctl restart user@$(id -u).service` (this stops all of
that user's containers, so let them come back before recreating QuestDB).

```bash
# On older Raspberry Pi kernels the memory controller is off at boot; add to
# /boot/cmdline.txt (one line) and reboot:
#   cgroup_enable=memory cgroup_memory=1
```

After the session restart, recreate the QuestDB container so the cap applies.

**B. `memory` is delegated — the 768 MB cap is enforced but too tight.** As the
database grows, QuestDB's peak memory (JVM heap plus off-heap memory-mapped
files, which spike during out-of-order merges) exceeds 768 MB and the cgroup
OOM-kills it. Raise **Memory limit** in the plugin config (e.g. `1g` or `1.5g`,
or empty to remove the cap on a roomier host), which recreates the container.

**Confirm the diagnosis:**

```bash
podman inspect sk-signalk-questdb \
  --format 'OOMKilled={{.State.OOMKilled}} RestartCount={{.RestartCount}} cap={{.HostConfig.Memory}}'
#   cap=805306368  -> 768 MB cap is in place (variant B)
#   cap=0          -> no cap applied (variant A, or cap intentionally removed)
podman events --since 2h --stream=false --filter container=sk-signalk-questdb \
  --filter event=oom --filter event=died
```

### History queries slow or timing out / "out-of-memory" errors with free RAM

QuestDB memory-maps every partition column file and every pending WAL segment
it touches. The Linux kernel caps how many memory mappings one process may
hold (`vm.max_map_count`), and the stock value on Debian, Ubuntu ≤ 22.04, and
RHEL (65530) is far below what QuestDB recommends (1048576). A fresh database
fits easily; months of daily partitions across the three tables — plus a
segment backlog if a table's WAL is suspended — can exhaust the limit. When
that happens `mmap` fails with out-of-memory errors (errno 12) **even though
plenty of RAM is free**: queries error out or crawl, and the WAL apply job can
suspend a table.

The QuestDB Web Console shows the same warning
(`vm.max_map_count limit is too low`), the plugin's config panel shows a
banner, and the server log gets a warning at plugin startup. Fix it in a
shell **on the host machine itself** — not inside the QuestDB container via
`podman exec`: the limit is kernel-global, a container cannot change it, and
the QuestDB image has neither `sudo` nor `/etc/sysctl.d`. Being kernel-global
also means this works no matter how Signal K itself is deployed:

```bash
echo 'vm.max_map_count=1048576' | sudo tee /etc/sysctl.d/99-questdb.conf
sudo sysctl --system
```

This takes effect immediately — no container or host restart needed. See
[QuestDB capacity planning](https://questdb.com/docs/getting-started/capacity-planning/#max-virtual-memory-areas-limit)
for background.

### QuestDB container never starts on macOS (podman machine)

On macOS the QuestDB container is created but may never start. What you
see depends on the signalk-container version: with signalk-container 1.26
or later, the manager names the problem outright (an open-files limit the
host refuses) and starts the container on the runtime's default limits
instead, with this plugin's config panel showing a **"request rejected by
the host"** banner — the remediation below applies unchanged. Older
versions never start the container at all and show either
a misleading **"Permission denied"** (the runtime's error text contains
"operation not permitted") or only a generic **"Unexpected error. See logs
for details."** — and the container log view stays empty, because a
container that never started has no logs.

The cause is the plugin's open-files request. On macOS, Signal K runs on the
Mac while podman runs inside a Fedora CoreOS VM ("podman machine"). The
plugin asks for 1048576 open files, and signalk-container normally clamps
that request to what the host can grant — but from macOS it cannot read the
VM's limits, so the full request reaches the VM, exceeds its default hard
limit (524288), and the OCI runtime refuses to start the container. On
current Fedora CoreOS the VM's `vm.max_map_count` already meets QuestDB's
1048576, so usually only the file-descriptor limit needs raising — but an
older machine image may sit lower, so check it first:

```bash
podman machine ssh -- sysctl -n vm.max_map_count
```

If that prints less than 1048576, raise it inside the VM before relying on
the file-descriptor fix alone (`podman machine ssh`, then
`sudo tee /etc/sysctl.d/99-signalk-questdb.conf <<< 'vm.max_map_count=1048576'`
and `sudo sysctl --system`).

Raise it inside the VM. From a macOS terminal:

```bash
podman machine ssh
```

Inside the VM, add a systemd drop-in for every user manager (the
`user@.service` template covers each user instance, including the machine's
`core` user, whose session runs rootless containers) and the same drop-in
for `podman.service` to cover the rootful connection:

```bash
sudo mkdir -p /etc/systemd/system/user@.service.d /etc/systemd/system/podman.service.d
sudo tee /etc/systemd/system/user@.service.d/nofile.conf >/dev/null <<'EOF'
[Service]
LimitNOFILE=1048576
EOF
sudo cp /etc/systemd/system/user@.service.d/nofile.conf /etc/systemd/system/podman.service.d/nofile.conf
sudo systemctl daemon-reload
exit
```

Back on macOS, restart the VM:

```bash
podman machine stop && podman machine start
```

and verify the new limit is grantable:

```bash
podman run --rm --ulimit nofile=1048576:1048576 docker.io/library/alpine sh -c 'ulimit -n -H'
```

This should print `1048576`. Finally, remove the half-created container so it
is recreated with the full limit — the limit is set at create time, so a
restart is not enough:

```bash
podman rm -f sk-signalk-questdb
```

then restart Signal K — on the next plugin start the container is recreated
with the full limit. (The container manager's **Start** button only starts an
existing container, so it cannot replace this step.)

The verify and `rm` commands run on podman's default connection — normally
the rootless one the plugin uses. Rootless and rootful connections keep
separate container stores, so on a setup with a non-default connection add
the same `--connection <name>` (list them with
`podman system connection list`) to both commands, so they hit the store
where `sk-signalk-questdb` actually lives (`podman ps -a` shows it).

## History API Provider

QuestDB registers as a Signal K v2 History API provider. Which registered
provider answers `/signalk/v2/api/history/` by default is the operator's
choice: pick it once under **Data → Preferences → Default History Provider**
(server ≥ 2.31); the server persists it as `historyApi.defaultProvider` in
`settings.json`, and it survives restarts and plugin load order. If no default
is configured, the server uses whichever provider registers first.

Versions 2.0.0 and earlier asked the server to make QuestDB the default on
every start. That is gone as of 2.0.1 — it could silently override a default
you had chosen, and on servers with security enabled it never worked at all.

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
- Podman >= 5.4 (the version Debian 13 "trixie" ships) or Docker, for managed mode. On rootless Podman below 5.5 the plugin's open-files request is inherited from the podman service rather than granted per container ([containers/podman#25881](https://github.com/containers/podman/issues/25881)); signalk-container accounts for this.
- Signal K server (≥ 2.31 to choose the default history provider in the admin UI)

## License

signalk-questdb 2.0.0 and later is **source available, not open source**.
See [LICENSE.md](LICENSE.md).

**You may**, free of charge: run it on your own boat or fleet, private or
commercial; use it for internal company operations; modify it for your own use;
use it in education and research; and provide professional services around it.

**You may not**: redistribute it, or publish a modified version of it to npm or
anywhere else. Verbatim copies of official releases may be mirrored and cached.

Versions 1.9.2 and earlier remain available under the MIT license, see
[LICENSE-MIT-through-v1.x.txt](LICENSE-MIT-through-v1.x.txt).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
