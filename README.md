# signalk-questdb

QuestDB history provider for Signal K — a drop-in replacement for
signalk-to-influxdb and signalk-to-influxdb2.

Records every vessel's data in [QuestDB](https://questdb.com), running as a
container managed for you by
[signalk-container](https://github.com/dirkwa/signalk-container), and serves it
back through Signal K's v2 History API and the legacy v1 playback API. Existing
InfluxDB history can be imported.

## Features

- **Managed QuestDB** — the container is pulled, created, updated and
  resource-capped for you; or point the plugin at an external QuestDB
- **v2 History API** — `getValues`, `getPaths`, `getContexts` with every
  aggregate method, per-source reads (`path|sourceRef`) and per-source splitting
  (`sourcePolicy=all`)
- **v1 playback API** — WebSocket replay at configurable speed
- **Recording controls** — per-path sampling rates, include/exclude filters with
  globs, own vessel and AIS targets separately, retention by days, LZ4/ZSTD
  on-disk compression
- **Startup restore** — AIS targets back on the chart right after a server
  restart, in their ship-type colour
- **WAL watchdog** — a table QuestDB has silently stopped applying is detected,
  alerted, and resumed
- **InfluxDB migration** — detect InfluxDB 1.x/2.x, pick a bucket, a vessel and
  a range, import with original timestamps; resumable
- **Export** — Parquet or CSV by date range, and a per-table full export for
  backup tooling
- **Console webapp** — QuestDB's own SQL console inside the Signal K admin UI,
  admin only
- **Config panel** — status, row counts, one-click QuestDB updates, and every
  setting in one place

## Getting started

### Requirements

- Node.js 22 or newer
- Signal K server — 2.31 or newer to choose the default history provider in the
  admin UI
- For managed mode: the
  [signalk-container](https://github.com/dirkwa/signalk-container) plugin, and
  Podman 5.4 or newer (what Debian 13 "trixie" ships) or Docker.
  signalk-container 1.29.0 or newer is recommended; the plugin runs with 1.14.0
  or newer, but older versions lose parts of it — below 1.19.0 the Danger zone
  cannot delete data kept on a named volume, below 1.26 the macOS open-files
  problem shows as a generic error, below 1.29.0 a container the runtime refuses
  to recreate is not reported (see Troubleshooting). On rootless Podman below
  5.5 the plugin's open-files request is inherited from the podman service
  rather than granted per container
  ([containers/podman#25881](https://github.com/containers/podman/issues/25881));
  signalk-container accounts for this.

### Install

1. Install **signalk-container** from the App Store and enable it (skip this for
   an external QuestDB).
2. Install **signalk-questdb** and enable it. On the first start the plugin
   pulls `questdb/questdb` at the tag chosen under **QuestDB image version**
   (`latest` by default), creates the container — named `signalk-questdb` behind
   the runtime's prefix, `sk-` unless configured otherwise — with its ports
   bound to loopback, and starts recording. The data lives under Signal K's
   configuration directory, in `plugin-config-data/signalk-questdb` (see Data
   storage).
3. Open the plugin's config panel: the status card shows **Running** and
   **Recording**, the total row count and the paths active today.

For an external QuestDB, switch **Manage QuestDB container** off and enter its
host and ports.

### Make it the history provider

Which registered provider answers `/signalk/v2/api/history/` is the operator's
choice: pick it once under **Data → Preferences → Default History Provider**
(server 2.31 or newer); the server persists it as `historyApi.defaultProvider`
in `settings.json`, and it survives restarts and plugin load order. If no
default is configured, the server uses whichever provider registers first.
Versions 2.0.0 and earlier asked the server to make QuestDB the default on every
start; that is gone as of 2.0.1 — it could silently override a default you had
chosen, and on servers with security enabled it never worked at all.

## Configuration

The config panel groups the settings as: the **status card** (running state,
rows, active paths, restore outcome, WAL state), the **update check** and
**image version** picker (latest, pre-releases and the last three stable
releases), **Connection**, **Recording**, **History**, and the collapsible
**Path filtering**, **Compression (on-disk)**, **InfluxDB Migration**, **Data
Export** and **Danger zone** sections. The Danger zone removes the container and
all of its data — including what Signal K's plugin uninstall cannot reach on
rootless Podman.

| Setting                      | Default      | Description                                                                                                                                                                     |
| ---------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| QuestDB image version        | `latest`     | Image tag; the dropdown lists the latest release, pre-releases, and the last three stable releases                                                                              |
| Manage QuestDB container     | `true`       | Let signalk-container run QuestDB; off = connect to an external QuestDB                                                                                                         |
| QuestDB host                 | `127.0.0.1`  | External mode only; in managed mode the address is resolved automatically                                                                                                       |
| HTTP port                    | `9000`       | External mode, or the host binding when "Bind to 0.0.0.0" is on; otherwise signalk-container allocates it                                                                       |
| ILP port                     | `9009`       | External mode, or the host binding when "Bind to 0.0.0.0" is on; otherwise signalk-container allocates it                                                                       |
| PostgreSQL port              | `8812`       | Host binding for Grafana/psql when "Bind to 0.0.0.0" is on                                                                                                                      |
| QuestDB memory limit         | `768m`       | Hard cgroup cap on the container (empty = unlimited); the JVM sizes its heap to a fraction of it, so heap plus off-heap stays inside                                            |
| QuestDB CPU limit (cores)    | `1.5`        | Fractional cores (0 = unlimited)                                                                                                                                                |
| Default sampling rate (ms)   | `2000`       | Minimum ms between writes for any one path (0 = every update)                                                                                                                   |
| Per-path sampling rates (ms) | _(empty)_    | Glob → ms overrides, e.g. `{ "environment.wind.*": 200, "tanks.*": 10000 }`                                                                                                     |
| Write batch interval (ms)    | `5000`       | How often buffered samples are committed — one WAL transaction per table per commit (see Performance)                                                                           |
| Record own vessel            | `true`       | Record the `self` context                                                                                                                                                       |
| Record AIS targets           | `true`       | Record other vessels                                                                                                                                                            |
| Restore vessels on startup   | `false`      | Replay each vessel's last recorded position after a restart (see Startup restore)                                                                                               |
| Restore max age (minutes)    | `9`          | Only replay values recorded within this window                                                                                                                                  |
| Allow `sourcePolicy=all`     | `false`      | Let History API callers split a path into one column per recording source (see History API)                                                                                     |
| QuestDB console webapp       | `true`       | Serve QuestDB's console inside the Signal K admin UI, admin only (see Console webapp)                                                                                           |
| Retention (days)             | `0`          | Drop daily partitions older than this (0 = keep forever)                                                                                                                        |
| Filter mode                  | `exclude`    | `exclude` the matching paths, or `include` only the matching paths                                                                                                              |
| Path patterns                | _(empty)_    | Glob patterns, one per line, e.g. `environment.wind.*`; empty records everything                                                                                                |
| Compression codec            | `lz4`        | On-disk WAL segments and Parquet exports: `none`, `lz4` (fast) or `zstd` (smaller)                                                                                              |
| Compression level            | `3`          | ZSTD level 1–22, used only with `zstd`                                                                                                                                          |
| Container network            | `sk-network` | Shared Podman/Docker network QuestDB joins in managed mode, so the companion signalk-grafana reaches it by container DNS; host-port publication is separate ("Bind to 0.0.0.0") |
| Bind to 0.0.0.0              | `false`      | Publish QuestDB's ports on all interfaces (see Connectivity)                                                                                                                    |

## Recording

### Schema

Three tables, all with WAL mode, daily partitioning, and deduplication:

| Table              | Purpose        | Columns                                                                                                    |
| ------------------ | -------------- | ---------------------------------------------------------------------------------------------------------- |
| `signalk`          | Numeric values | `ts`, `path` (SYMBOL), `context` (SYMBOL), `source` (SYMBOL), `value` (DOUBLE)                             |
| `signalk_str`      | String values  | `ts`, `path` (SYMBOL), `context` (SYMBOL), `source` (SYMBOL), `value_str` (VARCHAR), `value_kind` (SYMBOL) |
| `signalk_position` | Positions      | `ts`, `context` (SYMBOL), `source` (SYMBOL), `lat` (DOUBLE), `lon` (DOUBLE)                                |

The own vessel is stored as context `self`, whatever its identity; other vessels
keep their `vessels.urn:…` context. The history API translates `self` back to
`vessels.self`.

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

### What is recorded

Every value on the stream is routed by its type: numbers go to `signalk`,
strings and booleans to `signalk_str` (booleans tagged `value_kind=boolean`),
and `navigation.position` — that path only — to `signalk_position`. A vessel's
name, which arrives as an empty-path `{name}` delta, is stored in `signalk_str`
under the path `name` with `value_kind=identity`, and replayed in its original
shape. An object value is recorded as its scalar leaves, one level deep:
`navigation.attitude {roll, pitch, yaw}` becomes `navigation.attitude.roll` and
so on, and another lat/lon object such as `navigation.anchor.position` becomes
`.latitude` and `.longitude` leaves rather than rows in the track table. Nested
objects — notifications, resource documents — are not descended into; a value no
table can hold is skipped and reported in the status endpoint and the panel
rather than dropped silently.

**Sampling.** Each path is written at most once per **Default sampling rate**
(2000 ms), with per-path overrides by glob for the few paths that need finer
resolution and the many that need less. **Record own vessel** and **Record AIS
targets** switch the two kinds of context independently, and **Path patterns**
exclude (or include only) paths by glob.

**Batching.** Samples are buffered and committed every **Write batch interval**
(5 s), one WAL transaction per table per commit. The buffer holds 100 000 lines;
if QuestDB is unreachable for longer than that covers, the oldest lines are
dropped and counted. A clean Signal K shutdown flushes first; a hard crash loses
at most one interval.

**Retention** drops whole daily partitions once they are older than the
configured days; 0 keeps everything. **Compression** applies LZ4 or ZSTD to the
WAL segments on disk (and to Parquet exports).

## History API

### v2 (REST — `/signalk/v2/api/history/`)

Registered via `app.registerHistoryApiProvider()`. Supports all aggregate
methods:

| Method         | QuestDB mapping                                                                 |
| -------------- | ------------------------------------------------------------------------------- |
| `average`      | `avg(value)`                                                                    |
| `min`          | `min(value)`                                                                    |
| `max`          | `max(value)`                                                                    |
| `first`        | `first(value)`                                                                  |
| `last`         | `last(value)`                                                                   |
| `mid`          | `(min + max) / 2`                                                               |
| `middle_index` | the middle row of each bucket, by time — a recorded value, not a computed one   |
| `sma`          | the `average` bucket, then a moving window over N buckets (`sma:N`, default 5)  |
| `ema`          | the `average` bucket, then an exponential moving average (`ema:α`, default 0.2) |

A sample for `sma` and `ema` is one row of the series the window runs over: a
resolution bucket (its average) when the request names a resolution, a raw row
otherwise. So `sma:5` at `resolution=180` is a 15-minute moving average, on the
same grid as every other column in the response. An empty bucket keeps its place
in the window: the average there is of what the window still holds, and a value
leaves it N buckets after it arrived. `ema` carries its last value across an
empty bucket instead.

`navigation.position` answers `first`, `last` and `middle_index` — each a point
the vessel was at. Any other method runs `first`, and the column's `method` says
so. A text path (rows in `signalk_str`) answers `first` and `last`;
`middle_index` and the moving averages read the numeric table only.

Query example:

```http
GET /signalk/v2/api/history/values?paths=navigation.speedOverGround&duration=PT1H&resolution=60
```

A `navigation.position` value is a `[longitude, latitude]` pair — GeoJSON order,
as the History API defines it and as signalk-to-influxdb2 and signalk-parquet
return it — not the `{latitude, longitude}` object of the data model; a bucket
without a fix is `null`. The v1 playback API is unaffected: it replays deltas,
which carry the object.

**Limits.** A request may produce at most 1 000 000 sample buckets across all of
its columns (range ÷ resolution × columns); a finer one is rejected with an
error that says so — use a coarser resolution or a shorter range. Without a
resolution a column is its raw rows, capped at 10 000 (50 000 for
`middle_index`).

#### Reading one source

Append `|<sourceRef>` to a path to read one source's rows only (server 2.29+,
[signalk-server#2737](https://github.com/SignalK/signalk-server/pull/2737)). The
same path may appear once per source, giving one column per receiver:

```http
GET /signalk/v2/api/history/values?paths=navigation.position|gps.main,navigation.position|gps.backup&duration=PT1H
```

Without a sourceRef a path returns all sources mixed. A source-bearing column
reports its source as `$source` in its `values` entry — the key
[signalk-server#2817](https://github.com/SignalK/signalk-server/pull/2817)
settled on, and what v1 playback has always emitted — while the request side
keeps the name `sourceRef`.

#### sourcePolicy=all

`sourcePolicy=all`
([signalk-server#2817](https://github.com/SignalK/signalk-server/pull/2817))
asks for every source separated without naming them up front. Each path that
does not already specify a source is expanded into one column per source that
actually recorded it in range, with the source in the `$source` field of the
column's `values` entry:

```http
GET /signalk/v2/api/history/values?paths=navigation.speedOverGround&duration=PT1H&sourcePolicy=all
```

```jsonc
"values": [
  { "path": "navigation.speedOverGround", "method": "average", "$source": "gps.aux"  },
  { "path": "navigation.speedOverGround", "method": "average", "$source": "gps.main" },
  { "path": "navigation.speedOverGround", "method": "average" }  // unattributed rows
]
```

Named sources come first, sorted, so column order is stable between requests.
Rows whose source is unset — recorded before the `source` column existed, or
from a delta that carried none — form their own trailing column with no
`$source` claim.

**Off by default**, behind the plugin setting that allows the policy. Expansion
multiplies the work one request can ask for: a path recorded by four receivers
becomes four queries and four columns, which on a Pi-class host turns a cheap
request into an expensive one. The sample-bucket cap counts expanded columns, so
a resolution that is fine unexpanded may be rejected once expanded.

Two ceilings bound the fan-out, and a request past either is rejected rather
than truncated — returning some of the asked-for series without saying so would
be worse than refusing a request that is too broad. A single path may expand
into at most 16 columns: recorded by more sources in range, it fails the
request, and the error names the count, the ceiling and the way out (name the
sources with `path|sourceRef`, or ask for a shorter range). A whole request is
bounded at 64 columns, with its own error naming the limit.

An explicit `path|sourceRef` stays a filter and takes precedence over the
policy, matching the upstream contract. Requests that name their sources are
unaffected by the setting either way.

### v1 (WebSocket playback)

Registered via `app.registerHistoryProvider()`. Supports playback at
configurable speed multipliers using chunked reads from QuestDB. Replayed
updates carry the recorded sourceRef as `$source`, one update per source, so
consumers see the same attribution the live stream had.

## Console webapp

QuestDB ships its own web console — a full SQL workbench with schema browsing,
query history and CSV import. With **QuestDB console webapp** enabled (the
default) the plugin appears in the Signal K webapp list and serves that console
inside the admin UI, with Signal K's own navigation panel still in place. The
**[Web Console user guide](doc/web-console.md)** walks through it with
ready-to-paste queries: speed in knots, temperatures in °C, distance per day,
track export, and more.

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

The console is also reachable directly on QuestDB's HTTP port. On a bare-metal
Signal K with "Bind to 0.0.0.0" off, signalk-container binds it to loopback at
the address the status card shows (the port is the one it bound, not necessarily
9000); from another machine, tunnel that port over SSH. It is 9000 on the host
only with "Bind to 0.0.0.0" on. In managed mode a containerized Signal K reaches
QuestDB over the container network, so there is no host port to open unless
"Bind to 0.0.0.0" is on; in external mode the console is at the configured
QuestDB host and HTTP port, whatever the deployment. The HTTP port carries
QuestDB's REST API and ingestion too, with no authentication; enabling "Bind to
0.0.0.0" to reach it from the LAN publishes all of that, which is what the
webapp route exists to avoid.

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
9-minute default matches Freeboard's AIS expiry, so a restored target is one
that would still have been on the chart had the server never stopped. Raising it
puts progressively staler positions in front of you — treat the window as a
collision-avoidance setting, not a convenience one.

Only navigation and identity paths are replayed, so stale tank levels and engine
temperatures are not resurrected as though they were current readings. A vessel
with no position in the window is skipped, as is one that has already
transmitted since startup. Restore honours the recording toggles: with **Record
AIS targets** off, no AIS target is restored.

The config panel reports the outcome once startup finishes: **Vessels Restored**
with the count, or **Restore failed** if the replay could not run. Neither
appears when the option is off.

## Migrating from InfluxDB

The **InfluxDB Migration** section of the config panel copies history out of an
existing InfluxDB into QuestDB. It supports InfluxDB 1.x (InfluxQL, what
signalk-to-influxdb wrote) and 2.x (Flux, what signalk-to-influxdb2 writes).

### Running an import

1. **Detect** finds an InfluxDB on `localhost:8086`, or enter its URL —
   `localhost` or a loopback/private-network IP literal; anything else is
   refused.
2. Supply credentials — an API token and organisation for 2.x, or
   username/password for 1.x if authentication is enabled — and list the buckets
   (2.x) or databases (1.x). Credentials are used for the import only, are never
   written to the plugin's settings, and travel in the request body rather than
   the query string (Signal K logs full request URLs).
3. Pick a bucket. The panel then lists the vessels it holds (**Listing the
   vessels in the bucket…** — on a large bucket this takes a while) and shows
   **Which vessel in the source is this one**: that vessel's history is imported
   as `self`. The choice is made for you when the source says:
   signalk-to-influxdb2 tags its own vessel's points with `self=true`, so that
   vessel is preselected and marked "(recorded as the own vessel)" — as is the
   one matching this server's identity, marked "(this server)", or the only one
   there is. A signalk-to-influxdb 1.x source carries no such tag, so there the
   choice is yours whenever it holds several vessels and none is this server's
   identity — the usual case, since the recording server's identity is usually
   not this one's (a new install has a new one). Tick **Also import the other
   vessels (AIS targets), each under its own context** to bring those along; the
   history API serves them as it does live-recorded targets.
4. Pick a time range and **Start import**. Progress is polled while it runs;
   **Cancel** stops it, keeping the saved position (see Resuming).

After the import, check the row counts on the status card or in the console
(`SELECT count() FROM signalk WHERE source = 'influxdb-import'`), make sure this
plugin is the default history provider (see Getting started), and disable the
InfluxDB plugin once nothing else needs it — a consumer such as a track viewer
reads whichever provider is the default, not both.

### How the data maps

- A measurement with the conventional `value` field becomes the Signal K path of
  the same name. So do the typed fields signalk-to-influxdb 1.x writes
  (`stringValue`, `boolValue`, `jsonValue`). A measurement with any other named
  fields becomes `measurement.field` paths, so two fields cannot overwrite each
  other.
- Numbers go to `signalk`, strings and booleans to `signalk_str` (booleans
  tagged `value_kind=boolean`). A `jsonValue` is decoded first: an object is
  recorded as its scalar leaves, the same as live data.
- Positions go to `signalk_position`, whether stored as a `jsonValue`
  (signalk-to-influxdb 1.x) or as a `lat`/`lon` or `latitude`/`longitude` field
  pair (signalk-to-influxdb2).
- Both writers tag every point with the **context** of the vessel it belongs to;
  the vessel you picked is imported as `self`, the others under their own
  contexts or left out.
- Rows keep their **original timestamps**, to the microsecond (InfluxDB stores
  nanoseconds; QuestDB's `TIMESTAMP` holds microseconds), so imported history
  sorts and aggregates alongside live data.
- History is **streamed**: read in batches and written before more is asked for,
  pausing while QuestDB catches up. Memory use stays flat however dense the
  source is, so a large import is safe on a small board.
- Every imported row is tagged `source=influxdb-import`, which makes it
  distinguishable from live recording — and because the tables deduplicate on
  `(ts, path, context, source)`, **re-running the same range overwrites rather
  than duplicating**.
- Anything that cannot be mapped (a gap, an unsupported value type, a
  `jsonValue` that is not valid JSON, a latitude with no matching longitude) is
  counted in the run's `skipped` total rather than being dropped silently.

### Resuming an interrupted import

A large import runs for hours. If it stops part-way — cancelled, failed, or the
Signal K server restarted — the panel shows what was interrupted and offers
**Resume import**, which continues from the saved position instead of starting
again at the first measurement. It needs only the credentials, if InfluxDB asks
for any: the source and range are remembered, credentials never are. Starting
the identical import again the ordinary way continues it too. **Discard saved
position** starts over.

A position is saved only once QuestDB has been asked for the newest row written
before it and has it — ILP gives no acknowledgment, so the plugin reads the row
back — and once the position is about a minute old. The first keeps a stalled or
restarted QuestDB from letting the position past rows it never took, however
long the stall. The minute covers what a committed row still has to survive: a
crash of either process, and on a power cut the kernel's default write-back
timing for a QuestDB running on its default `nosync` — an assumption, not a
guarantee, which is why an external QuestDB should run with `sync` like the
managed container does (see Durability on power loss). The position file is
synced and renamed into place, so a power cut leaves the previous position or
the new one, never a damaged one; should none survive, the import starts again,
which is always safe. A position is a window boundary — the import reads each
measurement a window at a time, a day of source data by default — so a resumed
import repeats the window it was in plus up to a minute, which the deduplication
above makes harmless. An import shorter than a window saves nothing, and is
simply run again.

### Imports made before 2.1.5

Imports from a signalk-to-influxdb 1.x database made before 2.1.5 filed every
string, boolean and position under a path suffixed with the InfluxDB field name
— `navigation.state.stringValue`, `steering.autopilot.engaged.boolValue`,
`navigation.position.jsonValue` — which nothing reads. Running the import again
writes them under their real paths but cannot remove the old rows: the path is
part of the deduplication key, and QuestDB has no row delete. When such rows
exist, the panel says how many and offers **Remove these rows**, which rebuilds
`signalk_str` without them. Recording continues while it runs; only the swap of
the rebuilt table for the old one holds the writer, for a few seconds, and rows
recorded during the rebuild are carried across. Should recording be fast enough
to fill the writer's buffer during those seconds, the samples it could not keep
are counted and the panel says how many. Run the import again afterwards.

## Export and backup

**Data Export** in the panel, and
`GET /plugins/signalk-questdb/api/export?from=…&to=…&format=parquet|csv`,
download the `signalk` numeric table for a date range (both bounds required) as
Parquet — QuestDB's native export, compressed with the configured codec — or
CSV.

`GET /plugins/signalk-questdb/api/full-export/:table?from=…&to=…` streams one
whole table as Parquet, for snapshot and backup tooling that wants the full
content sliced into dedup-friendly shards (kopia, for instance). Tables:
`signalk`, `signalk_str`, `signalk_position` (`/full-export/tables` lists them).

- `from` and `to` are optional but must be set together: omit both for the whole
  table, or pass both as ISO 8601 timestamps for a half-open `[from, to)` window
  — no row appears in two adjacent windows.
- Repeated query parameters (`?from=A&from=B`) and empty strings (`?from=`) are
  rejected with HTTP 400; silently downgrading to a full-table export would hide
  bugs in the caller.
- Format and compression follow the plugin's compression setting (LZ4_RAW or
  ZSTD), the same as `/export`.

## REST endpoints

All under `/plugins/signalk-questdb/api/`; the console is mounted beside them at
`/plugins/signalk-questdb/console/`. Plugin routes are admin-only by Signal K's
default.

| Method | Path                                     | Description                                                                               |
| ------ | ---------------------------------------- | ----------------------------------------------------------------------------------------- |
| GET    | `/status`                                | QuestDB health, row counts, active paths, restore outcome, WAL state, unstorable paths    |
| GET    | `/query?sql=...`                         | Read-only SQL proxy (DDL/DML blocked)                                                     |
| GET    | `/paths`                                 | All recorded paths with row counts and time range                                         |
| GET    | `/versions`                              | QuestDB releases from GitHub (for the version picker)                                     |
| GET    | `/update/check`                          | Compare the running version against the latest release                                    |
| POST   | `/update/apply`                          | Pull the latest image, recreate the container, reconnect                                  |
| GET    | `/wal-diagnosis`                         | Suspended tables with the engine's reason and a bounded skip plan (see WAL suspension)    |
| POST   | `/resume-wal`                            | Lossless `RESUME WAL` of the suspended tables                                             |
| POST   | `/resume-wal/skip`                       | Guided skip of the unreadable segment — explicit, quantified, and only on request         |
| POST   | `/purge-data`                            | Remove the QuestDB container and delete all its data (rootless-Podman-safe)               |
| GET    | `/migration/detect`                      | Detect InfluxDB (supports `?url=` for a remote one)                                       |
| POST   | `/migration/buckets`                     | List buckets (2.x) or databases (1.x); credentials in the body, never the query string    |
| POST   | `/migration/measurements`                | List measurements and their field keys in a bucket/database                               |
| POST   | `/migration/contexts`                    | List the vessels a bucket/database holds, and which is this server's                      |
| POST   | `/migration/start`                       | Start an import, or continue a stopped one with `{"resume": true}`; returns immediately   |
| GET    | `/migration/status`                      | Progress and state of the current/last import, and any stopped import that can resume     |
| POST   | `/migration/cancel`                      | Cancel the running import; any saved position is kept                                     |
| POST   | `/migration/discard`                     | Forget a stopped import's saved position, so the next start begins again                  |
| GET    | `/migration/legacy-rows`                 | Count rows an import made before 2.1.5 filed under suffixed paths                         |
| POST   | `/migration/legacy-rows/remove`          | Rebuild the string table without those rows; recording continues meanwhile                |
| GET    | `/export?from=...&to=...&format=parquet` | Parquet or CSV export of the `signalk` numeric table (date range required)                |
| GET    | `/full-export/tables`                    | List the tables the full-export route serves                                              |
| GET    | `/full-export/:table?from=...&to=...`    | Stream a table as Parquet; optional half-open `[from, to)` window for slicing into shards |

## Deployment

### Connectivity

In **managed mode** the plugin does not need `QuestDB host` to be correct for
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
- **Bind to 0.0.0.0 = on.** QuestDB stays attached to the shared
  `Container network`; the setting publishes its HTTP/ILP/PostgreSQL ports on
  all host interfaces, using the configured port numbers. Enable this only to
  reach QuestDB from another machine or from a Grafana running in a separate
  Docker instance. When Signal K itself is containerized, the plugin probes
  which address reaches the published ports — its own loopback first, then the
  runtime's `host.containers.internal` gateway — and uses the one QuestDB
  answers on.

In **external mode** (`Manage QuestDB container` off) the plugin connects to the
QuestDB you point it at via `QuestDB host` and the HTTP/ILP ports.

### Data storage

QuestDB's data root is the plugin's own data directory,
`<config>/plugin-config-data/signalk-questdb`, whatever the deployment. Data
survives container restarts, image upgrades, and plugin disable/enable cycles.
How it reaches the container differs:

- **Bare-metal Signal K**: that directory, mounted at `/var/lib/questdb`.
- **Signal K in a container with its config directory bind-mounted**: the exact
  host path of that directory, mounted at `/var/lib/questdb`.
- **Signal K in a container with its config directory on a named volume**: the
  volume cannot be mounted from a subdirectory, so it is mounted whole at
  `/signalk-vols/<volume>` and QuestDB is pointed at the plugin's directory
  inside it (`QUESTDB_DATA_DIR`). A database found at the volume's root — where
  a whole-volume mount put it — is moved into the plugin's directory on start;
  both are the same volume, so nothing is copied. A purge deletes the directory
  from the Signal K process, since the runtime cannot mount the volume by Signal
  K's path; where QuestDB's user owns it and the Signal K user cannot delete it,
  the purge says so and the directory is deleted by hand.

QuestDB's entrypoint makes its data root its own on start, so the plugin keeps
the one file it writes alongside — the import checkpoint,
`signalk-questdb.influx-import-checkpoint.json` — next to the directory rather
than in it. A purge removes it, whether or not the data could be deleted.

### Durability on power loss

The managed container runs QuestDB with `cairo.commit.mode=sync`.

QuestDB's default is `nosync`: nothing on the ingest path is ever fsynced, and
durability is left to the OS page cache. On a power cut an arbitrary subset of
recent writes survives — including the case where a commit record reaches disk
while the row data it describes does not. QuestDB then fails while _opening_
that table's partition, which suspends the table permanently: neither a lossless
`RESUME WAL` nor a bounded `RESUME WAL FROM TXN` skip can get past it, because
the failure happens before any transaction is read. Repair means hand-patching
the commit record.

With `sync`, partition columns are fsynced before the commit record is written
and WAL segments are fsynced on commit, so the stored state can never claim data
that isn't durable. The cost is a few fsyncs per commit interval — writes are
already batched (see **Write batch interval**), so at boat data rates it is
negligible against losing a table.

**Running an external QuestDB?** Set this yourself in `server.conf`:

```ini
cairo.commit.mode=sync
```

### WAL suspension

QuestDB ingestion is two-stage: the sequencer durably commits every batch (the
writer sees success), then a background job applies the transactions to the
partitions. When that job hits an error it **suspends the table** — the
sequencer keeps accepting rows, nothing is applied, queries serve stale data,
and no client-visible error is raised anywhere. Left alone, recording stalls
silently for as long as nobody looks.

The plugin watches for this. A suspended table of its own (in external mode,
only the three tables above — never someone else's) is logged loudly, raised as
the Signal K notification `notifications.signalk-questdb.walSuspended`, and
shown on the status card as **WAL suspended**. Two causes need two remedies:

- **Transient** — disk full since cleared, out of memory, file-descriptor
  exhaustion: a plain `RESUME WAL` replays every pending transaction,
  losslessly. The plugin does this by itself, once per stall point; the panel's
  **Resume** button does the same on request, and the banner clears as the
  backlog drains.
- **Unreadable segment** — an unclean container stop mid-write corrupts the
  in-flight segment's files, and every apply attempt fails on them within
  milliseconds, so a plain resume re-suspends at once, forever. The only way
  forward is `RESUME WAL FROM TXN` past the unreadable data, which loses what it
  skips. The plugin prepares that but never applies it on its own:
  **/wal-diagnosis** scrapes the engine log for the real reason (the
  `wal_tables()` error fields often carry none, and a container recreate wipes
  the engine's memory of it) and computes the minimal skip — the first
  transaction of the segment after the stuck one — with the number of
  transactions and the wall-clock window of ingestion it would drop. The panel
  shows those numbers and offers **Skip**; if the stuck segment is the newest
  one, skipping drops the entire pending backlog, and the panel says so.

### Performance (Pi and other low-power hosts)

- **Default sampling rate** of 2000 ms limits each path to one write per two
  seconds, keeping write volume modest on busy NMEA 2000 buses; per-path
  overrides allow faster rates for critical paths
  (`{ "environment.wind.*": 200 }`) while slow-changing ones stay throttled.
- **Resource caps** of 768 MB RAM and 1.5 CPU cores (cgroup limits via
  signalk-container) keep QuestDB from squeezing co-resident containers like
  Grafana, mayara, or signalk-backup. The JVM sizes its heap to a fraction of
  the memory cap, so total footprint (heap plus off-heap) is bounded. Set the
  memory limit to empty or the CPU limit to `0` to lift the cap on roomier
  hosts.
- **Worker tuning** cuts idle CPU. QuestDB sizes its worker pools from the host
  core count and busy-polls before sleeping — sensible on a server, wasteful on
  a mostly idle boat database. The plugin pins its worker pools down and quiets
  QuestDB's per-commit logging, keeping the messages that matter. One caveat
  when upgrading the QuestDB image: an unrecognised setting is ignored silently,
  so a property renamed between releases looks exactly like one that worked.
- **Batching.** Each commit is one WAL transaction per table, and with
  deduplicated tables the apply cost of a transaction grows with partition size
  — frequent tiny commits eventually outpace what a Pi can apply and recording
  stalls. Bigger batches keep the WAL healthy; the cost is that at most one
  interval of buffered samples is lost on a hard crash.

The console's Monitoring view flags any table whose 90th-percentile WAL
transaction stays under QuestDB's recommended batch size (100 rows) as **"Small
transactions — consider batching"**. `signalk_position` triggers this
structurally: it holds exactly one path, so its share of every commit is just
the handful of GPS fixes since the last one, while the other tables spread
hundreds of paths across each transaction. At vessel data rates the alert is
cosmetic; the numbers that matter are on the same page — **Write Amplification**
near 1x, **Pending Rows** 0 and **Transaction Lag** 0 mean the WAL apply is
healthy. Clearing the alert for the position table would need roughly 100 fixes
per commit, a batch window in the minutes; the trade-off is up to one interval
of buffered data lost on a hard crash, and history that is that much less live.

### Grafana

Connect Grafana to QuestDB through the PostgreSQL data source (user `admin`,
password `quest`, database `qdb`).

- The companion [signalk-grafana](https://github.com/dirkwa/signalk-grafana)
  plugin wires this up for you: it runs Grafana as a managed container and
  reaches QuestDB by container DNS on the shared `sk-network`, so no port needs
  publishing.
- A Grafana **on the Signal K host** can reach the PostgreSQL port without
  binding: with "Bind to 0.0.0.0" off, signalk-container binds it to loopback at
  a host port of its choosing — `podman port <container>` shows which (the
  container is `signalk-questdb` behind the `sk-` prefix). With "Bind to
  0.0.0.0" on it is the configured port, 8812 by default.
- A Grafana in a **separate Docker instance or on another machine** needs "Bind
  to 0.0.0.0" and the host's LAN address (`192.168.0.122:8812`, say). That
  publishes QuestDB's ports to your entire network; only enable it behind a
  firewall you trust.

Example query:

```sql
SELECT ts AS time, avg(value) AS sog
FROM signalk
WHERE path = 'navigation.speedOverGround'
  AND context = 'self'
  AND ts BETWEEN $__timeFrom() AND $__timeTo()
SAMPLE BY $__interval
```

## Troubleshooting

### 'Module "signalk-questdb" is not available' right after install or update

Right after installing or updating the plugin, the admin UI can show
`Module "signalk-questdb" is not available. Make sure the webapp is installed.`
It is installed — the browser tab is still running the admin UI it loaded before
the update, which looks for the panel bundle it knew then. A hard refresh of the
tab (Shift-Reload) fixes it.

### "Cannot apply a change: the QuestDB container cannot be stopped or removed"

The plugin needed to recreate the QuestDB container to apply a change — a new
image tag, a network or resource setting, a raised file-descriptor limit — but
the container runtime refused to stop or remove the running container. The
recreate is **deferred, not abandoned**: QuestDB keeps running and keeps
recording, on its **previous** configuration. The plugin reports the error
rather than a "Recording" status, because the change you asked for has not been
applied and reporting success would be a lie.

The usual cause on rootless Podman is an orphaned user namespace left behind
when the Podman service restarted underneath a running container. The error text
carries the specific remedy; the general one is to clear the stale namespace and
remove the container so the plugin can recreate it:

```bash
podman system migrate                     # re-attaches orphaned user namespaces
podman ps -a | grep questdb               # find the managed container's name
podman rm -f <name-from-above>
```

Run all three as the **same OS user that owns the container**. Rootless and
rootful Podman keep separate container stores, so running them as root (or as
another user) will not find, and will not remove, a rootless container.

Look the name up rather than assuming it: the plugin's container is
`signalk-questdb` behind a prefix that defaults to `sk-` but is configurable
(`SIGNALK_CONTAINER_NAMESPACE`), so it is not always `sk-signalk-questdb`.

Then disable and re-enable the plugin, or restart Signal K. The plugin recreates
the container with the settings that were pending. Recorded data lives under
Signal K's data directory, not inside the container (see Data storage), so
removing the container does not touch it.

**Do not** mask `podman.socket` to work around this. That socket is how the
plugin reaches Podman at all, so masking it turns a deferred recreate into a
container the plugin cannot manage in any way. (`podman.service` is the unit the
socket activates, and masking that has the same effect.) If the socket itself is
unhealthy, restart it rather than masking it:

```bash
systemctl --user restart podman.socket
```

This error is reported with signalk-container 1.29.0 or later. On older versions
the wedge still happens, but appears only as a repeating drift message in the
Signal K server log with no plugin error to go with it.

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

When Signal K's containers run under rootless Podman, the kernel only enforces a
resource limit whose cgroup controller has been delegated to the user session.
Many distributions delegate `cpu`, `cpuset`, `io`, and `pids` by default but
**not** `memory`. signalk-container drops a limit whose controller is missing
rather than failing the container, so a configured cap can silently have no
effect.

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
OOM-kills it. Raise **QuestDB memory limit** in the plugin config (e.g. `1g` or
`1.5g`, or empty to remove the cap on a roomier host), which recreates the
container.

**Confirm the diagnosis** (substitute the container's name if the prefix is not
`sk-`):

```bash
podman inspect sk-signalk-questdb \
  --format 'OOMKilled={{.State.OOMKilled}} RestartCount={{.RestartCount}} cap={{.HostConfig.Memory}}'
#   cap=805306368  -> 768 MB cap is in place (variant B)
#   cap=0          -> no cap applied (variant A, or cap intentionally removed)
podman events --since 2h --stream=false --filter container=sk-signalk-questdb \
  --filter event=oom --filter event=died
```

### History queries slow or timing out / "out-of-memory" errors with free RAM

QuestDB memory-maps every partition column file and every pending WAL segment it
touches. The Linux kernel caps how many memory mappings one process may hold
(`vm.max_map_count`), and the stock value on Debian, Ubuntu ≤ 22.04, and RHEL
(65530) is far below what QuestDB recommends (1048576). A fresh database fits
easily; months of daily partitions across the three tables — plus a segment
backlog if a table's WAL is suspended — can exhaust the limit. When that happens
`mmap` fails with out-of-memory errors (errno 12) **even though plenty of RAM is
free**: queries error out or crawl, and the WAL apply job can suspend a table.

The QuestDB Web Console shows the same warning
(`vm.max_map_count limit is too low`), the plugin's config panel shows a banner,
and the server log gets a warning at plugin startup. Fix it in a shell **on the
host machine itself** — not inside the QuestDB container via `podman exec`: the
limit is kernel-global, a container cannot change it, and the QuestDB image has
neither `sudo` nor `/etc/sysctl.d`. Being kernel-global also means this works no
matter how Signal K itself is deployed:

```bash
echo 'vm.max_map_count=1048576' | sudo tee /etc/sysctl.d/99-questdb.conf
sudo sysctl --system
```

This takes effect immediately — no container or host restart needed. See
[QuestDB capacity planning](https://questdb.com/docs/getting-started/capacity-planning/#max-virtual-memory-areas-limit)
for background.

### QuestDB container never starts on macOS (podman machine)

On macOS the QuestDB container is created but may never start. What you see
depends on the signalk-container version: with signalk-container 1.26 or later,
the manager names the problem outright (an open-files limit the host refuses)
and starts the container on the runtime's default limits instead, with this
plugin's config panel showing a **"request rejected by the host"** banner — the
remediation below applies unchanged. Older versions never start the container at
all and show either a misleading **"Permission denied"** (the runtime's error
text contains "operation not permitted") or only a generic **"Unexpected error.
See logs for details."** — and the container log view stays empty, because a
container that never started has no logs.

The cause is the plugin's open-files request. On macOS, Signal K runs on the Mac
while podman runs inside a Fedora CoreOS VM ("podman machine"). The plugin asks
for 1048576 open files, and signalk-container normally clamps that request to
what the host can grant — but from macOS it cannot read the VM's limits, so the
full request reaches the VM, exceeds its default hard limit (524288), and the
OCI runtime refuses to start the container. On current Fedora CoreOS the VM's
`vm.max_map_count` already meets QuestDB's 1048576, so usually only the
file-descriptor limit needs raising — but an older machine image may sit lower,
so check it first:

```bash
podman machine ssh -- sysctl -n vm.max_map_count
```

If that prints less than 1048576, raise it inside the VM before relying on the
file-descriptor fix alone (`podman machine ssh`, then
`sudo tee /etc/sysctl.d/99-signalk-questdb.conf <<< 'vm.max_map_count=1048576'`
and `sudo sysctl --system`).

Raise the file-descriptor limit inside the VM. From a macOS terminal:

```bash
podman machine ssh
```

Inside the VM, add a systemd drop-in for every user manager (the `user@.service`
template covers each user instance, including the machine's `core` user, whose
session runs rootless containers) and the same drop-in for `podman.service` to
cover the rootful connection:

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

This should print `1048576`. Finally, remove the half-created container so it is
recreated with the full limit — the limit is set at create time, so a restart is
not enough:

```bash
podman rm -f sk-signalk-questdb
```

then restart Signal K — on the next plugin start the container is recreated with
the full limit. (The container manager's **Start** button only starts an
existing container, so it cannot replace this step.)

The verify and `rm` commands run on podman's default connection — normally the
rootless one the plugin uses. Rootless and rootful connections keep separate
container stores, so on a setup with a non-default connection add the same
`--connection <name>` (list them with `podman system connection list`) to both
commands, so they hit the store where `sk-signalk-questdb` actually lives
(`podman ps -a` shows it).

## License

signalk-questdb 2.0.0 and later is **source available, not open source**. See
[LICENSE.md](LICENSE.md).

**You may**, free of charge: run it on your own boat or fleet, private or
commercial; use it for internal company operations; modify it for your own use;
use it in education and research; and provide professional services around it.

**You may not**: redistribute it, or publish a modified version of it to npm or
anywhere else. Verbatim copies of official releases may be mirrored and cached.

Versions 1.9.2 and earlier remain available under the MIT license, see
[LICENSE-MIT-through-v1.x.txt](LICENSE-MIT-through-v1.x.txt).

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md).
