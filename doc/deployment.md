# Deployment

## Requirements

- Node.js 24 or newer
- QuestDB 8.1 or newer (resolution queries use `SAMPLE BY … FROM … TO`). This
  applies to an external QuestDB and to a pinned **QuestDB image version** in
  managed mode; `latest` always qualifies
- Signal K server — 2.31 or newer to choose the default history provider in the
  admin UI
- For managed mode: the
  [signalk-container](https://github.com/dirkwa/signalk-container) plugin, and
  Podman 5.4 or newer (what Debian 13 "trixie" ships) or Docker.
  signalk-container 1.29.0 or newer is recommended; the plugin runs with 1.14.0
  or newer, but older versions lose parts of it — below 1.19.0 the Danger zone
  cannot delete data kept on a named volume, below 1.26 the macOS open-files
  problem shows as a generic error, below 1.29.0 a container the runtime refuses
  to recreate is not reported (see [Troubleshooting](troubleshooting.md)). On rootless Podman below
  5.5 the plugin's open-files request is inherited from the podman service
  rather than granted per container
  ([containers/podman#25881](https://github.com/containers/podman/issues/25881));
  signalk-container accounts for this.

## Connectivity

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

## Data storage

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

## Durability on power loss

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

## WAL suspension

QuestDB ingestion is two-stage: the sequencer durably commits every batch (the
writer sees success), then a background job applies the transactions to the
partitions. When that job hits an error it **suspends the table** — the
sequencer keeps accepting rows, nothing is applied, queries serve stale data,
and no client-visible error is raised anywhere. Left alone, recording stalls
silently for as long as nobody looks.

The plugin watches for this. A suspended table of its own (in external mode,
only the three tables above and the two archive tables — never someone else's)
is logged loudly, raised as
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

## Out of memory or not answering

QuestDB caps the native memory it tracks below the container's memory limit
and fails any allocation past the cap with `global RSS memory limit exceeded`.
When something holds memory at the cap, queries fail, the WAL apply job then
suspends the tables, and in the end QuestDB stops accepting HTTP connections. The JVM stays up throughout, so the
container's restart policy never acts.

In managed mode the plugin checks for this every minute while it runs, and
restarts the QuestDB container when:

- QuestDB recently reported its global memory limit, and over consecutive
  checks its native memory is still held at that limit, it fails even the
  health check with it, or it has stopped answering. A single large query
  frees what it held when it ends, so it does not count, nor does a client that
  keeps asking for one.
- the container is running but QuestDB has answered nothing for several
  minutes, over consecutive checks. A QuestDB busy with a long statement still
  answers it in the end, and no check counts while a parquet export streams
  from it.

Before the restart the plugin writes QuestDB's message and its last error lines
to the server log, shows the reason on the status card, and raises the Signal K
notification `notifications.signalk-questdb.watchdog`. After the restart it
logs the outcome. Once QuestDB answers again the notification returns to normal;
when it answered right after the restart, the notification keeps the time and
the reason. Restarts are rate limited, spaced apart and capped per few hours of
the plugin running; past that the plugin logs that the restart is held back and
keeps the alert up. A restart that fails, because stopping or starting the
container failed, counts against none of these limits: the plugin tries again a
few minutes later, since the container may be left stopped. It reports the
first failure; while the retries keep failing, they go to the debug log only.

The plugin leaves a container alone if someone else stopped it, and starts one
that its own restart left stopped. One exception: a container stopped by hand
while a stop of the plugin's is still pending, or right after a restart of the
plugin's before the plugin has seen it running again, may be started again too.
While a stop of the plugin's hangs, it sends no further stop. An external
QuestDB is never restarted. A
QuestDB that is already stuck when the plugin starts, as after a Signal K
restart while it was stuck, is not covered: restarting the plugin stops the
container and starts it afresh.

The managed container runs with QuestDB's query caches off: history reads put
their time range into the SQL, so the caches never got a hit, and on older
QuestDB releases they held memory.

## Performance (Pi and other low-power hosts)

- **Default sampling rate** of 2000 ms limits each path to one write per two
  seconds, keeping write volume modest on busy NMEA 2000 buses; per-path
  overrides allow faster rates, or wire speed with 0, for critical paths while
  slow-changing ones stay throttled.
- **Change-only recording** skips a value that has not changed since it was last
  written until the heartbeat is due. Paths that hold steady — which is
  most of them — cost one row per heartbeat instead of one per sampling
  interval, which cuts both disk use and WAL apply work (see [Recording](recording.md#what-is-recorded)).
- **Resource caps** of 768 MB RAM and 1.5 CPU cores (cgroup limits via
  signalk-container) keep QuestDB from squeezing co-resident containers like
  Grafana, mayara, or signalk-backup. The JVM heap gets half of the memory
  cap, so total footprint (heap plus off-heap) is bounded, and a JVM that runs
  out of heap exits so the container restarts instead of hanging. Set the
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

## Grafana

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
