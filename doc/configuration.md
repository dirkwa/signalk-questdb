# Configuration

The config panel groups the settings as: the **status card** (running state,
rows, active paths, restore outcome, WAL state), the **update check** and
**image version** picker (latest, pre-releases and the last three stable
releases), **Connection**, **Recording**, **History**, and the collapsible
**Path filtering**, **Compression (on-disk)**, **InfluxDB Migration**, **Data
Export** and **Danger zone** sections. The Danger zone removes the container and
all of its data — including what Signal K's plugin uninstall cannot reach on
rootless Podman.

| Setting                       | Default      | Description                                                                                                                                                                                      |
| ----------------------------- | ------------ | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------------ |
| QuestDB image version         | `latest`     | Image tag; the dropdown lists the latest release, pre-releases, and the last three stable releases                                                                                               |
| Manage QuestDB container      | `true`       | Let signalk-container run QuestDB; off = connect to an external QuestDB                                                                                                                          |
| QuestDB host                  | `127.0.0.1`  | External mode only; in managed mode the address is resolved automatically                                                                                                                        |
| HTTP port                     | `9000`       | External mode, or the host binding when "Bind to 0.0.0.0" is on; otherwise signalk-container allocates it                                                                                        |
| ILP port                      | `9009`       | External mode, or the host binding when "Bind to 0.0.0.0" is on; otherwise signalk-container allocates it                                                                                        |
| PostgreSQL port               | `8812`       | Host binding for Grafana/psql when "Bind to 0.0.0.0" is on                                                                                                                                       |
| QuestDB memory limit          | `768m`       | Hard cgroup cap on the container (empty = unlimited); the JVM heap gets half of it, so heap plus off-heap stays inside                                                                           |
| QuestDB CPU limit (cores)     | `1.5`        | Fractional cores (0 = unlimited)                                                                                                                                                                 |
| Default sampling rate (ms)    | `2000`       | Minimum ms between writes for any one path (0 = every update)                                                                                                                                    |
| Per-path sampling rates (ms)  | _(empty)_    | Glob → ms overrides, one `pattern ms` per line in the panel (0 = wire speed, see [Sampling](recording.md#what-is-recorded))                                                                      |
| Unchanged-value heartbeat (s) | `300`        | A value equal to the last one recorded is written again only this often; changes are written at the sampling rate (0 = every sample, see [Change-only recording](recording.md#what-is-recorded)) |
| Write batch interval (ms)     | `5000`       | How often buffered samples are committed — one WAL transaction per table per commit (see [Performance](deployment.md#performance-pi-and-other-low-power-hosts))                                  |
| Record own vessel             | `true`       | Record the `self` context                                                                                                                                                                        |
| Record AIS targets            | `true`       | Record other vessels                                                                                                                                                                             |
| Record all sources            | `false`      | Record every source of a path, not only the one the source priorities prefer (see [Source priorities](recording.md#what-is-recorded))                                                            |
| Restore vessels on startup    | `false`      | Replay each vessel's last recorded position after a restart (see [Startup restore](recording.md#startup-restore))                                                                                |
| Restore max age (minutes)     | `9`          | Only replay values recorded within this window                                                                                                                                                   |
| Allow `sourcePolicy=all`      | `false`      | Let History API callers split a path into one column per recording source (see [sourcePolicy=all](history-api.md#sourcepolicyall))                                                               |
| QuestDB console webapp        | `true`       | Serve QuestDB's console inside the Signal K admin UI, admin only (see [Web Console](web-console.md))                                                                                             |
| Retention (days)              | `0`          | Drop daily partitions older than this (0 = keep forever)                                                                                                                                         |
| Keep own track                | `true`       | Archive the own vessel's positions before retention drops them (see [Retention and the archive](recording.md#retention-and-the-archive))                                                         |
| Archived own-vessel paths     | _(empty)_    | Glob patterns of own-vessel numeric paths to archive the same way, e.g. `environment.depth.*`                                                                                                    |
| Archive resolution (s)        | `10`         | Archived rows are thinned to the first sample per interval (0 = every row)                                                                                                                       |
| Filter mode                   | `exclude`    | `exclude` the matching paths, or `include` only the matching paths                                                                                                                               |
| Path patterns                 | _(empty)_    | Glob patterns, one per line, e.g. `environment.wind.*`; empty records everything                                                                                                                 |
| Compression codec             | `lz4`        | On-disk WAL segments and Parquet exports: `none`, `lz4` (fast) or `zstd` (smaller)                                                                                                               |
| Compression level             | `3`          | ZSTD level 1–22, used only with `zstd`                                                                                                                                                           |
| Container network             | `sk-network` | Shared Podman/Docker network QuestDB joins in managed mode, so the companion signalk-grafana reaches it by container DNS; host-port publication is separate ("Bind to 0.0.0.0")                  |
| Bind to 0.0.0.0               | `false`      | Publish QuestDB's ports on all interfaces (see [Connectivity](deployment.md#connectivity))                                                                                                       |

## Default history provider

Which registered provider answers `/signalk/v2/api/history/` is the operator's
choice: pick it once under **Data → Preferences → Default History Provider**
(server 2.31 or newer); the server persists it as `historyApi.defaultProvider`
in `settings.json`, and it survives restarts and plugin load order. If no
default is configured, the server uses whichever provider registers first.
Versions 2.0.0 and earlier asked the server to make QuestDB the default on every
start; that is gone as of 2.0.1 — it could silently override a default you had
chosen, and on servers with security enabled it never worked at all.

## QuestDB updates

The panel's **update check** compares the running QuestDB with its latest stable
release. **Update** pulls that release, recreates the container on it and saves
it as the image version.

After the plugin itself was updated, and at its very first start, the plugin
also checks once on its own, in the background. It does so only in managed mode,
and only where the image version is a tag that moves, such as the default
`latest`. If QuestDB runs an older release than the latest stable one, the
plugin pulls the tag again and recreates the container on it. The image version
stays `latest`. Without this check, QuestDB on `latest` would stay on whatever
release the first install pulled, because signalk-container pulls an image only
when it is missing. An image version that names one release, such as `9.4.3`,
stays as it is, and so does one that **Update** saved while the check was
waiting. QuestDB is down while it starts again, as with **Update**. Progress
goes to the debug log, and failures go to the server log. A recreate that fails
half way and leaves QuestDB stopped or without a container is followed by a
start, as the plugin's own start would do. Only if that fails too does the
status line say so.

With [signalk-internet](https://github.com/meri-imperiumi/signalk-internet)
enabled, the check waits for a free link. On a metered, captive or offline link
it asks again from time to time, for as long as the plugin runs. Without
signalk-internet, the link counts as free. Telling the two apart needs
signalk-container 1.38.0 or later; with an older one, the check does not run. A
failure to read the running or the latest version is also tried again later.

The check is done once QuestDB runs the latest release, or once its container
runs the image the pull brought, even if QuestDB still reports an older release
afterwards. It then waits for the next plugin update. The next start checks
again wherever the container keeps its old image, or where the new one cannot
be told from the old:

- the plugin stopped first;
- the download failed, or something else failed before the recreate;
- an InfluxDB import, a GPX import or an export was running;
- the string table was being rebuilt;
- the container was stopped, which the check leaves as it is;
- the container was wedged;
- the tag on Docker Hub did not name the latest release yet.

A tag that never reaches the latest release, such as `9` while the latest is
`10.1.0`, therefore costs a pull and a look at GitHub at every start.
