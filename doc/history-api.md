# History API

## v2 (REST — `/signalk/v2/api/history/`)

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

Buckets start at the request's `from` and cover the whole range, empty ones
included. A bucket with no row is not always missing data. Change-only recording (see
[Recording](recording.md#what-is-recorded)) writes an unchanged value only once per heartbeat, so an empty bucket
takes the last sample before it — in every aggregate, since every aggregate of a
steady value is that value — for up to twice the heartbeat after the sample was
taken, and never into a bucket that has not started yet. At the start of a
range the sample comes from just before it. Past that window, and with the
heartbeat set to 0, an empty bucket is `null`. This happens before `sma` and
`ema`, so a held value counts as a sample. `middle_index` and
`navigation.position` return recorded rows only.

A path recorded in radians — a heading, a course, a wind angle; `units: rad` in
the server's metadata for the path — is averaged as a vector: `average`, `sma`
and `ema` take the angle of the mean sine and cosine, so 359° and 1° average to
0°, not 180°. The result keeps the path's convention, `[0, 2π)` for a heading or
`(−π, π]` for an apparent wind angle; samples that cancel, 0° and 180° in equal
measure, have no mean direction and give `null`. `min`, `max` and `mid` stay
arithmetic.

`navigation.position` answers `first`, `last` and `middle_index` — each a point
the vessel was at. Any other method runs `first`, and the column's `method` says
so. A text path (rows in `signalk_str`) answers `first`, `last` and
`middle_index`. With a resolution, `average`, `min`, `max` and `mid` run `last`,
the state in force at the bucket's end, and the column's `method` says so.
`sma` and `ema` read the numeric table only, so a text path has no values for
them.

A method name outside the table above fails the request with an error naming
the path and the method.

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

### Reading one source

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

### sourcePolicy=all

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

## v1 (WebSocket playback)

Registered via `app.registerHistoryProvider()`. Supports playback at
configurable speed multipliers using chunked reads from QuestDB. Replayed
updates carry the recorded sourceRef as `$source`, one update per source, so
consumers see the same attribution the live stream had.

## REST endpoints

All under `/plugins/signalk-questdb/api/`; the console is mounted beside them at
`/plugins/signalk-questdb/console/`. Plugin routes are admin-only by Signal K's
default.

| Method | Path                                     | Description                                                                                                                       |
| ------ | ---------------------------------------- | --------------------------------------------------------------------------------------------------------------------------------- |
| GET    | `/status`                                | QuestDB health, row counts, active paths, restore outcome, WAL state, unstorable paths                                            |
| GET    | `/query?sql=...`                         | Read-only SQL proxy (DDL/DML blocked)                                                                                             |
| GET    | `/paths`                                 | All recorded paths with row counts and time range                                                                                 |
| GET    | `/versions`                              | QuestDB releases from GitHub (for the version picker)                                                                             |
| GET    | `/update/check`                          | Compare the running version against the latest release                                                                            |
| POST   | `/update/apply`                          | Pull the latest image, recreate the container, reconnect                                                                          |
| GET    | `/wal-diagnosis`                         | Suspended tables with the engine's reason and a bounded skip plan (see [WAL suspension](deployment.md#wal-suspension))            |
| POST   | `/resume-wal`                            | Lossless `RESUME WAL` of the suspended tables                                                                                     |
| POST   | `/resume-wal/skip`                       | Guided skip of the unreadable segment — explicit, quantified, and only on request                                                 |
| POST   | `/purge-data`                            | Remove the QuestDB container and delete all its data (rootless-Podman-safe)                                                       |
| GET    | `/migration/detect`                      | Detect InfluxDB (supports `?url=` for a remote one)                                                                               |
| POST   | `/migration/buckets`                     | List buckets (2.x) or databases (1.x); credentials in the body, never the query string                                            |
| POST   | `/migration/measurements`                | List measurements and their field keys in a bucket/database                                                                       |
| POST   | `/migration/contexts`                    | List the vessels a bucket/database holds, and which is this server's                                                              |
| POST   | `/migration/start`                       | Start an import, or continue a stopped one with `{"resume": true}`; returns immediately                                           |
| GET    | `/migration/status`                      | Progress and state of the current/last import, and any stopped import that can resume                                             |
| POST   | `/migration/cancel`                      | Cancel the running import; any saved position is kept                                                                             |
| POST   | `/migration/discard`                     | Forget a stopped import's saved position, so the next start begins again                                                          |
| GET    | `/migration/legacy-rows`                 | Count rows an import made before 2.1.5 filed under suffixed paths                                                                 |
| POST   | `/migration/legacy-rows/remove`          | Rebuild the string table without those rows; recording continues meanwhile                                                        |
| GET    | `/export?from=...&to=...&format=parquet` | Parquet or CSV export of the `signalk` numeric table (date range required)                                                        |
| GET    | `/full-export/tables`                    | List the tables the full-export route serves                                                                                      |
| GET    | `/full-export/:table?from=...&to=...`    | Stream a table as Parquet; optional half-open `[from, to)` window for slicing into shards                                         |
| GET    | `/gpx-import`                            | The current or last GPX import                                                                                                    |
| POST   | `/gpx-import`                            | Announce an import: `{filename, size, tracks, fillGapsOnly}`, see [Importing a GPX track](import-export.md#importing-a-gpx-track) |
| PUT    | `/gpx-import/:id?offset=N`               | One slice of the file, as `application/octet-stream`, at most 8 MiB                                                               |
| POST   | `/gpx-import/:id/finish`                 | End the import once every byte is in; answers when QuestDB has applied it                                                         |
| DELETE | `/gpx-import/:id`                        | Abort the import; what it wrote stays                                                                                             |
