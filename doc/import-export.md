# Import and export

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
plugin is the default history provider (see [Default history provider](configuration.md#default-history-provider)), and disable the
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
managed container does (see [Durability on power loss](deployment.md#durability-on-power-loss)). The position file is
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

## Importing a GPX track

A chartplotter's exported track goes into the position archive (see [Retention
and the archive](recording.md#retention-and-the-archive)), where retention never drops it, under the source
`import.gpx`. An export can be gigabytes, a four-year TimeZero export is
1.5 GB, so the file is sent in slices and parsed as it arrives. Memory stays at
one slice, whatever the file's size.

In the configuration panel, **GPX track import** reads the chosen file in the
browser first, which takes seconds even for a gigabyte, and lists its tracks by
name with their points and period. It proposes the own vessel's tracks: the
largest group whose name is not an AIS target's, and any other name that
recurs. It flags a group that lay more than a kilometre from a larger one at
the same time, since that was another vessel. Nothing is uploaded until
**Import** is pressed; the import then shows its progress and can be aborted.

- **Which tracks.** An export mixes the own track with AIS targets and with
  passages imported into the plotter from elsewhere, and GPX does not say whose
  a track is. The import takes the names of the tracks to import (`tracks`);
  every other track is skipped and listed in the summary. In a TimeZero export
  the own track is named `Own Ship`, or the plotter's translation of it, such
  as `Eigenes Schiff`.
- **Thinning.** Points are thinned to the **Archive resolution**: each is
  stamped on the start of its interval, and the archive keeps one row per
  interval. 11.5 million points over four years become 3.2 million rows,
  about 100 MB, at 10 s.
- **Overlap.** With `fillGapsOnly` (the default), a point is left out where the
  vessel already has recorded positions in the same five minutes. Two
  receivers' fixes interleaved in one series draw a sawtooth.
- **Doing it again.** Importing the same file again changes nothing, and a
  stopped import can simply be restarted: the archive's dedup keys absorb the
  rows already written.
- **Positions only.** The import takes a position and a time from each point;
  depth, temperature or speed in a point's extensions are not imported. Depth
  along the track beyond retention comes from live recording: list
  `environment.depth.*` under **Archived own-vessel paths**, and retention keeps
  it from then on.
- **Strict reading.** Coordinates must be decimals within range and times
  `xsd:dateTime`; anything else is counted as invalid. A longitude written just
  past ±180, as TimeZero does at the antimeridian, is wrapped. A file that is
  not well-formed XML stops the import at the error, keeping the rows before
  it.

## Export and backup

**Data Export** in the panel, and
`GET /plugins/signalk-questdb/api/export?from=…&to=…&format=parquet|csv`,
download the `signalk` numeric table for a date range (both bounds required) as
Parquet — QuestDB's native export, compressed with the configured codec — or
CSV.

`GET /plugins/signalk-questdb/api/full-export/:table?from=…&to=…` streams one
whole table as Parquet, for snapshot and backup tooling that wants the full
content sliced into dedup-friendly shards (kopia, for instance). Tables:
`signalk`, `signalk_str`, `signalk_position`, `signalk_archive`, `signalk_position_archive`
(`/full-export/tables` lists them). The archive tables hold the own vessel's
history beyond retention and every imported track, so a backup that is meant to
outlive retention needs them too.

- `from` and `to` are optional but must be set together: omit both for the whole
  table, or pass both as ISO 8601 timestamps for a half-open `[from, to)` window
  — no row appears in two adjacent windows.
- Repeated query parameters (`?from=A&from=B`) and empty strings (`?from=`) are
  rejected with HTTP 400; silently downgrading to a full-table export would hide
  bugs in the caller.
- Format and compression follow the plugin's compression setting (LZ4_RAW or
  ZSTD), the same as `/export`.
