# Recording

## Schema

Three tables, all with WAL mode, daily partitioning, and deduplication:

| Table              | Purpose        | Columns                                                                                                    |
| ------------------ | -------------- | ---------------------------------------------------------------------------------------------------------- |
| `signalk`          | Numeric values | `ts`, `path` (SYMBOL), `context` (SYMBOL), `source` (SYMBOL), `value` (DOUBLE)                             |
| `signalk_str`      | String values  | `ts`, `path` (SYMBOL), `context` (SYMBOL), `source` (SYMBOL), `value_str` (VARCHAR), `value_kind` (SYMBOL) |
| `signalk_position` | Positions      | `ts`, `context` (SYMBOL), `source` (SYMBOL), `lat` (DOUBLE), `lon` (DOUBLE)                                |

What retention keeps of the own vessel lives in two more tables,
`signalk_archive` and `signalk_position_archive`, with the same columns and
monthly partitions (see Retention and the archive).

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

## What is recorded

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

A number that means "no reading" is skipped and reported the same way: `NaN`
and `±Infinity`, and the NMEA 2000 "no data" marker on AC real, apparent and
reactive power (PGNs 65007–65029). Those fields cannot encode anything below
−2 000 000 000 W, but canboatjs decodes "no data" as −2 000 000 001 W, so real
and reactive power below that floor are dropped; real negative power, such as
export, is unaffected. Apparent power reaches the stream multiplied by the power
factor, which can move the marker anywhere below zero — and apparent power is
never negative, so any negative apparent power is dropped. Connections using the
canboat WASM decoder do not produce the marker at all.

**Sampling.** Each path is written at most once per **Default sampling rate**
(2000 ms), with per-path overrides by glob for the few paths that need finer
resolution and the many that need less. The first value after a window opens
is written and the rest of the window is dropped, not averaged, so a gust
between two samples is not in the history. A rate of 0, as default or per
path, removes the window: `environment.wind.* 0` records every change of the
wind at wire speed while everything else stays at the default. An unchanged
value is still written only once per **Unchanged-value heartbeat** (see
Change-only recording), except `navigation.position`, which is written even
when unchanged; set the heartbeat to 0 as well to record every update. Wire
speed costs rows: a wind sensor at 10 Hz writes about 1.7 million a day for
angle and speed, and every path of a busy NMEA 2000 boat at wire speed can be
a few hundred rows a second. **Record own vessel** and **Record AIS targets**
switch the two kinds of context independently, and **Path patterns** exclude
(or include only) paths by glob.

**Source priorities.** When Signal K's source priorities rank the sources of a
path, only the preferred source is recorded, the same value instruments show.
Paths without a priority are recorded from every source. **Record all sources**
records the sources a priority hides as well, for calibrating a sensor or
comparing two GPS receivers. Each source is then sampled on its own, so a path
with three sources writes up to three times the rows; with the option off, the
sources of a path share one sampling window.

**Change-only recording.** Most paths on a boat do not change from one sample
to the next: a switch that flips twice a day, an energy counter at rest, a tank
nobody is drawing from, a notification's state. Sampling alone would write each
of them at the sampling rate forever, and those repeats make up most of the
rows. So a value equal to the last one written for its path, vessel and source
is skipped until the **Unchanged-value heartbeat** is due, and then
written once more. Every change is written as it arrives, subject to the
sampling rate.

The heartbeat is what keeps the gaps readable. A path that is still being
updated has a row at least once per heartbeat, so a longer silence means the
data really stopped:

- **History API (v2).** A resolution bucket with no row takes the last sample
  before it, for up to twice the heartbeat (see [History API](history-api.md)). A switch that has
  been on for an hour reads as on for the whole hour, not as one point and
  then nothing.
- **Stale data.** When Signal K marks a path stale it sends `null` for it.
  Nothing is written for the `null`, so the history shows the last value for at
  most twice the heartbeat and then a gap. The next reading after that is
  always written, even if it equals the one before the gap.
- **Direct SQL and Grafana** see the rows as stored. Add `FILL(PREV)` to a
  `SAMPLE BY` query to carry values across buckets.
- **Averages** weight rows, not time. A bucket in which a value changes often
  and then holds steady averages mostly over the changes; `min`, `max`, `first`
  and `last` are exact.
- **Startup restore** replays motion values recorded within **Restore max
  age**. Keep the heartbeat below that, or a vessel lying still may come back
  with its position but without its speed, course and heading.

`navigation.position` is written even when unchanged. A heartbeat of 0 does the
same for every path: each sample the sampling rate lets through is written. With
a sampling rate of 0 as well, that is every update.

**Batching.** Samples are buffered and committed every **Write batch interval**
(5 s), one WAL transaction per table per commit. The buffer holds 100 000 lines;
if QuestDB is unreachable for longer than that covers, the oldest lines are
dropped and counted. A clean Signal K shutdown flushes first; a hard crash loses
at most one interval.

**Compression** applies LZ4 or ZSTD to the WAL segments on disk (and to
Parquet exports).

## Retention and the archive

Retention drops whole daily partitions once they lie entirely before the
configured number of days; 0 keeps everything. QuestDB cannot delete single
rows, so keeping part of a day means copying it out first. Before a day of
`signalk_position` or `signalk` is dropped, the own vessel's rows that the
settings ask to keep are copied into an archive table, thinned to one row per
**Archive resolution** interval, and the day is dropped only once QuestDB has
applied the copy. A copy that fails leaves the day in place for the next run.

| Table                      | Holds                                                           |
| -------------------------- | --------------------------------------------------------------- |
| `signalk_position_archive` | Own-vessel positions (**Keep own track**, on by default)        |
| `signalk_archive`          | Own-vessel numeric paths matching **Archived own-vessel paths** |

Both have the columns and dedup keys of their base table and are partitioned by
month. Retention never drops from them, other vessels are never archived, and
strings are not archived. The History API reads the archive together with the
base tables for any range it covers, so a track or a depth series continues
past the retention boundary without a seam. An archived row is the first sample
of its interval and is stamped on the interval's start.

Size, measured: a position row takes 32 bytes, so a vessel recorded around the
clock fills about 100 MB a year at a 10 s resolution, 34 MB at 30 s.

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
with no position in the window is skipped. Nothing the Signal K model already
holds is replaced: a vessel heard while QuestDB was still starting keeps its
newer position, and after a plugin restart every vessel keeps the one it has.
Restore honours the recording toggles: with **Record AIS targets** off, no AIS
target is restored.

The config panel reports the outcome once startup finishes: **Vessels Restored**
with the count, or **Restore failed** if the replay could not run. Neither
appears when the option is off.
