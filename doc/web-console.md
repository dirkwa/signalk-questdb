# Exploring Your Data with the QuestDB Web Console

QuestDB ships with a built-in web interface — the **Web Console** — where you
can browse everything the plugin has recorded and run SQL queries against it.
No extra software needed, just a browser.

This guide is for end users: it shows you how to open the console, what your
data looks like, and gives you a set of ready-to-paste queries you can adapt.

## Opening the Web Console

The console runs on QuestDB's HTTP port (default **9000**).

- **On the Signal K host itself:** open <http://localhost:9000>
- **From another computer** (e.g. your laptop), you have two options:
  - Tunnel over SSH (keeps QuestDB private):

    ```
    ssh -L 9000:127.0.0.1:9000 <user>@<signalk-host>
    ```

    then open <http://localhost:9000> on your laptop.

  - Or enable **"Bind to 0.0.0.0"** in the plugin config and open
    `http://<signalk-host-ip>:9000`. This exposes QuestDB to your whole
    network, and anyone who can reach port 9000 can not only read but also
    **modify or delete** your history — the SSH tunnel is the safer default;
    only bind to 0.0.0.0 behind a firewall you trust.

## A Quick Tour

- The **left panel** lists the tables. Click a table to see its columns.
- The **editor** at the top is where you type SQL. Put the cursor on a query
  and press **Run** (or `Ctrl+Enter` / `F9`) to execute it. You can keep many
  queries in the editor — only the one under the cursor runs.
- Results appear in the **grid** below. Use the **chart** view to plot a
  result (pick your time column as the X axis), and the **download** button to
  save results as CSV.

> **Careful:** the console is not read-only. Stick to `SELECT` queries —
> statements like `DROP TABLE` will really delete your history.

## Your Data: Three Tables

| Table              | What's in it   | Columns                              |
| ------------------ | -------------- | ------------------------------------ |
| `signalk`          | Numeric values | `ts`, `path`, `context`, `value`     |
| `signalk_str`      | Text values    | `ts`, `path`, `context`, `value_str` |
| `signalk_position` | GPS positions  | `ts`, `context`, `lat`, `lon`        |

- **`ts`** — timestamp of the sample (UTC).
- **`path`** — the Signal K path, e.g. `navigation.speedOverGround` or
  `environment.wind.speedApparent`.
- **`context`** — which vessel. Your own boat is `self`. Other vessels only
  appear if you enabled **Record AIS targets**. The examples below filter on
  `context = 'self'` where it matters, so they show only your own boat either
  way.

### Units: everything is SI

Signal K stores all values in SI units. Convert in the query to get familiar
numbers:

| You want       | Stored as | Conversion           |
| -------------- | --------- | -------------------- |
| knots          | m/s       | `value * 1.94384`    |
| °C             | Kelvin    | `value - 273.15`     |
| degrees        | radians   | `value * 180 / pi()` |
| hPa / mbar     | Pascal    | `value / 100`        |
| nautical miles | meters    | `value / 1852.0`     |

## Sample Queries

All of these are copy-paste ready. Adjust paths and time windows to taste —
`dateadd('d', -7, now())` means "7 days ago"; use `'h'` for hours, `'m'` for
minutes.

### What is being recorded?

Lists every recorded numeric path with its sample count and time range — a
good first query to discover the exact path names on _your_ boat:

```sql
SELECT path, count() AS samples,
       min(ts) AS first_seen, max(ts) AS last_seen
FROM signalk
GROUP BY path
ORDER BY samples DESC;
```

Run the same query against `signalk_str` to list the recorded text paths
(positions are always in `signalk_position`).

### Current snapshot of every numeric value

The most recent sample of each path (QuestDB's `LATEST ON` finds the newest
row per group very efficiently):

```sql
SELECT path, value, ts
FROM signalk
WHERE context = 'self'
LATEST ON ts PARTITION BY path
ORDER BY path;
```

### Last known position

```sql
SELECT ts, lat, lon
FROM signalk_position
WHERE context = 'self'
  AND lat IS NOT NULL
ORDER BY ts DESC
LIMIT 1;
```

### Speed over ground, hourly average in knots

`SAMPLE BY` is QuestDB's time-bucketing: this averages all samples within each
hour. Switch to the chart view to plot it.

```sql
SELECT ts, avg(value) * 1.94384 AS sog_knots
FROM signalk
WHERE path = 'navigation.speedOverGround'
  AND context = 'self'
  AND ts > dateadd('d', -2, now())
SAMPLE BY 1h;
```

### Wind: hourly average and gusts

```sql
SELECT ts,
       avg(value) * 1.94384 AS avg_knots,
       max(value) * 1.94384 AS gust_knots
FROM signalk
WHERE path = 'environment.wind.speedApparent'
  AND context = 'self'
  AND ts > dateadd('d', -1, now())
SAMPLE BY 1h;
```

### Outside temperature, daily min / avg / max in °C

```sql
SELECT ts,
       min(value) - 273.15 AS min_c,
       avg(value) - 273.15 AS avg_c,
       max(value) - 273.15 AS max_c
FROM signalk
WHERE path = 'environment.outside.temperature'
  AND context = 'self'
SAMPLE BY 1d;
```

### Barometric pressure trend (hPa)

A falling barometer is worth knowing about — chart this one:

```sql
SELECT ts, avg(value) / 100 AS pressure_hpa
FROM signalk
WHERE path = 'environment.outside.pressure'
  AND context = 'self'
  AND ts > dateadd('d', -3, now())
SAMPLE BY 3h;
```

### Battery voltage, daily min / max

Replace `66` with your battery instance (find it with the "What is being
recorded?" query):

```sql
SELECT ts, min(value) AS min_v, max(value) AS max_v
FROM signalk
WHERE path = 'electrical.batteries.66.voltage'
  AND context = 'self'
SAMPLE BY 1d;
```

### Distance travelled per day (nautical miles)

Uses the vessel's log (total distance counter in meters): last reading minus
first reading of each day.

```sql
SELECT ts, (last(value) - first(value)) / 1852.0 AS nm_travelled
FROM signalk
WHERE path = 'navigation.log'
  AND context = 'self'
SAMPLE BY 1d
ORDER BY ts DESC;
```

### Your track, one point per 10 minutes

Thinned-out positions, e.g. to paste into another tool or export as CSV:

```sql
SELECT ts, last(lat) AS lat, last(lon) AS lon
FROM signalk_position
WHERE context = 'self'
  AND lat IS NOT NULL
  AND ts > dateadd('d', -7, now())
SAMPLE BY 10m;
```

### Text values: GPS fix quality

Non-numeric data lives in `signalk_str`. For example, how good has the GPS fix
been?

```sql
SELECT value_str, count()
FROM signalk_str
WHERE path = 'navigation.gnss.methodQuality'
  AND context = 'self'
GROUP BY value_str;
```

### How much data is stored?

Samples per day, and disk usage per daily partition:

```sql
SELECT ts, count() AS samples
FROM signalk
SAMPLE BY 1d;
```

```sql
SELECT name, numRows, diskSizeHuman
FROM table_partitions('signalk')
ORDER BY name DESC;
```

## Tips

- **Always narrow by `path` and time.** The `signalk` table can hold hundreds
  of millions of rows; `WHERE path = '...' AND ts > dateadd(...)` keeps
  queries fast. Add `LIMIT 100` while experimenting.
- **`SAMPLE BY` is your friend.** Raw data arrives every couple of seconds;
  bucketing to `1m`, `1h` or `1d` with `avg()`/`min()`/`max()` gives readable
  results and nice charts.
- **Timestamps are UTC.** Convert for display with e.g.
  `to_timezone(ts, 'Pacific/Fiji')`.
- **Prefer dashboards for daily use.** The console is great for ad-hoc
  digging; for permanent charts use the companion
  [signalk-grafana](https://github.com/dirkwa/signalk-grafana) plugin.
- The full SQL reference is in the
  [QuestDB documentation](https://questdb.com/docs/reference/sql/overview/).
