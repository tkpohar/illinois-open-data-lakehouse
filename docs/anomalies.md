# Row-count anomaly detection

A source that silently delivers a tenth of its usual volume, or a sudden tenfold spike
caused by a re-publication, passes every per-row expectation. `lakehouse anomalies`
watches **daily row counts** and flags days that don't fit the recent past.

```bash
lakehouse anomalies                             # last day of every series with data
lakehouse anomalies events:silver.crimes --days 7
lakehouse anomalies --seasonal --window 56      # compare Mondays with Mondays, ...
lakehouse anomalies --threshold 4 --json
```

```
2026-11-25  ingestion:chicago_crimes               4812  ok: z=+0.41 vs mean 4655.2 (std 380.1, n=28)
2026-11-25  ingestion:chicago_311_requests          312  ANOMALY (low): z=-4.87 vs mean 5120.4 (std 987.2, n=28)
2026-11-18  events:silver.crimes                    611  ok: z=-0.62 vs mean 684.0 (std 117.3, n=28)
```

The exit status is 1 if any day is anomalous.

## Series

| Series | Count per day | Tells you |
|---|---|---|
| `ingestion:<source>` | records ingested, by run date (`bronze._ingestion_log`) | the feed's delivery volume changed |
| `events:silver.crimes` | crimes by `occurred_at` date | the volume of incidents changed (or records went missing) |
| `events:silver.service_requests` | 311 requests by `created_at` date | ditto for 311 |
| `events:silver.taxi_trips` | trips by `trip_start_at` date | ditto for taxi trips |

Only series with data in the database are evaluated by default.

## Method

For a day *d* with count *x*:

1. **Baseline:** the counts of the `window` days before *d* (default 28). *d* itself is
   never part of its baseline, so a spike can't dilute its own reference. Days in the
   window without rows count as **0**, since a day with no data is itself a signal, but
   days before the series' first observation are ignored.
2. **Score:** `z = (x − mean) / std` (sample standard deviation).
3. **Verdict:** `|z| > threshold` (default 3) is an **anomaly**, `high` or `low`. A flat
   baseline (std 0) flags any change. Fewer than `min_history` baseline days (default 7)
   gives `insufficient_history` instead of a verdict.

By default the last day of each series is checked (`--days N` checks the last N days,
each against its own trailing window).

### Seasonality

Crimes and 311 have strong weekly cycles. Against a mixed weekday and weekend baseline
the standard deviation is large, so unusual weekend values hide inside it. For example,
in the tests a Saturday with weekday-sized volume scores only z ≈ +0.6. With
`--seasonal` the baseline is the same weekday over the last `window // 7` weeks, and that
Saturday is flagged. Use `--seasonal --window 56` (8 weeks) for weekly-cyclic series.

### Caveats

- **Event dates lag.** Recent days are often incomplete because the portals publish with
  a delay (about a week for crimes; see [freshness](freshness.md)), so the newest event
  days look low. Judge event series on days older than that lag
  (e.g. `--days 14` and read the older entries) or rely on ingestion series for recency.
- The z-score assumes roughly stable volume within the window. Holidays and genuine
  trends will occasionally be flagged. They are worth a look, not an outage.
- Historical datasets (taxi up to 2023, Urbana arrests) have no new event days. Their
  ingestion series mainly catch re-publications.

## In code

```python
from lakehouse.quality.anomalies import detect, load_series, run_checks

counts = load_series(con, "events:silver.crimes")         # {date: count}
detect(counts, day, window=56, seasonal=True, min_history=4)
run_checks(con, days=7)                                     # every series with data
```
