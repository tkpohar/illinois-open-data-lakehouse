# Data quality: expectations

Data-quality rules are written as **expectations**: small, declarative statements about a
table that compile to one SQL query each. They are grouped into a **suite** per table
and run with:

```bash
lakehouse dq                       # every suite (missing tables are skipped)
lakehouse dq silver.crimes         # one table
lakehouse dq --json                # machine-readable results
```

```
silver.crimes: 9/10 expectations passed
  FAIL in_range:ward: 3/5000 rows (0.06%), allowed 0.00% e.g. 0, 51
silver.taxi_trips: skipped (table does not exist)
```

The exit status is 1 if any expectation failed, so the command can gate a pipeline or CI.

## Expectation types

| Type | Factory | Fails a row when | NULLs |
|---|---|---|---|
| not null | `not_null(table, col)` | the value is NULL | they are the failures |
| unique | `unique(table, *cols)` | another row has the same value(s) | ignored |
| in range | `in_range(table, col, min=None, max=None)` | value < min or value > max (bounds inclusive; either optional; numbers, dates, timestamps) | ignored |
| accepted values | `accepted_values(table, col, values)` | value not in the set | ignored |

Each run returns an `ExpectationResult`: `passed`, `failing_rows`, `total_rows`,
`failing_share`, up to 5 distinct offending values in `sample` (not for not-null), and
`error`. A SQL error, such as a missing column, is recorded and counts as a failure
rather than crashing the run. Values are always bound as query parameters, never pasted
into SQL.

### Tolerance: `mostly`

Real open data is never perfect. `mostly` is the share of rows that must pass:
`mostly=1.0` (default) allows no failures; `mostly=0.99` passes while at most 1 % of rows
fail. It is used where imperfection is expected and handled downstream, such as
coordinates, which are cleaned by [geo cleanup](geo-cleanup.md), or taxi outliers, which
are flagged in [silver taxi](silver-taxi.md).

## Suites

Declared in `lakehouse.quality.suites.SUITES`:

| Table | Expectations |
|---|---|
| `silver.crimes` | `not_null:crime_id`<br>`unique:crime_id`<br>`not_null:occurred_at` mostly 0.999<br>`in_range:occurred_at` [2001-01-01, ∞]<br>`in_range:year` [2001, 2100]<br>`in_range:community_area` [1, 77]<br>`in_range:ward` [1, 50]<br>`in_range:district` [1, 31]<br>`in_range:latitude` [41.6, 42.05] mostly 0.99<br>`in_range:longitude` [-87.95, -87.5] mostly 0.99 |
| `silver.service_requests` | `not_null:sr_number`<br>`unique:sr_number`<br>`not_null:created_at`<br>`accepted_values:status` {OPEN, COMPLETED, CANCELED, UNKNOWN}<br>`in_range:community_area` [1, 77]<br>`in_range:ward` [1, 50]<br>`in_range:resolution_hours` [0, ∞]<br>`in_range:latitude` [41.6, 42.05] mostly 0.99<br>`in_range:longitude` [-87.95, -87.5] mostly 0.99 |
| `silver.taxi_trips` | `not_null:trip_id`<br>`unique:trip_id`<br>`not_null:trip_start_at`<br>`in_range:pickup_community_area` [1, 77]<br>`in_range:dropoff_community_area` [1, 77]<br>`in_range:trip_seconds` [0, ∞] mostly 0.99<br>`in_range:fare` [0, ∞] mostly 0.99 |
| `reference.community_areas` | `not_null:community_area`<br>`unique:community_area`<br>`in_range:community_area` [1, 77]<br>`not_null:name`<br>`unique:name` |
| `reference.police_districts` | `not_null:district`<br>`unique:district`<br>`in_range:district` [1, 25]<br>`not_null:name` |

Latitude and longitude bounds are the padded Chicago bounding box used by geo cleanup.
Accepted 311 statuses are the canonical values from [status normalisation](silver-311.md).

## Adding an expectation

```python
from lakehouse.quality.expectations import in_range
from lakehouse.quality.suites import SUITES

SUITES["silver.taxi_trips"].append(in_range("silver.taxi_trips", "tip_pct", 0, 100, mostly=0.99))
```

Freshness per source is checked separately ([freshness](freshness.md)).

Daily volumes are screened separately ([anomalies](anomalies.md)).

Coming next in this phase: a persistent
results table with history, an HTML report, and fail-fast vs warn severities.
