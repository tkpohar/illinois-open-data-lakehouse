# Silver: `silver.taxi_trips`

Chicago taxi trips (`wrvz-psew`) typed and cleaned, **one row per trip ID**, built from
`bronze.chicago_taxi_trips__latest`. It uses the same typing rules as
[silver.crimes](silver-crimes.md), plus fare/duration metrics and **outlier flags**.

```bash
lakehouse ingest chicago_taxi_trips
lakehouse silver taxi_trips
```

```
silver.taxi_trips: 5000 rows from bronze.chicago_taxi_trips__latest
  quality: is_outlier=37
  quality: outlier_total_mismatch=21
  ...
```

## Typed columns

| Column | Type | Raw field | Description |
|---|---|---|---|
| `trip_id` | VARCHAR | `trip_id` | Unique trip ID |
| `taxi_id` | VARCHAR | `taxi_id` | Anonymised taxi ID |
| `trip_start_at` | TIMESTAMP | `trip_start_timestamp` | Trip start, rounded to 15 minutes (Chicago local time) |
| `trip_end_at` | TIMESTAMP | `trip_end_timestamp` | Trip end, rounded to 15 minutes (Chicago local time) |
| `trip_seconds` | BIGINT | `trip_seconds` | Trip duration in seconds |
| `trip_miles` | DOUBLE | `trip_miles` | Trip distance in miles |
| `pickup_census_tract` | VARCHAR | `pickup_census_tract` | Pickup census tract (often blank for privacy) |
| `dropoff_census_tract` | VARCHAR | `dropoff_census_tract` | Dropoff census tract |
| `pickup_community_area` | BIGINT | `pickup_community_area` | Pickup community area (1-77) |
| `dropoff_community_area` | BIGINT | `dropoff_community_area` | Dropoff community area (1-77) |
| `fare` | DOUBLE | `fare` | Fare in dollars |
| `tips` | DOUBLE | `tips` | Tip in dollars |
| `tolls` | DOUBLE | `tolls` | Tolls in dollars |
| `extras` | DOUBLE | `extras` | Extra charges in dollars |
| `trip_total` | DOUBLE | `trip_total` | Total charged in dollars |
| `payment_type` | VARCHAR | `payment_type` | Payment method |
| `company` | VARCHAR | `company` | Taxi company |
| `pickup_latitude` | DOUBLE | `pickup_centroid_latitude` | Pickup census-tract (or area) centroid latitude |
| `pickup_longitude` | DOUBLE | `pickup_centroid_longitude` | Pickup centroid longitude |
| `dropoff_latitude` | DOUBLE | `dropoff_centroid_latitude` | Dropoff centroid latitude |
| `dropoff_longitude` | DOUBLE | `dropoff_centroid_longitude` | Dropoff centroid longitude |

Lineage columns `record_key`, `run_id`, `ingested_at`, `silver_built_at` are added as for
every silver table. The portal rounds trip timestamps to 15 minutes and publishes pickup
and dropoff locations as census-tract or community-area **centroids**, not exact points.

## Derived metrics

| Column | Rule |
|---|---|
| `duration_minutes` | `trip_seconds / 60`, else end − start (2 dp) |
| `avg_speed_mph` | `trip_miles / hours`, when `trip_seconds > 0` |
| `fare_per_mile` | `fare / trip_miles`, when `trip_miles > 0` |
| `tip_pct` | `100 × tips / fare`, when `fare > 0` |

## Outlier flags

Implausible trips are **flagged, never dropped**, so analyses can choose to exclude
them (`WHERE NOT is_outlier`) or study them. Each row has:

- `outlier_reasons` — `VARCHAR[]` of every rule the trip breaks (empty if none);
- `is_outlier` — `len(outlier_reasons) > 0`.

| Rule | Flags a trip when |
|---|---|
| `negative_amount` | any of fare, tips, tolls, extras, trip_total is negative (refunds/disputes) |
| `nonpositive_duration` | `trip_seconds` ≤ 0, or the trip ends before it starts |
| `too_long_duration` | `trip_seconds` > 21600 (6 h) |
| `too_long_distance` | `trip_miles` > 200 |
| `implausible_speed` | average speed > 90 mph |
| `zero_distance_with_fare` | 0 miles but a positive fare and duration |
| `extreme_total` | `trip_total` > $1,000 |
| `total_mismatch` | `trip_total` differs from fare + tips + tolls + extras by more than $0.05 |

Rules only fire on values that are present: a missing fare or distance never flags a
trip. Thresholds are module constants in `lakehouse.silver.taxi_trips`
(`MAX_TRIP_SECONDS`, `MAX_TRIP_MILES`, `MAX_SPEED_MPH`, `MAX_TRIP_TOTAL`,
`TOTAL_TOLERANCE`). They are deliberately generous: their job is to catch broken records,
not to judge unusual but real trips.

Each build counts flagged trips overall (`is_outlier`) and per rule (`outlier_<rule>`).
The counts are printed, logged and stored in the `quality` column of `silver._build_log`.

```sql
SELECT unnest(outlier_reasons) AS reason, count(*)
FROM silver.taxi_trips GROUP BY 1 ORDER BY 2 DESC;
```

The taxi test fixture (`tests/fixtures/chicago_taxi_sample.json`) is synthetic, shaped like
the real dataset.
