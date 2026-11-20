"""``silver.taxi_trips``: typed Chicago taxi trips with fares, durations and outlier flags.

Built from ``bronze.chicago_taxi_trips__latest``. Column names follow the Socrata
dataset ``wrvz-psew``. Implausible trips are **flagged, not dropped**: each row carries
``outlier_reasons`` (a list of rule names) and ``is_outlier``. See ``docs/silver-taxi.md``.
"""

from __future__ import annotations

from lakehouse.silver.typing import (
    BuildResult,
    Column,
    MissingInputError,
    build_typed_table,
)
from lakehouse.storage.bronze import LATEST_SUFFIX, BronzeStore

SOURCE = "chicago_taxi_trips"
TABLE = "silver.taxi_trips"

# Thresholds for the outlier rules (documented in docs/silver-taxi.md).
MAX_TRIP_SECONDS = 6 * 3600  # longer than 6 hours
MAX_TRIP_MILES = 200.0
MAX_SPEED_MPH = 90.0
MAX_TRIP_TOTAL = 1000.0  # dollars
TOTAL_TOLERANCE = 0.05  # dollars between trip_total and the sum of its components

COLUMNS: list[Column] = [
    Column("trip_id", "trip_id", "text", description="Unique trip ID"),
    Column("taxi_id", "taxi_id", "text", description="Anonymised taxi ID"),
    Column("trip_start_at", "trip_start_timestamp", "timestamp",
           description="Trip start, rounded to 15 minutes (Chicago local time)"),
    Column("trip_end_at", "trip_end_timestamp", "timestamp",
           description="Trip end, rounded to 15 minutes (Chicago local time)"),
    Column("trip_seconds", "trip_seconds", "int", description="Trip duration in seconds"),
    Column("trip_miles", "trip_miles", "double", description="Trip distance in miles"),
    Column("pickup_census_tract", "pickup_census_tract", "text",
           description="Pickup census tract (often blank for privacy)"),
    Column("dropoff_census_tract", "dropoff_census_tract", "text",
           description="Dropoff census tract"),
    Column("pickup_community_area", "pickup_community_area", "int",
           description="Pickup community area (1-77)"),
    Column("dropoff_community_area", "dropoff_community_area", "int",
           description="Dropoff community area (1-77)"),
    Column("fare", "fare", "double", description="Fare in dollars"),
    Column("tips", "tips", "double", description="Tip in dollars"),
    Column("tolls", "tolls", "double", description="Tolls in dollars"),
    Column("extras", "extras", "double", description="Extra charges in dollars"),
    Column("trip_total", "trip_total", "double", description="Total charged in dollars"),
    Column("payment_type", "payment_type", "upper", description="Payment method"),
    Column("company", "company", "upper", description="Taxi company"),
    Column("pickup_latitude", "pickup_centroid_latitude", "double",
           description="Pickup census-tract (or area) centroid latitude"),
    Column("pickup_longitude", "pickup_centroid_longitude", "double",
           description="Pickup centroid longitude"),
    Column("dropoff_latitude", "dropoff_centroid_latitude", "double",
           description="Dropoff centroid latitude"),
    Column("dropoff_longitude", "dropoff_centroid_longitude", "double",
           description="Dropoff centroid longitude"),
]

_COMPONENTS = (
    "coalesce(fare, 0) + coalesce(tips, 0) + coalesce(tolls, 0) + coalesce(extras, 0)"
)
_SPEED = (
    "CASE WHEN trip_seconds > 0 AND trip_miles IS NOT NULL "
    "THEN trip_miles / (trip_seconds / 3600.0) END"
)

#: Outlier rules: name -> SQL condition over the typed columns. A NULL condition
#: (missing inputs) does not flag the row.
OUTLIER_RULES: dict[str, str] = {
    "negative_amount": "least(fare, tips, tolls, extras, trip_total) < 0",
    "nonpositive_duration": "trip_seconds <= 0 OR trip_end_at < trip_start_at",
    "too_long_duration": f"trip_seconds > {MAX_TRIP_SECONDS}",
    "too_long_distance": f"trip_miles > {MAX_TRIP_MILES}",
    "implausible_speed": f"({_SPEED}) > {MAX_SPEED_MPH}",
    "zero_distance_with_fare": "trip_miles = 0 AND fare > 0 AND trip_seconds > 0",
    "extreme_total": f"trip_total > {MAX_TRIP_TOTAL}",
    "total_mismatch": f"abs(trip_total - ({_COMPONENTS})) > {TOTAL_TOLERANCE}",
}

_REASONS = (
    "flatten(["
    + ", ".join(
        f"CASE WHEN {cond} THEN ['{name}'] ELSE []::VARCHAR[] END"
        for name, cond in OUTLIER_RULES.items()
    )
    + "])"
)

DERIVED: list[tuple[str, str]] = [
    (
        "duration_minutes",
        "round(coalesce(trip_seconds / 60.0, "
        "date_diff('second', trip_start_at, trip_end_at) / 60.0), 2)",
    ),
    ("avg_speed_mph", f"round({_SPEED}, 2)"),
    (
        "fare_per_mile",
        "CASE WHEN trip_miles > 0 AND fare IS NOT NULL THEN round(fare / trip_miles, 2) END",
    ),
    (
        "tip_pct",
        "CASE WHEN fare > 0 AND tips IS NOT NULL THEN round(100.0 * tips / fare, 1) END",
    ),
    ("outlier_reasons", _REASONS),
    ("is_outlier", f"len({_REASONS}) > 0"),
]

QUALITY_CHECKS: dict[str, str] = {
    "is_outlier": "is_outlier",
    **{f"outlier_{name}": f"list_contains(outlier_reasons, '{name}')" for name in OUTLIER_RULES},
}


def build(store: BronzeStore) -> BuildResult:
    """Rebuild ``silver.taxi_trips`` from the latest bronze version of every trip."""
    view = f"{SOURCE}{LATEST_SUFFIX}"
    exists = store.con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'bronze' AND table_name = ?",
        [view],
    ).fetchone()[0]
    if not exists:
        raise MissingInputError(
            f"No bronze data for '{SOURCE}'; run `lakehouse ingest {SOURCE}` first"
        )
    return build_typed_table(
        store.con,
        f"bronze.{view}",
        TABLE,
        COLUMNS,
        derived=DERIVED,
        quality_checks=QUALITY_CHECKS,
    )
