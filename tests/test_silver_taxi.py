"""silver.taxi_trips. The fixture is synthetic, shaped like dataset wrvz-psew."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.silver import taxi_trips as taxi
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_taxi_sample.json"
SRC = "chicago_taxi_trips"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def _store(recs):
    store = BronzeStore(":memory:")
    store.write(SRC, recs, primary_key="trip_id", incremental_column="trip_start_timestamp")
    return store


def _rows(store):
    cur = store.con.execute("SELECT * FROM silver.taxi_trips ORDER BY trip_id")
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


def _one(rec):
    store = _store([rec])
    result = taxi.build(store)
    (row,) = _rows(store).values()
    store.close()
    return row, result


def test_builds_typed_table(records):
    store = _store(records)
    result = taxi.build(store)
    assert result.table == "silver.taxi_trips" and result.rows == 4
    assert result.cast_failures == {}
    row = _rows(store)["t-normal"]
    assert row["trip_start_at"] == datetime(2023, 6, 1, 8, 0)
    assert row["trip_seconds"] == 900 and row["trip_miles"] == pytest.approx(3.5)
    assert row["fare"] == pytest.approx(12.25) and row["trip_total"] == pytest.approx(16.25)
    assert row["payment_type"] == "CREDIT CARD" and row["company"] == "FLASH CAB"
    assert row["pickup_census_tract"] == "17031081500"
    assert row["pickup_community_area"] == 8 and row["dropoff_community_area"] == 32
    assert row["pickup_latitude"] == pytest.approx(41.8992)
    store.close()


def test_derived_metrics(records):
    store = _store(records)
    taxi.build(store)
    row = _rows(store)["t-normal"]
    assert row["duration_minutes"] == pytest.approx(15.0)
    assert row["avg_speed_mph"] == pytest.approx(14.0)
    assert row["fare_per_mile"] == pytest.approx(3.5)
    assert row["tip_pct"] == pytest.approx(24.5)
    assert row["outlier_reasons"] == [] and row["is_outlier"] is False
    store.close()


def test_fixture_outliers(records):
    store = _store(records)
    result = taxi.build(store)
    rows = _rows(store)
    assert rows["t-airport"]["is_outlier"] is False
    assert rows["t-negative"]["outlier_reasons"] == ["negative_amount"]
    assert set(rows["t-teleport"]["outlier_reasons"]) == {"implausible_speed", "total_mismatch"}
    assert result.quality["is_outlier"] == 2
    assert result.quality["outlier_negative_amount"] == 1
    assert result.quality["outlier_implausible_speed"] == 1
    assert result.quality["outlier_too_long_distance"] == 0
    store.close()


@pytest.mark.parametrize(
    "changes, reason",
    [
        ({"trip_seconds": "0"}, "nonpositive_duration"),
        ({"trip_end_timestamp": "2023-06-01T07:45:00.000"}, "nonpositive_duration"),
        ({"trip_seconds": str(7 * 3600), "trip_miles": "100"}, "too_long_duration"),
        ({"trip_miles": "250", "trip_seconds": str(5 * 3600)}, "too_long_distance"),
        ({"trip_miles": "0"}, "zero_distance_with_fare"),
        ({"fare": "1200", "trip_total": "1204.25"}, "extreme_total"),
        ({"tips": "-1", "trip_total": "12.25"}, "negative_amount"),
        ({"trip_total": "99.99"}, "total_mismatch"),
    ],
)
def test_outlier_rules(records, changes, reason):
    row, result = _one({**records[0], **changes})
    assert reason in row["outlier_reasons"]
    assert row["is_outlier"] is True
    assert result.quality[f"outlier_{reason}"] == 1


def test_small_rounding_difference_is_not_a_mismatch(records):
    row, _ = _one({**records[0], "trip_total": "16.29"})
    assert "total_mismatch" not in row["outlier_reasons"]


def test_missing_values_do_not_flag(records):
    sparse = {"trip_id": "t-sparse", "trip_start_timestamp": "2023-06-03T00:00:00.000"}
    row, result = _one(sparse)
    assert row["outlier_reasons"] == [] and row["is_outlier"] is False
    assert row["duration_minutes"] is None and row["avg_speed_mph"] is None
    assert row["fare_per_mile"] is None and row["tip_pct"] is None
    assert result.cast_failures == {}


def test_duration_falls_back_to_timestamps(records):
    rec = {k: v for k, v in records[1].items() if k != "trip_seconds"}
    row, _ = _one(rec)
    assert row["trip_seconds"] is None
    assert row["duration_minutes"] == pytest.approx(45.0)


def test_cast_failures_counted(records):
    row, result = _one({**records[0], "fare": "twelve", "trip_seconds": "15m"})
    assert row["fare"] is None and row["trip_seconds"] is None
    assert result.cast_failures == {"fare": 1, "trip_seconds": 1}


def test_thresholds_are_used_in_rules():
    assert str(taxi.MAX_TRIP_SECONDS) in taxi.OUTLIER_RULES["too_long_duration"]
    assert str(taxi.MAX_SPEED_MPH) in taxi.OUTLIER_RULES["implausible_speed"]
    assert set(taxi.QUALITY_CHECKS) == {"is_outlier"} | {
        f"outlier_{n}" for n in taxi.OUTLIER_RULES
    }


def test_missing_bronze_raises():
    store = BronzeStore(":memory:")
    with pytest.raises(ValueError, match="No bronze data"):
        taxi.build(store)
    store.close()


def test_cli_silver_taxi_trips(tmp_path, records, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write(SRC, records, primary_key="trip_id")
    store.close()
    assert cli.main(["silver", "taxi_trips", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "silver.taxi_trips: 4 rows" in out
    assert "quality: is_outlier=2" in out
    assert "quality: outlier_negative_amount=1" in out
