"""silver.service_requests. The fixture is synthetic, shaped like dataset v6vf-nfxy."""

import json
from datetime import datetime
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.silver import service_requests as sr
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_311_sample.json"
SRC = "chicago_311_requests"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def _store(recs):
    store = BronzeStore(":memory:")
    store.write(
        SRC, recs, primary_key="sr_number", incremental_column="last_modified_date", run_id="r1"
    )
    return store


def _rows(store):
    cur = store.con.execute("SELECT * FROM silver.service_requests ORDER BY sr_number")
    cols = [d[0] for d in cur.description]
    return {r[0]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


def test_builds_typed_table(records):
    store = _store(records)
    result = sr.build(store)
    assert result.table == "silver.service_requests" and result.rows == 4
    assert result.cast_failures == {}
    row = _rows(store)["SR26-00000001"]
    assert row["sr_type"] == "POTHOLE IN STREET COMPLAINT"
    assert row["owner_department"] == "CDOT - DEPARTMENT OF TRANSPORTATION"
    assert row["created_at"] == datetime(2026, 1, 2, 8, 15)
    assert row["closed_at"] == datetime(2026, 1, 4, 14, 45)
    assert row["zip_code"] == "60607"
    assert row["community_area"] == 28 and row["ward"] == 27 and row["police_district"] == 12
    assert row["duplicate"] is False and row["latitude"] == pytest.approx(41.8818)
    assert row["record_key"] == "SR26-00000001" and row["run_id"] == "r1"
    store.close()


def test_status_normalisation_and_derived_columns(records):
    store = _store(records)
    sr.build(store)
    rows = _rows(store)
    got = {
        k: (r["status_raw"], r["status"], r["is_open"], r["is_duplicate"])
        for k, r in rows.items()
    }
    assert got == {
        "SR26-00000001": ("Completed", "COMPLETED", False, False),
        "SR26-00000002": ("Open", "OPEN", True, False),
        "SR26-00000003": ("Open - Dup", "OPEN", True, True),
        "SR26-00000004": ("Canceled", "CANCELED", False, False),
    }
    assert rows["SR26-00000001"]["resolution_hours"] == pytest.approx(54.5)
    assert rows["SR26-00000002"]["resolution_hours"] is None
    assert rows["SR26-00000004"]["resolution_hours"] == pytest.approx(1.67)
    store.close()


@pytest.mark.parametrize(
    "raw, expected",
    [
        ("Open", "OPEN"),
        ("  open  ", "OPEN"),
        ("OPEN - DUP", "OPEN"),
        ("Open-Dup", "OPEN"),
        ("In Progress", "OPEN"),
        ("Completed", "COMPLETED"),
        ("complete", "COMPLETED"),
        ("Closed", "COMPLETED"),
        ("Cancelled", "CANCELED"),
        ("CANCELED", "CANCELED"),
        ("Pending review", "UNKNOWN"),
    ],
)
def test_status_variants(records, raw, expected):
    store = _store([{**records[1], "status": raw}])
    sr.build(store)
    (row,) = _rows(store).values()
    assert row["status"] == expected
    assert row["status"] in sr.STATUSES
    store.close()


def test_missing_status_is_unknown(records):
    rec = {k: v for k, v in records[1].items() if k != "status"}
    store = _store([rec])
    result = sr.build(store)
    (row,) = _rows(store).values()
    assert row["status_raw"] is None and row["status"] == "UNKNOWN"
    assert result.quality["unknown_status"] == 1
    store.close()


def test_quality_checks_count_inconsistencies(records):
    bad = [
        {**records[1], "sr_number": "A", "closed_date": "2026-01-05T00:00:00.000"},
        {k: v for k, v in records[0].items() if k != "closed_date"} | {"sr_number": "B"},
        {**records[0], "sr_number": "C", "closed_date": "2026-01-01T00:00:00.000"},
        {**records[0], "sr_number": "D", "status": "Mystery"},
        records[0],
    ]
    store = _store(bad)
    result = sr.build(store)
    assert result.quality == {
        "unknown_status": 1,
        "open_with_closed_date": 1,
        "completed_without_closed_date": 1,
        "closed_before_created": 1,
    }
    rows = _rows(store)
    assert len(rows) == 5  # quality issues never drop rows
    assert rows["C"]["resolution_hours"] is None  # closed before created
    store.close()


def test_build_log_records_quality(records):
    store = _store(records)
    sr.build(store)
    (quality,) = store.con.execute(
        "SELECT quality FROM silver._build_log WHERE table_name = 'silver.service_requests'"
    ).fetchone()
    assert json.loads(quality) == {
        "unknown_status": 0,
        "open_with_closed_date": 0,
        "completed_without_closed_date": 0,
        "closed_before_created": 0,
    }
    store.close()


def test_zip_and_bool_cleaning(records):
    rec = {**records[0], "zip_code": "606", "duplicate": "N", "legacy_record": "Y",
           "ward": "x"}
    store = _store([rec])
    result = sr.build(store)
    (row,) = _rows(store).values()
    assert row["zip_code"] == "00606"
    assert row["duplicate"] is False and row["legacy_record"] is True
    assert row["ward"] is None and result.cast_failures == {"ward": 1}
    store.close()


def test_latest_version_wins(records):
    store = _store(records)
    store.write(
        SRC,
        [{**records[1], "status": "Completed", "closed_date": "2026-01-06T12:00:00.000",
          "last_modified_date": "2026-01-06T12:00:00.000"}],
        primary_key="sr_number",
        incremental_column="last_modified_date",
        run_id="r2",
    )
    sr.build(store)
    row = _rows(store)["SR26-00000002"]
    assert row["status"] == "COMPLETED" and row["run_id"] == "r2"
    assert row["resolution_hours"] == pytest.approx(72.0)
    store.close()


def test_crimes_build_log_still_works_alongside(records):
    from lakehouse.silver import crimes

    store = _store(records)
    sr.build(store)
    store.write("chicago_crimes", [{"id": "1", "arrest": "true"}], primary_key="id")
    crimes.build(store)
    tables = [
        r[0]
        for r in store.con.execute("SELECT table_name FROM silver._build_log").fetchall()
    ]
    assert tables == ["silver.service_requests", "silver.crimes"]
    store.close()


def test_missing_bronze_raises():
    store = BronzeStore(":memory:")
    with pytest.raises(ValueError, match="No bronze data"):
        sr.build(store)
    store.close()


def test_cli_silver_service_requests(tmp_path, records, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write(SRC, [*records, {**records[0], "sr_number": "X", "status": "??"}],
                primary_key="sr_number")
    store.close()
    assert cli.main(["silver", "service_requests", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "silver.service_requests: 5 rows" in out
    assert "quality: unknown_status=1" in out
