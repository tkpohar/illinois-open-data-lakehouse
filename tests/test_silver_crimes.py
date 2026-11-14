import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from lakehouse import cli
from lakehouse.silver import crimes
from lakehouse.silver.typing import Column, build_typed_table, typed_expr
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = "chicago_crimes"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def _store(recs):
    store = BronzeStore(":memory:")
    store.write(SRC, recs, primary_key="id", incremental_column="updated_on", run_id="r1")
    return store


def _rows(store, where=""):
    cur = store.con.execute(f"SELECT * FROM silver.crimes {where} ORDER BY crime_id")
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def test_builds_typed_table_from_fixture(records):
    store = _store(records)
    result = crimes.build(store)
    assert result.table == "silver.crimes" and result.rows == 5
    assert result.cast_failures == {}
    row = _rows(store)[0]
    assert row["crime_id"] == 13001001
    assert row["case_number"] == "JH100001"
    assert row["occurred_at"] == datetime(2025, 12, 30, 22, 15)
    assert row["updated_at"] == datetime(2026, 1, 2, 15, 40, 12)
    assert row["primary_type"] == "THEFT"
    assert row["arrest"] is False and row["domestic"] is False
    assert row["district"] == 18  # "018" -> 18
    assert row["community_area"] == 8
    assert row["latitude"] == pytest.approx(41.8925)
    assert row["longitude"] == pytest.approx(-87.6242)
    assert row["record_key"] == "13001001" and row["run_id"] == "r1"
    assert isinstance(row["silver_built_at"], datetime)
    store.close()


def test_column_types(records):
    store = _store(records)
    crimes.build(store)
    types = dict(
        store.con.execute(
            "SELECT column_name, data_type FROM information_schema.columns "
            "WHERE table_schema = 'silver' AND table_name = 'crimes'"
        ).fetchall()
    )
    assert types["crime_id"] == "BIGINT"
    assert types["occurred_at"] == types["updated_at"] == "TIMESTAMP"
    assert types["arrest"] == types["domestic"] == "BOOLEAN"
    assert types["latitude"] == "DOUBLE"
    assert types["iucr"] == types["beat"] == "VARCHAR"
    assert [c.name for c in crimes.COLUMNS] == list(types)[: len(crimes.COLUMNS)]
    store.close()


def test_uses_latest_version_only(records):
    store = _store(records)
    store.write(
        SRC,
        [{**records[0], "arrest": True, "updated_on": "2026-02-01T00:00:00.000"}],
        primary_key="id",
        incremental_column="updated_on",
        run_id="r2",
    )
    crimes.build(store)
    rows = _rows(store)
    assert len(rows) == 5
    first = rows[0]
    assert first["arrest"] is True and first["run_id"] == "r2"
    store.close()


def test_messy_values_are_cleaned_or_counted(records):
    messy = {
        **records[0],
        "id": "99",
        "case_number": "  jh9 ",
        "arrest": "Y",
        "domestic": "N",
        "iucr": "486",
        "beat": "331",
        "primary_type": "  criminal   damage ",
        "latitude": "not-a-number",
        "date": "31/12/2025",
        "community_area": "8.0",
        "ward": "12.5",
        "block": "",
        "fbi_code": "08b",
    }
    store = _store([messy])
    result = crimes.build(store)
    (row,) = _rows(store)
    assert row["case_number"] == "JH9"
    assert row["arrest"] is True and row["domestic"] is False
    assert row["iucr"] == "0486" and row["beat"] == "0331"
    assert row["primary_type"] == "CRIMINAL DAMAGE"
    assert row["community_area"] == 8
    assert row["latitude"] is None and row["occurred_at"] is None and row["ward"] is None
    assert row["block"] is None  # blank -> NULL, not a cast failure
    assert row["fbi_code"] == "08B"
    assert result.cast_failures == {"occurred_at": 1, "ward": 1, "latitude": 1}
    store.close()


def test_missing_fields_are_null_not_failures(records):
    sparse = {"id": "7", "updated_on": "2026-01-01T00:00:00.000"}
    store = _store([sparse])
    result = crimes.build(store)
    (row,) = _rows(store)
    assert row["crime_id"] == 7 and row["arrest"] is None and row["latitude"] is None
    assert result.cast_failures == {}
    store.close()


@pytest.mark.parametrize(
    "raw, expected",
    [("true", True), ("TRUE", True), ("t", True), ("Y", True), ("yes", True), ("1", True),
     ("false", False), ("F", False), ("n", False), ("No", False), ("0", False),
     ("maybe", None), ("", None)],
)
def test_bool_parsing(raw, expected):
    con = duckdb.connect()
    payload = json.dumps({"b": raw})
    expr = typed_expr(Column("b", "b", "bool"))
    assert con.execute(f"SELECT {expr} FROM (SELECT ?::JSON AS payload)", [payload]).fetchone()[
        0
    ] is expected


def test_json_booleans_are_parsed():
    con = duckdb.connect()
    expr = typed_expr(Column("b", "b", "bool"))
    for raw, expected in [(True, True), (False, False)]:
        got = con.execute(
            f"SELECT {expr} FROM (SELECT ?::JSON AS payload)", [json.dumps({"b": raw})]
        ).fetchone()[0]
        assert got is expected


def test_column_spec_validation():
    with pytest.raises(ValueError, match="Unknown kind"):
        Column("x", "x", "money")
    with pytest.raises(ValueError, match="needs a width"):
        Column("x", "x", "code")


def test_build_log_and_rebuild_is_idempotent(records):
    store = _store(records)
    crimes.build(store)
    crimes.build(store)
    assert store.con.execute("SELECT count(*) FROM silver.crimes").fetchone()[0] == 5
    log = store.con.execute(
        "SELECT table_name, source, rows, cast_failures FROM silver._build_log"
    ).fetchall()
    assert len(log) == 2
    assert log[0][:3] == ("silver.crimes", "bronze.chicago_crimes__latest", 5)
    assert json.loads(log[0][3]) == {}
    store.close()


def test_build_without_bronze_raises():
    store = BronzeStore(":memory:")
    with pytest.raises(ValueError, match="No bronze data"):
        crimes.build(store)
    store.close()


def test_failed_build_rolls_back(records):
    store = _store(records)
    crimes.build(store)
    with pytest.raises(duckdb.Error):
        build_typed_table(
            store.con, "bronze.does_not_exist", "silver.crimes", crimes.COLUMNS
        )
    assert store.con.execute("SELECT count(*) FROM silver.crimes").fetchone()[0] == 5
    assert store.con.execute("SELECT count(*) FROM silver._build_log").fetchone()[0] == 1
    store.close()


def test_cli_silver_crimes(tmp_path, records, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write(SRC, [*records, {**records[0], "id": "5", "latitude": "x"}], primary_key="id")
    store.close()
    assert cli.main(["silver", "crimes", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "silver.crimes: 6 rows from bronze.chicago_crimes__latest" in out
    assert "cast failures: latitude=1" in out

    assert cli.main(["silver", "crimes", "--db", str(tmp_path / "empty.duckdb")]) == 1
