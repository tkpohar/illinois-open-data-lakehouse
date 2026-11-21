import json
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.silver import crimes, geo
from lakehouse.silver import service_requests as sr
from lakehouse.silver.registry import REGISTRY, Context
from lakehouse.silver.typing import Column, build_typed_table
from lakehouse.storage.bronze import BronzeStore

FIX = Path(__file__).parent / "fixtures"
CRIMES = json.loads((FIX / "chicago_crimes_sample.json").read_text())
SR = json.loads((FIX / "chicago_311_sample.json").read_text())
SRC = "chicago_crimes"


def write(store, recs, run_id):
    store.write(SRC, recs, primary_key="id", incremental_column="updated_on", run_id=run_id)


def table(store, sql="SELECT crime_id, arrest, run_id FROM silver.crimes ORDER BY crime_id"):
    return store.con.execute(sql).fetchall()


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    yield s
    s.close()


def test_first_incremental_build_is_full(store):
    write(store, CRIMES, "r1")
    result = crimes.build(store, incremental=True)
    assert result.mode == "full" and result.full_reason == "target does not exist"
    assert result.rows == result.processed == 5


def test_no_new_rows_processes_nothing(store):
    write(store, CRIMES, "r1")
    crimes.build(store, incremental=True)
    before = table(store)
    result = crimes.build(store, incremental=True)
    assert (result.mode, result.processed, result.rows) == ("incremental", 0, 5)
    assert table(store) == before


def test_new_and_updated_rows_are_upserted(store):
    write(store, CRIMES, "r1")
    crimes.build(store, incremental=True)
    write(
        store,
        [
            {**CRIMES[0], "arrest": True, "updated_on": "2026-02-01T00:00:00.000"},  # update
            {**CRIMES[1], "id": "13009999"},  # brand-new crime
        ],
        "r2",
    )
    result = crimes.build(store, incremental=True)
    assert (result.mode, result.processed, result.rows) == ("incremental", 2, 6)
    rows = {r[0]: r for r in table(store)}
    assert rows[13001001] == (13001001, True, "r2")
    assert rows[13009999][2] == "r2"
    assert rows[13001002][2] == "r1"  # untouched rows keep their lineage
    assert len(rows) == 6


def test_incremental_equals_full_rebuild(store):
    write(store, CRIMES, "r1")
    crimes.build(store, incremental=True)
    write(store, [{**CRIMES[2], "description": "CHANGED",
                   "updated_on": "2026-03-01T00:00:00.000"}], "r2")
    write(store, [{**CRIMES[2], "description": "STALE COPY",
                   "updated_on": "2026-01-01T00:00:00.000"}], "r3")  # older version arrives late
    crimes.build(store, incremental=True)
    cols = "crime_id, description, arrest, latitude, run_id, ingested_at"
    inc = table(store, f"SELECT {cols} FROM silver.crimes ORDER BY crime_id")
    crimes.build(store)  # full
    full = table(store, f"SELECT {cols} FROM silver.crimes ORDER BY crime_id")
    assert inc == full
    assert any(r[1] == "CHANGED" for r in inc)


def test_spec_change_forces_full_build(store):
    write(store, CRIMES, "r1")
    cols = [Column("crime_id", "id", "int")]
    build_typed_table(store.con, f"bronze.{SRC}__latest", "silver.t", cols,
                      incremental=True, changes_relation=f"bronze.{SRC}")
    r = build_typed_table(store.con, f"bronze.{SRC}__latest", "silver.t",
                          [*cols, Column("arrest", "arrest", "bool")],
                          incremental=True, changes_relation=f"bronze.{SRC}")
    assert r.mode == "full" and r.full_reason == "column spec changed"
    assert "arrest" in {c[0] for c in store.con.execute("DESCRIBE silver.t").fetchall()}


def test_dropped_table_or_watermark_forces_full_build(store):
    write(store, CRIMES, "r1")
    crimes.build(store, incremental=True)
    store.con.execute("DELETE FROM silver._watermarks")
    assert crimes.build(store, incremental=True).full_reason == "no watermark"
    store.con.execute("DROP TABLE silver.crimes")
    assert crimes.build(store, incremental=True).full_reason == "target does not exist"


def test_watermark_and_build_log(store):
    write(store, CRIMES, "r1")
    crimes.build(store, incremental=True)
    write(store, [{**CRIMES[0], "id": "1"}], "r2")
    crimes.build(store, incremental=True)
    wm = store.con.execute(
        "SELECT source, max_ingested_at FROM silver._watermarks WHERE table_name = 'silver.crimes'"
    ).fetchone()
    latest = store.con.execute(f"SELECT max(ingested_at) FROM bronze.{SRC}").fetchone()[0]
    assert wm == (f"bronze.{SRC}", latest)
    log = store.con.execute(
        "SELECT mode, processed, rows FROM silver._build_log ORDER BY built_at"
    ).fetchall()
    assert log == [("full", 5, 5), ("incremental", 1, 6)]


def test_cast_failures_cover_processed_rows_quality_covers_table(store):
    store.write("chicago_311_requests", [*SR, {**SR[0], "sr_number": "BAD", "status": "??"}],
                primary_key="sr_number", incremental_column="last_modified_date")
    sr.build(store, incremental=True)
    store.write("chicago_311_requests", [{**SR[1], "sr_number": "NEW", "ward": "x"}],
                primary_key="sr_number", incremental_column="last_modified_date")
    r = sr.build(store, incremental=True)
    assert r.processed == 1 and r.cast_failures == {"ward": 1}
    assert r.quality["unknown_status"] == 1  # the earlier bad row is still in the table


def test_failed_incremental_build_rolls_back(store, monkeypatch):
    write(store, CRIMES, "r1")
    crimes.build(store, incremental=True)
    write(store, [{**CRIMES[0], "arrest": True, "updated_on": "2026-05-01T00:00:00.000"}], "r2")
    before = table(store)
    wm_before = store.con.execute("SELECT * FROM silver._watermarks").fetchall()
    with pytest.raises(Exception):  # noqa: B017 - any DuckDB error
        build_typed_table(store.con, f"bronze.{SRC}__latest", "silver.crimes", crimes.COLUMNS,
                          derived=[("boom", "no_such_function()")], incremental=True,
                          changes_relation=f"bronze.{SRC}")
    assert table(store) == before
    assert store.con.execute("SELECT * FROM silver._watermarks").fetchall() == wm_before


def test_geo_columns_survive_incremental_and_registry_reapplies_geo(store):
    write(store, CRIMES, "r1")
    REGISTRY.run(store, ["crimes"])
    write(store, [{**CRIMES[0], "id": "42", "latitude": "0", "longitude": "0"}], "r2")
    steps = {s.name: s for s in REGISTRY.run(store, ["crimes"])}
    assert steps["crimes"].result.mode == "incremental"
    assert steps["crimes"].result.processed == 1
    status = dict(store.con.execute(
        "SELECT crime_id, geo_status FROM silver.crimes").fetchall())
    assert status[42] == "null_island" and status[13001001] == "ok"
    assert isinstance(steps["crimes_geo"].result, geo.GeoResult)


def test_registry_full_refresh(store):
    write(store, CRIMES, "r1")
    REGISTRY.run(store, ["crimes"])
    step = REGISTRY.run(store, ["crimes"], Context(full_refresh=True))[0]
    assert step.result.mode == "full" and step.result.full_reason is None


def test_cli_incremental_and_full_refresh(tmp_path, capsys):
    db = tmp_path / "lh.duckdb"
    s = BronzeStore(db)
    write(s, CRIMES, "r1")
    s.close()
    assert cli.main(["silver", "crimes", "--db", str(db)]) == 0
    assert "full build (target does not exist)" in capsys.readouterr().out
    assert cli.main(["silver", "crimes", "--db", str(db)]) == 0
    assert "incremental: 0 rows processed" in capsys.readouterr().out
    assert cli.main(["silver", "crimes", "--db", str(db), "--full-refresh"]) == 0
    out = capsys.readouterr().out
    assert "incremental" not in out and "full build" not in out
