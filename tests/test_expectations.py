import json
from datetime import datetime
from pathlib import Path

import duckdb
import pytest

from lakehouse import cli
from lakehouse.quality.expectations import (
    AcceptedValues,
    InRange,
    Unique,
    accepted_values,
    in_range,
    not_null,
    unique,
)
from lakehouse.quality.suites import SUITES, run_all, run_suite
from lakehouse.reference import load_all
from lakehouse.silver import crimes, service_requests, taxi_trips
from lakehouse.storage.bronze import BronzeStore

FIX = Path(__file__).parent / "fixtures"
T = "s.t"


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE SCHEMA s")
    c.execute("CREATE TABLE s.t (id INTEGER, k VARCHAR, v DOUBLE, status VARCHAR, ts TIMESTAMP)")
    c.executemany("INSERT INTO s.t VALUES (?, ?, ?, ?, ?)", [
        (1, "a", 1.0, "OPEN", datetime(2020, 1, 1)),
        (2, "b", 5.0, "OPEN", datetime(2021, 1, 1)),
        (3, "b", 50.0, "CLOSED", datetime(1999, 1, 1)),
        (4, None, None, None, None),
        (5, None, -1.0, "BOGUS", datetime(2022, 1, 1)),
    ])
    yield c
    c.close()


# ------------------------------------------------------------------ expectation types

def test_not_null(con):
    r = not_null(T, "k").run(con)
    assert (r.passed, r.failing_rows, r.total_rows) == (False, 2, 5)
    assert r.sample == [] and r.kind == "not_null" and r.name == "not_null:k"
    assert not_null(T, "id").run(con).passed


def test_unique_ignores_nulls_and_samples_duplicates(con):
    r = unique(T, "k").run(con)
    assert (r.passed, r.failing_rows, r.sample) == (False, 2, ["b"])
    assert unique(T, "id").run(con).passed


def test_unique_multi_column(con):
    r = unique(T, "k", "status").run(con)
    assert r.passed  # (b, OPEN) and (b, CLOSED) differ
    con.execute("INSERT INTO s.t VALUES (6, 'b', 0, 'OPEN', NULL)")
    r = unique(T, "k", "status").run(con)
    assert (r.passed, r.failing_rows, r.sample) == (False, 2, [("b", "OPEN")])
    assert r.name == "unique:k+status"


@pytest.mark.parametrize("kw, failing, sample", [
    ({"min": 0}, 1, [-1.0]),
    ({"max": 10}, 1, [50.0]),
    ({"min": 0, "max": 10}, 2, [-1.0, 50.0]),
    ({"min": -5, "max": 100}, 0, []),
])
def test_in_range(con, kw, failing, sample):
    r = in_range(T, "v", **kw).run(con)
    assert (r.failing_rows, r.sample, r.passed) == (failing, sample, failing == 0)


def test_in_range_on_timestamps(con):
    r = in_range(T, "ts", min="2001-01-01").run(con)
    assert r.failing_rows == 1 and r.sample == [datetime(1999, 1, 1)]


def test_accepted_values(con):
    r = accepted_values(T, "status", ["OPEN", "CLOSED"]).run(con)
    assert (r.passed, r.failing_rows, r.sample) == (False, 1, ["BOGUS"])  # NULL ignored


def test_mostly_tolerance(con):
    assert not accepted_values(T, "status", ["OPEN", "CLOSED"]).run(con).passed
    assert accepted_values(T, "status", ["OPEN", "CLOSED"], mostly=0.8).run(con).passed  # 1/5
    assert not accepted_values(T, "status", ["OPEN", "CLOSED"], mostly=0.81).run(con).passed


def test_empty_table_passes(con):
    con.execute("DELETE FROM s.t")
    exps = (not_null(T, "k"), unique(T, "k"), in_range(T, "v", 0), accepted_values(T, "k", ["a"]))
    for e in exps:
        r = e.run(con)
        assert r.passed and r.total_rows == 0 and r.failing_share == 0.0


def test_sql_errors_become_failures(con):
    r = not_null(T, "no_such_column").run(con)
    assert not r.passed and r.error and "no_such_column" in r.error
    r = not_null("s.no_such_table", "x").run(con)
    assert not r.passed and r.error


def test_custom_name_and_to_dict(con):
    r = in_range(T, "v", 0, name="v_non_negative", description="x").run(con)
    d = r.to_dict()
    assert d["name"] == "v_non_negative" and d["columns"] == ["v"]
    assert d["failing_share"] == 0.2 and d["sample"] == ["-1.0"]  # min only: 50 is fine
    json.dumps(d)


@pytest.mark.parametrize("factory, message", [
    (lambda: in_range(T, "v"), "needs min, max or both"),
    (lambda: Unique(T), "at least one column"),
    (lambda: AcceptedValues(T, column="k"), "at least one value"),
    (lambda: InRange(T, column="v", min=0, mostly=1.5), "mostly must be within"),
])
def test_invalid_expectations(factory, message):
    with pytest.raises(ValueError, match=message):
        factory()


def test_parameters_are_bound_not_interpolated(con):
    tricky = "O'Brien'; DROP TABLE s.t; --"
    r = accepted_values(T, "k", [tricky]).run(con)
    assert r.error is None and r.failing_rows == 3
    assert con.execute("SELECT count(*) FROM s.t").fetchone() == (5,)


# --------------------------------------------------------------------------- suites

def _lakehouse() -> BronzeStore:
    store = BronzeStore(":memory:")
    for src, key, fixture, module in [
        ("chicago_crimes", "id", "chicago_crimes_sample.json", crimes),
        ("chicago_311_requests", "sr_number", "chicago_311_sample.json", service_requests),
        ("chicago_taxi_trips", "trip_id", "chicago_taxi_sample.json", taxi_trips),
    ]:
        store.write(src, json.loads((FIX / fixture).read_text()), primary_key=key)
        module.build(store)
    load_all(store)
    return store


def test_every_suite_expectation_references_real_columns():
    store = _lakehouse()
    for suite in run_all(store.con):
        assert not suite.skipped
        assert all(r.error is None for r in suite.results), [r.error for r in suite.results]
    store.close()


def test_fixture_lakehouse_results():
    store = _lakehouse()
    by_table = {s.table: s for s in run_all(store.con)}
    for table in ("silver.crimes", "silver.service_requests", "reference.community_areas",
                  "reference.police_districts"):
        assert by_table[table].passed, [r.to_dict() for r in by_table[table].failed]
    # the taxi fixture contains a deliberate negative-fare trip: 1 of 4 rows > 1 % tolerance
    taxi = by_table["silver.taxi_trips"]
    assert [r.name for r in taxi.failed] == ["in_range:fare"]
    store.close()


def test_suite_detects_bad_silver_data():
    store = _lakehouse()
    store.con.execute("UPDATE silver.service_requests SET status = 'WEIRD' WHERE sr_number = "
                      "'SR26-00000002'")
    store.con.execute("UPDATE silver.crimes SET community_area = 99 WHERE crime_id = 13001001")
    suites = {s.table: s for s in run_all(store.con, ["silver.crimes", "silver.service_requests"])}
    assert [r.name for r in suites["silver.service_requests"].failed] == ["accepted_values:status"]
    assert [r.name for r in suites["silver.crimes"].failed] == ["in_range:community_area"]
    assert suites["silver.crimes"].failed[0].sample == [99]
    store.close()


def test_missing_table_is_skipped_and_unknown_rejected():
    store = BronzeStore(":memory:")
    s = run_suite(store.con, "silver.crimes")
    assert s.skipped and s.passed and s.results == []
    with pytest.raises(ValueError, match="No suite for"):
        run_all(store.con, ["silver.nope"])
    store.close()


def test_suites_cover_every_silver_table():
    from lakehouse.silver.dictionary import MODULES

    assert {m.TABLE for m in MODULES} <= set(SUITES)
    for table, exps in SUITES.items():
        assert all(e.table == table for e in exps)
        assert len({e.label for e in exps}) == len(exps), f"duplicate labels in {table}"


# ------------------------------------------------------------------------------ CLI

def test_cli_dq(tmp_path, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write("chicago_crimes", json.loads((FIX / "chicago_crimes_sample.json").read_text()),
                primary_key="id")
    crimes.build(store)
    store.close()

    assert cli.main(["dq", "silver.crimes", "--db", str(db)]) == 0
    assert "silver.crimes: 10/10 expectations passed" in capsys.readouterr().out

    assert cli.main(["dq", "--db", str(db)]) == 0  # other tables don't exist: skipped
    assert "silver.taxi_trips: skipped" in capsys.readouterr().out

    con = duckdb.connect(str(db))
    con.execute("UPDATE silver.crimes SET ward = 0")
    con.close()
    assert cli.main(["dq", "silver.crimes", "--db", str(db)]) == 1
    out = capsys.readouterr().out
    assert "FAIL in_range:ward: 5/5 rows (100.00%), allowed 0.00% e.g. 0" in out

    assert cli.main(["dq", "silver.crimes", "--db", str(db), "--json"]) == 1
    data = json.loads(capsys.readouterr().out)
    assert data[0]["table"] == "silver.crimes" and data[0]["passed"] is False

    assert cli.main(["dq", "silver.nope", "--db", str(db)]) == 1
