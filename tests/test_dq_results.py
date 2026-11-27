import json
from datetime import date, datetime, timedelta
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.quality import results as qr
from lakehouse.quality.anomalies import AnomalyResult
from lakehouse.quality.expectations import in_range, not_null
from lakehouse.quality.freshness import FreshnessResult
from lakehouse.quality.suites import SuiteResult, run_suite
from lakehouse.silver import crimes
from lakehouse.storage.bronze import BronzeStore

FIX = Path(__file__).parent / "fixtures"
CRIMES = json.loads((FIX / "chicago_crimes_sample.json").read_text())


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    s.write("chicago_crimes", CRIMES, primary_key="id")
    crimes.build(s)
    yield s
    s.close()


def suite(store, area_max=77):
    # community_area is populated in the fixture (6..44); ward is not (all NULL)
    return run_suite(store.con, "silver.crimes", [
        not_null("silver.crimes", "crime_id"),
        in_range("silver.crimes", "community_area", 1, area_max),
    ])


def fresh(status):
    return FreshnessResult("chicago_crimes", "ingestion", status, 5.0, 36, 72, None, "x")


def anomaly(status, day=date(2026, 11, 26)):
    return AnomalyResult("events:silver.crimes", day, 42, status, 40.0, 1.0, 2.0)


# -------------------------------------------------------------------- normalisation

def test_status_normalisation(store):
    rows = (qr.from_suites([suite(store), SuiteResult("silver.x", skipped=True)])
            + qr.from_freshness([fresh("ok"), fresh("warn"), fresh("error")])
            + qr.from_anomalies([anomaly("ok"), anomaly("anomaly"),
                                 anomaly("insufficient_history")]))
    assert [(r.check_type, r.status) for r in rows] == [
        ("expectation", "pass"), ("expectation", "pass"), ("expectation", "skipped"),
        ("freshness", "pass"), ("freshness", "warn"), ("freshness", "fail"),
        ("anomaly", "pass"), ("anomaly", "fail"), ("anomaly", "skipped"),
    ]
    assert rows[1].check_name == "in_range:community_area" and rows[1].observed == 0.0
    assert rows[6].check_name == "z_score:2026-11-26" and rows[6].observed == 42.0


def test_failing_expectation_recorded_with_details(store):
    rows = qr.from_suites([suite(store, area_max=1)])
    failed = [r for r in rows if r.status == "fail"]
    assert len(failed) == 1 and failed[0].observed > 0
    assert failed[0].details["kind"] == "in_range" and failed[0].details["sample"]


# -------------------------------------------------------------------------- writing

def test_record_writes_results_and_run(store):
    run = qr.record(store.con, qr.from_suites([suite(store)]) + qr.from_freshness([fresh("warn")]))
    assert run.counts == {"pass": 2, "warn": 1, "fail": 0, "skipped": 0}
    assert run.worst == "warn"
    n, kinds = store.con.execute(
        "SELECT count(*), list(DISTINCT check_type ORDER BY check_type) FROM quality.results"
    ).fetchone()
    assert n == 3 and kinds == ["expectation", "freshness"]
    stored = store.con.execute("SELECT * FROM quality.check_runs").fetchall()
    assert len(stored) == 1 and stored[0][0] == run.check_run_id and stored[0][4:] == (2, 1, 0, 0)
    detail = json.loads(store.con.execute(
        "SELECT details FROM quality.results WHERE check_type = 'freshness'").fetchone()[0])
    assert detail["status"] == "warn" and detail["warn_after_hours"] == 36


def test_appending_to_an_existing_check_run(store):
    run = qr.record(store.con, qr.from_freshness([fresh("ok")]))
    again = qr.record(store.con, qr.from_anomalies([anomaly("anomaly")]), run.check_run_id)
    assert again.check_run_id == run.check_run_id
    assert again.counts == {"pass": 1, "warn": 0, "fail": 1, "skipped": 0}
    assert store.con.execute("SELECT count(*) FROM quality.check_runs").fetchone() == (1,)


def test_invalid_status_rejected_atomically(store):
    bad = qr.ResultRow("expectation", "t", "c", "maybe")
    with pytest.raises(ValueError, match="invalid status"):
        qr.record(store.con, [qr.ResultRow("expectation", "t", "ok", "pass"), bad])
    assert store.con.execute("SELECT count(*) FROM quality.results").fetchone() == (0,)


def test_worst_of_empty_run():
    assert qr.CheckRun("x", datetime.now()).worst == "skipped"


# -------------------------------------------------------------------------- history

def _set_times(store, start=datetime(2026, 11, 20, 3, 42)):
    """Give each check run its own day, in insertion order."""
    ids = [r[0] for r in store.con.execute(
        "SELECT check_run_id FROM quality.check_runs ORDER BY rowid").fetchall()]
    for i, cid in enumerate(ids):
        ts = start + timedelta(days=i)
        store.con.execute("UPDATE quality.results SET checked_at = ? WHERE check_run_id = ?",
                          [ts, cid])
        store.con.execute("UPDATE quality.check_runs SET started_at = ?, finished_at = ? "
                          "WHERE check_run_id = ?", [ts, ts, cid])


def test_latest_with_streaks_and_last_pass(store):
    for status in ("ok", "ok", "error", "error", "error"):
        qr.record(store.con, qr.from_freshness([fresh(status)]))
    for status in ("anomaly", "ok"):
        qr.record(store.con, qr.from_anomalies([anomaly(status)]))
    _set_times(store)
    rows = {r["check_type"]: r for r in qr.latest(store.con)}
    f = rows["freshness"]
    assert (f["status"], f["streak"]) == ("fail", 3)
    assert f["last_pass_at"] == datetime(2026, 11, 21, 3, 42)
    a = rows["anomaly"]
    assert (a["status"], a["streak"], a["check_name"]) == ("pass", 1, "z_score:2026-11-26")
    assert [r["status"] for r in qr.latest(store.con)] == ["fail", "pass"]  # failures first


def test_never_passed(store):
    qr.record(store.con, qr.from_freshness([fresh("error")]))
    (row,) = qr.latest(store.con)
    assert row["streak"] == 1 and row["last_pass_at"] is None


def test_history_filters_and_order(store):
    qr.record(store.con, qr.from_suites([suite(store)]))
    qr.record(store.con, qr.from_suites([suite(store, area_max=1)]))
    _set_times(store)
    h = qr.history(store.con, check_name="in_range:community_area")
    assert [r["status"] for r in h] == ["fail", "pass"]  # newest first
    assert len(qr.history(store.con, status="fail")) == 1
    assert len(qr.history(store.con, target="silver.crimes", limit=3)) == 3
    assert qr.history(store.con, check_type="freshness") == []


def test_pass_rate_by_day(store):
    qr.record(store.con, qr.from_suites([suite(store)]))
    qr.record(store.con, qr.from_suites([suite(store, area_max=1)])
              + qr.from_anomalies([anomaly("insufficient_history")]))
    today = datetime.now().replace(microsecond=0)
    _set_times(store, start=today - timedelta(days=1))
    rows = qr.pass_rate_by_day(store.con, days=7)
    rates = {(r["day"], r["check_type"]): (r["checks"], r["pass_rate"]) for r in rows}
    assert rates[(today.date(), "expectation")] == (2, 0.5)
    assert rates[(today.date() - timedelta(days=1), "expectation")] == (2, 1.0)
    assert rates[(today.date(), "anomaly")] == (0, None)  # only a skip


def test_queries_on_empty_db():
    s = BronzeStore(":memory:")
    assert qr.latest(s.con) == [] and qr.history(s.con) == [] and qr.runs(s.con) == []
    s.close()


# ------------------------------------------------------------------------------ CLI

def test_cli_record_flags_and_checks_commands(tmp_path, capsys):
    db = tmp_path / "lh.duckdb"
    s = BronzeStore(db)
    s.write("chicago_crimes", CRIMES, primary_key="id", incremental_column="updated_on")
    crimes.build(s)
    s.close()
    d = ["--db", str(db)]

    assert cli.main(["dq", "silver.crimes", *d, "--record"]) == 0
    assert cli.main(["freshness", "chicago_crimes", *d, "--record"]) == 1  # data is old
    cli.main(["anomalies", *d, "--record"])
    capsys.readouterr()

    assert cli.main(["checks", "latest", *d]) == 0
    out = capsys.readouterr().out
    assert "FAIL" in out and "freshness" in out and "chicago_crimes: data" in out

    assert cli.main(["checks", "runs", *d]) == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == 3

    assert cli.main(["checks", "history", *d, "--check", "unique:crime_id", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert [r["status"] for r in data] == ["pass"]

    assert cli.main(["checks", "trend", *d]) == 0
    assert "expectation" in capsys.readouterr().out

    assert cli.main(["checks", "run", *d]) == 1  # freshness errors for un-ingested sources
    out = capsys.readouterr().out
    assert out.startswith("check run ") and "FAIL freshness" in out
    assert cli.main(["checks", "runs", *d]) == 0
    assert len(capsys.readouterr().out.strip().splitlines()) == 4


def test_cli_checks_empty(tmp_path, capsys):
    assert cli.main(["checks", "latest", "--db", str(tmp_path / "e.duckdb")]) == 0
    assert "no stored results" in capsys.readouterr().out
