import json
import math
from datetime import date, datetime, timedelta

import pytest

from lakehouse import cli
from lakehouse.quality.anomalies import (
    baseline,
    detect,
    load_series,
    run_checks,
    scan,
    series_names,
)
from lakehouse.storage.bronze import BronzeStore

D0 = date(2026, 11, 2)  # a Monday
NOISE = [100, 104, 97, 101, 99, 103, 96, 102, 98, 100]


def steady(days=28, start=D0):
    return {start + timedelta(days=i): NOISE[i % len(NOISE)] for i in range(days)}


def test_normal_day_is_ok():
    s = steady()
    day = D0 + timedelta(days=28)
    s[day] = 101
    r = detect(s, day)
    assert r.status == "ok" and r.history_points == 28
    assert r.mean == pytest.approx(100.07, abs=0.01) and abs(r.z) < 1


@pytest.mark.parametrize("value, direction", [(300, "high"), (0, "low"), (60, "low")])
def test_spikes_and_drops_are_anomalies(value, direction):
    s = steady()
    day = D0 + timedelta(days=28)
    s[day] = value
    r = detect(s, day)
    assert r.status == "anomaly" and r.direction == direction and abs(r.z) > 3


def test_missing_target_day_counts_as_zero():
    s = steady()
    r = detect(s, D0 + timedelta(days=28))  # no entry for the day at all
    assert r.value == 0 and r.status == "anomaly" and r.direction == "low"


def test_gaps_in_baseline_count_as_zero_but_not_before_first_day():
    s = {D0: 10, D0 + timedelta(days=3): 10}
    assert baseline(s, D0 + timedelta(days=4), window=28) == [10, 0, 0, 10]


def test_target_day_excluded_from_its_own_baseline():
    s = steady()
    day = D0 + timedelta(days=28)
    s[day] = 10_000
    assert 10_000 not in baseline(s, day)


def test_insufficient_history():
    s = {D0 + timedelta(days=i): 100 for i in range(5)}
    r = detect(s, D0 + timedelta(days=5), min_history=7)
    assert r.status == "insufficient_history" and r.z is None and r.history_points == 5


def test_flat_baseline():
    s = {D0 + timedelta(days=i): 50 for i in range(14)}
    day = D0 + timedelta(days=14)
    s[day] = 50
    assert detect(s, day).status == "ok" and detect(s, day).z == 0
    s[day] = 51
    r = detect(s, day)
    assert r.status == "anomaly" and math.isinf(r.z) and r.to_dict()["z"] == "inf"


def test_threshold_and_window_are_configurable():
    s = steady()
    day = D0 + timedelta(days=28)
    s[day] = 110  # ~ +4 std
    assert detect(s, day, threshold=3).status == "anomaly"
    assert detect(s, day, threshold=5).status == "ok"
    assert detect(s, day, window=7).history_points == 7


def test_seasonal_mode_absorbs_weekly_cycle():
    # weekdays ~100, weekends ~20; 8 weeks of history
    s = {}
    for i in range(56):
        d = D0 + timedelta(days=i)
        s[d] = (20 + i % 3) if d.weekday() >= 5 else (100 + i % 5)
    saturday = D0 + timedelta(days=61)
    assert saturday.weekday() == 5
    s[saturday] = 21  # a normal Saturday: fine either way
    assert detect(s, saturday, window=56).status == "ok"
    r = detect(s, saturday, window=56, seasonal=True, min_history=4)
    assert r.status == "ok" and r.history_points == 8
    # A weekday-sized Saturday is the real anomaly. Against the mixed weekday/weekend
    # baseline it hides inside the large std; against other Saturdays it stands out.
    s[saturday] = 100
    plain = detect(s, saturday, window=56)
    assert plain.status == "ok" and plain.std > 30
    seasonal = detect(s, saturday, window=56, seasonal=True, min_history=4)
    assert seasonal.status == "anomaly" and seasonal.direction == "high"


def test_scan_last_days():
    s = steady(30)
    s[D0 + timedelta(days=29)] = 500
    results = scan(s, "x", days=3)
    assert [r.day for r in results] == [D0 + timedelta(days=d) for d in (27, 28, 29)]
    assert [r.status for r in results] == ["ok", "ok", "anomaly"]
    assert scan({}, "x") == []


# ---------------------------------------------------------------- series from the DB

def _store_with_history():
    store = BronzeStore(":memory:")
    for i in range(20):
        n = 300 if i == 19 else 10 + i % 3
        recs = [{"id": f"{i}-{k}", "date": f"2026-10-{i + 1:02d}T12:00:00"} for k in range(n)]
        store.write("chicago_crimes", recs, primary_key="id", run_id=f"r{i}")
        store.con.execute("UPDATE bronze._ingestion_log SET run_started = ? WHERE run_id = ?",
                          [datetime(2026, 10, i + 1, 3, 41), f"r{i}"])
    return store


def test_ingestion_and_event_series():
    from lakehouse.silver import crimes

    store = _store_with_history()
    crimes.build(store)
    assert series_names(store.con) == ["ingestion:chicago_crimes", "events:silver.crimes"]
    ing = load_series(store.con, "ingestion:chicago_crimes")
    ev = load_series(store.con, "events:silver.crimes")
    assert ing[date(2026, 10, 1)] == 10 and ing[date(2026, 10, 20)] == 300
    assert ev == ing  # one crime per record, dated on the same day
    results = run_checks(store.con, min_history=7)
    assert [(r.series, r.status, r.direction) for r in results] == [
        ("ingestion:chicago_crimes", "anomaly", "high"),
        ("events:silver.crimes", "anomaly", "high"),
    ]
    store.close()


def test_unknown_series_and_empty_db():
    store = BronzeStore(":memory:")
    with pytest.raises(ValueError, match="Unknown series"):
        load_series(store.con, "events:silver.nope")
    assert run_checks(store.con) == []
    store.close()


def test_cli_anomalies(tmp_path, capsys):
    db = tmp_path / "lh.duckdb"
    store = _store_with_history()
    store.con.execute(f"ATTACH '{db}' AS f")
    store.con.execute("CREATE SCHEMA f.bronze")
    store.con.execute("CREATE TABLE f.bronze._ingestion_log AS SELECT * FROM bronze._ingestion_log")
    store.close()

    assert cli.main(["anomalies", "--db", str(db)]) == 1
    out = capsys.readouterr().out
    assert "ingestion:chicago_crimes" in out and "ANOMALY (high)" in out

    assert cli.main(["anomalies", "--db", str(db), "--threshold", "1000"]) == 0
    capsys.readouterr()
    assert cli.main(["anomalies", "ingestion:chicago_crimes", "--db", str(db), "--json",
                     "--days", "2"]) == 1
    data = json.loads(capsys.readouterr().out)
    assert [d["status"] for d in data] == ["ok", "anomaly"]
    assert cli.main(["anomalies", "events:silver.nope", "--db", str(db)]) == 1
    assert cli.main(["anomalies", "--db", str(tmp_path / "empty.duckdb")]) == 0
    assert "no series with data" in capsys.readouterr().out
