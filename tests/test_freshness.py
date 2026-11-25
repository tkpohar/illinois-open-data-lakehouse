import json
from datetime import datetime, timedelta, timezone

import pytest

from lakehouse import cli
from lakehouse.config import FreshnessPolicy, SourceConfig, load_sources
from lakehouse.quality.freshness import check_all, check_source, parse_timestamp, worst
from lakehouse.storage.bronze import BronzeStore

NOW = datetime(2026, 11, 25, 12, 0, tzinfo=timezone.utc)
DAILY = SourceConfig("feed", "d.org", "abcd-1234", "id", "updated_on",
                     freshness=FreshnessPolicy(36, 72, 48, 120))
HISTORICAL = SourceConfig("old", "d.org", "abcd-5678", "id", "updated_on")


def ingest(store, source, updated_on, finished_hours_ago, run_id="r"):
    store.write(source.name, [{"id": run_id, "updated_on": updated_on}], primary_key="id",
                incremental_column="updated_on", run_id=run_id)
    finished = (NOW - timedelta(hours=finished_hours_ago)).replace(tzinfo=None)
    store.con.execute(
        "UPDATE bronze._ingestion_log SET run_finished = ? WHERE run_id = ?", [finished, run_id])


def by_check(results):
    return {r.check: r for r in results}


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    yield s
    s.close()


@pytest.mark.parametrize("run_age, data_age, ingestion, data", [
    (1, 1, "ok", "ok"),
    (40, 1, "warn", "ok"),
    (80, 1, "error", "ok"),
    (1, 60, "ok", "warn"),
    (1, 130, "ok", "error"),
])
def test_grading(store, run_age, data_age, ingestion, data):
    newest = (NOW - timedelta(hours=data_age)).replace(tzinfo=None).isoformat(
        timespec="milliseconds")
    ingest(store, DAILY, newest, run_age)
    r = by_check(check_source(store, DAILY, NOW))
    assert (r["ingestion"].status, r["data"].status) == (ingestion, data)
    assert r["ingestion"].age_hours == pytest.approx(run_age)
    assert r["data"].age_hours == pytest.approx(data_age)


def test_boundaries_are_exclusive(store):
    ingest(store, DAILY, (NOW - timedelta(hours=48)).replace(tzinfo=None).isoformat(), 36)
    r = by_check(check_source(store, DAILY, NOW))
    assert r["ingestion"].status == "ok" and r["data"].status == "ok"  # exactly at the limit


def test_never_ingested_is_error(store):
    r = by_check(check_source(store, DAILY, NOW))
    assert r["ingestion"].status == "error" and r["ingestion"].reason == "never ingested"
    assert r["data"].status == "error" and r["data"].reason == "no watermark yet"


def test_latest_run_and_watermark_are_used(store):
    ingest(store, DAILY, "2026-11-01T00:00:00.000", 200, "old")
    ingest(store, DAILY, "2026-11-25T06:00:00.000", 2, "new")
    r = by_check(check_source(store, DAILY, NOW))
    assert r["ingestion"].age_hours == pytest.approx(2) and r["data"].age_hours == 6


def test_historical_source_only_checks_ingestion(store):
    ingest(store, HISTORICAL, "2019-01-01T00:00:00.000", 1)
    results = check_source(store, HISTORICAL, NOW)
    assert [r.check for r in results] == ["ingestion"] and results[0].status == "ok"


def test_unparseable_watermark_is_error(store):
    ingest(store, DAILY, "yesterday-ish", 1)
    r = by_check(check_source(store, DAILY, NOW))
    assert r["data"].status == "error" and "unparseable" in r["data"].reason


@pytest.mark.parametrize("value, expected", [
    ("2026-11-25T06:00:00.000", datetime(2026, 11, 25, 6, tzinfo=timezone.utc)),
    ("2026-11-25T06:00:00Z", datetime(2026, 11, 25, 6, tzinfo=timezone.utc)),
    ("2026-11-25T01:00:00-05:00", datetime(2026, 11, 25, 6, tzinfo=timezone.utc)),
    ("2026-11-25", datetime(2026, 11, 25, tzinfo=timezone.utc)),
    ("nope", None), ("", None), (None, None),
])
def test_parse_timestamp(value, expected):
    assert parse_timestamp(value) == expected


def test_future_timestamps_have_zero_age(store):
    ingest(store, DAILY, "2027-01-01T00:00:00.000", 0)
    assert by_check(check_source(store, DAILY, NOW))["data"].age_hours == 0.0


def test_check_all_and_worst(store):
    ingest(store, DAILY, (NOW - timedelta(hours=60)).replace(tzinfo=None).isoformat(), 1)
    results = check_all(store, {"feed": DAILY, "old": HISTORICAL}, NOW)
    assert [(r.source, r.check, r.status) for r in results] == [
        ("feed", "ingestion", "ok"), ("feed", "data", "warn"), ("old", "ingestion", "error")]
    assert worst(results) == "error" and worst(results[:2]) == "warn" and worst([]) == "ok"
    json.dumps([r.to_dict() for r in results])


# ------------------------------------------------------------------------ config

def test_repo_config_policies():
    sources = load_sources()
    assert sources["chicago_crimes"].freshness.checks_data
    assert sources["chicago_311_requests"].freshness.data_warn_hours == 48
    assert not sources["chicago_taxi_trips"].freshness.checks_data  # historical
    assert not sources["illinois_urbana_arrests"].freshness.checks_data
    assert sources["chicago_taxi_trips"].freshness.ingestion_warn_hours == 36


@pytest.mark.parametrize("block, message", [
    ("    freshness:\n      bogus: 1\n", "unknown freshness keys"),
    ("    freshness:\n      data_warn_hours: 5\n", "set both"),
    ("    freshness:\n      data_warn_hours: 9\n      data_error_hours: 5\n", "must not exceed"),
    ("    freshness:\n      ingestion_warn_hours: 99\n", "must not exceed"),
])
def test_invalid_policy_rejected(tmp_path, block, message):
    p = tmp_path / "s.yaml"
    p.write_text("sources:\n  x:\n    domain: d.org\n    dataset_id: a-b\n    primary_key: id\n"
                 + block)
    with pytest.raises(ValueError, match=message):
        load_sources(p)


# --------------------------------------------------------------------------- CLI

def test_cli_freshness(tmp_path, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    recent = datetime.now(timezone.utc).replace(tzinfo=None).isoformat(timespec="milliseconds")
    store.write("chicago_311_requests", [{"sr_number": "1", "last_modified_date": recent}],
                primary_key="sr_number", incremental_column="last_modified_date")
    store.close()

    assert cli.main(["freshness", "chicago_311_requests", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert out.count("OK    chicago_311_requests") == 2

    assert cli.main(["freshness", "--db", str(db)]) == 1  # others never ingested
    assert "ERROR chicago_crimes" in capsys.readouterr().out

    assert cli.main(["freshness", "chicago_311_requests", "--db", str(db), "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert {d["check"] for d in data} == {"ingestion", "data"}

    assert cli.main(["freshness", "nope", "--db", str(db)]) == 1
