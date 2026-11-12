import json
from datetime import datetime, timezone
from pathlib import Path

import duckdb
import pytest

from lakehouse import cli
from lakehouse.ingestion.socrata import RequestStats
from lakehouse.status import collect_status, format_status, status_json
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = "chicago_crimes"
CONFIGURED = ["chicago_crimes", "chicago_311_requests"]
NOW = datetime(2026, 11, 12, 4, 0, 0, tzinfo=timezone.utc)


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def _set_run_times(store, run_id, started, finished):
    store.con.execute(
        "UPDATE bronze._ingestion_log SET run_started = ?, run_finished = ? WHERE run_id = ?",
        [started, finished, run_id],
    )


@pytest.fixture
def store(records):
    s = BronzeStore(":memory:")
    kw = {"primary_key": "id", "incremental_column": "updated_on"}
    s.write(SRC, records, run_id="r1", request_stats=RequestStats(1, 0, 0, 0.0), **kw)
    s.write(
        SRC,
        [{**r, "updated_on": "2026-02-01T00:00:00.000", "arrest": "1"} for r in records[:2]],
        run_id="r2",
        request_stats=RequestStats(requests=4, retries=2, rate_limited=2, waited_s=9.5),
        **kw,
    )
    s.write(SRC, [], run_id="r3", request_stats=RequestStats(1, 0, 0, 0.0), **kw)
    _set_run_times(s, "r1", datetime(2026, 11, 10, 3, 41), datetime(2026, 11, 10, 3, 42))
    _set_run_times(s, "r2", datetime(2026, 11, 11, 3, 41), datetime(2026, 11, 11, 3, 41, 30))
    _set_run_times(s, "r3", datetime(2026, 11, 12, 3, 41), datetime(2026, 11, 12, 3, 41, 5))
    yield s
    s.close()


def test_ingestion_log_stores_request_metrics(store):
    rows = store.con.execute(
        "SELECT run_id, api_requests, api_retries, rate_limited, waited_s "
        "FROM bronze._ingestion_log ORDER BY run_id"
    ).fetchall()
    assert rows == [("r1", 1, 0, 0, 0.0), ("r2", 4, 2, 2, 9.5), ("r3", 1, 0, 0, 0.0)]


def test_write_without_stats_leaves_metrics_null(records):
    s = BronzeStore(":memory:")
    s.write(SRC, records, primary_key="id")
    assert s.con.execute(
        "SELECT api_requests, waited_s FROM bronze._ingestion_log"
    ).fetchone() == (None, None)
    s.close()


def test_legacy_ingestion_log_is_migrated(tmp_path, records):
    db = tmp_path / "old.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE SCHEMA bronze")
    con.execute(
        "CREATE TABLE bronze._ingestion_log (source VARCHAR, run_started TIMESTAMP, "
        "run_finished TIMESTAMP, records BIGINT, watermark VARCHAR, run_id VARCHAR)"
    )
    con.execute("INSERT INTO bronze._ingestion_log VALUES ('x', now(), now(), 1, NULL, 'o')")
    con.close()
    s = BronzeStore(db)
    s.write(SRC, records, primary_key="id", request_stats=RequestStats(2, 0, 0, 0.0))
    report = collect_status(s, [SRC, "x"], now=NOW)
    assert report.sources[0].api_requests == 2
    assert report.sources[1].runs == 1 and report.sources[1].api_requests == 0
    s.close()


def test_collect_status_metrics(store):
    report = collect_status(store, CONFIGURED, stale_hours=36, now=NOW)
    crimes = report.sources[0]
    assert crimes.source == SRC and crimes.configured and crimes.ingested
    assert (crimes.rows, crimes.keys, crimes.duplicates) == (7, 5, 2)
    assert (crimes.runs, crimes.empty_runs) == (3, 1)
    assert crimes.last_run_id == "r3" and crimes.last_run_records == 0
    assert crimes.last_run_finished == "2026-11-12 03:41:05"
    assert crimes.last_run_duration_s == 5.0
    assert crimes.hours_since_last_run == pytest.approx(0.32, abs=0.01)
    assert not crimes.stale
    assert crimes.watermark == "2026-02-01T00:00:00.000"
    assert crimes.avg_records_recent == pytest.approx(7 / 3, abs=0.1)
    assert (crimes.api_requests, crimes.api_retries, crimes.rate_limited) == (6, 2, 2)
    assert crimes.waited_s == 9.5
    assert crimes.schema_fields == 13 and len(crimes.schema_fingerprint) == 16
    assert crimes.last_drift_severity == "breaking" and crimes.last_drift_run_id == "r2"


def test_never_ingested_and_unconfigured_sources(store, records):
    store.write("adhoc_table", records[:1], primary_key="id", run_id="a1")
    report = collect_status(store, CONFIGURED, now=NOW)
    names = [s.source for s in report.sources]
    assert names == ["chicago_crimes", "chicago_311_requests", "adhoc_table"]
    never = report.sources[1]
    assert not never.ingested and never.runs == 0 and never.stale
    assert never.hours_since_last_run is None and never.last_drift_severity is None
    adhoc = report.sources[2]
    assert not adhoc.configured and adhoc.rows == 1


def test_staleness_threshold(store):
    later = datetime(2026, 11, 14, 4, 0, tzinfo=timezone.utc)  # ~48h after r3
    assert collect_status(store, [SRC], stale_hours=36, now=later).sources[0].stale
    assert not collect_status(store, [SRC], stale_hours=72, now=later).sources[0].stale


def test_totals_and_json(store):
    report = collect_status(store, CONFIGURED, now=NOW)
    assert report.totals == {
        "sources": 2, "ingested": 1, "stale": 1, "rows": 7, "runs": 3, "rate_limited": 2,
    }
    data = json.loads(status_json(report))
    assert data["totals"]["rows"] == 7
    assert data["sources"][0]["duplicates"] == 2
    assert data["sources"][1]["last_run_id"] is None


def test_format_status(store):
    text = format_status(collect_status(store, CONFIGURED, now=NOW))
    crimes = next(x for x in text.splitlines() if x.startswith(SRC))
    assert crimes.split()[:4] == [SRC, "7", "5", "3"]
    assert "19m ago" in crimes and "breaking" in crimes and "2 superseded" in crimes
    never = next(x for x in text.splitlines() if x.startswith("chicago_311_requests"))
    assert "never" in never and "NEVER INGESTED" in never
    assert "1/2 sources ingested, 7 rows, 3 runs, 1 stale (> 36h)" in text


class _FakeClient:
    def __init__(self, records, **_):
        self.records = records
        self.stats = RequestStats()

    def fetch(self, source, since=None, max_records=None):
        self.stats.requests += 2
        self.stats.rate_limited += 1
        self.stats.waited_s += 3.0
        yield from self.records


def test_cli_ingest_records_client_stats_and_status(tmp_path, monkeypatch, records, capsys):
    monkeypatch.setattr(cli, "SocrataClient", lambda **kw: _FakeClient(records))
    db = str(tmp_path / "lh.duckdb")
    assert cli.main(["ingest", SRC, "--db", db, "--run-id", "cli1"]) == 0
    capsys.readouterr()

    assert cli.main(["status", "--db", db, "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    crimes = next(s for s in data["sources"] if s["source"] == SRC)
    assert crimes["rows"] == 5 and crimes["last_run_id"] == "cli1"
    assert (crimes["api_requests"], crimes["rate_limited"], crimes["waited_s"]) == (2, 1, 3.0)
    assert {s["source"] for s in data["sources"]} >= {
        "chicago_crimes", "chicago_311_requests", "chicago_taxi_trips", "illinois_urbana_arrests"
    }

    assert cli.main(["status", "--db", db]) == 0
    out = capsys.readouterr().out
    assert out.startswith("lakehouse status") and "1/4 sources ingested" in out
