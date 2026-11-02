import io
import json
import logging
import re
from pathlib import Path

import duckdb
import pytest

from lakehouse import cli
from lakehouse.logging_setup import (
    configure_logging,
    get_run_id,
    new_run_id,
    run_context,
)
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


@pytest.fixture(autouse=True)
def _restore_root_logger():
    root = logging.getLogger()
    handlers, level = list(root.handlers), root.level
    yield
    root.handlers[:] = handlers
    root.setLevel(level)


def test_new_run_id_format_and_uniqueness():
    a, b = new_run_id(), new_run_id()
    assert re.fullmatch(r"\d{8}T\d{6}Z-[0-9a-f]{8}", a)
    assert a != b


def test_run_context_binds_and_resets():
    assert get_run_id() is None
    with run_context("outer") as rid:
        assert rid == get_run_id() == "outer"
        with run_context() as inner:
            assert get_run_id() == inner != "outer"
        assert get_run_id() == "outer"
    assert get_run_id() is None


def test_json_logs_carry_run_id_and_extras():
    buf = io.StringIO()
    configure_logging(json_output=True, stream=buf)
    log = logging.getLogger("lakehouse.test")
    with run_context("run-123"):
        log.info("hello %s", "world", extra={"records": 5})
    log.info("outside")
    first, second = [json.loads(line) for line in buf.getvalue().splitlines()]
    assert first["run_id"] == "run-123"
    assert first["message"] == "hello world"
    assert first["records"] == 5
    assert first["level"] == "INFO" and first["logger"] == "lakehouse.test"
    assert second["run_id"] == "-"


def test_json_logs_include_exception():
    buf = io.StringIO()
    configure_logging(json_output=True, stream=buf)
    try:
        raise ValueError("boom")
    except ValueError:
        logging.getLogger("lakehouse.test").exception("failed")
    entry = json.loads(buf.getvalue())
    assert "ValueError: boom" in entry["exc_info"]


def test_text_logs_show_run_id_and_extras():
    buf = io.StringIO()
    configure_logging(stream=buf)
    with run_context("run-abc"):
        logging.getLogger("lakehouse.test").info("done", extra={"records": 3})
    line = buf.getvalue().strip()
    assert "[run-abc]" in line and line.endswith("done records=3")


def test_configure_logging_is_idempotent():
    root = logging.getLogger()
    before = len(root.handlers)
    configure_logging(stream=io.StringIO())
    configure_logging(json_output=True, stream=io.StringIO())
    assert len(root.handlers) == before + 1


def test_bronze_records_run_id(records):
    store = BronzeStore(":memory:")
    with run_context("run-xyz"):
        store.write("chicago_crimes", records, primary_key="id", incremental_column="updated_on")
    store.write("chicago_crimes", records[:1], primary_key="id", run_id="explicit")
    rows = store.con.execute(
        "SELECT run_id, count(*) FROM bronze.chicago_crimes GROUP BY run_id ORDER BY run_id"
    ).fetchall()
    assert rows == [("explicit", 1), ("run-xyz", len(records))]
    log_ids = [
        r[0] for r in store.con.execute("SELECT run_id FROM bronze._ingestion_log").fetchall()
    ]
    assert sorted(log_ids) == ["explicit", "run-xyz"]


def test_bronze_migrates_legacy_tables(tmp_path, records):
    db = tmp_path / "legacy.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE SCHEMA bronze")
    con.execute(
        "CREATE TABLE bronze._ingestion_log (source VARCHAR, run_started TIMESTAMP, "
        "run_finished TIMESTAMP, records BIGINT, watermark VARCHAR)"
    )
    con.execute(
        "CREATE TABLE bronze.chicago_crimes (record_key VARCHAR, payload JSON, "
        "ingested_at TIMESTAMP)"
    )
    con.execute("INSERT INTO bronze.chicago_crimes VALUES ('1', '{}', now())")
    con.close()

    store = BronzeStore(db)
    store.write("chicago_crimes", records, primary_key="id", run_id="new-run")
    assert store.count("chicago_crimes") == len(records) + 1
    nulls = store.con.execute(
        "SELECT count(*) FROM bronze.chicago_crimes WHERE run_id IS NULL"
    ).fetchone()[0]
    assert nulls == 1
    store.close()


class _FakeClient:
    def __init__(self, records=None, error=None, **_):
        self.records, self.error = records, error

    def fetch(self, source, since=None, max_records=None):
        if self.error:
            raise self.error
        yield from self.records


def test_cli_ingest_logs_run_id_end_to_end(tmp_path, monkeypatch, records, capsys):
    monkeypatch.setattr(cli, "SocrataClient", lambda **kw: _FakeClient(records))
    db = tmp_path / "lh.duckdb"
    rc = cli.main(
        ["--log-json", "ingest", "chicago_crimes", "--db", str(db), "--run-id", "cli-run"]
    )
    assert rc == 0
    lines = [json.loads(x) for x in capsys.readouterr().err.splitlines() if x.startswith("{")]
    assert lines and all(e["run_id"] == "cli-run" for e in lines)
    finished = [e for e in lines if e["message"].startswith("Ingestion finished")]
    assert finished[0]["records"] == len(records)
    con = duckdb.connect(str(db))
    assert con.execute("SELECT DISTINCT run_id FROM bronze.chicago_crimes").fetchall() == [
        ("cli-run",)
    ]
    con.close()


def test_cli_ingest_failure_returns_1_and_logs(tmp_path, monkeypatch, capsys):
    monkeypatch.setattr(
        cli, "SocrataClient", lambda **kw: _FakeClient(error=RuntimeError("api down"))
    )
    rc = cli.main(["--log-json", "ingest", "chicago_crimes", "--db", str(tmp_path / "x.duckdb")])
    assert rc == 1
    errors = [
        json.loads(x)
        for x in capsys.readouterr().err.splitlines()
        if x.startswith("{") and '"ERROR"' in x
    ]
    assert errors and "api down" in errors[0]["exc_info"]
