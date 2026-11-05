import json
import logging
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.drift import SchemaChange, compare, severity_rank
from lakehouse.schema import infer_schema
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = "chicago_crimes"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def snap(recs, run_id):
    s = infer_schema(SRC, recs)
    s.run_id = run_id
    return s


def kinds(report):
    return {(c.field, c.kind, c.severity) for c in report.changes}


def test_identical_schemas_have_no_drift(records):
    report = compare(snap(records, "a"), snap(records[:2], "b"))
    assert not report.has_drift
    assert report.severity is None
    assert report.old_fingerprint == report.new_fingerprint
    assert "no schema drift" in report.format()


def test_added_field_is_info(records):
    new = [{**r, "beat": "1834"} for r in records]
    report = compare(snap(records, "a"), snap(new, "b"))
    assert kinds(report) == {("beat", "added", "info")}
    assert report.changes[0].new == "integer"
    assert report.severity == "info"


def test_removed_field_is_warning(records):
    new = [{k: v for k, v in r.items() if k != "location_description"} for r in records]
    report = compare(snap(records, "a"), snap(new, "b"))
    assert kinds(report) == {("location_description", "removed", "warning")}


def test_widened_type_is_warning(records):
    new = [{**r, "id": f"{r['id']}.5"} for r in records]  # integer -> number
    report = compare(snap(records, "a"), snap(new, "b"))
    assert kinds(report) == {("id", "type_widened", "warning")}
    change = report.changes[0]
    assert (change.old, change.new) == ("integer", "number")


def test_narrowed_type_is_info(records):
    mixed = [{**records[0], "latitude": "41"}, {**records[1], "latitude": "41.5"}]
    only_ints = [{**records[0], "latitude": "41"}]
    report = compare(snap(mixed, "a"), snap(only_ints, "b"))
    assert kinds(report) == {("latitude", "type_narrowed", "info")}


def test_incompatible_type_change_is_breaking(records):
    new = [{**r, "updated_on": "yesterday-ish"} for r in records]  # timestamp -> string
    report = compare(snap(records, "a"), snap(new, "b"))
    assert kinds(report) == {("updated_on", "type_widened", "warning")}

    new = [{**r, "arrest": "1"} for r in records]  # boolean -> integer: incompatible
    report = compare(snap(records, "a"), snap(new, "b"))
    assert kinds(report) == {("arrest", "type_changed", "breaking")}
    assert report.severity == "breaking"


def test_all_null_side_is_info(records):
    nulls = [{**r, "latitude": None} for r in records]
    report = compare(snap(records, "a"), snap(nulls, "b"))
    assert ("latitude", "type_from_nulls", "info") in kinds(report)
    assert ("latitude", "became_nullable", "info") in kinds(report)
    assert report.severity == "info"


def test_became_nullable_is_info(records):
    new = [{**records[0], "district": None}, records[1]]
    report = compare(snap(records, "a"), snap(new, "b"))
    assert kinds(report) == {("district", "became_nullable", "info")}


def test_severity_ordering_and_filtering(records):
    new = [{**r, "arrest": "1", "beat": "x"} for r in records]
    report = compare(snap(records, "a"), snap(new, "b"))
    assert report.severity == "breaking"
    assert {c.field for c in report.at_least("warning")} == {"arrest"}
    assert len(report.at_least("info")) == 2
    assert severity_rank("info") < severity_rank("warning") < severity_rank("breaking")


def test_report_serialisation(records):
    new = [{**r, "beat": "x"} for r in records]
    report = compare(snap(records, "a"), snap(new, "b"))
    data = json.loads(report.to_json())
    assert data["old_run_id"] == "a" and data["new_run_id"] == "b"
    assert data["severity"] == "info"
    assert data["changes"] == [
        {"field": "beat", "kind": "added", "severity": "info", "old": None, "new": "string"}
    ]
    assert "[info    ] + beat (string)" in report.format()


def test_describe():
    assert SchemaChange("x", "removed", "warning", "integer").describe() == "- x (integer)"
    assert (
        SchemaChange("x", "type_widened", "warning", "integer", "number").describe()
        == "~ x: type_widened integer -> number"
    )


def test_bronze_write_detects_and_stores_drift(records, caplog):
    store = BronzeStore(":memory:")
    store.write(SRC, records, primary_key="id", run_id="r1")
    assert store.last_drift is None  # first snapshot: nothing to compare

    store.write(SRC, records, primary_key="id", run_id="r2")
    assert store.last_drift is not None and not store.last_drift.has_drift

    with caplog.at_level(logging.INFO, logger="lakehouse.storage.bronze"):
        store.write(SRC, [{**r, "arrest": "1"} for r in records], primary_key="id", run_id="r3")
    assert store.last_drift.severity == "breaking"
    drift_logs = [r for r in caplog.records if r.getMessage().startswith("Schema drift")]
    assert drift_logs and drift_logs[0].levelno == logging.ERROR
    assert drift_logs[0].drift_severity == "breaking"

    events = store.drift_history(SRC)
    assert [(e["old_run_id"], e["new_run_id"], e["field"], e["kind"]) for e in events] == [
        ("r2", "r3", "arrest", "type_changed")
    ]
    store.close()


def test_empty_run_does_not_reset_drift_baseline(records):
    store = BronzeStore(":memory:")
    store.write(SRC, records, primary_key="id", run_id="r1")
    store.write(SRC, [], primary_key="id", run_id="empty")
    assert store.last_drift is None
    store.write(SRC, [{**r, "beat": "x"} for r in records], primary_key="id", run_id="r3")
    assert store.last_drift.old_run_id == "r1"
    store.close()


def _db_with_runs(tmp_path, records):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write(SRC, records, primary_key="id", run_id="r1")
    store.write(SRC, [{**r, "beat": "x"} for r in records], primary_key="id", run_id="r2")
    store.write(SRC, [{**r, "arrest": "1"} for r in records], primary_key="id", run_id="r3")
    store.close()
    return db


def test_cli_drift_default_compares_last_two(tmp_path, records, capsys):
    db = _db_with_runs(tmp_path, records)
    assert cli.main(["drift", SRC, "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "r2 -> r3" in out and "type_changed boolean -> integer" in out
    assert "- beat (string)" in out


def test_cli_drift_explicit_runs_json_and_fail_on(tmp_path, records, capsys):
    db = _db_with_runs(tmp_path, records)
    assert cli.main(["drift", SRC, "--db", str(db), "--from", "r1", "--to", "r2", "--json"]) == 0
    data = json.loads(capsys.readouterr().out)
    assert data["severity"] == "info" and data["changes"][0]["field"] == "beat"

    args = ["drift", SRC, "--db", str(db), "--from", "r1", "--to", "r2"]
    assert cli.main([*args, "--fail-on", "warning"]) == 0
    assert cli.main([*args, "--fail-on", "info"]) == 2
    assert cli.main(["drift", SRC, "--db", str(db), "--fail-on", "breaking"]) == 2
    assert cli.main(["drift", SRC, "--db", str(db), "--from", "nope"]) == 1


def test_cli_drift_history_and_insufficient_snapshots(tmp_path, records, capsys):
    db = _db_with_runs(tmp_path, records)
    assert cli.main(["drift", SRC, "--db", str(db), "--history"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert any("[info] added beat" in x for x in lines)
    assert any("[breaking] type_changed arrest boolean -> integer" in x for x in lines)

    one = tmp_path / "one.duckdb"
    store = BronzeStore(one)
    store.write(SRC, records, primary_key="id", run_id="only")
    store.close()
    assert cli.main(["drift", SRC, "--db", str(one)]) == 1
    assert cli.main(["drift", SRC, "--db", str(one), "--history"]) == 0
    assert "no drift recorded" in capsys.readouterr().out


class _FakeClient:
    def __init__(self, records):
        self.records = records

    def fetch(self, source, since=None, max_records=None):
        yield from self.records


def test_cli_ingest_fail_on_drift(tmp_path, monkeypatch, records):
    db = str(tmp_path / "lh.duckdb")
    batches = iter([records, [{**r, "arrest": "1"} for r in records]])
    monkeypatch.setattr(cli, "SocrataClient", lambda **kw: _FakeClient(next(batches)))
    base = ["ingest", SRC, "--db", db, "--full", "--fail-on-drift", "breaking"]
    assert cli.main([*base, "--run-id", "a"]) == 0
    assert cli.main([*base, "--run-id", "b"]) == 2
    store = BronzeStore(db)
    assert store.count(SRC) == 2 * len(records)  # data still landed; only the exit code fails
    store.close()
