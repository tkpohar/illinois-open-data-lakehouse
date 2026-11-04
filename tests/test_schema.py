import json
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.logging_setup import run_context
from lakehouse.schema import (
    SchemaSnapshot,
    format_schema,
    infer_schema,
    value_type,
    widen,
)
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = "chicago_crimes"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, "null"),
        (True, "boolean"),
        (3, "integer"),
        (3.5, "number"),
        ({"a": 1}, "object"),
        ([1], "array"),
        ("13001001", "integer"),
        ("-12", "integer"),
        ("018", "string"),  # leading zero: a code, not a number
        ("0", "integer"),
        ("41.8925", "number"),
        ("-87.6242", "number"),
        ("0.5", "number"),
        ("1e5", "number"),
        ("true", "boolean"),
        ("FALSE", "boolean"),
        ("2026-01-02T15:40:12.000", "timestamp"),
        ("2026-01-02 15:40", "timestamp"),
        ("2026-01-02T15:40:12Z", "timestamp"),
        ("2026-01-02T15:40:12+05:30", "timestamp"),
        ("2026-01-02", "date"),
        ("THEFT", "string"),
        ("", "string"),
    ],
)
def test_value_type(value, expected):
    assert value_type(value) == expected


@pytest.mark.parametrize(
    "a, b, expected",
    [
        ("integer", "integer", "integer"),
        ("integer", "number", "number"),
        ("number", "integer", "number"),
        ("date", "timestamp", "timestamp"),
        ("null", "integer", "integer"),
        ("integer", "null", "integer"),
        ("integer", "boolean", "string"),
        ("timestamp", "string", "string"),
    ],
)
def test_widen(a, b, expected):
    assert widen(a, b) == expected


def test_infer_schema_from_fixture(records):
    snap = infer_schema(SRC, records)
    assert snap.record_count == len(records)
    f = snap.fields
    assert f["id"].inferred_type == "integer" and f["id"].json_type == "string"
    assert f["district"].inferred_type == "string"  # "018", "011", ...
    assert f["latitude"].inferred_type == "number"
    assert f["updated_on"].inferred_type == "timestamp"
    assert f["arrest"].inferred_type == "boolean" and f["arrest"].json_type == "boolean"
    assert f["primary_type"].inferred_type == "string"
    assert not any(x.nullable(snap.record_count) for x in f.values())


def test_nullability_and_mixed_types():
    snap = infer_schema(
        SRC,
        [
            {"a": "1", "b": "x", "c": None},
            {"a": "2.5", "c": None},
            {"a": None, "b": "7"},
        ],
    )
    a, b, c = snap.fields["a"], snap.fields["b"], snap.fields["c"]
    assert a.inferred_type == "number" and a.nulls == 1 and a.nullable(3)
    assert b.inferred_type == "string" and b.present == 2 and b.nullable(3)
    assert c.inferred_type == "null" and c.json_type == "null"


def test_fingerprint_depends_only_on_shape(records):
    base = infer_schema(SRC, records)
    same_shape = infer_schema(SRC, records[:2])
    assert base.fingerprint == same_shape.fingerprint
    extra_field = infer_schema(SRC, [{**records[0], "new_col": "x"}])
    retyped = infer_schema(SRC, [{**records[0], "latitude": "not a number"}])
    assert len({base.fingerprint, extra_field.fingerprint, retyped.fingerprint}) == 3


def test_snapshot_json_roundtrip(records):
    snap = infer_schema(SRC, records)
    snap.run_id = "r1"
    back = SchemaSnapshot.from_row(SRC, snap.record_count, snap.to_json(), "r1")
    assert back.fields == snap.fields
    assert back.fingerprint == snap.fingerprint
    assert json.loads(snap.to_json())["id"]["nullable"] is False


def test_bronze_write_stores_snapshot_per_run(records):
    store = BronzeStore(":memory:")
    store.write(SRC, records, primary_key="id", run_id="r1")
    with run_context("r2"):
        store.write(SRC, [{**records[0], "beat": "1834"}], primary_key="id")
    history = store.schema_history(SRC)
    assert [s.run_id for s in history] == ["r1", "r2"]
    assert history[0].record_count == len(records)
    assert "beat" not in history[0].fields and "beat" in history[1].fields
    assert store.schema_snapshot(SRC).run_id == "r2"
    assert store.schema_snapshot(SRC, run_id="r1").fingerprint == history[0].fingerprint
    assert store.schema_snapshot(SRC, run_id="missing") is None
    row = store.con.execute(
        "SELECT field_count, fingerprint FROM bronze._schema_snapshots WHERE run_id = 'r1'"
    ).fetchone()
    assert row == (len(records[0]), history[0].fingerprint)
    store.close()


def test_empty_run_records_no_snapshot(records):
    store = BronzeStore(":memory:")
    store.write(SRC, records, primary_key="id", run_id="r1")
    store.write(SRC, [], primary_key="id", run_id="empty")
    assert [s.run_id for s in store.schema_history(SRC)] == ["r1"]
    assert store.schema_snapshot("never_ingested") is None
    store.close()


def test_format_schema(records):
    snap = infer_schema(SRC, records)
    text = format_schema(snap)
    assert snap.fingerprint in text
    line = next(x for x in text.splitlines() if x.startswith("latitude"))
    assert line.split()[:4] == ["latitude", "number", "string", "no"]


def test_cli_schema_commands(tmp_path, records, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write(SRC, records, primary_key="id", run_id="r1")
    store.write(SRC, records[:1], primary_key="id", run_id="r2")
    store.close()

    assert cli.main(["schema", SRC, "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "run=r2" in out and "updated_on" in out

    assert cli.main(["schema", SRC, "--db", str(db), "--run-id", "r1", "--json"]) == 0
    assert json.loads(capsys.readouterr().out)["latitude"]["inferred_type"] == "number"

    assert cli.main(["schema", SRC, "--db", str(db), "--history"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    assert len(lines) == 2 and "run=r1" in lines[0] and "run=r2" in lines[1]

    assert cli.main(["schema", SRC, "--db", str(db), "--run-id", "nope"]) == 1
    assert cli.main(["schema", "unknown", "--db", str(db)]) == 1
