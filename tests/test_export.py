import json
from datetime import datetime
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.storage.bronze import BronzeStore
from lakehouse.storage.export import bronze_tables, export_all, export_source, read_export

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = "chicago_crimes"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


def _store_with_two_days(records):
    """Bronze with 5 rows ingested on 2026-11-01 and 2 newer versions on 2026-11-02."""
    store = BronzeStore(":memory:")
    store.write(SRC, records, primary_key="id", incremental_column="updated_on", run_id="d1")
    store.write(
        SRC,
        [{**r, "updated_on": "2026-02-01T00:00:00.000"} for r in records[:2]],
        primary_key="id",
        incremental_column="updated_on",
        run_id="d2",
    )
    store.con.execute(
        f"UPDATE bronze.{SRC} SET ingested_at = CASE run_id "
        "WHEN 'd1' THEN TIMESTAMP '2026-11-01 03:41:00' "
        "ELSE TIMESTAMP '2026-11-02 03:41:00' END"
    )
    return store


def test_export_partitions_by_ingest_date(tmp_path, records):
    store = _store_with_two_days(records)
    result = export_source(store, SRC, tmp_path)
    assert result.path == tmp_path / "bronze" / SRC
    assert result.rows == 7
    assert result.partitions == {"2026-11-01": 5, "2026-11-02": 2}
    dirs = sorted(p.name for p in result.path.iterdir())
    assert dirs == ["ingest_date=2026-11-01", "ingest_date=2026-11-02"]
    assert all(f.suffix == ".parquet" for f in result.files) and result.files
    store.close()


def test_export_roundtrip_preserves_rows_and_lineage(tmp_path, records):
    store = _store_with_two_days(records)
    result = export_source(store, SRC, tmp_path)
    cur = read_export(store, result.path)
    cols = [d[0] for d in cur.description]
    rows = [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]
    assert set(cols) >= {"record_key", "payload", "ingested_at", "run_id", "ingest_seq",
                         "ingest_date"}
    assert len(rows) == 7
    by_run = {}
    for r in rows:
        by_run.setdefault(r["run_id"], []).append(r)
    assert len(by_run["d1"]) == 5 and len(by_run["d2"]) == 2
    payload = json.loads(by_run["d1"][0]["payload"])
    assert payload["id"] == by_run["d1"][0]["record_key"]
    assert {str(r["ingest_date"]) for r in by_run["d2"]} == {"2026-11-02"}
    assert isinstance(by_run["d2"][0]["ingested_at"], datetime)
    store.close()


def test_export_is_idempotent_and_drops_stale_partitions(tmp_path, records):
    store = _store_with_two_days(records)
    export_source(store, SRC, tmp_path)
    store.compact(SRC)  # removes the two superseded 2026-11-01 versions
    store.con.execute(f"DELETE FROM bronze.{SRC} WHERE run_id = 'd2'")
    result = export_source(store, SRC, tmp_path)
    assert result.partitions == {"2026-11-01": 3}
    assert [p.name for p in result.path.iterdir()] == ["ingest_date=2026-11-01"]
    assert len(read_export(store, result.path).fetchall()) == 3
    store.close()


def test_export_latest_view(tmp_path, records):
    store = _store_with_two_days(records)
    result = export_source(store, SRC, tmp_path, latest_only=True)
    assert result.path == tmp_path / "bronze_latest" / SRC
    assert result.rows == 5
    assert result.partitions == {"2026-11-01": 3, "2026-11-02": 2}
    store.close()


def test_export_all_and_table_discovery(tmp_path, records):
    store = _store_with_two_days(records)
    store.write("other_source", records[:1], primary_key="id")
    assert bronze_tables(store) == [SRC, "other_source"]  # no _ingestion_log etc.
    results = export_all(store, tmp_path)
    assert [r.source for r in results] == [SRC, "other_source"]
    assert results[1].rows == 1
    store.close()


def test_export_unknown_source_raises(tmp_path):
    store = BronzeStore(":memory:")
    with pytest.raises(ValueError, match="No bronze table"):
        export_source(store, "nope", tmp_path)
    store.close()


def test_cli_export(tmp_path, records, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write(SRC, records, primary_key="id", incremental_column="updated_on")
    store.close()
    out = tmp_path / "export"

    assert cli.main(["export", SRC, "--db", str(db), "--out", str(out)]) == 0
    assert f"{SRC}: 5 rows in 1 partitions" in capsys.readouterr().out
    assert list((out / "bronze" / SRC).rglob("*.parquet"))

    assert cli.main(["export", "--db", str(db), "--out", str(out), "--latest"]) == 0
    assert (out / "bronze_latest" / SRC).is_dir()

    assert cli.main(["export", "nope", "--db", str(db), "--out", str(out)]) == 1

    empty = tmp_path / "empty.duckdb"
    assert cli.main(["export", "--db", str(empty), "--out", str(out)]) == 0
    assert "no bronze tables to export" in capsys.readouterr().out
