import json
from pathlib import Path

import duckdb
import pytest

from lakehouse import cli
from lakehouse.storage.bronze import BronzeStore

FIXTURE = Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json"
SRC = "chicago_crimes"


@pytest.fixture
def records():
    return json.loads(FIXTURE.read_text())


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    yield s
    s.close()


def _write(store, recs, inc="updated_on"):
    return store.write(SRC, recs, primary_key="id", incremental_column=inc)


def _updated(rec, updated_on, **changes):
    return {**rec, **changes, "updated_on": updated_on}


def test_latest_view_has_one_row_per_key(store, records):
    _write(store, records)
    _write(store, [_updated(records[0], "2026-02-01T00:00:00.000", arrest=True)])
    assert store.count(SRC) == len(records) + 1
    latest = store.latest(SRC)
    assert len(latest) == len(records)
    first = next(r for r in latest if r["id"] == records[0]["id"])
    assert first["arrest"] is True
    view_rows = store.con.execute(f"SELECT count(*) FROM bronze.{SRC}__latest").fetchone()[0]
    assert view_rows == len(records)


def test_latest_prefers_incremental_value_over_ingest_order(store, records):
    newer = _updated(records[0], "2026-03-01T00:00:00.000", description="NEWER")
    older = _updated(records[0], "2026-02-01T00:00:00.000", description="OLDER")
    _write(store, [newer])
    _write(store, [older])  # e.g. a --full reload that returned a stale copy
    (rec,) = [r for r in store.latest(SRC) if r["id"] == records[0]["id"]]
    assert rec["description"] == "NEWER"


def test_same_batch_duplicates_keep_last_seen(store, records):
    a = _updated(records[0], records[0]["updated_on"], description="FIRST")
    b = _updated(records[0], records[0]["updated_on"], description="SECOND")
    _write(store, [a, b])
    (rec,) = store.latest(SRC)
    assert rec["description"] == "SECOND"


def test_without_incremental_column_latest_ingest_wins(store, records):
    _write(store, [records[0]], inc=None)
    _write(store, [{**records[0], "description": "LATER"}], inc=None)
    (rec,) = store.latest(SRC)
    assert rec["description"] == "LATER"


def test_records_missing_incremental_value_rank_last(store, records):
    with_value = _updated(records[0], "2026-01-10T00:00:00.000", description="HAS VALUE")
    without = {k: v for k, v in records[0].items() if k != "updated_on"}
    _write(store, [with_value])
    _write(store, [without])
    (rec,) = store.latest(SRC)
    assert rec["description"] == "HAS VALUE"


def test_dedupe_stats_and_compact(store, records):
    _write(store, records)
    _write(store, [_updated(r, "2026-02-01T00:00:00.000") for r in records[:2]])
    stats = store.dedupe_stats(SRC)
    assert (stats.rows, stats.keys, stats.duplicates) == (7, 5, 2)

    before = store.latest(SRC)
    assert store.compact(SRC) == 2
    assert store.count(SRC) == 5
    assert store.latest(SRC) == before
    assert store.compact(SRC) == 0  # idempotent


def test_compact_preserves_lineage_columns(store, records):
    store.write(SRC, records, primary_key="id", incremental_column="updated_on", run_id="r1")
    store.write(
        SRC,
        [_updated(records[0], "2026-02-01T00:00:00.000")],
        primary_key="id",
        incremental_column="updated_on",
        run_id="r2",
    )
    store.compact(SRC)
    rows = dict(
        store.con.execute(f"SELECT record_key, run_id FROM bronze.{SRC}").fetchall()
    )
    assert rows[records[0]["id"]] == "r2"
    assert {rows[r["id"]] for r in records[1:]} == {"r1"}


def test_watermark_unaffected_by_compaction(store, records):
    _write(store, records)
    wm = store.last_watermark(SRC)
    store.compact(SRC)
    assert store.last_watermark(SRC) == wm


def test_legacy_table_without_ingest_seq_is_migrated(tmp_path, records):
    db = tmp_path / "legacy.duckdb"
    con = duckdb.connect(str(db))
    con.execute("CREATE SCHEMA bronze")
    con.execute(
        f"CREATE TABLE bronze.{SRC} (record_key VARCHAR, payload JSON, "
        "ingested_at TIMESTAMP, run_id VARCHAR)"
    )
    old = _updated(records[0], "2026-01-01T00:00:00.000", description="OLD")
    con.execute(
        f"INSERT INTO bronze.{SRC} VALUES (?, ?, TIMESTAMP '2026-01-01', 'old')",
        [old["id"], json.dumps(old)],
    )
    con.close()

    store = BronzeStore(db)
    _write(store, [_updated(records[0], "2026-02-01T00:00:00.000", description="NEW")])
    (rec,) = store.latest(SRC)
    assert rec["description"] == "NEW"
    assert store.compact(SRC) == 1
    store.close()


def test_cli_dedupe_dry_run_and_compact(tmp_path, records, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    _write(store, records)
    _write(store, [_updated(records[0], "2026-02-01T00:00:00.000")])
    store.close()

    assert cli.main(["dedupe", SRC, "--db", str(db), "--dry-run"]) == 0
    assert "6 rows, 5 keys, 1 superseded" in capsys.readouterr().out
    assert cli.main(["dedupe", SRC, "--db", str(db)]) == 0
    assert "Removed 1 superseded rows" in capsys.readouterr().out
    assert cli.main(["dedupe", SRC, "--db", str(db), "--dry-run"]) == 0
    assert "5 rows, 5 keys, 0 superseded" in capsys.readouterr().out


def test_cli_dedupe_unknown_table(tmp_path):
    assert cli.main(["dedupe", "nope", "--db", str(tmp_path / "x.duckdb")]) == 1
