import shutil
from datetime import datetime

import duckdb
import pytest

from lakehouse import cli
from lakehouse.reference import SEEDS_DIR, load_all
from lakehouse.scd import apply_scd2, as_of_sql
from lakehouse.storage.bronze import BronzeStore

T1, T2, T3, T4 = (datetime(2026, 1, d) for d in (1, 2, 3, 4))
HIST = "ref.areas_history"


@pytest.fixture
def con():
    c = duckdb.connect()
    c.execute("CREATE TABLE src (id INTEGER, name VARCHAR, side VARCHAR)")
    yield c
    c.close()


def load(con, rows):
    con.execute("DELETE FROM src")
    if rows:
        con.executemany("INSERT INTO src VALUES (?, ?, ?)", rows)


def run(con, as_of, **kw):
    return apply_scd2(con, "src", HIST, "id", ["name", "side"], as_of=as_of, **kw)


def history(con, key=None):
    where = f" WHERE id = {key}" if key is not None else ""
    return con.execute(
        f"SELECT id, name, side, valid_from, valid_to, is_current, version "
        f"FROM {HIST}{where} ORDER BY id, version"
    ).fetchall()


def test_initial_load_creates_version_one(con):
    load(con, [(1, "Loop", "Central"), (2, "Uptown", "Far North")])
    r = run(con, T1)
    assert (r.inserted, r.changed, r.closed, r.reopened, r.unchanged) == (2, 0, 0, 0, 0)
    assert history(con) == [
        (1, "Loop", "Central", T1, None, True, 1),
        (2, "Uptown", "Far North", T1, None, True, 1),
    ]


def test_rerun_with_same_source_is_noop(con):
    load(con, [(1, "Loop", "Central")])
    run(con, T1)
    r = run(con, T2)
    assert r.touched == 0 and r.unchanged == 1
    assert len(history(con)) == 1


def test_change_closes_old_and_opens_new_version(con):
    load(con, [(1, "Loop", "Central"), (2, "Uptown", "Far North")])
    run(con, T1)
    load(con, [(1, "The Loop", "Central"), (2, "Uptown", "Far North")])
    r = run(con, T2)
    assert (r.inserted, r.changed, r.unchanged) == (0, 1, 1)
    assert history(con, 1) == [
        (1, "Loop", "Central", T1, T2, False, 1),
        (1, "The Loop", "Central", T2, None, True, 2),
    ]
    assert history(con, 2) == [(2, "Uptown", "Far North", T1, None, True, 1)]


def test_missing_key_is_closed_and_can_reopen(con):
    load(con, [(1, "Loop", "Central"), (2, "Uptown", "Far North")])
    run(con, T1)
    load(con, [(1, "Loop", "Central")])
    r = run(con, T2)
    assert r.closed == 1
    assert history(con, 2) == [(2, "Uptown", "Far North", T1, T2, False, 1)]
    load(con, [(1, "Loop", "Central"), (2, "Uptown", "North")])
    r = run(con, T3)
    assert r.reopened == 1 and r.inserted == 0
    assert history(con, 2)[-1] == (2, "Uptown", "North", T3, None, True, 2)


def test_close_missing_false_keeps_absent_keys_current(con):
    load(con, [(1, "Loop", "Central"), (2, "Uptown", "Far North")])
    run(con, T1)
    load(con, [(1, "Loop", "Central")])
    r = run(con, T2, close_missing=False)
    assert r.closed == 0
    assert history(con, 2)[0][5] is True


def test_null_and_empty_are_distinct_changes(con):
    load(con, [(1, "Loop", None)])
    run(con, T1)
    load(con, [(1, "Loop", "")])
    assert run(con, T2).changed == 1
    load(con, [(1, "Loop", None)])
    assert run(con, T3).changed == 1
    load(con, [(1, "Loop", None)])
    assert run(con, T4).unchanged == 1
    assert [v[6] for v in history(con, 1)] == [1, 2, 3]


def test_exactly_one_current_version_per_key(con):
    for i, name in enumerate(["A", "B", "C", "B"], start=1):
        load(con, [(1, name, "x"), (2, "Same", "y")])
        run(con, datetime(2026, 2, i))
    per_key = con.execute(
        f"SELECT id, count(*) FILTER (WHERE is_current), count(*) FROM {HIST} GROUP BY 1 "
        "ORDER BY 1"
    ).fetchall()
    assert per_key == [(1, 1, 4), (2, 1, 1)]
    # intervals are contiguous: each valid_to equals the next valid_from
    rows = history(con, 1)
    assert all(a[4] == b[3] for a, b in zip(rows, rows[1:], strict=False))


def test_point_in_time_query(con):
    load(con, [(1, "Loop", "Central")])
    run(con, T1)
    load(con, [(1, "The Loop", "Central")])
    run(con, T3)
    sql = as_of_sql(HIST)
    assert con.execute(sql, [T2, T2]).fetchall()[0][1] == "Loop"
    assert con.execute(sql, [T3, T3]).fetchall()[0][1] == "The Loop"
    assert con.execute(sql, [datetime(2025, 12, 31)] * 2).fetchall() == []


def test_history_cannot_move_backwards(con):
    load(con, [(1, "Loop", "Central")])
    run(con, T2)
    load(con, [(1, "X", "Central")])
    with pytest.raises(ValueError, match="only move forward"):
        run(con, T1)
    assert len(history(con)) == 1


@pytest.mark.parametrize(
    "rows, tracked, message",
    [
        ([(1, "A", "x"), (1, "B", "y")], ["name", "side"], "duplicate id"),
        ([(None, "A", "x")], ["name", "side"], "NULL id"),
        ([(1, "A", "x")], [], "must not be empty"),
        ([(1, "A", "x")], ["id", "name"], "must not be a tracked column"),
    ],
)
def test_invalid_input(con, rows, tracked, message):
    load(con, rows)
    with pytest.raises(ValueError, match=message):
        apply_scd2(con, "src", HIST, "id", tracked, as_of=T1)


def test_failed_run_rolls_back(con, monkeypatch):
    load(con, [(1, "Loop", "Central")])
    run(con, T1)
    load(con, [(1, "Changed", "Central")])
    import lakehouse.scd as scd

    monkeypatch.setattr(scd, "_hash_expr", lambda cols, alias: "no_such_function()")
    with pytest.raises(duckdb.Error):
        run(con, T2)
    assert history(con) == [(1, "Loop", "Central", T1, None, True, 1)]


# ------------------------------------------------------------ reference-data integration

def test_seed_load_records_history(tmp_path):
    seeds = tmp_path / "seeds"
    shutil.copytree(SEEDS_DIR, seeds)
    store = BronzeStore(":memory:")
    first = {r.name: r for r in load_all(store, seeds)}
    assert first["police_districts"].history.inserted == 22
    assert store.con.execute(
        "SELECT count(*) FROM reference.community_areas_history WHERE is_current"
    ).fetchone() == (77,)

    path = seeds / "police_districts.csv"
    path.write_text(path.read_text().replace("18,Near North", "18,Near North Side"))
    second = {r.name: r for r in load_all(store, seeds)}
    assert second["police_districts"].history.changed == 1
    assert second["community_areas"].history is None  # unchanged file: skipped
    rows = store.con.execute(
        "SELECT name, is_current, version FROM reference.police_districts_history "
        "WHERE district = 18 ORDER BY version"
    ).fetchall()
    assert rows == [("Near North", False, 1), ("Near North Side", True, 2)]
    store.close()


def test_cli_seed_reports_history(tmp_path, capsys):
    db = str(tmp_path / "lh.duckdb")
    assert cli.main(["seed", "--db", db]) == 0
    out = capsys.readouterr().out
    assert "history reference.community_areas_history: 77 new" in out
    assert cli.main(["seed", "--db", db, "--force"]) == 0
    assert "history" not in capsys.readouterr().out  # forced reload, nothing changed
