import json
import shutil
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.reference import SEEDS, SEEDS_DIR, SeedError, load_all, load_seed, parse_seed
from lakehouse.silver import crimes
from lakehouse.storage.bronze import BronzeStore

CRIMES = json.loads((Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json").read_text())


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    yield s
    s.close()


@pytest.fixture
def seeds_copy(tmp_path):
    d = tmp_path / "seeds"
    shutil.copytree(SEEDS_DIR, d)
    return d


def test_committed_seeds_are_valid():
    for seed in SEEDS.values():
        parse_seed(seed)


def test_community_areas_seed_contents():
    rows = parse_seed(SEEDS["community_areas"]).rows
    assert [r[0] for r in rows] == list(range(1, 78))
    names = {n: name for n, name in rows}
    assert names[1] == "Rogers Park" and names[32] == "Loop" and names[77] == "Edgewater"
    assert names[8] == "Near North Side" and names[76] == "O'Hare"
    assert len(set(names.values())) == 77


def test_police_districts_seed_contents():
    rows = parse_seed(SEEDS["police_districts"]).rows
    numbers = [r[0] for r in rows]
    assert len(numbers) == 22 and numbers == sorted(numbers)
    assert {13, 21, 23}.isdisjoint(numbers)  # consolidated districts are not current
    assert dict(rows)[18] == "Near North"


def test_load_all_creates_reference_tables(store):
    results = load_all(store)
    assert [(r.table, r.rows, r.loaded) for r in results] == [
        ("reference.community_areas", 77, True),
        ("reference.police_districts", 22, True),
    ]
    assert store.con.execute(
        "SELECT name FROM reference.community_areas WHERE community_area = 6"
    ).fetchone() == ("Lake View",)
    log = store.con.execute("SELECT seed, rows FROM reference._seed_log ORDER BY seed").fetchall()
    assert log == [("community_areas", 77), ("police_districts", 22)]


def test_unchanged_seed_is_skipped_unless_forced(store):
    load_all(store)
    assert [r.loaded for r in load_all(store)] == [False, False]
    assert [r.loaded for r in load_all(store, force=True)] == [True, True]
    assert store.con.execute("SELECT count(*) FROM reference._seed_log").fetchone()[0] == 4


def test_changed_seed_is_reloaded(store, seeds_copy):
    load_all(store, seeds_copy)
    path = seeds_copy / "police_districts.csv"
    path.write_text(path.read_text().replace("Central", "Central District"))
    results = {r.name: r.loaded for r in load_all(store, seeds_copy)}
    assert results == {"community_areas": False, "police_districts": True}
    assert store.con.execute(
        "SELECT name FROM reference.police_districts WHERE district = 1"
    ).fetchone() == ("Central District",)


@pytest.mark.parametrize(
    "file, mutate, message",
    [
        ("police_districts.csv", lambda t: t + "18,Duplicate\n", "duplicate district 18"),
        ("police_districts.csv", lambda t: t + "x,Bad\n", "is not an integer"),
        ("police_districts.csv", lambda t: t + "26,\n", "empty name"),
        ("police_districts.csv", lambda t: t + "99,Too High\n", "outside"),
        ("police_districts.csv", lambda t: t.replace("district,name", "dist,name"), "header"),
        ("community_areas.csv", lambda t: t.rsplit("77,", 1)[0], "76 rows, expected 77"),
    ],
)
def test_invalid_seed_is_rejected_and_old_table_kept(store, seeds_copy, file, mutate, message):
    load_all(store, seeds_copy)
    path = seeds_copy / file
    path.write_text(mutate(path.read_text()))
    with pytest.raises(SeedError, match=message):
        load_all(store, seeds_copy)
    assert store.con.execute("SELECT count(*) FROM reference.police_districts").fetchone() == (22,)
    assert store.con.execute("SELECT count(*) FROM reference.community_areas").fetchone() == (77,)


def test_missing_seed_file(seeds_copy, store):
    (seeds_copy / "community_areas.csv").unlink()
    with pytest.raises(SeedError, match="not found"):
        load_seed(store, SEEDS["community_areas"], seeds_copy)


def test_primary_key_enforced_in_table(store):
    load_all(store)
    import duckdb

    with pytest.raises(duckdb.ConstraintException):
        store.con.execute("INSERT INTO reference.community_areas VALUES (1, 'Again')")


def test_join_silver_crimes_to_reference(store):
    store.write("chicago_crimes", CRIMES, primary_key="id")
    crimes.build(store)
    load_all(store)
    rows = store.con.execute(
        "SELECT c.crime_id, ca.name, pd.name FROM silver.crimes c "
        "LEFT JOIN reference.community_areas ca USING (community_area) "
        "LEFT JOIN reference.police_districts pd USING (district) ORDER BY c.crime_id"
    ).fetchall()
    assert rows[0] == (13001001, "Near North Side", "Near North")
    assert all(area is not None for _, area, _ in rows)


def test_cli_seed(tmp_path, capsys):
    db = str(tmp_path / "lh.duckdb")
    assert cli.main(["seed", "--db", db]) == 0
    out = capsys.readouterr().out
    assert "reference.community_areas: 77 rows (loaded" in out
    assert cli.main(["seed", "--db", db]) == 0
    assert "(unchanged" in capsys.readouterr().out
    assert cli.main(["seed", "--db", db, "--force"]) == 0
    assert "(loaded" in capsys.readouterr().out
