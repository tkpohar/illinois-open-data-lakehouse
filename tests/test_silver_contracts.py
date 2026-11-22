"""Contract tests every silver transform must pass.

Each silver table is listed once in ``TABLES``; every test below runs against all of
them. Adding a silver table means adding one ``TableCase`` — the suite then checks its
schema, keys, robustness, idempotence and incremental behaviour like the others.
"""

from __future__ import annotations

import json
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path

import duckdb
import pytest

from lakehouse.silver import crimes, geo, service_requests, taxi_trips
from lakehouse.silver.registry import REGISTRY
from lakehouse.silver.typing import KINDS, SQL_TYPES, Column, MissingInputError, typed_expr
from lakehouse.storage.bronze import BronzeStore

FIX = Path(__file__).parent / "fixtures"
LINEAGE = {"record_key": "VARCHAR", "run_id": "VARCHAR", "ingested_at": "TIMESTAMP",
           "silver_built_at": "TIMESTAMP"}


@dataclass(frozen=True)
class TableCase:
    name: str  # transform name
    module: object
    source: str
    key: str
    incremental_column: str
    fixture: str
    update: Callable[[dict], dict]  # produce a newer version of a record

    @property
    def table(self) -> str:
        return self.module.TABLE

    def records(self) -> list[dict]:
        return json.loads((FIX / self.fixture).read_text())


TABLES = [
    TableCase("crimes", crimes, "chicago_crimes", "id", "updated_on",
              "chicago_crimes_sample.json",
              lambda r: {**r, "arrest": True, "updated_on": "2027-01-01T00:00:00.000"}),
    TableCase("service_requests", service_requests, "chicago_311_requests", "sr_number",
              "last_modified_date", "chicago_311_sample.json",
              lambda r: {**r, "status": "Completed", "closed_date": "2027-01-02T00:00:00.000",
                         "last_modified_date": "2027-01-02T00:00:00.000"}),
    TableCase("taxi_trips", taxi_trips, "chicago_taxi_trips", "trip_id",
              "trip_start_timestamp", "chicago_taxi_sample.json",
              lambda r: {**r, "tips": "9.99", "trip_total": "99.99"}),
]
CASES = pytest.mark.parametrize("case", TABLES, ids=[c.name for c in TABLES])


def write(store: BronzeStore, case: TableCase, recs, run_id="r1") -> None:
    store.write(case.source, recs, primary_key=case.key,
                incremental_column=case.incremental_column, run_id=run_id)


def snapshot(store: BronzeStore, case: TableCase) -> list[tuple]:
    cols = [c.name for c in case.module.COLUMNS] + ["record_key", "run_id", "ingested_at"]
    return store.con.execute(
        f"SELECT {', '.join(cols)} FROM {case.table} ORDER BY record_key").fetchall()


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    yield s
    s.close()


# ---------------------------------------------------------------------- registration

def test_every_silver_module_is_covered_by_the_contract_suite():
    builds = {n for n, t in REGISTRY.transforms.items()
              if not n.endswith("_geo") and n != "reference_seeds"}
    assert builds == {c.name for c in TABLES}


@CASES
def test_registered_with_geo_step(case):
    assert case.name in REGISTRY.transforms
    if case.table in geo.GEO_TABLES:
        assert REGISTRY.transforms[f"{case.name}_geo"].depends_on == (case.name,)


@CASES
def test_column_specs_are_well_formed(case):
    names = [c.name for c in case.module.COLUMNS]
    derived = [n for n, _ in getattr(case.module, "DERIVED", [])]
    assert len(set(names + derived)) == len(names + derived), "duplicate column names"
    assert not set(names + derived) & set(LINEAGE), "clashes with lineage columns"
    for col in case.module.COLUMNS:
        assert col.kind in KINDS
        assert col.description, f"{col.name} has no description"
        assert col.name == col.name.lower() and " " not in col.name


@CASES
def test_geo_point_columns_exist(case):
    names = {c.name for c in case.module.COLUMNS}
    for pc in geo.GEO_TABLES.get(case.table, []):
        assert {pc.lat, pc.lon} <= names
        assert pc.area is None or pc.area in names


# ------------------------------------------------------------------------ the build

@CASES
def test_schema_and_types(case, store):
    write(store, case, case.records())
    case.module.build(store)
    got = {row[0]: row[1] for row in store.con.execute(f"DESCRIBE {case.table}").fetchall()}
    for col in case.module.COLUMNS:
        assert got[col.name] == SQL_TYPES[col.kind], col.name
    for name, typ in LINEAGE.items():
        assert got[name] == typ
    for name, _ in getattr(case.module, "DERIVED", []):
        assert name in got
    expected_order = [c.name for c in case.module.COLUMNS] + list(LINEAGE)
    assert list(got)[: len(expected_order)] == expected_order


@CASES
def test_fixture_builds_cleanly(case, store):
    recs = case.records()
    write(store, case, recs)
    result = case.module.build(store)
    assert result.rows == result.processed == len(recs)
    assert result.cast_failures == {}
    assert result.table == case.table


@CASES
def test_one_row_per_key_despite_bronze_versions(case, store):
    recs = case.records()
    write(store, case, recs, "r1")
    write(store, case, [case.update(r) for r in recs], "r2")
    write(store, case, recs[:1], "r3")  # stale copy of an old version, ingested last
    case.module.build(store)
    dupes = store.con.execute(
        f"SELECT count(*) - count(DISTINCT record_key) FROM {case.table}").fetchone()[0]
    assert dupes == 0
    assert store.con.execute(f"SELECT count(*) FROM {case.table}").fetchone()[0] == len(recs)


@CASES
def test_empty_bronze_builds_empty_table(case, store):
    write(store, case, [])
    result = case.module.build(store)
    assert result.rows == 0
    assert store.con.execute(f"SELECT count(*) FROM {case.table}").fetchone() == (0,)


@CASES
def test_missing_bronze_is_missing_input(case, store):
    with pytest.raises(MissingInputError):
        case.module.build(store)


@CASES
def test_key_only_records_give_nulls_not_failures(case, store):
    write(store, case, [{case.key: "101"}, {case.key: "102"}])  # numeric: some keys are ints
    result = case.module.build(store)
    assert result.rows == 2 and result.cast_failures == {}
    nulls = store.con.execute(
        f"SELECT count(*) FROM {case.table} WHERE "
        + " AND ".join(f"{c.name} IS NULL" for c in case.module.COLUMNS
                       if c.raw != case.key)
    ).fetchone()[0]
    assert nulls == 2


@CASES
def test_garbage_values_never_crash_and_are_counted(case, store):
    garbage = {c.raw: "%%garbage%%" for c in case.module.COLUMNS}
    garbage[case.key] = "103"
    write(store, case, [garbage])
    result = case.module.build(store)
    strict = {c.name for c in case.module.COLUMNS if c.kind in ("int", "double", "bool",
                                                                 "timestamp")}
    assert set(result.cast_failures) == strict - {
        c.name for c in case.module.COLUMNS if c.raw == case.key}
    assert all(n == 1 for n in result.cast_failures.values())


@CASES
def test_full_rebuild_is_idempotent(case, store):
    write(store, case, case.records())
    case.module.build(store)
    first = snapshot(store, case)
    case.module.build(store)
    assert snapshot(store, case) == first


@CASES
def test_incremental_matches_full(case, store):
    recs = case.records()
    write(store, case, recs[:2], "r1")
    case.module.build(store, incremental=True)
    write(store, case, [case.update(recs[0]), *recs[2:]], "r2")
    write(store, case, [recs[1]], "r3")  # unchanged re-delivery
    result = case.module.build(store, incremental=True)
    assert result.mode == "incremental"
    incremental = snapshot(store, case)
    case.module.build(store)
    assert snapshot(store, case) == incremental


@CASES
def test_quality_checks_are_valid_sql(case, store):
    write(store, case, case.records())
    result = case.module.build(store)
    assert set(result.quality) == set(getattr(case.module, "QUALITY_CHECKS", {}))
    assert all(isinstance(n, int) and n >= 0 for n in result.quality.values())


# ------------------------------------------------------------------ typing edge cases

def _typed(kind: str, raw, width=None):
    con = duckdb.connect()
    expr = typed_expr(Column("x", "x", kind, width=width))
    return con.execute(f"SELECT {expr} FROM (SELECT ?::JSON AS payload)",
                       [json.dumps({"x": raw})]).fetchone()[0]


@pytest.mark.parametrize("raw, expected", [
    ("12", 12), ("12.0", 12), (" 7 ", 7), ("-3", -3), ("1e3", 1000), (12, 12),
    ("12.5", None), ("", None), ("abc", None), (None, None)])
def test_int_kind(raw, expected):
    assert _typed("int", raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("41.5", 41.5), ("-87.6", -87.6), ("1e-3", 0.001), (3, 3.0), ("NaN?", None), ("", None)])
def test_double_kind(raw, expected):
    assert _typed("double", raw) == expected


@pytest.mark.parametrize("raw, expected", [
    ("2026-01-02T03:04:05.000", "2026-01-02 03:04:05"), ("2026-01-02 03:04", "2026-01-02 03:04:00"),
    ("2026-01-02", "2026-01-02 00:00:00"), ("02/01/2026", None), ("", None)])
def test_timestamp_kind(raw, expected):
    got = _typed("timestamp", raw)
    assert (str(got) if got is not None else None) == expected


@pytest.mark.parametrize("kind, raw, width, expected", [
    ("code", "7", 4, "0007"), ("code", "12345", 4, "1234"), ("code", " 42 ", 3, "042"),
    ("upper", "  a   b\tc ", None, "A B C"), ("text", "  keep Case ", None, "keep Case"),
    ("text", "   ", None, None)])
def test_text_kinds(kind, raw, width, expected):
    assert _typed(kind, raw, width) == expected


def test_geo_rejects_non_numeric_area_property(tmp_path):
    p = tmp_path / "a.geojson"
    p.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {"area_numbe": "eight"},
        "geometry": {"type": "Polygon", "coordinates": [[[0, 0], [1, 0], [1, 1], [0, 0]]]}}]}))
    with pytest.raises(ValueError, match="non-numeric area_numbe='eight'"):
        geo.load_areas(p)


def test_incremental_without_changes_relation_does_full_build(store):
    from lakehouse.silver.typing import build_typed_table

    write(store, TABLES[0], TABLES[0].records())
    r = build_typed_table(store.con, "bronze.chicago_crimes__latest", "silver.x",
                          [Column("crime_id", "id", "int")], incremental=True)
    assert r.mode == "full" and r.full_reason == "no changes relation"
