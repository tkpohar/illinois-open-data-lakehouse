import json
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.silver.registry import (
    REGISTRY,
    Context,
    CycleError,
    Registry,
    Transform,
    default_registry,
)
from lakehouse.silver.typing import BuildResult, MissingInputError
from lakehouse.storage.bronze import BronzeStore

FIX = Path(__file__).parent / "fixtures"
CRIMES = json.loads((FIX / "chicago_crimes_sample.json").read_text())


def noop(store, ctx):
    return "done"


def reg_of(edges: dict[str, tuple[str, ...]], runs=None) -> Registry:
    reg = Registry()
    for name, deps in edges.items():
        reg.add(Transform(name, (runs or {}).get(name, noop), deps))
    return reg


# ------------------------------------------------------------------------ planning

def test_plan_respects_dependencies_and_is_deterministic():
    reg = reg_of({"d": ("b", "c"), "b": ("a",), "c": ("a",), "a": (), "z": ()})
    order = reg.plan()
    assert order == ["a", "b", "c", "d", "z"]
    for name, t in reg.transforms.items():
        assert all(order.index(dep) < order.index(name) for dep in t.depends_on)


def test_selection_adds_downstream_by_default():
    reg = reg_of({"a": (), "b": ("a",), "c": ("b",), "x": ()})
    assert reg.plan(["b"]) == ["b", "c"]
    assert reg.plan(["b"], with_downstream=False) == ["b"]
    assert reg.plan(["c"], with_downstream=False, with_upstream=True) == ["a", "b", "c"]
    assert reg.plan(["b"], with_upstream=True) == ["a", "b", "c"]


def test_unknown_selection_and_dependencies():
    reg = reg_of({"a": ()})
    with pytest.raises(ValueError, match="Unknown transform"):
        reg.plan(["nope"])
    reg.add(Transform("b", noop, ("missing",)))
    with pytest.raises(ValueError, match="unknown transform"):
        reg.validate()


def test_cycle_detected():
    reg = reg_of({"a": ("c",), "b": ("a",), "c": ("b",), "ok": ()})
    with pytest.raises(CycleError, match=r"\['a', 'b', 'c'\]"):
        reg.validate()


def test_duplicate_registration_rejected():
    reg = reg_of({"a": ()})
    with pytest.raises(ValueError, match="already registered"):
        reg.add(Transform("a", noop))


# ------------------------------------------------------------------------- running

def test_run_executes_in_order_with_results():
    calls = []
    runs = {n: (lambda s, c, n=n: calls.append(n) or n.upper()) for n in "abc"}
    reg = reg_of({"c": ("b",), "b": ("a",), "a": ()}, runs)
    steps = reg.run(BronzeStore(":memory:"))
    assert calls == ["a", "b", "c"]
    assert [(s.name, s.status, s.result) for s in steps] == [
        ("a", "ok", "A"), ("b", "ok", "B"), ("c", "ok", "C")]
    assert all(s.duration_s >= 0 for s in steps)


def test_failure_skips_downstream_but_not_other_branches():
    def boom(store, ctx):
        raise RuntimeError("kaput")

    reg = reg_of({"a": (), "b": ("a",), "c": ("b",), "x": ()}, {"a": boom})
    steps = {s.name: s for s in reg.run(BronzeStore(":memory:"))}
    assert steps["a"].status == "failed" and "RuntimeError: kaput" in steps["a"].reason
    assert steps["b"].status == "skipped" and "upstream not ok: a" in steps["b"].reason
    assert steps["c"].status == "skipped" and "b" in steps["c"].reason
    assert steps["x"].status == "ok"


def test_missing_input_is_a_skip_not_a_failure():
    def missing(store, ctx):
        raise MissingInputError("no bronze yet")

    reg = reg_of({"a": (), "b": ("a",)}, {"a": missing})
    steps = reg.run(BronzeStore(":memory:"))
    assert [(s.name, s.status) for s in steps] == [("a", "skipped"), ("b", "skipped")]
    assert steps[0].reason == "no bronze yet"


# ------------------------------------------------------------------ default registry

def test_default_registry_shape():
    reg = default_registry()
    assert set(reg.transforms) == {
        "reference_seeds", "crimes", "crimes_geo", "service_requests",
        "service_requests_geo", "taxi_trips", "taxi_trips_geo",
    }
    assert reg.transforms["crimes_geo"].depends_on == ("crimes",)
    assert reg.plan(["crimes"]) == ["crimes", "crimes_geo"]


def test_default_run_with_partial_bronze():
    store = BronzeStore(":memory:")
    store.write("chicago_crimes", CRIMES, primary_key="id")
    steps = {s.name: s for s in REGISTRY.run(store)}
    assert steps["crimes"].status == "ok" and isinstance(steps["crimes"].result, BuildResult)
    assert steps["crimes_geo"].status == "ok"
    assert steps["crimes_geo"].result.status_counts["point"]["ok"] == 5
    assert steps["reference_seeds"].status == "ok"
    for name in ("service_requests", "taxi_trips"):
        assert steps[name].status == "skipped" and "No bronze data" in steps[name].reason
        assert steps[f"{name}_geo"].status == "skipped"
    cols = {c[0] for c in store.con.execute("DESCRIBE silver.crimes").fetchall()}
    assert "geo_status" in cols  # geo re-applied after the rebuild
    store.close()


def test_context_loads_areas_once(tmp_path):
    p = tmp_path / "a.geojson"
    p.write_text(json.dumps({"type": "FeatureCollection", "features": [{
        "type": "Feature", "properties": {"area_numbe": "8"},
        "geometry": {"type": "Polygon", "coordinates": [
            [[-87.7, 41.85], [-87.5, 41.85], [-87.5, 41.95], [-87.7, 41.95], [-87.7, 41.85]]]},
    }]}))
    ctx = Context(p)
    assert ctx.areas() is ctx.areas() and len(ctx.areas()) == 1
    assert Context().areas() is None


# ------------------------------------------------------------------------------ CLI

def test_cli_silver_list(capsys):
    assert cli.main(["silver", "--list"]) == 0
    lines = capsys.readouterr().out.strip().splitlines()
    names = [line.split(". ")[1].split()[0] for line in lines]
    assert names.index("crimes") < names.index("crimes_geo")
    assert "(after crimes)" in next(x for x in lines if "crimes_geo" in x)
    assert cli.main(["silver", "taxi_trips", "--list"]) == 0
    assert capsys.readouterr().out.split() == ["1.", "taxi_trips", "2.", "taxi_trips_geo",
                                               "(after", "taxi_trips)"]


def test_cli_silver_run_all_and_selected(tmp_path, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write("chicago_crimes", CRIMES, primary_key="id")
    store.close()

    assert cli.main(["silver", "--db", str(db)]) == 0  # missing sources are skips
    out = capsys.readouterr().out
    assert "silver.crimes: 5 rows from bronze.chicago_crimes__latest" in out
    assert "[skipped] taxi_trips: No bronze data" in out
    assert "3 ok, 4 skipped, 0 failed" in out

    assert cli.main(["silver", "crimes", "--db", str(db)]) == 0
    assert "2 ok, 0 skipped, 0 failed" in capsys.readouterr().out

    # explicitly requested but impossible -> non-zero exit
    assert cli.main(["silver", "taxi_trips", "--db", str(db)]) == 1
    assert cli.main(["silver", "nope", "--db", str(db)]) == 1
    assert cli.main(["silver", "--db", str(db), "--areas", str(tmp_path / "x.geojson")]) == 1
