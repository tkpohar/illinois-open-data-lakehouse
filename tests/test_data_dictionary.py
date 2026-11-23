import types

import pytest

from lakehouse import cli
from lakehouse.silver import crimes, dictionary, geo, service_requests, taxi_trips
from lakehouse.silver.typing import SQL_TYPES


def test_committed_dictionary_is_up_to_date():
    assert dictionary.check(), "docs/data-dictionary.md is stale: run `lakehouse docs dictionary`"


@pytest.mark.parametrize("module", dictionary.MODULES, ids=lambda m: m.TABLE)
def test_every_column_documented_with_real_type(module):
    cols = dictionary.describe_table(module)
    names = [c.name for c in cols]
    assert len(names) == len(set(names))
    assert all(c.description for c in cols)
    by_name = {c.name: c for c in cols}
    for spec in module.COLUMNS:
        assert by_name[spec.name].type == SQL_TYPES[spec.kind]
        assert by_name[spec.name].origin == f"`{spec.raw}` ({spec.kind})"
    for name in getattr(module, "DERIVED_DESCRIPTIONS", {}):
        assert by_name[name].origin == "derived"
    assert {"record_key", "run_id", "ingested_at", "silver_built_at"} <= set(names)
    for pc in geo.GEO_TABLES.get(module.TABLE, []):
        assert by_name[f"{pc.prefix}geo_status"].origin == "geo"
        assert f"{pc.prefix}community_area_resolved" in by_name


def test_derived_descriptions_match_derived_columns():
    for module in (service_requests, taxi_trips):
        assert set(module.DERIVED_DESCRIPTIONS) == {n for n, _ in module.DERIVED}
    assert not hasattr(crimes, "DERIVED")


def test_modules_cover_all_registered_silver_builds():
    from lakehouse.silver.registry import REGISTRY

    builds = {n for n in REGISTRY.transforms if not n.endswith("_geo") and n != "reference_seeds"}
    assert {m.TABLE.split(".")[1] for m in dictionary.MODULES} == builds


def test_render_contents():
    text = dictionary.render()
    assert text.startswith("# Data dictionary\n")
    for m in dictionary.MODULES:
        assert f"## {m.TABLE}" in text and m.GRAIN in text
    assert "| `status` | VARCHAR | derived |" in text
    assert "| `outlier_reasons` | VARCHAR[] | derived |" in text
    assert "| `pickup_geo_status` | VARCHAR | geo |" in text
    assert "`unknown_status`" in text  # quality checks listed
    assert "`reference.community_areas`" in text


def test_undocumented_column_is_an_error():
    fake = types.SimpleNamespace(**{k: getattr(service_requests, k) for k in
                                    ("TABLE", "SOURCE", "COLUMNS", "GRAIN", "QUALITY_CHECKS")})
    fake.DERIVED_DESCRIPTIONS = {}  # derived columns exist but are undocumented
    fake.build = service_requests.build
    with pytest.raises(ValueError, match="has no description"):
        dictionary.describe_table(fake)


def test_pipe_characters_are_escaped():
    assert dictionary._md("a | b") == "a \\| b"


def test_cli_docs_dictionary(tmp_path, capsys):
    out = tmp_path / "dd.md"
    assert cli.main(["docs", "dictionary", "--out", str(out), "--check"]) == 1
    assert "stale" in capsys.readouterr().out
    assert cli.main(["docs", "dictionary", "--out", str(out)]) == 0
    assert out.read_text() == dictionary.render()
    assert cli.main(["docs", "dictionary", "--out", str(out), "--check"]) == 0
    assert "up to date" in capsys.readouterr().out.splitlines()[-1]
    out.write_text(out.read_text() + "edited by hand\n")
    assert cli.main(["docs", "dictionary", "--out", str(out), "--check"]) == 1
