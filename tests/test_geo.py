"""Geospatial cleanup. Boundaries here are SYNTHETIC squares, not real community areas."""

import json
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.silver import crimes, geo
from lakehouse.silver import taxi_trips as taxi
from lakehouse.storage.bronze import BronzeStore

CRIMES = json.loads((Path(__file__).parent / "fixtures" / "chicago_crimes_sample.json").read_text())
TAXI = json.loads((Path(__file__).parent / "fixtures" / "chicago_taxi_sample.json").read_text())


def square(min_lon, min_lat, max_lon, max_lat):
    return [[min_lon, min_lat], [max_lon, min_lat], [max_lon, max_lat],
            [min_lon, max_lat], [min_lon, min_lat]]


AREAS_GEOJSON = {
    "type": "FeatureCollection",
    "features": [
        {   # area 8 with a hole in the middle
            "type": "Feature",
            "properties": {"area_numbe": "8", "community": "SYNTHETIC A"},
            "geometry": {"type": "Polygon", "coordinates": [
                square(-87.65, 41.88, -87.60, 41.92),
                square(-87.63, 41.895, -87.62, 41.905),
            ]},
        },
        {   # area 32 as a MultiPolygon of two squares
            "type": "Feature",
            "properties": {"area_numbe": "32", "community": "SYNTHETIC B"},
            "geometry": {"type": "MultiPolygon", "coordinates": [
                [square(-87.65, 41.86, -87.60, 41.88)],
                [square(-87.80, 41.70, -87.75, 41.75)],
            ]},
        },
    ],
}


@pytest.fixture
def areas_file(tmp_path):
    p = tmp_path / "areas.geojson"
    p.write_text(json.dumps(AREAS_GEOJSON))
    return p


@pytest.fixture
def index(areas_file):
    return geo.load_areas(areas_file)


def _crime(i, lat, lon, area="8"):
    return {**CRIMES[0], "id": str(i), "latitude": lat, "longitude": lon, "community_area": area}


def _built_crimes(recs):
    store = BronzeStore(":memory:")
    store.write("chicago_crimes", recs, primary_key="id")
    crimes.build(store)
    return store


def _rows(store, table="silver.crimes", key="crime_id"):
    cur = store.con.execute(f"SELECT * FROM {table} ORDER BY {key}")
    cols = [d[0] for d in cur.description]
    return {r[cols.index(key)]: dict(zip(cols, r, strict=True)) for r in cur.fetchall()}


# ------------------------------------------------------------------- polygon lookup

def test_load_areas_and_lookup(index):
    assert len(index) == 2
    assert index.lookup(41.885, -87.61) == 8  # in area 8, outside the hole
    assert index.lookup(41.90, -87.625) is None  # inside the hole
    assert index.lookup(41.87, -87.62) == 32  # first piece of the MultiPolygon
    assert index.lookup(41.72, -87.78) == 32  # second piece
    assert index.lookup(41.99, -87.70) is None  # in no polygon


def test_load_areas_explicit_property_and_errors(tmp_path):
    data = {"type": "FeatureCollection", "features": [
        {"type": "Feature", "properties": {"ca": 5},
         "geometry": {"type": "Polygon", "coordinates": [square(0, 0, 1, 1)]}}]}
    p = tmp_path / "a.geojson"
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="no community-area number property"):
        geo.load_areas(p)
    assert geo.load_areas(p, area_property="ca").lookup(0.5, 0.5) == 5

    bad = tmp_path / "b.geojson"
    bad.write_text(json.dumps({"type": "FeatureCollection", "features": []}))
    with pytest.raises(ValueError, match="FeatureCollection"):
        geo.load_areas(bad)

    data["features"][0]["geometry"] = {"type": "Point", "coordinates": [0, 0]}
    p.write_text(json.dumps(data))
    with pytest.raises(ValueError, match="unsupported geometry"):
        geo.load_areas(p, area_property="ca")


# ---------------------------------------------------------------------- validation

@pytest.mark.parametrize(
    "lat, lon, status, clean",
    [
        ("41.8925", "-87.6242", "ok", (41.8925, -87.6242)),
        (None, "-87.6", "missing", (None, None)),
        ("0", "0", "null_island", (None, None)),
        ("-87.6242", "41.8925", "swapped", (41.8925, -87.6242)),
        ("123", "-87.6", "out_of_range", (None, None)),
        ("40.7128", "-74.0060", "outside_chicago", (None, None)),
    ],
)
def test_geo_status(lat, lon, status, clean):
    rec = {**CRIMES[0], "latitude": lat, "longitude": lon}
    store = _built_crimes([rec])
    result = geo.clean_table(store.con, "silver.crimes")
    (row,) = _rows(store).values()
    assert row["geo_status"] == status
    got = (row["latitude_clean"], row["longitude_clean"])
    assert got == pytest.approx(clean) if clean[0] else got == clean
    assert result.status_counts["point"][status] == 1
    assert set(result.status_counts["point"]) == set(geo.GEO_STATUSES)
    store.close()


def test_validation_only_without_areas_adds_no_area_columns():
    store = _built_crimes(CRIMES)
    result = geo.clean_table(store.con, "silver.crimes")
    cols = {c[0] for c in store.con.execute("DESCRIBE silver.crimes").fetchall()}
    assert {"geo_status", "latitude_clean", "longitude_clean"} <= cols
    assert "community_area_geo" not in cols
    assert result.mismatches == {} and result.looked_up == 0
    store.close()


# ------------------------------------------------------------------ area lookup

def test_community_area_lookup_fill_and_mismatch(index):
    recs = [
        _crime(1, "41.885", "-87.61", "8"),     # agrees
        _crime(2, "41.87", "-87.62", "8"),      # reported 8, geometry says 32
        _crime(3, "41.885", "-87.61", None),    # missing area -> filled from geometry
        _crime(4, "41.885", "-87.61", "99"),    # invalid area -> filled from geometry
        _crime(5, "41.99", "-87.70", "3"),      # clean point in no polygon
        _crime(6, None, None, "8"),             # no point: keeps reported area
        _crime(7, "-87.61", "41.885", None),    # swapped -> corrected and looked up
    ]
    store = _built_crimes(recs)
    result = geo.clean_table(store.con, "silver.crimes", index)
    rows = _rows(store)
    got = {
        k: (r["community_area_geo"], r["community_area_mismatch"], r["community_area_resolved"])
        for k, r in rows.items()
    }
    assert got == {
        1: (8, False, 8),
        2: (32, True, 8),
        3: (8, None, 8),
        4: (8, None, 8),
        5: (None, None, 3),
        6: (None, None, 8),
        7: (8, None, 8),
    }
    assert result.mismatches == {"point": 1}
    assert result.filled == {"point": 3}
    assert result.unmatched == {"point": 1}
    assert result.looked_up == 3  # distinct clean points
    store.close()


def test_rerun_is_idempotent(index):
    store = _built_crimes([_crime(1, "41.885", "-87.61"), _crime(2, "41.87", "-87.62")])
    first = geo.clean_table(store.con, "silver.crimes", index)
    before = _rows(store)
    second = geo.clean_table(store.con, "silver.crimes", index)
    assert _rows(store) == before
    assert first.mismatches == second.mismatches
    store.close()


def test_taxi_pickup_and_dropoff_prefixes(index):
    store = BronzeStore(":memory:")
    trip = {**TAXI[0], "pickup_centroid_latitude": "41.885", "pickup_centroid_longitude": "-87.61",
            "dropoff_centroid_latitude": "0", "dropoff_centroid_longitude": "0",
            "pickup_community_area": "8", "dropoff_community_area": "32"}
    store.write("chicago_taxi_trips", [trip], primary_key="trip_id")
    taxi.build(store)
    result = geo.clean_table(store.con, "silver.taxi_trips", index)
    (row,) = _rows(store, "silver.taxi_trips", "trip_id").values()
    assert row["pickup_geo_status"] == "ok" and row["pickup_community_area_geo"] == 8
    assert row["dropoff_geo_status"] == "null_island"
    assert row["dropoff_community_area_geo"] is None
    assert row["dropoff_community_area_resolved"] == 32
    assert set(result.status_counts) == {"pickup_", "dropoff_"}
    store.close()


def test_unknown_or_missing_table():
    store = BronzeStore(":memory:")
    with pytest.raises(ValueError, match="No point columns"):
        geo.clean_table(store.con, "silver.nope")
    with pytest.raises(ValueError, match="build it first"):
        geo.clean_table(store.con, "silver.crimes")
    store.close()


def test_rebuild_then_geo_again(index):
    store = _built_crimes([_crime(1, "41.885", "-87.61")])
    geo.clean_table(store.con, "silver.crimes", index)
    crimes.build(store)  # a rebuild drops the geo columns ...
    cols = {c[0] for c in store.con.execute("DESCRIBE silver.crimes").fetchall()}
    assert "geo_status" not in cols
    geo.clean_table(store.con, "silver.crimes", index)  # ... and geo re-adds them
    assert _rows(store)[1]["community_area_geo"] == 8
    store.close()


# ------------------------------------------------------------------------------ CLI

def test_cli_geo(tmp_path, areas_file, capsys):
    db = tmp_path / "lh.duckdb"
    store = BronzeStore(db)
    store.write("chicago_crimes", [_crime(1, "41.885", "-87.61"), _crime(2, "0", "0")],
                primary_key="id")
    crimes.build(store)
    store.close()

    assert cli.main(["geo", "crimes", "--db", str(db)]) == 0
    out = capsys.readouterr().out
    assert "silver.crimes: 2 rows" in out and "ok=1, null_island=1" in out
    assert "lookup skipped" in out

    assert cli.main(["geo", "crimes", "--db", str(db), "--areas", str(areas_file)]) == 0
    assert "community area filled=0 mismatched=0 unmatched=0" in capsys.readouterr().out

    assert cli.main(["geo", "crimes", "--db", str(db), "--areas", str(tmp_path / "x")]) == 1
    assert cli.main(["geo", "taxi_trips", "--db", str(db)]) == 1  # not built yet
