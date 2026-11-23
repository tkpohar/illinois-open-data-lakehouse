"""Geospatial cleanup for silver tables: coordinate validation and community-area lookup.

Two independent steps, both run *after* a silver table is built (``lakehouse geo``):

1. **Validation** (no extra data needed): each point gets a ``geo_status`` —
   ``ok``, ``missing``, ``null_island`` (0, 0), ``swapped`` (latitude/longitude
   exchanged; corrected), ``out_of_range`` (impossible degrees) or ``outside_chicago``
   (outside a padded bounding box). ``latitude_clean`` / ``longitude_clean`` hold usable
   coordinates (NULL unless the status is ``ok`` or ``swapped``).
2. **Community-area lookup** (needs boundary polygons): clean points are matched to the
   community-area polygon that contains them (``community_area_geo``), the reported
   value is compared with it (``community_area_mismatch``) and
   ``community_area_resolved`` prefers the reported area, falling back to the geometric
   one. Boundaries are read from a GeoJSON ``FeatureCollection`` — e.g. the City of
   Chicago's "Boundaries - Community Areas" export.

Point-in-polygon is implemented in pure Python (ray casting with holes and
MultiPolygons), so there is no dependency on a spatial extension. See
``docs/geo-cleanup.md``.
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable, Sequence
from dataclasses import dataclass, field
from pathlib import Path

import duckdb

from lakehouse.silver.typing import MissingInputError

log = logging.getLogger(__name__)

# Padded bounding box around the City of Chicago (city limits are roughly
# 41.64..42.02 N and 87.94..87.52 W; the padding keeps edge addresses).
CHICAGO_BBOX = {"min_lat": 41.60, "max_lat": 42.05, "min_lon": -87.95, "max_lon": -87.50}
COMMUNITY_AREA_RANGE = (1, 77)
GEO_STATUSES = ("ok", "missing", "null_island", "swapped", "out_of_range", "outside_chicago")
AREA_PROPERTY_CANDIDATES = ("area_numbe", "area_num_1", "area_number", "community_area")


# --------------------------------------------------------------------------- polygons

Ring = Sequence[Sequence[float]]  # [[lon, lat], ...]


def _in_ring(lon: float, lat: float, ring: Ring) -> bool:
    inside = False
    n = len(ring)
    j = n - 1
    for i in range(n):
        xi, yi = ring[i][0], ring[i][1]
        xj, yj = ring[j][0], ring[j][1]
        if (yi > lat) != (yj > lat):
            x_cross = (xj - xi) * (lat - yi) / (yj - yi) + xi
            if lon < x_cross:
                inside = not inside
        j = i
    return inside


def _in_polygon(lon: float, lat: float, rings: Sequence[Ring]) -> bool:
    """GeoJSON polygon: first ring is the outer boundary, the rest are holes."""
    if not rings or not _in_ring(lon, lat, rings[0]):
        return False
    return not any(_in_ring(lon, lat, hole) for hole in rings[1:])


@dataclass
class Area:
    number: int
    polygons: list[list[Ring]]  # list of polygons, each a list of rings
    bbox: tuple[float, float, float, float] = (0, 0, 0, 0)  # min_lon, min_lat, max_lon, max_lat

    def __post_init__(self) -> None:
        pts = [p for poly in self.polygons for ring in poly[:1] for p in ring]
        lons, lats = [p[0] for p in pts], [p[1] for p in pts]
        self.bbox = (min(lons), min(lats), max(lons), max(lats))

    def contains(self, lat: float, lon: float) -> bool:
        min_lon, min_lat, max_lon, max_lat = self.bbox
        if not (min_lon <= lon <= max_lon and min_lat <= lat <= max_lat):
            return False
        return any(_in_polygon(lon, lat, poly) for poly in self.polygons)


@dataclass
class AreaIndex:
    areas: list[Area] = field(default_factory=list)

    def lookup(self, lat: float, lon: float) -> int | None:
        for area in self.areas:
            if area.contains(lat, lon):
                return area.number
        return None

    def __len__(self) -> int:
        return len(self.areas)


def load_areas(path: Path | str, area_property: str | None = None) -> AreaIndex:
    """Read community-area polygons from a GeoJSON FeatureCollection."""
    data = json.loads(Path(path).read_text())
    features = data.get("features") if isinstance(data, dict) else None
    if not features:
        raise ValueError(f"{path}: not a GeoJSON FeatureCollection with features")
    props0 = features[0].get("properties") or {}
    prop = area_property or next(
        (c for c in AREA_PROPERTY_CANDIDATES if c in props0), None
    )
    if prop is None:
        raise ValueError(
            f"{path}: no community-area number property found (tried "
            f"{', '.join(AREA_PROPERTY_CANDIDATES)}); pass area_property"
        )
    index = AreaIndex()
    for feat in features:
        geom = feat.get("geometry") or {}
        raw = (feat.get("properties") or {}).get(prop)
        try:
            number = int(float(raw))
        except (TypeError, ValueError) as exc:
            raise ValueError(f"{path}: feature has non-numeric {prop}={raw!r}") from exc
        if geom.get("type") == "Polygon":
            polygons = [geom["coordinates"]]
        elif geom.get("type") == "MultiPolygon":
            polygons = list(geom["coordinates"])
        else:
            raise ValueError(f"{path}: unsupported geometry {geom.get('type')!r}")
        index.areas.append(Area(number, polygons))
    return index


# ------------------------------------------------------------------------- validation


def geo_status_sql(lat: str, lon: str, bbox: dict[str, float] = CHICAGO_BBOX) -> str:
    """SQL CASE classifying a (lat, lon) pair; see module docstring."""
    def inside(la: str, lo: str) -> str:
        return (
            f"{la} BETWEEN {bbox['min_lat']} AND {bbox['max_lat']} "
            f"AND {lo} BETWEEN {bbox['min_lon']} AND {bbox['max_lon']}"
        )

    return (
        f"CASE WHEN {lat} IS NULL OR {lon} IS NULL THEN 'missing' "
        f"WHEN {lat} = 0 AND {lon} = 0 THEN 'null_island' "
        f"WHEN {inside(lat, lon)} THEN 'ok' "
        f"WHEN {inside(lon, lat)} THEN 'swapped' "
        f"WHEN abs({lat}) > 90 OR abs({lon}) > 180 THEN 'out_of_range' "
        "ELSE 'outside_chicago' END"
    )


#: Columns added by ``clean_table`` (prefixed per point, e.g. ``pickup_``).
GEO_COLUMN_DESCRIPTIONS: dict[str, str] = {
    "geo_status": "Coordinate check: ok, missing, null_island, swapped, out_of_range "
    "or outside_chicago",
    "latitude_clean": "Usable latitude (swapped pairs corrected; NULL unless ok/swapped)",
    "longitude_clean": "Usable longitude (swapped pairs corrected; NULL unless ok/swapped)",
    "community_area_geo": "Community area whose polygon contains the clean point "
    "(needs --areas)",
    "community_area_mismatch": "Reported community area differs from the geometric one "
    "(needs --areas)",
    "community_area_resolved": "Reported community area if valid (1-77), else the "
    "geometric one (needs --areas)",
}


@dataclass(frozen=True)
class PointColumns:
    """Where a table keeps one point. ``prefix`` names the added columns."""

    lat: str
    lon: str
    area: str | None
    prefix: str = ""


#: Silver tables with coordinates, and their point columns.
GEO_TABLES: dict[str, list[PointColumns]] = {
    "silver.crimes": [PointColumns("latitude", "longitude", "community_area")],
    "silver.service_requests": [PointColumns("latitude", "longitude", "community_area")],
    "silver.taxi_trips": [
        PointColumns("pickup_latitude", "pickup_longitude", "pickup_community_area", "pickup_"),
        PointColumns(
            "dropoff_latitude", "dropoff_longitude", "dropoff_community_area", "dropoff_"
        ),
    ],
}


@dataclass
class GeoResult:
    table: str
    rows: int
    status_counts: dict[str, dict[str, int]] = field(default_factory=dict)  # prefix -> status
    looked_up: int = 0  # distinct clean points matched against polygons
    unmatched: dict[str, int] = field(default_factory=dict)  # clean points in no polygon
    mismatches: dict[str, int] = field(default_factory=dict)  # reported != geometric
    filled: dict[str, int] = field(default_factory=dict)  # area filled from geometry


def _add_columns(con, table: str, cols: Iterable[tuple[str, str]]) -> None:
    for name, typ in cols:
        con.execute(f"ALTER TABLE {table} ADD COLUMN IF NOT EXISTS {name} {typ}")


def clean_table(
    con: duckdb.DuckDBPyConnection,
    table: str,
    areas: AreaIndex | None = None,
    points: Sequence[PointColumns] | None = None,
) -> GeoResult:
    """Validate coordinates (and, given ``areas``, look up community areas) in place."""
    points = points if points is not None else GEO_TABLES.get(table)
    if not points:
        raise ValueError(f"No point columns known for {table}")
    exists = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema || '.' || table_name = ?",
        [table],
    ).fetchone()[0]
    if not exists:
        raise MissingInputError(f"{table} does not exist; build it first")

    rows = con.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
    result = GeoResult(table=table, rows=rows)
    lo, hi = COMMUNITY_AREA_RANGE
    for pc in points:
        p = pc.prefix
        status, lat_c, lon_c = f"{p}geo_status", f"{p}latitude_clean", f"{p}longitude_clean"
        _add_columns(con, table, [(status, "VARCHAR"), (lat_c, "DOUBLE"), (lon_c, "DOUBLE")])
        con.execute(
            f"UPDATE {table} SET {status} = {geo_status_sql(pc.lat, pc.lon)}"
        )
        con.execute(
            f"UPDATE {table} SET "
            f"{lat_c} = CASE {status} WHEN 'ok' THEN {pc.lat} WHEN 'swapped' THEN {pc.lon} END, "
            f"{lon_c} = CASE {status} WHEN 'ok' THEN {pc.lon} WHEN 'swapped' THEN {pc.lat} END"
        )
        counts = dict(
            con.execute(f"SELECT {status}, count(*) FROM {table} GROUP BY 1").fetchall()
        )
        result.status_counts[p or "point"] = {s: int(counts.get(s, 0)) for s in GEO_STATUSES}

        if areas is None or pc.area is None:
            continue
        geo, mism, resolved = (
            f"{p}community_area_geo",
            f"{p}community_area_mismatch",
            f"{p}community_area_resolved",
        )
        _add_columns(con, table, [(geo, "INTEGER"), (mism, "BOOLEAN"), (resolved, "INTEGER")])
        distinct = con.execute(
            f"SELECT DISTINCT {lat_c}, {lon_c} FROM {table} WHERE {lat_c} IS NOT NULL"
        ).fetchall()
        mapping = [(la, lo_, areas.lookup(la, lo_)) for la, lo_ in distinct]
        result.looked_up += len(mapping)
        con.execute(
            "CREATE OR REPLACE TEMP TABLE _geo_lookup (lat DOUBLE, lon DOUBLE, area INTEGER)"
        )
        if mapping:
            con.executemany("INSERT INTO _geo_lookup VALUES (?, ?, ?)", mapping)
        con.execute(f"UPDATE {table} SET {geo} = NULL")
        con.execute(
            f"UPDATE {table} t SET {geo} = g.area FROM _geo_lookup g "
            f"WHERE t.{lat_c} = g.lat AND t.{lon_c} = g.lon"
        )
        reported_ok = f"{pc.area} BETWEEN {lo} AND {hi}"
        con.execute(
            f"UPDATE {table} SET "
            f"{mism} = CASE WHEN {reported_ok} AND {geo} IS NOT NULL "
            f"THEN {pc.area} <> {geo} END, "
            f"{resolved} = CASE WHEN {reported_ok} THEN {pc.area} ELSE {geo} END"
        )
        key = p or "point"
        result.unmatched[key] = int(
            con.execute(
                f"SELECT count(*) FROM {table} WHERE {lat_c} IS NOT NULL AND {geo} IS NULL"
            ).fetchone()[0]
        )
        result.mismatches[key] = int(
            con.execute(f"SELECT count(*) FROM {table} WHERE {mism}").fetchone()[0]
        )
        result.filled[key] = int(
            con.execute(
                f"SELECT count(*) FROM {table} WHERE NOT coalesce({reported_ok}, FALSE) "
                f"AND {geo} IS NOT NULL"
            ).fetchone()[0]
        )
        con.execute("DROP TABLE _geo_lookup")

    bad = {
        k: {s: n for s, n in v.items() if s != "ok" and n}
        for k, v in result.status_counts.items()
    }
    log.info(
        "Geo cleanup of %s: %d rows",
        table,
        result.rows,
        extra={
            "table": table,
            "geo_issues": bad,
            "unmatched": result.unmatched,
            "mismatches": result.mismatches,
            "filled": result.filled,
        },
    )
    return result
