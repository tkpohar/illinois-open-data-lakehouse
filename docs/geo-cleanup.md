# Geospatial cleanup

`lakehouse geo <table>` validates the coordinates in a silver table and, given
community-area boundaries, finds the community area each point lies in. It runs on a
built silver table and adds columns in place:

```bash
lakehouse silver crimes
lakehouse geo crimes                                   # validation only
lakehouse geo crimes --areas community_areas.geojson   # + community-area lookup
```

```
silver.crimes: 5000 rows
  point: ok=4962, missing=31, null_island=2, swapped=1, outside_chicago=4
  point: community area filled=12 mismatched=3 unmatched=0
```

| Table | Points |
|---|---|
| `crimes` | `latitude` / `longitude` / `community_area` |
| `service_requests` | `latitude` / `longitude` / `community_area` |
| `taxi_trips` | `pickup_…` and `dropoff_…` centroids and community areas (columns prefixed `pickup_` / `dropoff_`) |

A silver rebuild recreates the table and drops these columns. `lakehouse silver <table>`
re-runs the matching geo step automatically ([silver framework](silver-framework.md)).

## 1. Coordinate validation

Each point gets `geo_status`, checked in this order:

| `geo_status` | Meaning | Clean coordinates |
|---|---|---|
| `missing` | latitude or longitude is NULL | NULL |
| `null_island` | exactly (0, 0), a classic placeholder | NULL |
| `ok` | inside the Chicago bounding box | as published |
| `swapped` | only inside the box with latitude and longitude exchanged | **corrected** (swapped back) |
| `out_of_range` | \|lat\| > 90 or \|lon\| > 180 | NULL |
| `outside_chicago` | a real coordinate, but outside the box | NULL |

`latitude_clean` / `longitude_clean` hold the usable coordinates. The original columns
are never changed. The bounding box is padded around the city limits:
latitude 41.6–42.05, longitude -87.95–-87.5
(`geo.CHICAGO_BBOX`).

## 2. Community-area lookup

With `--areas FILE`, every distinct clean point is matched against community-area
polygons. Columns added:

| Column | Meaning |
|---|---|
| `community_area_geo` | number of the polygon containing the point (NULL if none, or no clean point) |
| `community_area_mismatch` | TRUE when a valid reported area (1–77) differs from the geometric one; NULL when either is unknown |
| `community_area_resolved` | the reported area if valid (1–77), otherwise the geometric one |

The reported value is preferred because it is assigned by the source agency. The
geometric value fills gaps and flags disagreements for review.

### Boundary file

`--areas` takes a GeoJSON `FeatureCollection` of `Polygon` / `MultiPolygon` features (holes
supported), such as the City of Chicago's **Boundaries – Community Areas** dataset
exported as GeoJSON from the data portal. The area number is read from the first property
found among `area_numbe, area_num_1, area_number, community_area`, or from `--area-property NAME`.
The boundary file is not committed. Download it once and keep it next to the database
(e.g. under `data/`, which git ignores).

Point-in-polygon uses pure Python ray casting with a bounding-box pre-check, so no
spatial extension is required. Each distinct point is looked up once, which is fast at
this project's scale.

## Result

`clean_table()` returns a `GeoResult` with per-point status counts and, when areas were
given, the number of distinct points looked up and how many rows were filled,
mismatched or left unmatched. The same numbers are logged with structured fields.

```python
from lakehouse.silver import geo

areas = geo.load_areas("data/community_areas.geojson")
result = geo.clean_table(store.con, "silver.crimes", areas)
result.status_counts["point"]     # {"ok": 4962, "missing": 31, ...}
```

Tests use synthetic square "areas" (one with a hole, one MultiPolygon), not real boundaries.
