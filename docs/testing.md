# Testing

All tests are offline: no network, no live APIs. Each test builds its own in-memory
DuckDB (`BronzeStore(":memory:")`) from JSON fixtures in `tests/fixtures/`. The non-Chicago-crimes
fixtures are synthetic and only mimic each dataset's shape.

```bash
pytest -q                                     # whole suite
pytest -q --cov=lakehouse                     # with coverage (pytest-cov is a dev dependency)
coverage report --include="src/lakehouse/silver/*" --fail-under=95
```

CI (`.github/workflows/ci.yml`) runs `ruff check .`, the suite with coverage, and a
**coverage gate: the silver package must stay at ≥ 95 % line coverage** (it is at 100 %).

## Silver transform contracts

`tests/test_silver_contracts.py` defines every silver table once in `TABLES`, and every
contract test runs against all of them:

| Contract | What it guarantees |
|---|---|
| suite completeness | every silver build in the transform registry has a `TableCase`. A new table can't be added without joining the suite |
| registration | each table is registered, and tables with coordinates have a `*_geo` step depending on the build |
| column specs | unique names, no clash with lineage columns, a valid kind and a description for every column; geo point columns exist |
| schema & types | each column has the SQL type of its kind, in spec order, followed by the lineage columns; derived columns are present |
| clean fixture | the fixture builds with zero cast failures |
| one row per key | several bronze versions (including a stale copy ingested last) give exactly one silver row per key |
| empty / missing input | an empty bronze table builds an empty table; a missing one raises `MissingInputError` (a skip, not a failure) |
| sparse records | key-only records give NULLs, not cast failures |
| garbage values | non-castable values never crash a build; exactly the strict columns (int, double, bool, timestamp) are counted as cast failures |
| idempotent | two full builds give identical rows |
| incremental = full | an incremental build after updates and re-deliveries matches a full rebuild |
| quality checks | every declared check runs and returns a non-negative count |

They run alongside the per-table suites (`test_silver_crimes.py`, `test_silver_311.py`,
`test_silver_taxi.py`, `test_geo.py`, `test_silver_registry.py`,
`test_silver_incremental.py`), which cover table-specific rules such as status
normalisation and outlier flags.

The typing layer also has edge-case matrices for every column kind (`int` accepts
`"12.0"` and `"1e3"` but rejects `"12.5"`; `code` pads and truncates to its width; and so on).

## Adding a silver table

1. Write the module (`COLUMNS`, optional `DERIVED` / `QUALITY_CHECKS`, `build()`).
2. Register it in `lakehouse.silver.registry`.
3. Add a fixture and a `TableCase` to `TABLES` in `tests/test_silver_contracts.py`.
   The completeness test fails until you do.
