# Silver transform framework

Silver is built by a small set of named **transforms** with explicit dependencies. A
registry plans them in dependency order and runs them with per-step status, so one
command rebuilds everything that needs rebuilding:

```bash
lakehouse silver                 # run every transform
lakehouse silver crimes          # crimes + everything downstream (crimes_geo)
lakehouse silver --list          # show the plan without running
lakehouse silver crimes_geo --upstream --no-downstream --list
lakehouse silver --areas data/community_areas.geojson   # geo steps also look up areas
lakehouse silver --full-refresh  # rebuild instead of processing only new rows
```

```
[ok] reference_seeds
silver.crimes: 5000 rows from bronze.chicago_crimes__latest
silver.crimes: 5000 rows
  point: ok=4962, missing=31, null_island=2, swapped=1, outside_chicago=4
[skipped] taxi_trips: No bronze data for 'chicago_taxi_trips'; run `lakehouse ingest chicago_taxi_trips` first
[skipped] taxi_trips_geo: upstream not ok: taxi_trips
...
4 ok, 3 skipped, 0 failed
```

## Registered transforms

| Transform | Depends on | Does |
|---|---|---|
| `reference_seeds` | — | load `seeds/` into `reference.*` ([reference data](reference-data.md)) |
| `crimes` | — | build `silver.crimes` ([details](silver-crimes.md)) |
| `crimes_geo` | `crimes` | coordinate validation / community areas ([geo](geo-cleanup.md)) |
| `service_requests` | — | build `silver.service_requests` ([details](silver-311.md)) |
| `service_requests_geo` | `service_requests` | geo cleanup |
| `taxi_trips` | — | build `silver.taxi_trips` ([details](silver-taxi.md)) |
| `taxi_trips_geo` | `taxi_trips` | geo cleanup (pickup and dropoff) |

## Planning

- **Order.** Kahn's topological sort; among steps ready at the same time, alphabetical
  order, so the plan is deterministic. A dependency cycle raises `CycleError`, and a
  dependency on an unregistered step is rejected when the registry is validated.
- **Selection.** Naming a step also selects everything **downstream** of it. A silver
  rebuild recreates the table and drops columns added later (e.g. geo), so its
  dependents must run again. `--no-downstream` turns this off. `--upstream` also runs
  the selected steps' dependencies. With no names, every step runs.

Silver builds run **incrementally** by default; see [incremental builds](silver-incremental.md).

## Run semantics

| Outcome | When | Effect on dependents |
|---|---|---|
| `ok` | the step returned | they run |
| `skipped` | its input does not exist yet (`MissingInputError`, e.g. a source never ingested), or an upstream step was not ok | skipped |
| `failed` | any other exception (logged with traceback) | skipped; **other branches still run** |

Each `StepResult` has the status, duration, the step's own result (`BuildResult`,
`GeoResult`, seed results) and a reason for skips and failures.

Exit status of `lakehouse silver`: **1** if any step failed, or if a step you named
explicitly was skipped. Otherwise **0**. A full run where some sources haven't been
ingested yet is normal and exits 0.

## Adding a transform

```python
from lakehouse.silver.registry import REGISTRY, Transform

REGISTRY.add(Transform(
    "crimes_daily", lambda store, ctx: build_daily(store), depends_on=("crimes",),
    description="daily crime counts",
))
```

Raise `lakehouse.silver.typing.MissingInputError` when an input is absent, so the step is
skipped rather than failed.
