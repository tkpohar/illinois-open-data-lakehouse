# Ingestion metrics: `lakehouse status`

One command answers "is the lakehouse healthy?" for every source: what is in bronze, how
fresh it is, how the last runs went, how hard the API pushed back, and whether the schema
changed.

```bash
lakehouse status                    # table
lakehouse status --json             # machine-readable (dashboards, alerting)
lakehouse status --stale-hours 12   # stricter freshness threshold (default 36h)
```

```
lakehouse status  (2026-11-12T04:00:00+00:00)
source                          rows      keys  runs     last run  last n  429s drift     flags
-----------------------------------------------------------------------------------------------
chicago_crimes                     7         5     3      19m ago       0     2 breaking  2 superseded
chicago_311_requests               0         0     0        never       -     0 -         NEVER INGESTED
-----------------------------------------------------------------------------------------------
1/2 sources ingested, 7 rows, 3 runs, 1 stale (> 36h), 2 rate-limited requests
```

Every configured source is listed (even if never ingested), plus any bronze table that is
not in `config/sources.yaml` (flagged `not in config`).

## Metrics per source

| Field | Source of truth | Meaning |
|---|---|---|
| `rows`, `keys`, `duplicates` | `bronze.<source>` | raw versions, distinct keys, superseded versions |
| `runs`, `empty_runs` | `bronze._ingestion_log` | ingestion runs, runs that returned nothing |
| `last_run_id`, `last_run_finished`, `last_run_records`, `last_run_duration_s` | `_ingestion_log` | the most recent run |
| `hours_since_last_run`, `stale` | `_ingestion_log` | stale = last run older than `--stale-hours`, or never ingested |
| `watermark` | `_ingestion_log` | where the next incremental run starts |
| `avg_records_recent` | `_ingestion_log` | mean records over the last 7 runs |
| `api_requests`, `api_retries`, `rate_limited`, `waited_s` | `_ingestion_log` | totals over all runs, from `SocrataClient.stats` (see [rate-limits.md](rate-limits.md)) |
| `schema_fingerprint`, `schema_fields` | `_schema_snapshots` | latest snapshot ([schema-snapshots.md](schema-snapshots.md)) |
| `last_drift_severity`, `last_drift_run_id` | `_schema_drift` | highest severity of the most recent drifting run ([schema-drift.md](schema-drift.md)) |

## Where request metrics come from

`lakehouse ingest` passes the client's `RequestStats` to `BronzeStore.write`, which stores
them on the run's `_ingestion_log` row (`api_requests`, `api_retries`, `rate_limited`,
`waited_s`). They are read after the records are consumed, so they cover the whole run.
Runs written without a client (tests, backfills from files) store `NULL`, counted as 0.
Older databases get the columns added automatically.

## In code

```python
from lakehouse.status import collect_status, format_status, status_json

report = collect_status(store, load_sources(), stale_hours=36)
report.totals          # {"sources": 4, "ingested": 3, "stale": 1, "rows": ..., ...}
[s for s in report.sources if s.stale]
```
