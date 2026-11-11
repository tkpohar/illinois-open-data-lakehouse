# Illinois Open-Data Lakehouse

An end-to-end data platform built on public datasets from the **City of Chicago** and the
**State of Illinois**: ingestion, a medallion (bronze → silver → gold) lakehouse, data-quality
checks, orchestration, and analytics.

It is built step by step; see [ROADMAP.md](ROADMAP.md) for what's done and what's next, and
[CHANGELOG.md](CHANGELOG.md) for daily progress.

## Architecture

```
 Socrata APIs ──► Ingestion (Python, incremental, retries)
                       │
                       ▼
                 BRONZE  raw JSON + lineage            (DuckDB)
                       │
                       ▼
                 SILVER  typed, deduplicated, validated (DuckDB / PySpark)
                       │
                       ▼
                 GOLD    star schema & business metrics
                       │
                       ▼
          Analytics · dashboards · data-quality reports
```

## Data sources

| Source | Dataset | Incremental column |
|---|---|---|
| Chicago Crimes (2001–present) | `ijzp-q8t2` | `updated_on` |
| Chicago 311 Service Requests | `v6vf-nfxy` | `last_modified_date` |
| Chicago Taxi Trips | `wrvz-psew` | `trip_start_timestamp` |
| Illinois: City of Urbana Police Arrests (data.illinois.gov) | `s2ps-ct5e` | `:updated_at` (Socrata system field) |

See [docs/sources.md](docs/sources.md) for keys, system fields and how to add a source.

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

lakehouse sources                                  # list configured sources
lakehouse ingest chicago_crimes --max-records 5000 # land raw data in bronze
lakehouse dedupe chicago_crimes --dry-run          # versions vs. distinct keys in bronze
lakehouse schema chicago_crimes                    # inferred schema of the latest run
lakehouse drift chicago_crimes                     # schema changes vs. the previous run
lakehouse export                                   # bronze -> Parquet, partitioned by ingest date
pytest -q                                          # run the test suite
```

Add `--log-json` before the subcommand for JSON-lines logs; every ingestion run gets a run ID
that is stamped on its log lines and bronze rows (see [docs/logging.md](docs/logging.md)).
Bronze is append-only; query `bronze.<source>__latest` for one row per key
(see [docs/bronze-dedupe.md](docs/bronze-dedupe.md)).
Each run's inferred source schema is snapshotted for drift tracking
(see [docs/schema-snapshots.md](docs/schema-snapshots.md)), and drift between runs is graded
info / warning / breaking (see [docs/schema-drift.md](docs/schema-drift.md)).
Bronze can be exported as ingest-date-partitioned Parquet
(see [docs/parquet-export.md](docs/parquet-export.md)).

Set `SOCRATA_APP_TOKEN` (free from the data portal) for higher API rate limits. HTTP 429 /
`Retry-After` responses are waited out automatically (see [docs/rate-limits.md](docs/rate-limits.md)).

## Project layout

```
config/sources.yaml       dataset definitions
src/lakehouse/
  ingestion/socrata.py    paginated, incremental API client with retries & rate-limit handling
  storage/bronze.py       raw layer, ingestion log, watermarks, latest-per-key view
  storage/export.py       Parquet export partitioned by ingest date
  logging_setup.py        structured logging + run IDs
  schema.py               schema inference & snapshots
  drift.py                schema-drift detection between runs
  cli.py                  `lakehouse` command
tests/                    unit tests (offline, fixture-based)
docs/                     design notes and guides
```

## License

MIT
