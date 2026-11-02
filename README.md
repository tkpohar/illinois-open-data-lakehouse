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

## Quick start

```bash
python -m venv .venv && source .venv/bin/activate
pip install -e ".[dev]"

lakehouse sources                                  # list configured sources
lakehouse ingest chicago_crimes --max-records 5000 # land raw data in bronze
pytest -q                                          # run the test suite
```

Set `SOCRATA_APP_TOKEN` (free from the data portal) for higher API rate limits.

## Project layout

```
config/sources.yaml       dataset definitions
src/lakehouse/
  ingestion/socrata.py    paginated, incremental API client with retries
  storage/bronze.py       raw layer + ingestion log & watermarks
  cli.py                  `lakehouse` command
tests/                    unit tests (offline, fixture-based)
```

## License

MIT
