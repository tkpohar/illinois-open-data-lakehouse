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

Data-quality expectations run per table with `lakehouse dq` (see [docs/data-quality.md](docs/data-quality.md)).
Freshness is checked per source with `lakehouse freshness` (see [docs/freshness.md](docs/freshness.md)).
Daily volumes are screened for anomalies with `lakehouse anomalies` (see [docs/anomalies.md](docs/anomalies.md)).
All check results are stored with history (see [docs/dq-results.md](docs/dq-results.md)).
Every silver column is documented in the generated [data dictionary](docs/data-dictionary.md).
Testing approach and the silver contract suite are described in [docs/testing.md](docs/testing.md).
Key design decisions are recorded as ADRs in [docs/adr](docs/adr/README.md), starting with
[ADR-0001: DuckDB vs Postgres](docs/adr/0001-duckdb-vs-postgres.md).

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
lakehouse status                                   # per-source ingestion metrics & freshness
lakehouse silver                                   # build all silver tables in dependency order
lakehouse silver crimes                            # one table (+ its downstream geo step)
lakehouse silver --full-refresh                    # rebuild instead of processing only new rows
lakehouse docs dictionary                          # regenerate docs/data-dictionary.md
lakehouse dq                                       # run data-quality expectation suites
lakehouse freshness                                # ingestion & data freshness per source
lakehouse anomalies --seasonal                     # z-score checks on daily row counts
lakehouse checks run                               # run all checks and store results
lakehouse checks latest                            # current status, failure streaks
lakehouse geo crimes --areas areas.geojson         # validate coordinates, look up community areas
lakehouse seed                                     # load reference data (community areas, districts)
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
`lakehouse status` summarises freshness, volumes, API pressure and drift per source
(see [docs/status.md](docs/status.md)).
Silver is built by a dependency-ordered transform runner
([docs/silver-framework.md](docs/silver-framework.md)), incrementally by default
([docs/silver-incremental.md](docs/silver-incremental.md)). Silver tables: `silver.crimes` ([docs/silver-crimes.md](docs/silver-crimes.md)),
`silver.service_requests` ([docs/silver-311.md](docs/silver-311.md)) and
`silver.taxi_trips` ([docs/silver-taxi.md](docs/silver-taxi.md)).
Coordinates are validated and mapped to community areas with `lakehouse geo`
(see [docs/geo-cleanup.md](docs/geo-cleanup.md)).
Community areas and police districts are reference tables loaded from `seeds/`
(see [docs/reference-data.md](docs/reference-data.md)).
Changes to reference data are kept as SCD Type 2 history (see [docs/scd2.md](docs/scd2.md)).

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
  status.py               ingestion metrics report
  silver/typing.py        declarative column typing for silver tables
  silver/crimes.py        silver.crimes
  silver/service_requests.py  silver.service_requests (311)
  silver/taxi_trips.py    silver.taxi_trips
  silver/geo.py           coordinate validation & community-area lookup
  silver/registry.py      transform registry & dependency-ordered runner
  silver/dictionary.py    data-dictionary generator
  quality/                data-quality expectations & suites
  reference.py            seed loading into the reference schema
  scd.py                  SCD Type 2 history helper
  cli.py                  `lakehouse` command
seeds/                    reference-data CSVs
tests/                    unit tests (offline, fixture-based)
docs/                     design notes and guides
```

## License

MIT
