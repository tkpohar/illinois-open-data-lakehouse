# Changelog

## 2026-11-13 — Milestone 10: ADR-001 DuckDB vs Postgres (Phase 1 complete)
- New `docs/adr/`: index, ADR template and ADR-0001 recording why DuckDB (embedded,
  columnar, native Parquet) is the lakehouse engine instead of Postgres, with the accepted
  trade-offs (single writer, no serving layer, DuckDB-specific SQL kept in storage modules)
  and the conditions for revisiting it
- New `tests/test_docs.py`: every relative link in README and docs resolves; ADRs have a
  numbered title, valid status and date, the required sections in order, sequential
  numbering, and an up-to-date entry in the ADR index

## 2026-11-12 — Milestone 9: ingestion metrics report (`lakehouse status`)
- New `lakehouse.status`: per-source rows/keys/duplicates, run counts, last run (id, time,
  records, duration), freshness and staleness, watermark, recent average, API request
  metrics, latest schema fingerprint and last drift severity; totals across sources
- `bronze._ingestion_log` now stores `api_requests`, `api_retries`, `rate_limited`,
  `waited_s` per run from `SocrataClient.stats` (`BronzeStore.write(request_stats=...)`);
  older databases migrated in place
- CLI: `lakehouse status [--json] [--stale-hours H]`; lists never-ingested configured
  sources and unconfigured bronze tables
- Docs: `docs/status.md`; tests in `tests/test_status.py`

## 2026-11-11 — Milestone 8: rate-limit handling (HTTP 429, Retry-After)
- `SocrataClient` treats HTTP 429 (and 503 with `Retry-After`) as rate limits: waits as
  long as `Retry-After` asks (seconds or HTTP date), else exponential backoff
- Separate budgets: `max_rate_limit_retries` (default 5) vs. `max_retries` for other errors;
  waits above `max_retry_after` (default 300 s) raise `RateLimitError` instead of sleeping
- `parse_retry_after`, `RateLimitError`, per-client `RequestStats` (requests, retries,
  rate_limited, waited_s); structured WARNING log per wait; injectable `sleep`
- Docs: `docs/rate-limits.md`; tests in `tests/test_rate_limit.py`

## 2026-11-10 — Milestone 7: Parquet export of bronze (partitioned by ingest date)
- New `lakehouse.storage.export`: `export_source` / `export_all` write bronze tables (or
  their `__latest` views) as Hive-partitioned Parquet (`ingest_date=YYYY-MM-DD/`) via
  DuckDB `COPY`, with per-partition row counts; `read_export` queries them back
- Exports rewrite the source directory, so they are idempotent and drop stale partitions
- CLI: `lakehouse export [source] [--latest] [--out DIR]`
- Docs: `docs/parquet-export.md`; tests in `tests/test_export.py`

## 2026-11-09 — Milestone 6: State of Illinois dataset as a fourth source
- New source `illinois_urbana_arrests`: City of Urbana Police Arrests (`s2ps-ct5e`) on
  data.illinois.gov, the lakehouse's second portal
- Keyed on Socrata system fields `:id` / `:updated_at` because the dataset has no unique
  business key (`incident_number` repeats across arrests in one incident)
- `SourceConfig.uses_system_fields`; `SocrataClient` adds `$select=:*, *` for such sources
- Synthetic fixture `tests/fixtures/illinois_urbana_arrests_sample.json`
- Docs: `docs/sources.md`; tests in `tests/test_illinois_source.py`

## 2026-11-05 — Milestone 5: schema-drift detection between runs
- New `lakehouse.drift`: `compare(old, new)` → `DriftReport` of added/removed fields, type
  widening/narrowing/incompatible changes and nullability, each graded info/warning/breaking
  (tuned for Socrata, which omits null keys and returns small incremental samples)
- `BronzeStore.write` compares each new snapshot with the previous one, logs drift at the
  matching level and stores changes in `bronze._schema_drift`; `last_drift`, `drift_history`
- CLI: `lakehouse drift <source> [--from RUN --to RUN] [--json] [--history] [--fail-on SEV]`
  and `lakehouse ingest --fail-on-drift SEV` (exit status 2, data still lands)
- Docs: `docs/schema-drift.md`; tests in `tests/test_drift.py`

## 2026-11-04 — Milestone 4: schema snapshot per run
- New `lakehouse.schema`: streaming `SchemaInferrer` with JSON vs. inferred types (integer,
  number, boolean, date, timestamp from Socrata strings), type widening, nullability,
  and a shape fingerprint; leading-zero codes like `"018"` stay strings
- `BronzeStore.write` captures a snapshot per non-empty run into `bronze._schema_snapshots`
  (same single pass, keyed by `run_id`); `schema_history` / `schema_snapshot` to read them
- CLI: `lakehouse schema <source> [--run-id ID] [--json] [--history]`
- Docs: `docs/schema-snapshots.md`; tests in `tests/test_schema.py`

## 2026-11-03 — Milestone 3: bronze upsert/dedupe (latest record per key)
- Bronze stays append-only; new `bronze.<source>__latest` view returns one row per key,
  ranked by the source's incremental column, then ingest time, then position in the batch
- `BronzeStore.latest`, `dedupe_stats` and transactional `compact` (drops superseded versions)
- New `ingest_seq` lineage column and `bronze._sources` registry; older tables migrated in place
- CLI: `lakehouse dedupe <source> [--dry-run]`
- Tests isolate root-logger state via a shared `tests/conftest.py`
- Docs: `docs/bronze-dedupe.md`; tests in `tests/test_bronze_dedupe.py`

## 2026-11-02 — Milestone 2: structured logging + run IDs
- New `lakehouse.logging_setup`: per-run IDs via `run_context`, JSON and text formatters,
  idempotent `configure_logging`; `extra=` fields become structured keys
- Every ingestion log line carries the run ID; Socrata retries and page fetches log structured fields
- Bronze rows and `bronze._ingestion_log` record `run_id`; older databases are migrated in place
- CLI: global `--log-json` / `--log-level`, `ingest --run-id`; failures log a traceback and exit 1
- Docs: `docs/logging.md`; tests in `tests/test_logging.py`

## 2026-11-02 — Milestone 1: project foundation
- Project scaffold with `pyproject.toml`, `lakehouse` CLI and source config (`config/sources.yaml`)
- `SocrataClient`: paginated, incremental fetching with exponential-backoff retries
- `BronzeStore`: raw JSON landing in DuckDB with an ingestion log and watermarks
- Offline unit tests with a fixture of sample Chicago crime records
- GitHub Actions CI (ruff + pytest on Python 3.10 and 3.12)
