# Changelog

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
