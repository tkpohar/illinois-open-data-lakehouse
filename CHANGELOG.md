# Changelog

## 2026-11-02 — Milestone 1: project foundation
- Project scaffold with `pyproject.toml`, `lakehouse` CLI and source config (`config/sources.yaml`)
- `SocrataClient`: paginated, incremental fetching with exponential-backoff retries
- `BronzeStore`: raw JSON landing in DuckDB with an ingestion log and watermarks
- Offline unit tests with a fixture of sample Chicago crime records
- GitHub Actions CI (ruff + pytest on Python 3.10 and 3.12)
