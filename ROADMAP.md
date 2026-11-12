# Roadmap

One milestone per working session. Each item should land with code, tests and docs.
Check items off (`[x]`) as they are completed and log them in CHANGELOG.md.

## Phase 1 — Foundations & ingestion
- [x] 1. Project scaffold, config loader, Socrata client, bronze store, CLI, CI
- [x] 2. Structured logging + run IDs across ingestion
- [x] 3. Upsert/dedupe strategy in bronze (latest record per key)
- [x] 4. Schema snapshot: infer & store source schema per run
- [x] 5. Schema-drift detection between runs
- [x] 6. Add Illinois state dataset (data.illinois.gov) as a fourth source
- [x] 7. Parquet export of bronze tables (partitioned by ingest date)
- [x] 8. Rate-limit handling (HTTP 429, Retry-After)
- [x] 9. Ingestion metrics report (`lakehouse status`)
- [ ] 10. Docs: architecture decision record (ADR-001 DuckDB vs Postgres)

## Phase 2 — Silver layer (cleaning & conformance)
- [ ] 11. Silver crimes: typed columns, timestamps, booleans
- [ ] 12. Silver 311: typed columns, status normalization
- [ ] 13. Silver taxi trips: fares, durations, outlier flags
- [ ] 14. Geospatial cleanup: lat/long validation, community-area lookup
- [ ] 15. Shared reference data: community areas, districts (seed CSVs)
- [ ] 16. SCD Type 2 helper for slowly-changing reference data
- [ ] 17. Silver transform framework (registry + dependency ordering)
- [ ] 18. Incremental silver builds (process only new bronze rows)
- [ ] 19. Unit tests for every silver transform
- [ ] 20. Docs: data dictionary for silver tables

## Phase 3 — Data quality
- [ ] 21. Expectation framework: not-null, unique, range, accepted values
- [ ] 22. Freshness checks per source
- [ ] 23. Row-count anomaly detection (z-score on daily volumes)
- [ ] 24. Data-quality results table + history
- [ ] 25. HTML data-quality report generator
- [ ] 26. Fail-fast vs warn severity levels
- [ ] 27. Data profiling command (`lakehouse profile <table>`)
- [ ] 28. Referential integrity checks silver → reference data
- [ ] 29. DQ checks in CI on fixture data
- [ ] 30. Docs: data-quality playbook

## Phase 4 — Gold layer (dimensional modeling)
- [ ] 31. Date & time dimensions
- [ ] 32. Location dimension (community area, district, ward)
- [ ] 33. Crime-type dimension
- [ ] 34. fact_crime_incidents
- [ ] 35. fact_service_requests (311) with resolution time
- [ ] 36. fact_taxi_trips
- [ ] 37. Aggregates: daily crime by community area
- [ ] 38. Aggregates: 311 SLA performance by department
- [ ] 39. Gold-layer tests (grain, uniqueness, reconciliation to silver)
- [ ] 40. Docs: star-schema ERD (Mermaid)

## Phase 5 — Orchestration & operations
- [ ] 41. DAG model for pipeline steps (dependencies, retries)
- [ ] 42. `lakehouse run` — full pipeline execution
- [ ] 43. Scheduled GitHub Actions pipeline on sample data
- [ ] 44. Backfill command with date ranges
- [ ] 45. Pipeline run history + duration metrics
- [ ] 46. Alerting hooks (failed run / DQ failure)
- [ ] 47. Dockerfile + docker-compose
- [ ] 48. Config profiles (dev / prod)
- [ ] 49. PySpark implementation of a silver transform (scale-out path)
- [ ] 50. Docs: runbook

## Phase 6 — Analytics & insights (Information Systems focus)
- [ ] 51. SQL analysis notebook: crime trends by community area
- [ ] 52. 311 response-time analysis by department
- [ ] 53. Taxi demand patterns by hour/day
- [ ] 54. Correlation: 311 requests vs crime by area
- [ ] 55. Streamlit (or static HTML) dashboard
- [ ] 56. KPI definitions document (metric layer)
- [ ] 57. Semantic metrics module (one definition, many consumers)
- [ ] 58. Data catalog page generated from table metadata
- [ ] 59. Data governance: PII review & retention policy doc
- [ ] 60. Final write-up: architecture, findings, lessons learned

## Phase 7 — Extensions (ongoing)
- [ ] 61. Add CTA ridership dataset
- [ ] 62. Add weather data (NOAA) and join to crime/taxi facts
- [ ] 63. Simple forecasting model for daily 311 volume
- [ ] 64. Performance benchmarks (DuckDB vs pandas vs PySpark)
- [ ] 65. Apache Iceberg / Delta table format experiment
