# ADR-0001: DuckDB (not Postgres) as the lakehouse engine

- **Status:** Accepted
- **Date:** 2026-11-13 (records the choice made in Milestone 1 and relied on since)

## Context

The project ingests public Socrata datasets (Chicago crimes, 311, taxi trips; Urbana
arrests from data.illinois.gov) into bronze → silver → gold layers, then runs analytical
queries, data-quality checks and dashboards over them. The workload has these properties:

- **Analytical, not transactional.** Writes are a few batch appends per source per day;
  reads are scans, aggregations and window functions over many rows and few columns.
- **One writer.** A single scheduled pipeline loads data; there are no concurrent
  application users writing rows.
- **Semi-structured input.** Bronze keeps each record as raw JSON (`payload`); schemas
  drift (see [schema drift](../schema-drift.md)) and are typed only in silver.
- **File-based interchange.** Bronze is exported as partitioned Parquet
  ([Parquet export](../parquet-export.md)) for other engines (Spark in Milestone 49).
- **Zero-ops and reproducible.** It must run on a laptop, in CI (GitHub Actions) and in
  ephemeral cloud containers with `pip install` only — no server to provision, no
  credentials, no network service for tests. The suite builds hundreds of throwaway
  databases (`BronzeStore(":memory:")`) in a few seconds.
- **Portfolio scale.** Tens of millions of rows at most (the crimes dataset is the
  largest), comfortably within a single machine.

## Options considered

### A. DuckDB (embedded, columnar, in-process)

- Columnar, vectorised execution built for scans and aggregates.
- Embedded: a Python dependency and a single `.duckdb` file (or `:memory:`) — nothing to
  run, which keeps CI and tests trivial.
- Reads and writes Parquet natively, including Hive partitioning
  (`COPY ... (FORMAT PARQUET, PARTITION_BY ...)`, `read_parquet(..., hive_partitioning = true)`),
  which Milestone 7 relies on with no extra dependency.
- `QUALIFY` for "latest row per key" (Milestone 3) and JSON operators (`payload->>'col'`).
- Limits: one process may open a database file for writing at a time; no roles, row-level
  security or network access protocol; not suited to many concurrent writers.

### B. PostgreSQL (client/server, row-oriented)

- Mature, ACID, excellent concurrency, roles and security; huge ecosystem (BI tools,
  PostGIS for the geospatial work in Milestone 14).
- Row storage: analytical scans over wide tables are slower without extensions (e.g. a
  columnar access method) or careful indexing.
- No built-in Parquet read/write; exporting partitioned Parquet needs extensions or an
  extra tool in the pipeline.
- Needs a running server: a service container in CI, a database in each cloud session,
  connection credentials and migrations tooling — operational weight with no matching
  benefit for a single batch writer.
- No `QUALIFY`; latest-per-key needs a subquery or `DISTINCT ON` (workable, less direct).

### C. Plain Parquet files + pandas

- Simplest storage, but no SQL engine, no transactions around a load, and every
  dedupe/drift/status query becomes hand-written dataframe code. Rejected; Parquet stays
  as the *interchange* format via (A).

## Decision

Use **DuckDB** as the storage and query engine for bronze, silver and gold, accessed through
the `duckdb` Python package. Use **Parquet** as the open file format for exchanging data
with other engines. Keep SQL close to standard so the logic is portable.

## Consequences

**Easier**

- Tests and CI need no services; each test gets an isolated in-memory database.
- Analytical SQL (window functions, `QUALIFY`, JSON extraction, aggregates) is fast and
  concise; Parquet export/import is one statement.
- The whole lakehouse is one file that can be copied, inspected or deleted.

**Harder / accepted risks**

- **Single writer.** Two pipeline processes must not write the same database file at
  once. Orchestration (Milestone 41) must serialise writes per database.
- **No multi-user serving layer.** Dashboards read DuckDB or exported Parquet directly;
  there is no shared server with roles and permissions.
- **Some DuckDB-specific SQL** (`QUALIFY`, `COPY ... PARTITION_BY`, `read_parquet`,
  `->>` on JSON) would need rewriting to move engines. It is kept to the storage modules
  (`storage/bronze.py`, `storage/export.py`).

**Revisit when**

- Several independent writers or interactive multi-user applications need the data
  concurrently → put a serving database (e.g. Postgres) *downstream* of gold, fed from
  Parquet, rather than replacing the lakehouse engine.
- Data outgrows one machine → the PySpark path (Milestone 49) and an open table format
  (Milestone 65: Iceberg/Delta) over the same Parquet layout.
- Geospatial needs exceed DuckDB's spatial extension → evaluate PostGIS for that layer only.
