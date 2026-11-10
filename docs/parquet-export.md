# Parquet export of bronze

`lakehouse export` writes bronze tables out of DuckDB as Parquet files, **Hive-partitioned
by ingest date**, so they can be read by Spark, pandas/pyarrow, Athena/Trino, or DuckDB
itself without the `.duckdb` file.

## Layout

```
data/export/
  bronze/<source>/ingest_date=2026-11-01/data_0.parquet
  bronze/<source>/ingest_date=2026-11-02/data_0.parquet
  bronze_latest/<source>/ingest_date=.../data_0.parquet      # with --latest
```

| Column | Type | Notes |
|---|---|---|
| `record_key` | string | source primary key |
| `payload` | string | the raw record as JSON text |
| `ingested_at` | timestamp | UTC, from the ingestion run |
| `run_id` | string | joins to logs and `bronze._ingestion_log` |
| `ingest_seq` | int64 | position within the run's batch |
| `ingest_date` | date | partition column (`CAST(ingested_at AS DATE)`), encoded in the path |

Partitioning by **ingest date** (not event date) matches how bronze grows: each daily run
adds one partition, readers can prune to "what arrived on day X", and backfills never
rewrite old event-date partitions.

## Usage

```bash
lakehouse export                          # every bronze table -> data/export/bronze/...
lakehouse export chicago_crimes           # one source
lakehouse export --latest                 # the __latest views (one row per key)
lakehouse export --out /mnt/lake          # another output directory
```

```
chicago_crimes: 5000 rows in 3 partitions -> data/export/bronze/chicago_crimes
```

Reading it back:

```sql
SELECT ingest_date, count(*)
FROM read_parquet('data/export/bronze/chicago_crimes/**/*.parquet', hive_partitioning = true)
GROUP BY 1 ORDER BY 1;
```

```python
from lakehouse.storage.export import export_source, read_export

result = export_source(store, "chicago_crimes")       # ExportResult(rows, partitions, path)
result.partitions                                      # {"2026-11-01": 5000, ...}
read_export(store, result.path).fetchall()
```

## Semantics

- **Full rewrite per source.** Each export deletes and rewrites `<out>/bronze/<source>`, so
  it is idempotent and never leaves stale partitions (e.g. after `lakehouse dedupe`).
- **`--latest`** exports `bronze.<source>__latest`; each row stays in the partition of the
  day its winning version was ingested.
- Internal tables (`_ingestion_log`, `_schema_snapshots`, ...) are not exported.
- Unknown source → exit status 1. No bronze tables → nothing written, exit status 0.
- No new dependency: DuckDB writes and reads Parquet natively.
