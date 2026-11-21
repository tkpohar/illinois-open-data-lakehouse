# Incremental silver builds

Bronze grows every day, but most records don't change. Instead of retyping the whole
table, `lakehouse silver` recomputes **only the keys that received new bronze rows**
since the last build and upserts them.

```bash
lakehouse silver crimes                  # incremental (falls back to full when needed)
lakehouse silver crimes --full-refresh   # rebuild from scratch
```

```
silver.crimes: 5214 rows from bronze.chicago_crimes__latest
  incremental: 214 rows processed
```

## How it works

Each silver table has a **watermark** in `silver._watermarks`:

| Column | Meaning |
|---|---|
| `table_name` | e.g. `silver.crimes` |
| `source` | the append-only bronze table watched for changes (`bronze.chicago_crimes`) |
| `max_ingested_at` | highest bronze `ingested_at` already processed |
| `spec_hash` | hash of the table's column spec (typed columns, derived columns, filter, source) |
| `updated_at` | when the watermark last moved |

An incremental build, in **one transaction**:

1. reads the current high-water mark `H = max(ingested_at)` of the bronze table;
2. collects the **changed keys**: `record_key`s with bronze rows where
   `watermark < ingested_at <= H`;
3. deletes those keys from the silver table and re-inserts them, typed from the
   `__latest` view (so the latest version still wins, even if an older copy arrived
   later);
4. recomputes the quality counts over the **whole** table and moves the watermark to `H`.

Rows for untouched keys, and their lineage (`run_id`, `ingested_at`, `silver_built_at`),
stay exactly as they were. The result is the same as a full rebuild. A test checks this,
including a stale version that arrives after a newer one.

## When it falls back to a full build

| Reason (shown in output and `BuildResult.full_reason`) | Why |
|---|---|
| `target does not exist` | first build, or the table was dropped |
| `no watermark` | the watermark row is missing |
| `column spec changed` | a column, derived column or filter was edited: every row must be recomputed |

`--full-refresh` (or `Context(full_refresh=True)`) always rebuilds. Calling a builder
directly (`crimes.build(store)`) is a full build unless `incremental=True` is passed.

## What is reported

| Field | Full build | Incremental build |
|---|---|---|
| `rows` | rows in the table | rows in the table after the upsert |
| `processed` | all rows | rows recomputed |
| `cast_failures` | over all rows | over the **processed** rows only |
| `quality` | over the table | over the **whole** table |
| `mode` | `full` | `incremental` |

`silver._build_log` now also records `mode` and `processed` for every build.

## Interaction with other steps

- **Geo columns survive.** An incremental upsert inserts rows by column name, so columns
  added by the geo step stay on untouched rows. New rows get them when the downstream
  `*_geo` step runs, which the [framework](silver-framework.md) does automatically.
- **Bronze compaction** (`lakehouse dedupe`) keeps the surviving rows' `ingested_at`, so it
  does not trigger reprocessing.
- The watermark compares timestamps: bronze rows written in the same microsecond as the
  previous high-water mark, but after the build read it, could be missed. With one
  writer running steps in sequence, ingestion and silver builds never overlap.
