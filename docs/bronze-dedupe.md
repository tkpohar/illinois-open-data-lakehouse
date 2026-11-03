# Bronze upserts & deduplication

Incremental ingestion re-fetches a record every time the source updates it (its
`incremental_column`, e.g. `updated_on`, moves past the watermark). Bronze therefore
accumulates several **versions** of the same record. The strategy:

1. **Bronze stays append-only.** Every version the source ever returned is kept, with
   lineage (`ingested_at`, `run_id`, `ingest_seq`). Nothing is overwritten on ingest, so
   history and re-processing are always possible.
2. **A `__latest` view gives the upserted state.** For each source, `bronze.<source>__latest`
   has exactly one row per `record_key` — the latest version. Downstream (silver) reads
   this view, not the raw table.
3. **Compaction is explicit and optional.** `lakehouse dedupe <source>` physically removes
   superseded versions when storage matters more than history.

## What "latest" means

Versions of a key are ranked, newest first, by:

| Order | Column | Why |
|---|---|---|
| 1 | `payload->>incremental_column` DESC, NULLs last | the source's own notion of recency; a stale copy re-fetched by `--full` never wins over a newer one |
| 2 | `ingested_at` DESC | later ingestion wins when the source timestamp ties or is absent |
| 3 | `ingest_seq` DESC | position within one batch: the last copy seen in a batch wins |

Sources without an `incremental_column` fall back to rules 2–3 (last ingested wins).
Source timestamps are ISO-8601 strings, which order correctly as text.

The incremental column for each source is recorded in `bronze._sources` on every write,
and the view is recreated after every write and compaction.

## Usage

```bash
lakehouse dedupe chicago_crimes --dry-run   # bronze.chicago_crimes: 7 rows, 5 keys, 2 superseded
lakehouse dedupe chicago_crimes             # ...and removes the 2 superseded rows
```

```sql
SELECT * FROM bronze.chicago_crimes__latest;       -- one row per crime id
SELECT record_key, count(*) FROM bronze.chicago_crimes
GROUP BY 1 HAVING count(*) > 1;                     -- keys with history
```

```python
store = BronzeStore()
store.latest("chicago_crimes")         # list of payload dicts, one per key
store.dedupe_stats("chicago_crimes")   # DedupeStats(rows=7, keys=5) -> .duplicates == 2
store.compact("chicago_crimes")        # -> 2 (rows removed), single transaction
```

Compaction keeps lineage columns of the surviving rows and does not touch
`bronze._ingestion_log`, so watermarks are unaffected. Tables created by earlier versions
are migrated in place (an `ingest_seq` column is added; old rows have `NULL`, ranked last
among rows with the same `ingested_at`).
