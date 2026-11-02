# Logging & run IDs

Every `lakehouse ingest` invocation is an **ingestion run** with a unique run ID
(`YYYYMMDDTHHMMSSZ-<8 hex>`, e.g. `20261102T200512Z-3f9a1c2e`). The run ID:

- appears on **every log line** emitted during the run (CLI, Socrata client, bronze store);
- is stored on **every bronze row** (`bronze.<source>.run_id`);
- is stored in the **ingestion log** (`bronze._ingestion_log.run_id`).

That makes it possible to go from a log line to the exact rows a run landed, and back.

## Output formats

Text (default):

```
2026-11-02 20:05:12,345 INFO [20261102T200512Z-3f9a1c2e] lakehouse.storage.bronze: Wrote 5000 records to bronze.chicago_crimes source=chicago_crimes records=5000 watermark=...
```

JSON lines (`--log-json`, placed before the subcommand) — one object per line, suited to
log shippers and `jq`:

```bash
lakehouse --log-json ingest chicago_crimes --max-records 1000 2> run.log
jq 'select(.run_id == "20261102T200512Z-3f9a1c2e")' run.log
```

```json
{"ts": "2026-11-02T20:05:12.345+00:00", "level": "INFO", "logger": "lakehouse.cli",
 "run_id": "20261102T200512Z-3f9a1c2e", "message": "Ingestion finished for chicago_crimes",
 "source": "chicago_crimes", "records": 1000, "duration_s": 3.214}
```

Failures are logged at `ERROR` with the traceback in `exc_info`, and the command exits with
status 1.

## Options

| Flag | Where | Meaning |
|---|---|---|
| `--log-json` | before subcommand | JSON-lines output |
| `--log-level LEVEL` | before subcommand | `DEBUG` shows one line per fetched API page |
| `--run-id ID` | `ingest` | use a caller-supplied run ID (e.g. from an orchestrator) |

## Querying by run

```sql
SELECT * FROM bronze._ingestion_log ORDER BY run_started DESC;
SELECT count(*) FROM bronze.chicago_crimes WHERE run_id = '20261102T200512Z-3f9a1c2e';
```

Databases created before run IDs existed are migrated automatically (a nullable `run_id`
column is added; older rows keep `NULL`).

## In code

```python
from lakehouse.logging_setup import configure_logging, run_context

configure_logging(json_output=True)
with run_context() as run_id:      # or run_context("my-id")
    store.write(...)                # picks up run_id automatically
```
