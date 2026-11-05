# Schema-drift detection

Every ingestion run snapshots the source schema (see [schema-snapshots.md](schema-snapshots.md)).
After saving a snapshot, `BronzeStore.write` compares it with the **previous snapshot of the
same source** and records every difference. Runs that return no records don't create a
snapshot, so they never become the baseline.

## Severities

Open-data APIs are noisy: Socrata **omits keys whose value is null**, and incremental runs
are small samples. A naive diff would page someone every night. Each change is therefore
classified:

| Change | Kind | Severity | Why |
|---|---|---|---|
| New field | `added` | info | additive; nothing downstream breaks |
| Field only null on one side | `type_from_nulls` | info | no type evidence on that side |
| Type narrowed (`number` → `integer`) | `type_narrowed` | info | a smaller sample with less variety |
| Field became nullable | `became_nullable` | info | common in sparse data |
| Field missing | `removed` | **warning** | may be a real removal, or just all-null in this run |
| Type widened (`integer` → `number`, `date` → `timestamp`, anything → `string`) | `type_widened` | **warning** | casts may start failing or losing precision |
| Incompatible type (`boolean` → `integer`, `timestamp` → `number`, …) | `type_changed` | **breaking** | downstream casts will fail |

"Narrowed" and "widened" use the same widening rules as type inference: if
`widen(old, new) == new` the type widened, if it equals `old` it narrowed, otherwise the
types are incompatible.

## Where drift shows up

- **Logs** — one line per drifting run at INFO / WARNING / ERROR matching the highest
  severity, with structured fields `drift_severity`, `drift_changes`, `previous_run_id`
  (and the usual `run_id`).
- **`bronze._schema_drift`** — one row per change: `source, old_run_id, new_run_id,
  detected_at, field, kind, severity, old_type, new_type`.
- **`BronzeStore.last_drift`** — the `DriftReport` of the most recent write (`None` for the
  first snapshot of a source or an empty run).

## CLI

```bash
lakehouse drift chicago_crimes                       # latest run vs. the one before
lakehouse drift chicago_crimes --from RUN_A --to RUN_B
lakehouse drift chicago_crimes --json                # machine-readable report
lakehouse drift chicago_crimes --history             # all stored drift events
lakehouse drift chicago_crimes --fail-on warning     # exit 2 if drift >= warning (CI gate)

lakehouse ingest chicago_crimes --fail-on-drift breaking
```

```
chicago_crimes: 20261104T034130Z-1a2b3c4d -> 20261105T034140Z-5e6f7a8b  (3e1f0c9a7b2d4e65 -> 9b0c1d2e3f4a5b6c)
  [info    ] + beat (integer)
  [breaking] ~ arrest: type_changed boolean -> integer
```

`--fail-on-drift` never blocks the data from landing in bronze (bronze keeps everything
raw); it only turns the exit status into **2** so an orchestrator can stop downstream steps.
Exit status 1 is reserved for errors (ingestion failure, unknown run, fewer than two
snapshots).

## In code

```python
from lakehouse.drift import compare

report = compare(store.schema_snapshot(src, "run-a"), store.schema_snapshot(src, "run-b"))
report.severity                 # None | "info" | "warning" | "breaking"
report.at_least("warning")      # [SchemaChange(field, kind, severity, old, new), ...]
```
