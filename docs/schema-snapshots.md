# Schema snapshots

Every ingestion run that lands at least one record also records **the schema of what the
source returned in that run**. Snapshots are the basis for schema-drift detection
([schema-drift.md](schema-drift.md)) and document how each source actually looks, not how it is described.

## How it works

`BronzeStore.write` feeds every record through a `SchemaInferrer` in the same single pass
that writes it, so inference costs no extra API calls or table scans. At the end of the run
a row goes into `bronze._schema_snapshots`:

| Column | Meaning |
|---|---|
| `source`, `run_id`, `captured_at` | which run (same `run_id` as logs and bronze rows) |
| `record_count`, `field_count` | how many records/fields were seen |
| `fingerprint` | 16-hex hash of the sorted `(field, inferred_type)` pairs — equal fingerprints mean an identical shape |
| `schema` | JSON: per field `json_type`, `inferred_type`, `present`, `nulls`, `nullable` |

Runs that return no records store no snapshot (an empty page says nothing about the schema).

## Type inference

Socrata serialises most values as JSON strings, so each field has two types:

- **`json_type`** — what the JSON actually contained (`string`, `integer`, `number`,
  `boolean`, `object`, `array`, or `null` if only nulls were seen);
- **`inferred_type`** — what the values represent. If *every* non-null value of a string
  field looks like one of these, the field is refined to it:

| Looks like | Inferred |
|---|---|
| `"13001001"`, `"-12"` | `integer` |
| `"41.8925"`, `"1e5"` | `number` |
| `"true"`, `"FALSE"` | `boolean` |
| `"2026-01-02T15:40:12.000"`, `"2026-01-02 15:40"`, with optional `Z`/offset | `timestamp` |
| `"2026-01-02"` | `date` |

Rules:

- **Widening** across values: `integer`+`number` → `number`, `date`+`timestamp` →
  `timestamp`, any other mix → `string`. Nulls never decide a type.
- **Leading zeros stay strings.** `"018"` (a police district) is a code; treating it as
  `18` would lose information.
- **Nullable** means the field was missing from some records or null in some.

## Usage

```bash
lakehouse schema chicago_crimes                 # latest snapshot as a table
lakehouse schema chicago_crimes --run-id <id>   # a specific run
lakehouse schema chicago_crimes --json          # machine-readable
lakehouse schema chicago_crimes --history       # one line per run, with fingerprints
```

```
chicago_crimes  run=20261104T034130Z-1a2b3c4d  records=5000  fingerprint=3e1f0c9a7b2d4e65
field                        inferred   json     nullable  present
-------------------------------------------------------------------
arrest                       boolean    boolean  no           5000
district                     string     string   no           5000
latitude                     number     string   yes          4987
updated_on                   timestamp  string   no           5000
```

```python
from lakehouse.schema import infer_schema

store.schema_history("chicago_crimes")          # [SchemaSnapshot, ...] oldest first
store.schema_snapshot("chicago_crimes")         # latest (or run_id=...)
infer_schema("adhoc", records).fingerprint      # inference without storage
```
