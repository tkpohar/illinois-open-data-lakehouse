# SCD Type 2: history of slowly changing data

Reference data changes rarely but does change: a district is renamed, a boundary is
redrawn, a code is retired. Overwriting the old value would silently re-label history.
`lakehouse.scd.apply_scd2` keeps **every version** with the period it was valid.

## Table layout

`apply_scd2(con, source, target, key, tracked, as_of=None, close_missing=True)` merges the
current state in `source` (one row per key) into the history table `target`. The history
table is created on first use:

| Column | Meaning |
|---|---|
| `<key>`, `<tracked…>` | the business key and the attribute values of this version |
| `valid_from` | when this version became current (inclusive) |
| `valid_to` | when it was superseded (exclusive); `NULL` while current |
| `is_current` | `TRUE` for the live version; exactly one per key that is still present |
| `version` | 1, 2, 3 … per key |
| `row_hash` | MD5 of the tracked values, used to detect changes (NULL and `''` hash differently) |

## What a run does

| Situation | Effect | Counted as |
|---|---|---|
| key not in history | open version 1 | `inserted` |
| tracked values changed | close current version at `as_of`, open version *n+1* | `changed` |
| values identical | nothing | `unchanged` |
| key missing from source | close current version (unless `close_missing=False`) | `closed` |
| a closed key appears again | open version *n+1* | `reopened` |

- **Idempotent.** Re-running with the same source changes nothing.
- **Atomic.** Each run is one transaction; a failure leaves the history untouched.
- **Forward-only.** `as_of` must not be earlier than the latest `valid_from`, so history
  can't be rewritten.
- **Validated.** The source must have exactly one row per key and no NULL keys.
- Validity intervals for a key are contiguous: each `valid_to` equals the next version's
  `valid_from`.

## Point-in-time queries

```python
from lakehouse.scd import as_of_sql

con.execute(as_of_sql("reference.police_districts_history"), [ts, ts]).fetchall()
```

which expands to

```sql
SELECT * FROM reference.police_districts_history
WHERE valid_from <= ? AND (valid_to IS NULL OR valid_to > ?)
```

Joining facts to the version valid **at the event time** (rather than today's) is how the
gold layer will avoid re-labelling old crimes after a rename.

## Reference data history

`lakehouse seed` applies SCD2 after every load that actually loaded something (see
[reference data](reference-data.md)). Each seed gets a history table, keyed on the seed
key and tracking all other columns:

| Current table | History table |
|---|---|
| `reference.community_areas` | `reference.community_areas_history` |
| `reference.police_districts` | `reference.police_districts_history` |

```
$ lakehouse seed
reference.police_districts: 22 rows (loaded, sha256 7d2c…)
  history reference.police_districts_history: 0 new, 1 changed, 0 closed, 0 reopened
```

Seeds whose file is unchanged are skipped, so their history is untouched. A forced
reload with identical content records nothing new.
