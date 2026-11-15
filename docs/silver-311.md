# Silver: `silver.service_requests` (311)

Chicago 311 service requests (`v6vf-nfxy`) typed and cleaned, **one row per SR number**,
built from `bronze.chicago_311_requests__latest`. It uses the same typing rules as
[silver.crimes](silver-crimes.md), plus **status normalisation**, derived columns and
data-consistency counts.

```bash
lakehouse ingest chicago_311_requests
lakehouse silver service_requests
```

```
silver.service_requests: 5000 rows from bronze.chicago_311_requests__latest
  quality: completed_without_closed_date=4
```

## Typed columns

| Column | Type | Raw field | Kind | Description |
|---|---|---|---|---|
| `sr_number` | VARCHAR | `sr_number` | upper | Service request number |
| `sr_type` | VARCHAR | `sr_type` | upper | Request type |
| `sr_short_code` | VARCHAR | `sr_short_code` | upper | Request type code |
| `owner_department` | VARCHAR | `owner_department` | upper | Department responsible |
| `status_raw` | VARCHAR | `status` | text | Status exactly as published |
| `origin` | VARCHAR | `origin` | upper | Channel the request came in through |
| `created_at` | TIMESTAMP | `created_date` | timestamp | When the request was created (Chicago local time) |
| `last_modified_at` | TIMESTAMP | `last_modified_date` | timestamp | Last modification on the portal |
| `closed_at` | TIMESTAMP | `closed_date` | timestamp | When the request was closed |
| `street_address` | VARCHAR | `street_address` | upper | Address of the request |
| `zip_code` | VARCHAR | `zip_code` | code (5) | ZIP code |
| `duplicate` | BOOLEAN | `duplicate` | bool | Flagged as a duplicate request |
| `legacy_record` | BOOLEAN | `legacy_record` | bool | Migrated from the legacy 311 system |
| `parent_sr_number` | VARCHAR | `parent_sr_number` | upper | Parent request for duplicates |
| `community_area` | BIGINT | `community_area` | int | Community area (1-77) |
| `ward` | BIGINT | `ward` | int | City Council ward |
| `police_district` | BIGINT | `police_district` | int | Police district |
| `latitude` | DOUBLE | `latitude` | double | WGS84 latitude |
| `longitude` | DOUBLE | `longitude` | double | WGS84 longitude |

Lineage columns `record_key`, `run_id`, `ingested_at`, `silver_built_at` are added as for
every silver table.

## Status normalisation

The portal's free-text `status` is kept unchanged in `status_raw`. `status` holds one of
four canonical values: **OPEN, COMPLETED, CANCELED, UNKNOWN**. The raw value is trimmed,
upper-cased and whitespace-collapsed, then mapped:

| Normalised raw value | `status` |
|---|---|
| `OPEN` | `OPEN` |
| `OPEN - DUP` | `OPEN` |
| `OPEN-DUP` | `OPEN` |
| `IN PROGRESS` | `OPEN` |
| `COMPLETED` | `COMPLETED` |
| `COMPLETE` | `COMPLETED` |
| `CLOSED` | `COMPLETED` |
| `CANCELED` | `CANCELED` |
| `CANCELLED` | `CANCELED` |
| anything else, or missing | `UNKNOWN` |

`OPEN - DUP` means an open request that was marked as a duplicate of another, so it maps
to `OPEN` and also sets `is_duplicate`. New spellings that show up in the data land in
`UNKNOWN` and are counted (see below), so they're noticed instead of silently mis-mapped.
To support one, add it to `STATUS_MAP`.

## Derived columns

| Column | Type | Rule |
|---|---|---|
| `status` | VARCHAR | canonical status (above) |
| `is_open` | BOOLEAN | `status = 'OPEN'` |
| `is_duplicate` | BOOLEAN | the `duplicate` flag, or a `... - DUP` status |
| `resolution_hours` | DOUBLE | `closed_at − created_at` in hours (2 dp); NULL if either is missing or closed precedes created |

`resolution_hours` is the basis for the 311 SLA analysis in Phase 4/6.

## Consistency checks

Each build counts rows that violate these conditions. **Rows are kept**: the counts are
printed by the CLI, logged (WARNING if any are non-zero) and stored in the `quality` JSON
column of `silver._build_log`.

| Check | Condition |
|---|---|
| `unknown_status` | `status = 'UNKNOWN'` |
| `open_with_closed_date` | `status = 'OPEN' AND closed_at IS NOT NULL` |
| `completed_without_closed_date` | `status = 'COMPLETED' AND closed_at IS NULL` |
| `closed_before_created` | `closed_at < created_at` |

## In code

```python
from lakehouse.silver import service_requests

result = service_requests.build(store)
result.quality      # {"unknown_status": 0, "open_with_closed_date": 0, ...}
```

The shared builder (`lakehouse.silver.typing.build_typed_table`) now accepts `derived`
columns and named `quality_checks` for any silver table.

The 311 test fixture (`tests/fixtures/chicago_311_sample.json`) is synthetic, shaped like
the real dataset.
