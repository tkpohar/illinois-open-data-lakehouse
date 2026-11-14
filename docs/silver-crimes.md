# Silver: `silver.crimes`

The first silver table: Chicago crimes (`ijzp-q8t2`) typed and cleaned, **one row per
crime ID**, built from `bronze.chicago_crimes__latest` (the latest version of each record;
see [bronze dedupe](bronze-dedupe.md)).

```bash
lakehouse ingest chicago_crimes
lakehouse silver crimes
```

```
silver.crimes: 5000 rows from bronze.chicago_crimes__latest
  cast failures: latitude=3
```

Each build is a full rebuild (`CREATE OR REPLACE TABLE`) in one transaction; incremental
builds come in Milestone 18.

## Columns

| Column | Type | Raw field | Kind | Description |
|---|---|---|---|---|
| `crime_id` | BIGINT | `id` | int | Unique crime record ID |
| `case_number` | VARCHAR | `case_number` | upper | CPD Records Division number |
| `occurred_at` | TIMESTAMP | `date` | timestamp | When the incident occurred (Chicago local time, no time zone) |
| `updated_at` | TIMESTAMP | `updated_on` | timestamp | Last update on the portal |
| `year` | BIGINT | `year` | int | Year of the incident |
| `block` | VARCHAR | `block` | upper | Partially redacted address block |
| `iucr` | VARCHAR | `iucr` | code (4) | Illinois Uniform Crime Reporting code |
| `primary_type` | VARCHAR | `primary_type` | upper | Primary IUCR category |
| `description` | VARCHAR | `description` | upper | IUCR secondary description |
| `location_description` | VARCHAR | `location_description` | upper | Kind of location |
| `arrest` | BOOLEAN | `arrest` | bool | Whether an arrest was made |
| `domestic` | BOOLEAN | `domestic` | bool | Domestic-related incident |
| `beat` | VARCHAR | `beat` | code (4) | Police beat |
| `district` | BIGINT | `district` | int | Police district number |
| `ward` | BIGINT | `ward` | int | City Council ward |
| `community_area` | BIGINT | `community_area` | int | Community area (1-77) |
| `fbi_code` | VARCHAR | `fbi_code` | upper | FBI NIBRS offense code |
| `x_coordinate` | DOUBLE | `x_coordinate` | double | State Plane Illinois East (feet) |
| `y_coordinate` | DOUBLE | `y_coordinate` | double | State Plane Illinois East (feet) |
| `latitude` | DOUBLE | `latitude` | double | WGS84 latitude |
| `longitude` | DOUBLE | `longitude` | double | WGS84 longitude |
| `record_key` | VARCHAR | — | lineage | bronze key (`id` as text) |
| `run_id` | VARCHAR | — | lineage | ingestion run that landed this version |
| `ingested_at` | TIMESTAMP | — | lineage | when that version was ingested (UTC) |
| `silver_built_at` | TIMESTAMP | — | lineage | when this silver build ran (UTC) |

## Cleaning rules

Typing is declarative (`lakehouse.silver.typing.Column`), so the 311 and taxi tables
(Milestones 12–13) reuse the same rules:

| Kind | Rule |
|---|---|
| `text` | trimmed; blank → NULL |
| `upper` | trimmed, upper-cased, runs of whitespace collapsed (`"  criminal   damage "` → `CRIMINAL DAMAGE`) |
| `code` | trimmed and left-padded with zeros to a fixed width, so codes compare reliably (`"486"` → `0486` for IUCR, `"331"` → `0331` for beats) |
| `int` | whole numbers, also written as `"8.0"`; `"12.5"` is rejected; leading zeros dropped (`"018"` → 18 for districts) |
| `double` | decimal numbers |
| `bool` | `true/t/y/yes/1` → TRUE, `false/f/n/no/0` → FALSE (any case, JSON booleans too); anything else → NULL |
| `timestamp` | ISO-8601 / Socrata floating timestamps |

**Timestamps have no time zone.** Socrata "floating timestamps" are Chicago local time
as entered; they are kept as naive `TIMESTAMP`s (not converted to UTC) so that
hour-of-day analysis stays correct. Lineage timestamps (`ingested_at`, `silver_built_at`)
are UTC.

District, ward and community area are integers so they join cleanly to reference data
(Milestone 15). Beat and IUCR stay zero-padded text: they are codes, not quantities.

## Cast failures

A value that is present and non-blank but cannot be converted becomes NULL and is counted
as a **cast failure** for its column. A missing or blank value is just NULL, not a
failure. Counts are:

- printed by `lakehouse silver crimes` and logged (WARNING when non-zero, with a
  structured `cast_failures` field);
- stored per build in `silver._build_log` (`table_name, source, built_at, rows,
  cast_failures` as JSON).

Rows are never dropped for a cast failure; data-quality rules on these columns arrive in
Phase 3.

## In code

```python
from lakehouse.silver import crimes

result = crimes.build(store)     # BuildResult(table, source, rows, cast_failures, built_at)
result.total_cast_failures
```
