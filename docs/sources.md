# Data sources

All four sources are Socrata (SODA) APIs, configured in `config/sources.yaml` and fetched
by the same `SocrataClient`. A source needs a `domain`, a `dataset_id` and a
`primary_key`; `incremental_column` enables watermark-based incremental loads.

| Source | Portal | Dataset | Primary key | Incremental column |
|---|---|---|---|---|
| `chicago_crimes` | data.cityofchicago.org | `ijzp-q8t2` | `id` | `updated_on` |
| `chicago_311_requests` | data.cityofchicago.org | `v6vf-nfxy` | `sr_number` | `last_modified_date` |
| `chicago_taxi_trips` | data.cityofchicago.org | `wrvz-psew` | `trip_id` | `trip_start_timestamp` |
| `illinois_urbana_arrests` | data.illinois.gov | `s2ps-ct5e` | `:id` | `:updated_at` |

## State of Illinois: City of Urbana Police Arrests

[`s2ps-ct5e`](https://data.illinois.gov/Local-Government/City-Of-Urbana-Police-Arrests/s2ps-ct5e)
on the State of Illinois portal: City of Urbana police arrest records up to 2023 (25
columns, including `incident_number`, `arrest_code`, `date_of_arrest`, `year_of_arrest`,
`month_of_arrest` and `arrest_type_description`). It adds a second portal and a
non-Chicago jurisdiction to the lakehouse.

### Why `:id` and `:updated_at`

The dataset has no documented unique business key. `incident_number` is **not** unique:
one incident can produce several arrests, and keying on it would make the bronze
`__latest` view collapse those arrests into one row. `date_of_arrest` is an event time, not
a modification time, so it is a poor watermark.

Every Socrata dataset carries **system fields** instead:

| Field | Meaning |
|---|---|
| `:id` | stable internal row identifier |
| `:created_at` | when the row was created on the portal |
| `:updated_at` | when the row was last modified on the portal |

They are omitted from responses unless requested, so whenever a source's `primary_key` or
`incremental_column` starts with `:`, the client adds `$select=:*, *` (system fields plus
all regular columns) to every request (`SourceConfig.uses_system_fields`). Ordering and
the incremental `$where` filter then use `:updated_at`, and bronze keys rows on `:id`.
Any other Socrata dataset without a reliable key can use the same two lines of config.

### Notes

- The dataset is historical (up to 2023), so incremental runs will usually return nothing
  new; a `--full` run reloads it. Empty runs don't create schema snapshots.
- The test fixture `tests/fixtures/illinois_urbana_arrests_sample.json` is **synthetic**:
  it uses the dataset's confirmed column names and system fields, not real records.
- The live endpoint has not yet been exercised from this project's automation (tests are
  offline by design); the first real `lakehouse ingest illinois_urbana_arrests` is the
  end-to-end check.

## Adding a source

```yaml
sources:
  my_source:
    domain: data.example.gov
    dataset_id: abcd-1234
    description: Short description
    primary_key: id                  # or ":id" for the Socrata row id
    incremental_column: updated_at   # or ":updated_at"; omit for full loads only
```

Then `lakehouse sources` lists it and `lakehouse ingest my_source` loads it into
`bronze.my_source`, with the `__latest` view, schema snapshots and drift tracking
included automatically.
