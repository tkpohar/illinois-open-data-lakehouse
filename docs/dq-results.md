# Data-quality results and history

Data-quality checks are only useful over time: *is this new, or has it been failing for a
week?* Results of all three check families are stored in one place and can be queried
as history.

```bash
lakehouse checks run                 # expectations + freshness + anomalies, stored as one check run
lakehouse dq --record                # store results of a single family ...
lakehouse freshness --record
lakehouse anomalies --record

lakehouse checks latest              # current status of every check, failures first
lakehouse checks history --check unique:crime_id
lakehouse checks history --target silver.crimes --status fail --limit 20
lakehouse checks runs                # recent check runs with counts
lakehouse checks trend --days 30     # daily pass rate per check family
```

```
$ lakehouse checks latest
FAIL      3x freshness   chicago_311_requests: data, last pass 2026-11-23 03:42
WARN      1x freshness   chicago_crimes: ingestion, last pass 2026-11-26 03:42
PASS         expectation silver.crimes: unique:crime_id
...
```

`lakehouse checks run` exits with status 1 when any check fails (warnings don't count).

## Tables

**`quality.results`**: one row per check result.

| Column | Meaning |
|---|---|
| `check_run_id` | the check run the result belongs to |
| `checked_at` | when it was recorded (UTC) |
| `check_type` | `expectation`, `freshness` or `anomaly` |
| `target` | table (`silver.crimes`), source (`chicago_crimes`) or series (`events:silver.crimes`) |
| `check_name` | e.g. `in_range:ward`, `data`, `z_score:2026-11-25` |
| `status` | `pass`, `warn`, `fail` or `skipped` |
| `observed` | failing rows (expectations), age in hours (freshness), the day's count (anomalies) |
| `details` | the full original result as JSON (sample values, thresholds, z-score, …) |

**`quality.check_runs`**: one row per check run: `check_run_id`, `started_at`,
`finished_at`, `run_id` (the [logging run ID](logging.md), when one is active) and counts
of `passed`, `warned`, `failed`, `skipped`.

Each recording is one transaction. A result with an invalid status rejects the whole
batch.

## Status normalisation

| Check family | Original | Stored |
|---|---|---|
| [expectations](data-quality.md) | passed / failed / suite skipped (table missing) | `pass` / `fail` / `skipped` |
| [freshness](freshness.md) | ok / warn / error | `pass` / `warn` / `fail` |
| [anomalies](anomalies.md) | ok / anomaly / insufficient history | `pass` / `fail` / `skipped` |

Severity levels (which failures should stop a pipeline) come in Milestone 26.

## History queries

`lakehouse.quality.results`:

| Function | Returns |
|---|---|
| `latest(con)` | latest status per check, with `streak` (how many consecutive most-recent results share that status) and `last_pass_at`; failures first. Anomaly results are named per day but grouped by series here |
| `history(con, target=, check_name=, check_type=, status=, limit=)` | results, newest first |
| `runs(con, limit=)` | recent check runs |
| `pass_rate_by_day(con, days=)` | per day and check family: non-skipped checks and the share that passed |

```sql
-- checks that started failing in the last week
SELECT target, check_name, min(checked_at) AS first_failure
FROM quality.results WHERE status = 'fail'
GROUP BY ALL HAVING min(checked_at) > now() - INTERVAL 7 DAY;
```

The HTML data-quality report (Milestone 25) is built from these tables.
