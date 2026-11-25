# Freshness checks

`lakehouse freshness` answers, per source: **is the data current?** It asks two
different questions, because a pipeline can fail in two different ways:

| Check | Measures | Catches |
|---|---|---|
| **ingestion** | hours since the last ingestion run finished (`bronze._ingestion_log.run_finished`) | the pipeline stopped running |
| **data** | hours since the newest record the source delivered: the watermark, i.e. the maximum of the source's `incremental_column` | the source stopped *publishing*, even though our runs keep succeeding and fetch nothing new |

Each check is `ok`, `warn` (older than the warn threshold) or `error` (older than the
error threshold). Thresholds are exclusive, so an age exactly at the limit is still
`ok`. A source that has never been ingested, or has no parseable watermark, is `error`.

```bash
lakehouse freshness                       # all sources
lakehouse freshness chicago_crimes        # one source
lakehouse freshness --fail-on warn        # exit 1 on warnings too (default: errors only)
lakehouse freshness --json
```

```
OK    chicago_crimes             ingestion last run 2.3 h ago (warn > 36 h, error > 72 h)
OK    chicago_crimes             data      newest record 171.0 h old (warn > 240 h, error > 480 h)
WARN  chicago_311_requests       data      newest record 52.6 h old (warn > 48 h, error > 120 h)
ERROR illinois_urbana_arrests    ingestion never ingested (warn > 36 h, error > 72 h)
```

## Policies

Set per source in `config/sources.yaml` under `freshness` (hours):

| Key | Default | Meaning |
|---|---|---|
| `ingestion_warn_hours` | 36 | warn when the last run is older |
| `ingestion_error_hours` | 72 | error when the last run is older |
| `data_warn_hours` | not set | warn when the newest record is older. The data check runs only when both data keys are set |
| `data_error_hours` | not set | error when the newest record is older |

Warn must not exceed error, and the data keys come as a pair; invalid or unknown keys
are rejected when the config is loaded.

| Source | Data check | Why |
|---|---|---|
| `chicago_crimes` | warn 240 h, error 480 h | the portal publishes crimes with a delay of about a week |
| `chicago_311_requests` | warn 48 h, error 120 h | requests are modified around the clock |
| `chicago_taxi_trips` | none | historical dataset (2013–2023) |
| `illinois_urbana_arrests` | none | historical dataset (up to 2023) |

All sources use the default ingestion thresholds (a daily pipeline gets a day and a half
of slack before a warning).

## Timestamps

Watermarks are ISO-8601 strings. Socrata *floating* timestamps (no zone, e.g.
`2026-11-24T18:00:00.000`, Chicago local time) are treated as UTC, so ages can be off by
the 5–6 h Chicago offset. That's negligible against thresholds measured in days. System
fields such as `:updated_at` carry a `Z` and are exact. Watermarks in the future count as
age 0.

## Relation to `lakehouse status`

[`lakehouse status`](status.md) shows a single *stale* flag per source from the time
since its last run. `lakehouse freshness` is the policy-driven check with per-source
thresholds, warn and error levels, the data-age check and a non-zero exit status for
use as a pipeline gate.
