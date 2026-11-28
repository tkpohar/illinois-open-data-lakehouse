# HTML data-quality report

`lakehouse checks report` turns the stored [data-quality results](dq-results.md) into a
single, self-contained HTML page.

```bash
lakehouse checks run                                  # run and store all checks
lakehouse checks report                               # -> data/reports/dq-report.html
lakehouse checks report --out site/dq/index.html
```

The file has **no JavaScript and no external assets**: CSS and the chart are inline. It
can be opened from disk, attached to an alert email or uploaded as a CI artifact, and it
follows the reader's light or dark colour scheme.

## Sections

| Section | Shows |
|---|---|
| **Summary cards** | how many checks currently fail, warn, are skipped and pass (latest result per check) |
| **Needs attention** | every failing or warning check with its streak (consecutive runs in that state), when it last passed, and a plain-language detail. For expectations that is failing rows and sample values, for freshness the age against the thresholds, for anomalies the count, z-score and mean |
| **Daily pass rate** | one bar per day over the last 30 days: the share of non-skipped results that passed. Days below 95 % are drawn in the failure colour; hovering a bar shows the exact numbers |
| **By target** | status counts per table, source and series, worst first |
| **All checks** | the latest result of every check with its observed value |
| **Recent check runs** | the last 10 check runs and their counts |

With no stored results yet, the report says so and points to `lakehouse checks run`.

## Safety

Targets, check names, sample values and error messages come from data. Every one of them
is HTML-escaped. A test feeds `<script>` tags through every field and checks that none
survive. The output is also checked for balanced tags.

## In code

```python
from lakehouse.quality import report

html = report.render(con)                         # str
report.write(con, "data/reports/dq-report.html")  # creates parent directories
```

`data/` is git-ignored, so generated reports stay out of the repository. Publishing
them from the scheduled pipeline comes with orchestration (Phase 5).
