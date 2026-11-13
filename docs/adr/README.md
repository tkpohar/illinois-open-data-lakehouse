# Architecture decision records

Short records of significant, hard-to-reverse decisions: the context at the time, the
options considered, what was chosen and what it costs. One file per decision, numbered,
never deleted: a reversed decision gets a new ADR that *supersedes* the old one.

| ADR | Title | Status |
|---|---|---|
| [0001](0001-duckdb-vs-postgres.md) | DuckDB (not Postgres) as the lakehouse engine | Accepted |

## Template

Copy this into `docs/adr/NNNN-short-title.md`:

```markdown
# ADR-NNNN: Title

- **Status:** Proposed | Accepted | Superseded by ADR-XXXX
- **Date:** YYYY-MM-DD

## Context
What forces are at play; what problem needs a decision.

## Options considered
The realistic alternatives, with their trade-offs.

## Decision
What we chose, stated plainly.

## Consequences
What becomes easier, what becomes harder, and what would make us revisit this.
```

`tests/test_docs.py` checks that every ADR listed here exists, has the sections above and
a valid status, and that relative links across the docs resolve.
