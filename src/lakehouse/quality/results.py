"""Persistent data-quality results and history.

Every check — expectations, freshness, anomalies — is stored in one long table,
``quality.results``, with a normalised status, so history can be queried the same way
for all of them. A **check run** (``quality.check_runs``) groups the results written
together. See ``docs/dq-results.md``.

Status normalisation:

===============  ==========================  =====================================
check type       original                    stored status
===============  ==========================  =====================================
expectation      passed / failed / skipped   ``pass`` / ``fail`` / ``skipped``
freshness        ok / warn / error           ``pass`` / ``warn`` / ``fail``
anomaly          ok / anomaly / insuff.      ``pass`` / ``fail`` / ``skipped``
===============  ==========================  =====================================
"""

from __future__ import annotations

import json
import uuid
from collections.abc import Iterable
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any

import duckdb

from lakehouse.logging_setup import get_run_id
from lakehouse.quality.anomalies import AnomalyResult
from lakehouse.quality.freshness import FreshnessResult
from lakehouse.quality.suites import SuiteResult

STATUSES = ("pass", "warn", "fail", "skipped")


@dataclass
class ResultRow:
    check_type: str  # expectation | freshness | anomaly
    target: str  # table, source or series
    check_name: str
    status: str  # pass | warn | fail | skipped
    observed: float | None = None  # failing rows / age in hours / day's count
    details: dict[str, Any] = field(default_factory=dict)


@dataclass
class CheckRun:
    check_run_id: str
    started_at: datetime
    finished_at: datetime | None = None
    counts: dict[str, int] = field(default_factory=lambda: dict.fromkeys(STATUSES, 0))

    @property
    def worst(self) -> str:
        for s in ("fail", "warn", "pass"):
            if self.counts.get(s):
                return s
        return "skipped"


def ensure_tables(con: duckdb.DuckDBPyConnection) -> None:
    con.execute("CREATE SCHEMA IF NOT EXISTS quality")
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quality.check_runs (
            check_run_id VARCHAR PRIMARY KEY, started_at TIMESTAMP, finished_at TIMESTAMP,
            run_id VARCHAR, passed INTEGER, warned INTEGER, failed INTEGER, skipped INTEGER
        )
        """
    )
    con.execute(
        """
        CREATE TABLE IF NOT EXISTS quality.results (
            check_run_id VARCHAR, checked_at TIMESTAMP, check_type VARCHAR, target VARCHAR,
            check_name VARCHAR, status VARCHAR, observed DOUBLE, details JSON
        )
        """
    )


# ----------------------------------------------------------------------- conversion

def from_suites(suites: Iterable[SuiteResult]) -> list[ResultRow]:
    rows = []
    for s in suites:
        if s.skipped:
            rows.append(ResultRow("expectation", s.table, "suite", "skipped",
                                  details={"reason": "table does not exist"}))
            continue
        for r in s.results:
            rows.append(ResultRow(
                "expectation", r.table, r.name, "pass" if r.passed else "fail",
                float(r.failing_rows), r.to_dict()))
    return rows


def from_freshness(results: Iterable[FreshnessResult]) -> list[ResultRow]:
    status = {"ok": "pass", "warn": "warn", "error": "fail"}
    return [
        ResultRow("freshness", r.source, r.check, status[r.status], r.age_hours, r.to_dict())
        for r in results
    ]


def from_anomalies(results: Iterable[AnomalyResult]) -> list[ResultRow]:
    status = {"ok": "pass", "anomaly": "fail", "insufficient_history": "skipped"}
    return [
        ResultRow("anomaly", r.series, f"z_score:{r.day.isoformat()}", status[r.status],
                  float(r.value), r.to_dict())
        for r in results
    ]


# -------------------------------------------------------------------------- writing

def record(
    con: duckdb.DuckDBPyConnection, rows: Iterable[ResultRow], check_run_id: str | None = None
) -> CheckRun:
    """Store ``rows`` as one check run (or append to ``check_run_id``) in one transaction."""
    ensure_tables(con)
    now = datetime.now(timezone.utc).replace(tzinfo=None)
    rows = list(rows)
    run = CheckRun(check_run_id or uuid.uuid4().hex[:12], now, now)
    for r in rows:
        if r.status not in STATUSES:
            raise ValueError(f"invalid status {r.status!r} for {r.check_name}")
        run.counts[r.status] += 1
    con.execute("BEGIN TRANSACTION")
    try:
        if rows:
            con.executemany(
                "INSERT INTO quality.results VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                [[run.check_run_id, now, r.check_type, r.target, r.check_name, r.status,
                  r.observed, json.dumps(r.details, default=str)] for r in rows],
            )
        existing = con.execute(
            "SELECT passed, warned, failed, skipped, started_at FROM quality.check_runs "
            "WHERE check_run_id = ?", [run.check_run_id]).fetchone()
        if existing:
            for key, n in zip(("pass", "warn", "fail", "skipped"), existing[:4], strict=True):
                run.counts[key] += n
            run.started_at = existing[4]
            con.execute("DELETE FROM quality.check_runs WHERE check_run_id = ?",
                        [run.check_run_id])
        con.execute(
            "INSERT INTO quality.check_runs VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [run.check_run_id, run.started_at, now, get_run_id(), run.counts["pass"],
             run.counts["warn"], run.counts["fail"], run.counts["skipped"]],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    return run


# -------------------------------------------------------------------------- reading

def _rows(con: duckdb.DuckDBPyConnection, sql: str, params: list) -> list[dict[str, Any]]:
    cur = con.execute(sql, params)
    cols = [d[0] for d in cur.description]
    return [dict(zip(cols, r, strict=True)) for r in cur.fetchall()]


def history(
    con: duckdb.DuckDBPyConnection,
    target: str | None = None,
    check_name: str | None = None,
    check_type: str | None = None,
    status: str | None = None,
    limit: int = 50,
) -> list[dict[str, Any]]:
    """Most recent results first, optionally filtered."""
    ensure_tables(con)
    where, params = [], []
    for col, value in (("target", target), ("check_name", check_name),
                       ("check_type", check_type), ("status", status)):
        if value is not None:
            where.append(f"{col} = ?")
            params.append(value)
    sql = ("SELECT checked_at, check_run_id, check_type, target, check_name, status, observed "
           "FROM quality.results")
    if where:
        sql += " WHERE " + " AND ".join(where)
    sql += " ORDER BY checked_at DESC, rowid DESC LIMIT ?"
    return _rows(con, sql, [*params, limit])


def latest(con: duckdb.DuckDBPyConnection) -> list[dict[str, Any]]:
    """Latest status of every check, with its failure streak and when it last passed.

    ``streak`` counts consecutive most-recent results with the latest status
    (e.g. failing for 3 runs). Anomaly checks are named per day, so they are grouped
    by series and type here (``check_name`` shows the latest day).
    """
    ensure_tables(con)
    return _rows(
        con,
        """
        WITH r AS (
          SELECT *, CASE WHEN check_type = 'anomaly' THEN 'z_score' ELSE check_name END AS key,
                 row_number() OVER (PARTITION BY check_type, target,
                   CASE WHEN check_type = 'anomaly' THEN 'z_score' ELSE check_name END
                   ORDER BY checked_at DESC, rowid DESC) AS rn
          FROM quality.results
        ),
        cur AS (SELECT * FROM r WHERE rn = 1),
        breaks AS (
          SELECT r.check_type, r.target, r.key, min(r.rn) AS first_break
          FROM r JOIN cur USING (check_type, target, key)
          WHERE r.status <> cur.status GROUP BY ALL
        ),
        passes AS (
          SELECT check_type, target, key, max(checked_at) AS last_pass_at
          FROM r WHERE status = 'pass' GROUP BY ALL
        ),
        totals AS (
          SELECT check_type, target, key, count(*) AS n FROM r GROUP BY ALL
        )
        SELECT cur.check_type, cur.target, cur.check_name, cur.status, cur.observed,
               cur.checked_at, coalesce(b.first_break - 1, t.n) AS streak, p.last_pass_at
        FROM cur
        JOIN totals t USING (check_type, target, key)
        LEFT JOIN breaks b USING (check_type, target, key)
        LEFT JOIN passes p USING (check_type, target, key)
        ORDER BY CASE cur.status WHEN 'fail' THEN 0 WHEN 'warn' THEN 1 WHEN 'skipped' THEN 2
                 ELSE 3 END, cur.check_type, cur.target, cur.check_name
        """,
        [],
    )


def pass_rate_by_day(con: duckdb.DuckDBPyConnection, days: int = 30) -> list[dict[str, Any]]:
    """Share of non-skipped results that passed, per day and check type."""
    ensure_tables(con)
    return _rows(
        con,
        """
        SELECT CAST(checked_at AS DATE) AS day, check_type,
               count(*) FILTER (WHERE status <> 'skipped') AS checks,
               round(count(*) FILTER (WHERE status = 'pass')
                     / nullif(count(*) FILTER (WHERE status <> 'skipped'), 0), 4) AS pass_rate
        FROM quality.results
        WHERE checked_at >= current_date - CAST(? AS INTEGER)
        GROUP BY ALL ORDER BY day DESC, check_type
        """,
        [days],
    )


def runs(con: duckdb.DuckDBPyConnection, limit: int = 20) -> list[dict[str, Any]]:
    ensure_tables(con)
    return _rows(con, "SELECT * FROM quality.check_runs ORDER BY started_at DESC LIMIT ?",
                 [limit])
