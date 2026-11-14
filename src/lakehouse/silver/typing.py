"""Declarative typing of raw JSON payloads into silver columns.

A silver table is described by a list of ``Column`` specs. Each spec names the target
column, the raw payload key it comes from, and a *kind* that decides how the text is
cleaned and cast. All casts are ``TRY_CAST``-style: a value that cannot be converted
becomes NULL and is counted as a **cast failure** for that column (a raw value that was
present and non-blank but produced NULL). See ``docs/silver-crimes.md``.

Kinds:

==============  ===============================================================
``text``        trimmed string; blank → NULL
``upper``       trimmed, upper-cased, inner whitespace collapsed; blank → NULL
``code``        trimmed string left-padded with zeros to ``width`` (e.g. IUCR)
``int``         BIGINT
``double``      DOUBLE
``bool``        true/t/y/yes/1 → TRUE, false/f/n/no/0 → FALSE, else NULL
``timestamp``   TIMESTAMP (Socrata floating timestamps, ISO-8601)
==============  ===============================================================
"""

from __future__ import annotations

import json
import logging
from collections.abc import Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb

log = logging.getLogger(__name__)

KINDS = ("text", "upper", "code", "int", "double", "bool", "timestamp")
TRUE_VALUES = ("true", "t", "y", "yes", "1")
FALSE_VALUES = ("false", "f", "n", "no", "0")


@dataclass(frozen=True)
class Column:
    name: str
    raw: str
    kind: str
    width: int | None = None  # for kind="code"
    description: str = ""

    def __post_init__(self) -> None:
        if self.kind not in KINDS:
            raise ValueError(f"Unknown kind {self.kind!r} for column {self.name!r}")
        if self.kind == "code" and not self.width:
            raise ValueError(f"Column {self.name!r} of kind 'code' needs a width")


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def raw_text(col: Column) -> str:
    """The trimmed raw value as text, NULL when missing or blank."""
    return f"NULLIF(trim(payload->>{_lit(col.raw)}), '')"


def typed_expr(col: Column) -> str:
    raw = raw_text(col)
    kind = col.kind
    if kind == "text":
        return raw
    if kind == "upper":
        return f"upper(regexp_replace({raw}, '\\s+', ' ', 'g'))"
    if kind == "code":
        return f"lpad({raw}, {int(col.width)}, '0')"
    if kind == "int":
        # Accept "12" and "12.0" but not "12.5".
        return (
            f"CASE WHEN TRY_CAST({raw} AS DOUBLE) = TRY_CAST(TRY_CAST({raw} AS DOUBLE) "
            f"AS BIGINT) THEN TRY_CAST(TRY_CAST({raw} AS DOUBLE) AS BIGINT) END"
        )
    if kind == "double":
        return f"TRY_CAST({raw} AS DOUBLE)"
    if kind == "bool":
        trues = ", ".join(_lit(v) for v in TRUE_VALUES)
        falses = ", ".join(_lit(v) for v in FALSE_VALUES)
        return (
            f"CASE WHEN lower({raw}) IN ({trues}) THEN TRUE "
            f"WHEN lower({raw}) IN ({falses}) THEN FALSE END"
        )
    return f"TRY_CAST({raw} AS TIMESTAMP)"  # timestamp


SQL_TYPES = {
    "text": "VARCHAR",
    "upper": "VARCHAR",
    "code": "VARCHAR",
    "int": "BIGINT",
    "double": "DOUBLE",
    "bool": "BOOLEAN",
    "timestamp": "TIMESTAMP",
}


@dataclass
class BuildResult:
    table: str
    source: str
    rows: int
    cast_failures: dict[str, int] = field(default_factory=dict)
    built_at: str | None = None

    @property
    def total_cast_failures(self) -> int:
        return sum(self.cast_failures.values())


def build_typed_table(
    con: duckdb.DuckDBPyConnection,
    source_relation: str,
    target: str,
    columns: Sequence[Column],
    where: str | None = None,
) -> BuildResult:
    """(Re)build ``target`` from the JSON ``payload`` of ``source_relation``.

    Lineage columns ``record_key``, ``run_id`` and ``ingested_at`` are carried over from
    bronze, and ``silver_built_at`` records the build time. The build and its entry in
    ``silver._build_log`` happen in one transaction.
    """
    schema = target.split(".")[0]
    built_at = datetime.now(timezone.utc).replace(tzinfo=None)
    selects = [f"CAST({typed_expr(c)} AS {SQL_TYPES[c.kind]}) AS {c.name}" for c in columns]
    lineage = ["record_key", "run_id", "ingested_at", "CAST(? AS TIMESTAMP) AS silver_built_at"]
    filter_sql = f" WHERE {where}" if where else ""
    failure_sums = [
        f"count(*) FILTER (WHERE {raw_text(c)} IS NOT NULL AND ({typed_expr(c)}) IS NULL)"
        for c in columns
    ]

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}._build_log (table_name VARCHAR, "
        "source VARCHAR, built_at TIMESTAMP, rows BIGINT, cast_failures JSON)"
    )
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"CREATE OR REPLACE TABLE {target} AS SELECT {', '.join(selects + lineage)} "
            f"FROM {source_relation}{filter_sql} ORDER BY record_key",
            [built_at],
        )
        counts = con.execute(
            f"SELECT count(*), {', '.join(failure_sums)} FROM {source_relation}{filter_sql}"
        ).fetchone()
        failures = {
            c.name: int(n) for c, n in zip(columns, counts[1:], strict=True) if n
        }
        result = BuildResult(
            table=target,
            source=source_relation,
            rows=int(counts[0]),
            cast_failures=failures,
            built_at=built_at.isoformat(sep=" ", timespec="seconds"),
        )
        con.execute(
            f"INSERT INTO {schema}._build_log VALUES (?, ?, ?, ?, ?)",
            [target, source_relation, built_at, result.rows, json.dumps(failures)],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    level = logging.WARNING if failures else logging.INFO
    log.log(
        level,
        "Built %s: %d rows, %d cast failures",
        target,
        result.rows,
        result.total_cast_failures,
        extra={"table": target, "rows": result.rows, "cast_failures": failures},
    )
    return result
