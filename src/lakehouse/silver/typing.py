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

import hashlib
import json
import logging
from collections.abc import Mapping, Sequence
from dataclasses import dataclass, field
from datetime import datetime, timezone

import duckdb

log = logging.getLogger(__name__)

KINDS = ("text", "upper", "code", "int", "double", "bool", "timestamp")
TRUE_VALUES = ("true", "t", "y", "yes", "1")
FALSE_VALUES = ("false", "f", "n", "no", "0")


class MissingInputError(ValueError):
    """A transform's input (bronze view or upstream table) does not exist yet."""


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


LINEAGE_DESCRIPTIONS: dict[str, str] = {
    "record_key": "Bronze record key (the source primary key as text)",
    "run_id": "Ingestion run that landed this version of the record",
    "ingested_at": "When that version was ingested into bronze (UTC)",
    "silver_built_at": "When the silver build that wrote this row ran (UTC)",
}

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
    quality: dict[str, int] = field(default_factory=dict)  # check name -> violating rows
    mode: str = "full"  # full | incremental
    processed: int = 0  # rows (re)computed by this build
    full_reason: str | None = None  # why an incremental request did a full build

    @property
    def total_cast_failures(self) -> int:
        return sum(self.cast_failures.values())


def _exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    schema, name = table.split(".")
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()[0]
    )


def build_typed_table(
    con: duckdb.DuckDBPyConnection,
    source_relation: str,
    target: str,
    columns: Sequence[Column],
    where: str | None = None,
    derived: Sequence[tuple[str, str]] = (),
    quality_checks: Mapping[str, str] | None = None,
    incremental: bool = False,
    changes_relation: str | None = None,
) -> BuildResult:
    """Build ``target`` from the JSON ``payload`` of ``source_relation``.

    Lineage columns ``record_key``, ``run_id`` and ``ingested_at`` are carried over from
    bronze, and ``silver_built_at`` records the build time. ``derived`` adds
    ``(name, sql)`` columns computed from the typed columns. ``quality_checks`` maps a
    check name to a SQL condition over the built table; the number of rows matching each
    condition is recorded (rows are kept).

    With ``incremental=True`` and ``changes_relation`` (the append-only bronze table
    behind ``source_relation``), only keys with bronze rows ingested after the table's
    watermark are recomputed and upserted. A full rebuild happens instead when the
    table or its watermark is missing or the column spec changed. The build, the
    watermark and the ``<schema>._build_log`` entry are written in one transaction.
    See ``docs/silver-incremental.md``.
    """
    schema = target.split(".")[0]
    built_at = datetime.now(timezone.utc).replace(tzinfo=None)
    built_lit = f"TIMESTAMP '{built_at.isoformat(sep=' ')}'"
    selects = [f"CAST({typed_expr(c)} AS {SQL_TYPES[c.kind]}) AS {c.name}" for c in columns]
    lineage = ["record_key", "run_id", "ingested_at", f"{built_lit} AS silver_built_at"]
    derived_sql = "".join(f", {expr} AS {name}" for name, expr in derived)
    spec_hash = hashlib.sha256(
        "\n".join([*selects, derived_sql, where or "", source_relation]).encode()
    ).hexdigest()[:16]

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}._build_log (table_name VARCHAR, "
        "source VARCHAR, built_at TIMESTAMP, rows BIGINT, cast_failures JSON)"
    )
    for col, typ in (("quality", "JSON"), ("mode", "VARCHAR"), ("processed", "BIGINT")):
        con.execute(f"ALTER TABLE {schema}._build_log ADD COLUMN IF NOT EXISTS {col} {typ}")
    con.execute(
        f"CREATE TABLE IF NOT EXISTS {schema}._watermarks (table_name VARCHAR PRIMARY KEY, "
        "source VARCHAR, max_ingested_at TIMESTAMP, spec_hash VARCHAR, updated_at TIMESTAMP)"
    )

    watermark, reason = None, None
    if incremental:
        row = con.execute(
            f"SELECT max_ingested_at, spec_hash FROM {schema}._watermarks WHERE table_name = ?",
            [target],
        ).fetchone()
        if changes_relation is None:
            reason = "no changes relation"
        elif not _exists(con, target):
            reason = "target does not exist"
        elif row is None or row[0] is None:
            reason = "no watermark"
        elif row[1] != spec_hash:
            reason = "column spec changed"
        else:
            watermark = row[0]
    mode = "incremental" if watermark is not None else "full"
    high = con.execute(
        f"SELECT max(ingested_at) FROM {changes_relation or source_relation}"
    ).fetchone()[0]

    conditions = [where] if where else []
    if mode == "incremental":
        conditions.append("record_key IN (SELECT record_key FROM _silver_changed)")
    filter_sql = f" WHERE {' AND '.join(conditions)}" if conditions else ""
    typed_select = (
        f"SELECT {', '.join(selects + lineage)} FROM {source_relation}{filter_sql}"
    )
    failure_sums = [
        f"count(*) FILTER (WHERE {raw_text(c)} IS NOT NULL AND ({typed_expr(c)}) IS NULL)"
        for c in columns
    ]

    con.execute("BEGIN TRANSACTION")
    try:
        if mode == "full":
            con.execute(
                f"CREATE OR REPLACE TABLE {target} AS SELECT t.*{derived_sql} "
                f"FROM ({typed_select}) t ORDER BY record_key"
            )
        else:
            con.execute(
                "CREATE OR REPLACE TEMP TABLE _silver_changed AS "
                f"SELECT DISTINCT record_key FROM {changes_relation} "
                "WHERE ingested_at > ? AND ingested_at <= ?",
                [watermark, high],
            )
            con.execute(
                f"DELETE FROM {target} WHERE record_key IN "
                "(SELECT record_key FROM _silver_changed)"
            )
            con.execute(
                f"INSERT INTO {target} BY NAME SELECT t.*{derived_sql} "
                f"FROM ({typed_select}) t ORDER BY record_key"
            )
        counts = con.execute(
            f"SELECT count(*), {', '.join(failure_sums)} FROM {source_relation}{filter_sql}"
        ).fetchone()
        if mode == "incremental":
            con.execute("DROP TABLE _silver_changed")
        processed = int(counts[0])
        failures = {c.name: int(n) for c, n in zip(columns, counts[1:], strict=True) if n}
        quality = {
            name: int(con.execute(f"SELECT count(*) FROM {target} WHERE {cond}").fetchone()[0])
            for name, cond in (quality_checks or {}).items()
        }
        total = int(con.execute(f"SELECT count(*) FROM {target}").fetchone()[0])
        result = BuildResult(
            table=target,
            source=source_relation,
            rows=total,
            cast_failures=failures,
            built_at=built_at.isoformat(sep=" ", timespec="seconds"),
            quality=quality,
            mode=mode,
            processed=processed,
            full_reason=reason if incremental and mode == "full" else None,
        )
        con.execute(
            f"INSERT INTO {schema}._build_log "
            "(table_name, source, built_at, rows, cast_failures, quality, mode, processed) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            [target, source_relation, built_at, total, json.dumps(failures),
             json.dumps(quality), mode, processed],
        )
        if high is not None or mode == "full":
            con.execute(
                f"INSERT OR REPLACE INTO {schema}._watermarks VALUES (?, ?, ?, ?, ?)",
                [target, changes_relation or source_relation,
                 high if high is not None else watermark, spec_hash, built_at],
            )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    issues = {k: v for k, v in quality.items() if v}
    level = logging.WARNING if failures or issues else logging.INFO
    log.log(
        level,
        "Built %s (%s): %d rows, %d processed, %d cast failures",
        target,
        mode,
        result.rows,
        processed,
        result.total_cast_failures,
        extra={
            "table": target,
            "mode": mode,
            "rows": result.rows,
            "processed": processed,
            "cast_failures": failures,
            "quality": issues,
        },
    )
    return result
