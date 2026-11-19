"""Slowly changing dimensions, Type 2: keep every version of a keyed record.

``apply_scd2`` compares a *source* relation (the current state, one row per key) with a
*history* table and records changes as validity intervals:

========================  ======================================================
column                    meaning
========================  ======================================================
``<key>``                 business key
``<tracked columns>``     attribute values of this version
``valid_from``            when this version became current (inclusive)
``valid_to``              when it was superseded (exclusive); NULL while current
``is_current``            TRUE for the live version of a key
``version``               1, 2, 3 ... per key
``row_hash``              hash of the tracked values, used to detect changes
========================  ======================================================

For each run (at ``as_of``): new keys get version 1; keys whose tracked values changed
get their current version closed and a new version opened; keys missing from the source
are closed (``close_missing=True``); a closed key that reappears opens a new version.
Unchanged keys are untouched, so re-running with the same source is a no-op. The whole
run is one transaction. See ``docs/scd2.md``.
"""

from __future__ import annotations

import logging
from collections.abc import Sequence
from dataclasses import dataclass
from datetime import datetime, timezone

import duckdb

log = logging.getLogger(__name__)

SCD_COLUMNS = ("valid_from", "valid_to", "is_current", "version", "row_hash")
_NULL = "∅"  # sentinel so NULL and '' hash differently


@dataclass
class ScdResult:
    table: str
    as_of: datetime
    inserted: int = 0  # brand-new keys
    changed: int = 0  # keys with a new version because values changed
    closed: int = 0  # keys closed because they left the source
    reopened: int = 0  # previously closed keys that came back
    unchanged: int = 0

    @property
    def touched(self) -> int:
        return self.inserted + self.changed + self.closed + self.reopened


def _hash_expr(columns: Sequence[str], alias: str) -> str:
    parts = ", ".join(f"coalesce(CAST({alias}.{c} AS VARCHAR), '{_NULL}')" for c in columns)
    return f"md5(concat_ws('\x1f', {parts}))"


def _exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    schema, name = table.split(".")
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()[0]
    )


def apply_scd2(
    con: duckdb.DuckDBPyConnection,
    source: str,
    target: str,
    key: str,
    tracked: Sequence[str],
    as_of: datetime | None = None,
    close_missing: bool = True,
) -> ScdResult:
    """Merge the current state in ``source`` into the SCD2 history table ``target``."""
    if not tracked:
        raise ValueError("tracked columns must not be empty")
    if key in tracked:
        raise ValueError(f"key {key!r} must not be a tracked column")
    as_of = (as_of or datetime.now(timezone.utc)).replace(tzinfo=None)
    result = ScdResult(table=target, as_of=as_of)
    cols = [key, *tracked]
    col_list = ", ".join(cols)

    dupes = con.execute(
        f"SELECT count(*) FROM (SELECT {key} FROM {source} GROUP BY 1 HAVING count(*) > 1)"
    ).fetchone()[0]
    if dupes:
        raise ValueError(f"{source} has {dupes} duplicate {key} values; SCD2 needs one per key")
    if con.execute(f"SELECT count(*) FROM {source} WHERE {key} IS NULL").fetchone()[0]:
        raise ValueError(f"{source} has NULL {key} values")

    con.execute(f"CREATE SCHEMA IF NOT EXISTS {target.split('.')[0]}")
    if not _exists(con, target):
        con.execute(
            f"CREATE TABLE {target} AS SELECT {col_list}, "
            "CAST(NULL AS TIMESTAMP) AS valid_from, CAST(NULL AS TIMESTAMP) AS valid_to, "
            "CAST(NULL AS BOOLEAN) AS is_current, CAST(NULL AS INTEGER) AS version, "
            f"CAST(NULL AS VARCHAR) AS row_hash FROM {source} WHERE FALSE"
        )
    else:
        latest = con.execute(f"SELECT max(valid_from) FROM {target}").fetchone()[0]
        if latest is not None and as_of < latest:
            raise ValueError(
                f"as_of {as_of} is earlier than the latest change in {target} ({latest}); "
                "SCD2 history can only move forward"
            )

    src_hash = _hash_expr(tracked, "s")
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _scd_src AS SELECT {', '.join('s.' + c for c in cols)}, "
            f"{src_hash} AS row_hash FROM {source} s"
        )
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _scd_cur AS SELECT {key}, row_hash, version "
            f"FROM {target} WHERE is_current"
        )
        con.execute(
            f"CREATE OR REPLACE TEMP TABLE _scd_last AS SELECT {key}, max(version) AS version "
            f"FROM {target} GROUP BY 1"
        )
        counts = con.execute(
            f"""
            SELECT
              count(*) FILTER (WHERE c.{key} IS NULL AND l.{key} IS NULL),
              count(*) FILTER (WHERE c.{key} IS NOT NULL AND c.row_hash <> s.row_hash),
              count(*) FILTER (WHERE c.{key} IS NULL AND l.{key} IS NOT NULL),
              count(*) FILTER (WHERE c.row_hash = s.row_hash)
            FROM _scd_src s
            LEFT JOIN _scd_cur c ON c.{key} = s.{key}
            LEFT JOIN _scd_last l ON l.{key} = s.{key}
            """
        ).fetchone()
        result.inserted, result.changed, result.reopened, result.unchanged = map(int, counts)
        if close_missing:
            result.closed = int(
                con.execute(
                    f"SELECT count(*) FROM _scd_cur c WHERE c.{key} NOT IN "
                    f"(SELECT {key} FROM _scd_src)"
                ).fetchone()[0]
            )

        # 1. close current versions that changed (or left the source)
        missing = f" OR t.{key} NOT IN (SELECT {key} FROM _scd_src)" if close_missing else ""
        con.execute(
            f"UPDATE {target} t SET valid_to = ?, is_current = FALSE "
            f"WHERE t.is_current AND (EXISTS (SELECT 1 FROM _scd_src s "
            f"WHERE s.{key} = t.{key} AND s.row_hash <> t.row_hash){missing})",
            [as_of],
        )
        # 2. open a version for every key without a current one: new, changed, reopened
        con.execute(
            f"INSERT INTO {target} ({col_list}, valid_from, valid_to, is_current, version, "
            f"row_hash) SELECT {', '.join('s.' + c for c in cols)}, ?, NULL, TRUE, "
            f"coalesce(l.version, 0) + 1, s.row_hash FROM _scd_src s "
            f"LEFT JOIN _scd_last l ON l.{key} = s.{key} "
            f"WHERE NOT EXISTS (SELECT 1 FROM {target} t WHERE t.{key} = s.{key} "
            "AND t.is_current)",
            [as_of],
        )
        for tmp in ("_scd_src", "_scd_cur", "_scd_last"):
            con.execute(f"DROP TABLE {tmp}")
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    log.info(
        "SCD2 %s: %d new, %d changed, %d closed, %d reopened, %d unchanged",
        target,
        result.inserted,
        result.changed,
        result.closed,
        result.reopened,
        result.unchanged,
        extra={"table": target, "as_of": as_of.isoformat(), "touched": result.touched},
    )
    return result


def as_of_sql(target: str, as_of: str = "?") -> str:
    """SQL selecting the versions of ``target`` that were current at ``as_of``."""
    return (
        f"SELECT * FROM {target} WHERE valid_from <= {as_of} "
        f"AND (valid_to IS NULL OR valid_to > {as_of})"
    )
