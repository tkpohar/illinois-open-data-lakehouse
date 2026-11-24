"""Declarative data-quality expectations, compiled to SQL and run in DuckDB.

Four expectation types cover the basics:

==================  ==========================================================
``NotNull``         the column has no NULLs
``Unique``          no two rows share the same (non-NULL) value(s)
``InRange``         values lie within ``[min, max]`` (either bound optional)
``AcceptedValues``  values are in a fixed set
==================  ==========================================================

``Unique``, ``InRange`` and ``AcceptedValues`` ignore NULLs (that is ``NotNull``'s job).
Every expectation counts its **failing rows**, keeps a small sample of offending values,
and passes when the failing share is at most ``1 - mostly`` (``mostly=1.0``, the default,
means "no failures"; ``mostly=0.99`` tolerates 1 %). See ``docs/data-quality.md``.
"""

from __future__ import annotations

from collections.abc import Sequence
from dataclasses import dataclass, field
from typing import Any

import duckdb

SAMPLE_SIZE = 5


@dataclass
class ExpectationResult:
    name: str
    table: str
    kind: str
    columns: tuple[str, ...]
    passed: bool
    failing_rows: int
    total_rows: int
    mostly: float = 1.0
    sample: list[Any] = field(default_factory=list)
    error: str | None = None  # SQL error (e.g. missing column): counts as a failure

    @property
    def failing_share(self) -> float:
        return self.failing_rows / self.total_rows if self.total_rows else 0.0

    def to_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "table": self.table,
            "kind": self.kind,
            "columns": list(self.columns),
            "passed": self.passed,
            "failing_rows": self.failing_rows,
            "total_rows": self.total_rows,
            "failing_share": round(self.failing_share, 6),
            "mostly": self.mostly,
            "sample": [str(v) for v in self.sample],
            "error": self.error,
        }


@dataclass(frozen=True)
class Expectation:
    """Base class. Subclasses define ``kind``, ``columns`` and ``_failing_sql``."""

    table: str
    mostly: float = 1.0
    name: str | None = None
    description: str = ""

    kind = "expectation"

    def __post_init__(self) -> None:
        if not 0.0 <= self.mostly <= 1.0:
            raise ValueError(f"mostly must be within [0, 1], got {self.mostly}")

    @property
    def columns(self) -> tuple[str, ...]:  # pragma: no cover - overridden
        raise NotImplementedError

    @property
    def label(self) -> str:
        return self.name or f"{self.kind}:{'+'.join(self.columns)}"

    def _failing_sql(self) -> tuple[str, list[Any]]:  # pragma: no cover - overridden
        """A WHERE condition selecting failing rows, and its parameters."""
        raise NotImplementedError

    def _sample_sql(self, condition: str) -> str:
        cols = ", ".join(self.columns)
        return (
            f"SELECT DISTINCT {cols} FROM {self.table} WHERE {condition} "
            f"ORDER BY {cols} LIMIT {SAMPLE_SIZE}"
        )

    def run(self, con: duckdb.DuckDBPyConnection) -> ExpectationResult:
        result = ExpectationResult(
            self.label, self.table, self.kind, self.columns, False, 0, 0, self.mostly
        )
        try:
            condition, params = self._failing_sql()
            total, failing = con.execute(
                f"SELECT count(*), count(*) FILTER (WHERE {condition}) FROM {self.table}",
                params,
            ).fetchone()
            sample_rows = con.execute(self._sample_sql(condition), params).fetchall()
        except duckdb.Error as exc:
            result.error = f"{type(exc).__name__}: {exc}".splitlines()[0]
            return result
        result.total_rows, result.failing_rows = int(total), int(failing)
        result.sample = [r[0] if len(r) == 1 else tuple(r) for r in sample_rows]
        result.passed = result.failing_share <= (1.0 - self.mostly) + 1e-12
        return result


@dataclass(frozen=True)
class NotNull(Expectation):
    column: str = ""
    kind = "not_null"

    @property
    def columns(self) -> tuple[str, ...]:
        return (self.column,)

    def _failing_sql(self) -> tuple[str, list[Any]]:
        return f"{self.column} IS NULL", []

    def _sample_sql(self, condition: str) -> str:
        return "SELECT 1 WHERE FALSE"  # the failing value is always NULL: nothing to show


@dataclass(frozen=True)
class Unique(Expectation):
    column_list: tuple[str, ...] = ()
    kind = "unique"

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.column_list:
            raise ValueError("Unique needs at least one column")

    @property
    def columns(self) -> tuple[str, ...]:
        return tuple(self.column_list)

    def _failing_sql(self) -> tuple[str, list[Any]]:
        cols = ", ".join(self.columns)
        not_null = " AND ".join(f"{c} IS NOT NULL" for c in self.columns)
        return (
            f"({cols}) IN (SELECT ({cols}) FROM {self.table} WHERE {not_null} "
            f"GROUP BY {cols} HAVING count(*) > 1)",
            [],
        )


@dataclass(frozen=True)
class InRange(Expectation):
    column: str = ""
    min: float | int | str | None = None
    max: float | int | str | None = None
    kind = "in_range"

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.min is None and self.max is None:
            raise ValueError("InRange needs min, max or both")

    @property
    def columns(self) -> tuple[str, ...]:
        return (self.column,)

    def _failing_sql(self) -> tuple[str, list[Any]]:
        parts, params = [], []
        if self.min is not None:
            parts.append(f"{self.column} < ?")
            params.append(self.min)
        if self.max is not None:
            parts.append(f"{self.column} > ?")
            params.append(self.max)
        return f"{self.column} IS NOT NULL AND ({' OR '.join(parts)})", params


@dataclass(frozen=True)
class AcceptedValues(Expectation):
    column: str = ""
    values: Sequence[Any] = ()
    kind = "accepted_values"

    def __post_init__(self) -> None:
        super().__post_init__()
        if not self.values:
            raise ValueError("AcceptedValues needs at least one value")

    @property
    def columns(self) -> tuple[str, ...]:
        return (self.column,)

    def _failing_sql(self) -> tuple[str, list[Any]]:
        marks = ", ".join("?" for _ in self.values)
        return f"{self.column} IS NOT NULL AND {self.column} NOT IN ({marks})", list(self.values)


def not_null(table: str, column: str, **kw) -> NotNull:
    return NotNull(table, column=column, **kw)


def unique(table: str, *columns: str, **kw) -> Unique:
    return Unique(table, column_list=tuple(columns), **kw)


def in_range(table: str, column: str, min=None, max=None, **kw) -> InRange:  # noqa: A002
    return InRange(table, column=column, min=min, max=max, **kw)


def accepted_values(table: str, column: str, values: Sequence[Any], **kw) -> AcceptedValues:
    return AcceptedValues(table, column=column, values=tuple(values), **kw)
