"""Expectation suites per table, and a runner.

A suite runs only if its table exists; otherwise it is reported as skipped. The suites
encode what must hold for each silver and reference table. Tolerances (``mostly``) are
used where real open data is known to be imperfect, e.g. coordinates.
"""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import dataclass, field

import duckdb

from lakehouse.quality.expectations import (
    Expectation,
    ExpectationResult,
    accepted_values,
    in_range,
    not_null,
    unique,
)
from lakehouse.silver.geo import CHICAGO_BBOX
from lakehouse.silver.service_requests import STATUSES

_LAT = (CHICAGO_BBOX["min_lat"], CHICAGO_BBOX["max_lat"])
_LON = (CHICAGO_BBOX["min_lon"], CHICAGO_BBOX["max_lon"])

C, SR, TX = "silver.crimes", "silver.service_requests", "silver.taxi_trips"
CA, PD = "reference.community_areas", "reference.police_districts"

SUITES: dict[str, list[Expectation]] = {
    C: [
        not_null(C, "crime_id"),
        unique(C, "crime_id"),
        not_null(C, "occurred_at", mostly=0.999),
        in_range(C, "occurred_at", min="2001-01-01"),
        in_range(C, "year", 2001, 2100),
        in_range(C, "community_area", 1, 77),
        in_range(C, "ward", 1, 50),
        in_range(C, "district", 1, 31),
        in_range(C, "latitude", *_LAT, mostly=0.99),
        in_range(C, "longitude", *_LON, mostly=0.99),
    ],
    SR: [
        not_null(SR, "sr_number"),
        unique(SR, "sr_number"),
        not_null(SR, "created_at"),
        accepted_values(SR, "status", STATUSES),
        in_range(SR, "community_area", 1, 77),
        in_range(SR, "ward", 1, 50),
        in_range(SR, "resolution_hours", 0),
        in_range(SR, "latitude", *_LAT, mostly=0.99),
        in_range(SR, "longitude", *_LON, mostly=0.99),
    ],
    TX: [
        not_null(TX, "trip_id"),
        unique(TX, "trip_id"),
        not_null(TX, "trip_start_at"),
        in_range(TX, "pickup_community_area", 1, 77),
        in_range(TX, "dropoff_community_area", 1, 77),
        in_range(TX, "trip_seconds", 0, mostly=0.99),
        in_range(TX, "fare", 0, mostly=0.99),
    ],
    CA: [
        not_null(CA, "community_area"),
        unique(CA, "community_area"),
        in_range(CA, "community_area", 1, 77),
        not_null(CA, "name"),
        unique(CA, "name"),
    ],
    PD: [
        not_null(PD, "district"),
        unique(PD, "district"),
        in_range(PD, "district", 1, 25),
        not_null(PD, "name"),
    ],
}


@dataclass
class SuiteResult:
    table: str
    skipped: bool = False
    results: list[ExpectationResult] = field(default_factory=list)

    @property
    def passed(self) -> bool:
        return all(r.passed for r in self.results)

    @property
    def failed(self) -> list[ExpectationResult]:
        return [r for r in self.results if not r.passed]


def _exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    schema, name = table.split(".")
    return bool(
        con.execute(
            "SELECT count(*) FROM information_schema.tables "
            "WHERE table_schema = ? AND table_name = ?",
            [schema, name],
        ).fetchone()[0]
    )


def run_suite(
    con: duckdb.DuckDBPyConnection, table: str, expectations: Iterable[Expectation] | None = None
) -> SuiteResult:
    expectations = list(expectations if expectations is not None else SUITES[table])
    if not _exists(con, table):
        return SuiteResult(table, skipped=True)
    return SuiteResult(table, results=[e.run(con) for e in expectations])


def run_all(
    con: duckdb.DuckDBPyConnection, tables: Iterable[str] | None = None
) -> list[SuiteResult]:
    tables = list(tables) if tables else list(SUITES)
    unknown = [t for t in tables if t not in SUITES]
    if unknown:
        raise ValueError(f"No suite for: {unknown}. Known: {sorted(SUITES)}")
    return [run_suite(con, t) for t in tables]
