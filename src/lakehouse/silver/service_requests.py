"""``silver.service_requests``: typed Chicago 311 service requests, one row per SR number.

Built from ``bronze.chicago_311_requests__latest``. Column names follow the Socrata
dataset ``v6vf-nfxy``. Besides typing, the raw ``status`` is normalised to a small set
of canonical values and resolution time is derived; see ``docs/silver-311.md``.
"""

from __future__ import annotations

from lakehouse.silver.typing import (
    BuildResult,
    Column,
    MissingInputError,
    build_typed_table,
)
from lakehouse.storage.bronze import LATEST_SUFFIX, BronzeStore

SOURCE = "chicago_311_requests"
TABLE = "silver.service_requests"
GRAIN = "One row per 311 service request (`sr_number`); latest bronze version of each record."

COLUMNS: list[Column] = [
    Column("sr_number", "sr_number", "upper", description="Service request number"),
    Column("sr_type", "sr_type", "upper", description="Request type"),
    Column("sr_short_code", "sr_short_code", "upper", description="Request type code"),
    Column("owner_department", "owner_department", "upper",
           description="Department responsible"),
    Column("status_raw", "status", "text", description="Status exactly as published"),
    Column("origin", "origin", "upper", description="Channel the request came in through"),
    Column("created_at", "created_date", "timestamp",
           description="When the request was created (Chicago local time)"),
    Column("last_modified_at", "last_modified_date", "timestamp",
           description="Last modification on the portal"),
    Column("closed_at", "closed_date", "timestamp", description="When the request was closed"),
    Column("street_address", "street_address", "upper", description="Address of the request"),
    Column("zip_code", "zip_code", "code", width=5, description="ZIP code"),
    Column("duplicate", "duplicate", "bool", description="Flagged as a duplicate request"),
    Column("legacy_record", "legacy_record", "bool",
           description="Migrated from the legacy 311 system"),
    Column("parent_sr_number", "parent_sr_number", "upper",
           description="Parent request for duplicates"),
    Column("community_area", "community_area", "int", description="Community area (1-77)"),
    Column("ward", "ward", "int", description="City Council ward"),
    Column("police_district", "police_district", "int", description="Police district"),
    Column("latitude", "latitude", "double", description="WGS84 latitude"),
    Column("longitude", "longitude", "double", description="WGS84 longitude"),
]

#: Canonical status per normalised raw value (upper-cased, whitespace collapsed).
STATUS_MAP: dict[str, str] = {
    "OPEN": "OPEN",
    "OPEN - DUP": "OPEN",
    "OPEN-DUP": "OPEN",
    "IN PROGRESS": "OPEN",
    "COMPLETED": "COMPLETED",
    "COMPLETE": "COMPLETED",
    "CLOSED": "COMPLETED",
    "CANCELED": "CANCELED",
    "CANCELLED": "CANCELED",
}
STATUSES = ("OPEN", "COMPLETED", "CANCELED", "UNKNOWN")
#: Raw values that mark the request itself as a duplicate.
DUPLICATE_STATUSES = ("OPEN - DUP", "OPEN-DUP")


def _lit(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


_NORM = "upper(regexp_replace(trim(status_raw), '\\s+', ' ', 'g'))"
_STATUS_CASE = (
    "CASE "
    + " ".join(f"WHEN {_NORM} = {_lit(k)} THEN {_lit(v)}" for k, v in STATUS_MAP.items())
    + " ELSE 'UNKNOWN' END"
)

DERIVED: list[tuple[str, str]] = [
    ("status", _STATUS_CASE),
    ("is_open", f"({_STATUS_CASE}) = 'OPEN'"),
    (
        "is_duplicate",
        f"coalesce(duplicate, FALSE) OR coalesce({_NORM} IN "
        f"({', '.join(_lit(s) for s in DUPLICATE_STATUSES)}), FALSE)",
    ),
    (
        "resolution_hours",
        "CASE WHEN closed_at IS NOT NULL AND created_at IS NOT NULL "
        "AND closed_at >= created_at "
        "THEN round(date_diff('second', created_at, closed_at) / 3600.0, 2) END",
    ),
]

DERIVED_DESCRIPTIONS: dict[str, str] = {
    "status": "Canonical status: OPEN, COMPLETED, CANCELED or UNKNOWN (from `status_raw`)",
    "is_open": "`status = 'OPEN'`",
    "is_duplicate": "Duplicate flag set, or a '... - Dup' status",
    "resolution_hours": "Hours from `created_at` to `closed_at` (NULL if either is missing "
    "or closed precedes created)",
}

QUALITY_CHECKS: dict[str, str] = {
    "unknown_status": "status = 'UNKNOWN'",
    "open_with_closed_date": "status = 'OPEN' AND closed_at IS NOT NULL",
    "completed_without_closed_date": "status = 'COMPLETED' AND closed_at IS NULL",
    "closed_before_created": "closed_at < created_at",
}


def build(store: BronzeStore, incremental: bool = False) -> BuildResult:
    """Rebuild ``silver.service_requests`` from the latest bronze version of every SR."""
    view = f"{SOURCE}{LATEST_SUFFIX}"
    exists = store.con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'bronze' AND table_name = ?",
        [view],
    ).fetchone()[0]
    if not exists:
        raise MissingInputError(
            f"No bronze data for '{SOURCE}'; run `lakehouse ingest {SOURCE}` first"
        )
    return build_typed_table(
        store.con,
        f"bronze.{view}",
        TABLE,
        COLUMNS,
        derived=DERIVED,
        quality_checks=QUALITY_CHECKS,
        incremental=incremental,
        changes_relation=f"bronze.{SOURCE}",
    )
