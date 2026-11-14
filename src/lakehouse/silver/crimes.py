"""``silver.crimes``: typed Chicago crimes, one row per crime ID.

Built from ``bronze.chicago_crimes__latest`` (the latest version of each record). Column
names follow the Socrata dataset ``ijzp-q8t2``; see ``docs/silver-crimes.md``.
"""

from __future__ import annotations

from lakehouse.silver.typing import BuildResult, Column, build_typed_table
from lakehouse.storage.bronze import LATEST_SUFFIX, BronzeStore

SOURCE = "chicago_crimes"
TABLE = "silver.crimes"

COLUMNS: list[Column] = [
    Column("crime_id", "id", "int", description="Unique crime record ID"),
    Column("case_number", "case_number", "upper", description="CPD Records Division number"),
    Column("occurred_at", "date", "timestamp", description="When the incident occurred "
           "(Chicago local time, no time zone)"),
    Column("updated_at", "updated_on", "timestamp", description="Last update on the portal"),
    Column("year", "year", "int", description="Year of the incident"),
    Column("block", "block", "upper", description="Partially redacted address block"),
    Column("iucr", "iucr", "code", width=4, description="Illinois Uniform Crime Reporting code"),
    Column("primary_type", "primary_type", "upper", description="Primary IUCR category"),
    Column("description", "description", "upper", description="IUCR secondary description"),
    Column("location_description", "location_description", "upper",
           description="Kind of location"),
    Column("arrest", "arrest", "bool", description="Whether an arrest was made"),
    Column("domestic", "domestic", "bool", description="Domestic-related incident"),
    Column("beat", "beat", "code", width=4, description="Police beat"),
    Column("district", "district", "int", description="Police district number"),
    Column("ward", "ward", "int", description="City Council ward"),
    Column("community_area", "community_area", "int", description="Community area (1-77)"),
    Column("fbi_code", "fbi_code", "upper", description="FBI NIBRS offense code"),
    Column("x_coordinate", "x_coordinate", "double",
           description="State Plane Illinois East (feet)"),
    Column("y_coordinate", "y_coordinate", "double",
           description="State Plane Illinois East (feet)"),
    Column("latitude", "latitude", "double", description="WGS84 latitude"),
    Column("longitude", "longitude", "double", description="WGS84 longitude"),
]


def build(store: BronzeStore) -> BuildResult:
    """Rebuild ``silver.crimes`` from the latest bronze version of every crime."""
    relation = f"bronze.{SOURCE}{LATEST_SUFFIX}"
    exists = store.con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'bronze' AND table_name = ?",
        [f"{SOURCE}{LATEST_SUFFIX}"],
    ).fetchone()[0]
    if not exists:
        raise ValueError(f"No bronze data for '{SOURCE}'; run `lakehouse ingest {SOURCE}` first")
    return build_typed_table(store.con, relation, TABLE, COLUMNS)
