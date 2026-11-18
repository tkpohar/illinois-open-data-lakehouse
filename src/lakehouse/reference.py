"""Shared reference data loaded from seed CSVs in ``seeds/`` into the ``reference`` schema.

Each seed is declared in ``SEEDS`` with its columns, key and validation rules. Loading
validates the file *before* touching the database (unique non-null key, required
columns, expected key range); a bad seed raises ``SeedError`` and leaves the previous
table in place. A seed whose file is unchanged (same SHA-256) is skipped unless
``force=True``. See ``docs/reference-data.md``.
"""

from __future__ import annotations

import csv
import hashlib
import logging
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from lakehouse.storage.bronze import BronzeStore

SEEDS_DIR = Path(__file__).resolve().parents[2] / "seeds"

log = logging.getLogger(__name__)


class SeedError(ValueError):
    """A seed file failed validation."""


@dataclass(frozen=True)
class Seed:
    name: str
    file: str
    columns: dict[str, str]  # column -> DuckDB type, in file order
    key: str
    key_range: tuple[int, int] | None = None  # inclusive bounds for an integer key
    expected_rows: int | None = None
    description: str = ""

    @property
    def table(self) -> str:
        return f"reference.{self.name}"


SEEDS: dict[str, Seed] = {
    "community_areas": Seed(
        name="community_areas",
        file="community_areas.csv",
        columns={"community_area": "INTEGER", "name": "VARCHAR"},
        key="community_area",
        key_range=(1, 77),
        expected_rows=77,
        description="Chicago's 77 official community areas",
    ),
    "police_districts": Seed(
        name="police_districts",
        file="police_districts.csv",
        columns={"district": "INTEGER", "name": "VARCHAR"},
        key="district",
        key_range=(1, 25),
        description="Current Chicago Police Department districts",
    ),
}


@dataclass
class SeedResult:
    name: str
    table: str
    rows: int
    checksum: str
    loaded: bool  # False when skipped because the file was unchanged


@dataclass
class _Parsed:
    rows: list[list[object]] = field(default_factory=list)
    checksum: str = ""


def _cast(value: str, typ: str, where: str) -> object:
    if typ == "INTEGER":
        try:
            return int(value)
        except ValueError as exc:
            raise SeedError(f"{where}: {value!r} is not an integer") from exc
    return value


def parse_seed(seed: Seed, seeds_dir: Path | str = SEEDS_DIR) -> _Parsed:
    """Read and validate a seed file; raise ``SeedError`` on any problem."""
    path = Path(seeds_dir) / seed.file
    if not path.exists():
        raise SeedError(f"{seed.name}: seed file {path} not found")
    raw = path.read_bytes()
    reader = csv.DictReader(raw.decode("utf-8-sig").splitlines())
    header = reader.fieldnames or []
    if header != list(seed.columns):
        raise SeedError(f"{seed.name}: header {header} != expected {list(seed.columns)}")

    parsed = _Parsed(checksum=hashlib.sha256(raw).hexdigest())
    seen: set[object] = set()
    for lineno, rec in enumerate(reader, start=2):
        where = f"{seed.file}:{lineno}"
        row = []
        for col, typ in seed.columns.items():
            value = (rec.get(col) or "").strip()
            if not value:
                raise SeedError(f"{where}: empty {col}")
            row.append(_cast(value, typ, f"{where} {col}"))
        key = row[list(seed.columns).index(seed.key)]
        if key in seen:
            raise SeedError(f"{where}: duplicate {seed.key} {key!r}")
        if seed.key_range and not (seed.key_range[0] <= key <= seed.key_range[1]):
            raise SeedError(f"{where}: {seed.key} {key} outside {seed.key_range}")
        seen.add(key)
        parsed.rows.append(row)

    if seed.expected_rows is not None and len(parsed.rows) != seed.expected_rows:
        raise SeedError(
            f"{seed.name}: {len(parsed.rows)} rows, expected {seed.expected_rows}"
        )
    return parsed


def _ensure_log(store: BronzeStore) -> None:
    store.con.execute("CREATE SCHEMA IF NOT EXISTS reference")
    store.con.execute(
        "CREATE TABLE IF NOT EXISTS reference._seed_log "
        "(seed VARCHAR, loaded_at TIMESTAMP, rows BIGINT, checksum VARCHAR)"
    )


def _last_checksum(store: BronzeStore, name: str) -> str | None:
    row = store.con.execute(
        "SELECT checksum FROM reference._seed_log WHERE seed = ? "
        "ORDER BY loaded_at DESC, rowid DESC LIMIT 1",
        [name],
    ).fetchone()
    return row[0] if row else None


def load_seed(
    store: BronzeStore, seed: Seed, seeds_dir: Path | str = SEEDS_DIR, force: bool = False
) -> SeedResult:
    parsed = parse_seed(seed, seeds_dir)  # validate before touching the database
    _ensure_log(store)
    con = store.con
    table_exists = con.execute(
        "SELECT count(*) FROM information_schema.tables "
        "WHERE table_schema = 'reference' AND table_name = ?",
        [seed.name],
    ).fetchone()[0]
    if not force and table_exists and _last_checksum(store, seed.name) == parsed.checksum:
        return SeedResult(seed.name, seed.table, len(parsed.rows), parsed.checksum, False)

    cols = ", ".join(f"{c} {t}" for c, t in seed.columns.items())
    con.execute("BEGIN TRANSACTION")
    try:
        con.execute(f"CREATE OR REPLACE TABLE {seed.table} ({cols}, PRIMARY KEY ({seed.key}))")
        placeholders = ", ".join("?" for _ in seed.columns)
        con.executemany(f"INSERT INTO {seed.table} VALUES ({placeholders})", parsed.rows)
        con.execute(
            "INSERT INTO reference._seed_log VALUES (?, ?, ?, ?)",
            [
                seed.name,
                datetime.now(timezone.utc).replace(tzinfo=None),
                len(parsed.rows),
                parsed.checksum,
            ],
        )
        con.execute("COMMIT")
    except Exception:
        con.execute("ROLLBACK")
        raise
    log.info(
        "Loaded %s: %d rows",
        seed.table,
        len(parsed.rows),
        extra={"seed": seed.name, "rows": len(parsed.rows), "checksum": parsed.checksum[:12]},
    )
    return SeedResult(seed.name, seed.table, len(parsed.rows), parsed.checksum, True)


def load_all(
    store: BronzeStore, seeds_dir: Path | str = SEEDS_DIR, force: bool = False
) -> list[SeedResult]:
    """Validate every seed first, then load them; nothing is loaded if any seed is bad."""
    for seed in SEEDS.values():
        parse_seed(seed, seeds_dir)
    return [load_seed(store, seed, seeds_dir, force) for seed in SEEDS.values()]
