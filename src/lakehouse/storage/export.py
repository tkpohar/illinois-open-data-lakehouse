"""Export bronze tables to Parquet, Hive-partitioned by ingest date.

Layout (``out_dir`` defaults to ``data/export``)::

    <out_dir>/bronze/<source>/ingest_date=YYYY-MM-DD/data_0.parquet
    <out_dir>/bronze_latest/<source>/ingest_date=YYYY-MM-DD/data_0.parquet   # --latest

Each export rewrites the source's directory from the DuckDB table, so it is idempotent and
never leaves stale partitions behind. Columns: ``record_key``, ``payload`` (JSON text),
``ingested_at``, ``run_id``, ``ingest_seq`` and the partition column ``ingest_date``.
See ``docs/parquet-export.md``.
"""

from __future__ import annotations

import logging
import shutil
from dataclasses import dataclass, field
from pathlib import Path

from lakehouse.storage.bronze import LATEST_SUFFIX, BronzeStore

DEFAULT_EXPORT_DIR = Path("data/export")

log = logging.getLogger(__name__)


@dataclass
class ExportResult:
    source: str
    path: Path
    rows: int
    partitions: dict[str, int] = field(default_factory=dict)  # ingest_date -> rows

    @property
    def files(self) -> list[Path]:
        return sorted(self.path.rglob("*.parquet"))


def _sql_str(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


def bronze_tables(store: BronzeStore) -> list[str]:
    """Names of the source tables in the bronze schema (internal ``_`` tables excluded)."""
    rows = store.con.execute(
        "SELECT table_name FROM information_schema.tables "
        "WHERE table_schema = 'bronze' AND table_type = 'BASE TABLE' "
        "AND table_name NOT LIKE '\\_%' ESCAPE '\\' ORDER BY table_name"
    ).fetchall()
    return [r[0] for r in rows]


def export_source(
    store: BronzeStore,
    source: str,
    out_dir: Path | str = DEFAULT_EXPORT_DIR,
    latest_only: bool = False,
) -> ExportResult:
    """Write one bronze table (or its ``__latest`` view) as partitioned Parquet."""
    if source not in bronze_tables(store):
        raise ValueError(f"No bronze table for source '{source}'")
    relation = f"bronze.{source}{LATEST_SUFFIX}" if latest_only else f"bronze.{source}"
    target = Path(out_dir) / ("bronze_latest" if latest_only else "bronze") / source
    if target.exists():
        shutil.rmtree(target)
    target.mkdir(parents=True)

    query = (
        "SELECT record_key, CAST(payload AS VARCHAR) AS payload, ingested_at, run_id, "
        f"ingest_seq, CAST(ingested_at AS DATE) AS ingest_date FROM {relation} "
        "ORDER BY ingested_at, ingest_seq"
    )
    store.con.execute(
        f"COPY ({query}) TO {_sql_str(str(target))} "
        "(FORMAT PARQUET, PARTITION_BY (ingest_date), OVERWRITE_OR_IGNORE)"
    )
    counts = store.con.execute(
        f"SELECT CAST(CAST(ingested_at AS DATE) AS VARCHAR), count(*) FROM {relation} "
        "GROUP BY 1 ORDER BY 1"
    ).fetchall()
    result = ExportResult(
        source=source,
        path=target,
        rows=sum(n for _, n in counts),
        partitions={day: n for day, n in counts},
    )
    log.info(
        "Exported %d rows of %s to %s",
        result.rows,
        relation,
        target,
        extra={"source": source, "rows": result.rows, "partitions": len(result.partitions)},
    )
    return result


def export_all(
    store: BronzeStore, out_dir: Path | str = DEFAULT_EXPORT_DIR, latest_only: bool = False
) -> list[ExportResult]:
    return [export_source(store, s, out_dir, latest_only) for s in bronze_tables(store)]


def read_export(store: BronzeStore, path: Path | str):
    """Query an export back through DuckDB (partition column restored from the path)."""
    glob = str(Path(path) / "**" / "*.parquet")
    return store.con.execute(
        f"SELECT * FROM read_parquet({_sql_str(glob)}, hive_partitioning = true)"
    )
