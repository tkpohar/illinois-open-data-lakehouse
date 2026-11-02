"""Bronze layer: raw records landed in DuckDB with ingestion metadata."""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from lakehouse.logging_setup import get_run_id

DEFAULT_DB = Path("data/lakehouse.duckdb")

log = logging.getLogger(__name__)


class BronzeStore:
    """Append-only raw storage. Each record is kept as JSON plus lineage columns.

    Lineage columns: ``record_key``, ``ingested_at`` and ``run_id`` (the ingestion run
    that landed the row, matching ``bronze._ingestion_log.run_id``).
    """

    def __init__(self, db_path: Path | str = DEFAULT_DB) -> None:
        if str(db_path) != ":memory:":
            Path(db_path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(str(db_path))
        self.con.execute("CREATE SCHEMA IF NOT EXISTS bronze")
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze._ingestion_log (
                source        VARCHAR,
                run_started   TIMESTAMP,
                run_finished  TIMESTAMP,
                records       BIGINT,
                watermark     VARCHAR,
                run_id        VARCHAR
            )
            """
        )
        # Migrate databases created before run IDs existed.
        self.con.execute(
            "ALTER TABLE bronze._ingestion_log ADD COLUMN IF NOT EXISTS run_id VARCHAR"
        )

    def _ensure_table(self, source: str) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS bronze.{source} (
                record_key   VARCHAR,
                payload      JSON,
                ingested_at  TIMESTAMP,
                run_id       VARCHAR
            )
            """
        )
        self.con.execute(f"ALTER TABLE bronze.{source} ADD COLUMN IF NOT EXISTS run_id VARCHAR")

    def write(
        self,
        source: str,
        records: Iterable[dict[str, Any]],
        primary_key: str,
        incremental_column: str | None = None,
        run_id: str | None = None,
    ) -> int:
        """Append records; ``run_id`` defaults to the current logging run context."""
        run_id = run_id or get_run_id()
        self._ensure_table(source)
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        rows, watermark = [], None
        for rec in records:
            rows.append((str(rec.get(primary_key)), json.dumps(rec), started, run_id))
            if incremental_column and rec.get(incremental_column):
                value = rec[incremental_column]
                watermark = value if watermark is None or value > watermark else watermark
        if rows:
            self.con.executemany(
                f"INSERT INTO bronze.{source} (record_key, payload, ingested_at, run_id) "
                "VALUES (?, ?, ?, ?)",
                rows,
            )
        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        self.con.execute(
            "INSERT INTO bronze._ingestion_log "
            "(source, run_started, run_finished, records, watermark, run_id) "
            "VALUES (?, ?, ?, ?, ?, ?)",
            [source, started, finished, len(rows), watermark, run_id],
        )
        log.info(
            "Wrote %d records to bronze.%s",
            len(rows),
            source,
            extra={"source": source, "records": len(rows), "watermark": watermark},
        )
        return len(rows)

    def last_watermark(self, source: str) -> str | None:
        row = self.con.execute(
            "SELECT max(watermark) FROM bronze._ingestion_log WHERE source = ?", [source]
        ).fetchone()
        return row[0] if row else None

    def count(self, source: str) -> int:
        return self.con.execute(f"SELECT count(*) FROM bronze.{source}").fetchone()[0]

    def close(self) -> None:
        self.con.close()
