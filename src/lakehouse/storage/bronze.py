"""Bronze layer: raw records landed in DuckDB with ingestion metadata."""

from __future__ import annotations

import json
from collections.abc import Iterable
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

DEFAULT_DB = Path("data/lakehouse.duckdb")


class BronzeStore:
    """Append-only raw storage. Each record is kept as JSON plus lineage columns."""

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
                watermark     VARCHAR
            )
            """
        )

    def _ensure_table(self, source: str) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS bronze.{source} (
                record_key   VARCHAR,
                payload      JSON,
                ingested_at  TIMESTAMP
            )
            """
        )

    def write(
        self,
        source: str,
        records: Iterable[dict[str, Any]],
        primary_key: str,
        incremental_column: str | None = None,
    ) -> int:
        self._ensure_table(source)
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        rows, watermark = [], None
        for rec in records:
            rows.append((str(rec.get(primary_key)), json.dumps(rec), started))
            if incremental_column and rec.get(incremental_column):
                value = rec[incremental_column]
                watermark = value if watermark is None or value > watermark else watermark
        if rows:
            self.con.executemany(f"INSERT INTO bronze.{source} VALUES (?, ?, ?)", rows)
        finished = datetime.now(timezone.utc).replace(tzinfo=None)
        self.con.execute(
            "INSERT INTO bronze._ingestion_log VALUES (?, ?, ?, ?, ?)",
            [source, started, finished, len(rows), watermark],
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
