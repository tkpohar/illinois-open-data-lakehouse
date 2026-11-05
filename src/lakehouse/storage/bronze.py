"""Bronze layer: raw records landed in DuckDB with ingestion metadata.

Bronze is append-only: every version of a record that the source ever returned is kept.
For each source a view ``bronze.<source>__latest`` exposes exactly one row per key, the
latest version (see ``docs/bronze-dedupe.md``). ``BronzeStore.compact`` optionally
rewrites the table to drop superseded versions.

Each non-empty write also records the inferred source schema of that run in
``bronze._schema_snapshots`` (see ``docs/schema-snapshots.md``) and compares it with the
previous snapshot; any drift is logged and stored in ``bronze._schema_drift`` (see
``docs/schema-drift.md``).
"""

from __future__ import annotations

import json
import logging
from collections.abc import Iterable
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import duckdb

from lakehouse.drift import DriftReport, compare
from lakehouse.logging_setup import get_run_id
from lakehouse.schema import SchemaInferrer, SchemaSnapshot

DEFAULT_DB = Path("data/lakehouse.duckdb")
LATEST_SUFFIX = "__latest"

log = logging.getLogger(__name__)


@dataclass(frozen=True)
class DedupeStats:
    """Row counts for a bronze table: raw rows, distinct keys, superseded versions."""

    source: str
    rows: int
    keys: int

    @property
    def duplicates(self) -> int:
        return self.rows - self.keys


def _sql_literal(value: str) -> str:
    return "'" + value.replace("'", "''") + "'"


class BronzeStore:
    """Append-only raw storage. Each record is kept as JSON plus lineage columns.

    Lineage columns: ``record_key``, ``ingested_at``, ``run_id`` (the ingestion run that
    landed the row, matching ``bronze._ingestion_log.run_id``) and ``ingest_seq`` (the
    record's position within its write batch, used to order same-batch duplicates).
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
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze._sources (
                source              VARCHAR PRIMARY KEY,
                primary_key         VARCHAR,
                incremental_column  VARCHAR
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze._schema_snapshots (
                source        VARCHAR,
                run_id        VARCHAR,
                captured_at   TIMESTAMP,
                record_count  BIGINT,
                field_count   INTEGER,
                fingerprint   VARCHAR,
                schema        JSON
            )
            """
        )
        self.con.execute(
            """
            CREATE TABLE IF NOT EXISTS bronze._schema_drift (
                source        VARCHAR,
                old_run_id    VARCHAR,
                new_run_id    VARCHAR,
                detected_at   TIMESTAMP,
                field         VARCHAR,
                kind          VARCHAR,
                severity      VARCHAR,
                old_type      VARCHAR,
                new_type      VARCHAR
            )
            """
        )
        self.last_drift: DriftReport | None = None

    def _ensure_table(self, source: str) -> None:
        self.con.execute(
            f"""
            CREATE TABLE IF NOT EXISTS bronze.{source} (
                record_key   VARCHAR,
                payload      JSON,
                ingested_at  TIMESTAMP,
                run_id       VARCHAR,
                ingest_seq   BIGINT
            )
            """
        )
        # Migrate tables created by earlier versions.
        self.con.execute(f"ALTER TABLE bronze.{source} ADD COLUMN IF NOT EXISTS run_id VARCHAR")
        self.con.execute(
            f"ALTER TABLE bronze.{source} ADD COLUMN IF NOT EXISTS ingest_seq BIGINT"
        )

    def _register_source(
        self, source: str, primary_key: str, incremental_column: str | None
    ) -> None:
        self.con.execute(
            "INSERT OR REPLACE INTO bronze._sources VALUES (?, ?, ?)",
            [source, primary_key, incremental_column],
        )

    def _incremental_column(self, source: str) -> str | None:
        row = self.con.execute(
            "SELECT incremental_column FROM bronze._sources WHERE source = ?", [source]
        ).fetchone()
        return row[0] if row else None

    def _latest_order_by(self, source: str) -> str:
        """Newest version first: incremental column, then ingest time, then batch order."""
        terms = []
        inc = self._incremental_column(source)
        if inc:
            terms.append(f"payload->>{_sql_literal(inc)} DESC NULLS LAST")
        terms += ["ingested_at DESC NULLS LAST", "ingest_seq DESC NULLS LAST"]
        return ", ".join(terms)

    def _latest_select(self, source: str) -> str:
        return (
            f"SELECT * FROM bronze.{source} "
            f"QUALIFY row_number() OVER ("
            f"PARTITION BY record_key ORDER BY {self._latest_order_by(source)}) = 1"
        )

    def _refresh_latest_view(self, source: str) -> None:
        self.con.execute(
            f"CREATE OR REPLACE VIEW bronze.{source}{LATEST_SUFFIX} AS "
            + self._latest_select(source)
        )

    def write(
        self,
        source: str,
        records: Iterable[dict[str, Any]],
        primary_key: str,
        incremental_column: str | None = None,
        run_id: str | None = None,
    ) -> int:
        """Append records; ``run_id`` defaults to the current logging run context.

        Also (re)creates the ``bronze.<source>__latest`` view and, when at least one
        record was written, stores the run's inferred schema snapshot, compares it with
        the previous one and records any drift (afterwards available as ``last_drift``).
        """
        run_id = run_id or get_run_id()
        self._ensure_table(source)
        self._register_source(source, primary_key, incremental_column)
        started = datetime.now(timezone.utc).replace(tzinfo=None)
        rows, watermark = [], None
        inferrer = SchemaInferrer(source)
        for seq, rec in enumerate(records):
            inferrer.observe(rec)
            rows.append((str(rec.get(primary_key)), json.dumps(rec), started, run_id, seq))
            if incremental_column and rec.get(incremental_column):
                value = rec[incremental_column]
                watermark = value if watermark is None or value > watermark else watermark
        if rows:
            self.con.executemany(
                f"INSERT INTO bronze.{source} "
                "(record_key, payload, ingested_at, run_id, ingest_seq) VALUES (?, ?, ?, ?, ?)",
                rows,
            )
        self._refresh_latest_view(source)
        self.last_drift = None
        if rows:
            snap = inferrer.snapshot(run_id)
            previous = self.schema_snapshot(source)
            self._save_snapshot(snap, started)
            if previous is not None:
                self.last_drift = compare(previous, snap)
                self._save_drift(self.last_drift, started)
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

    def _save_snapshot(self, snap: SchemaSnapshot, captured_at: datetime) -> None:
        self.con.execute(
            "INSERT INTO bronze._schema_snapshots VALUES (?, ?, ?, ?, ?, ?, ?)",
            [
                snap.source,
                snap.run_id,
                captured_at,
                snap.record_count,
                len(snap.fields),
                snap.fingerprint,
                snap.to_json(),
            ],
        )
        log.info(
            "Captured schema of %s: %d fields",
            snap.source,
            len(snap.fields),
            extra={"source": snap.source, "fingerprint": snap.fingerprint},
        )

    def _save_drift(self, report: DriftReport, detected_at: datetime) -> None:
        if not report.has_drift:
            return
        self.con.executemany(
            "INSERT INTO bronze._schema_drift VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)",
            [
                [
                    report.source,
                    report.old_run_id,
                    report.new_run_id,
                    detected_at,
                    c.field,
                    c.kind,
                    c.severity,
                    c.old,
                    c.new,
                ]
                for c in report.changes
            ],
        )
        level = {"info": logging.INFO, "warning": logging.WARNING}.get(
            report.severity, logging.ERROR
        )
        log.log(
            level,
            "Schema drift in %s (%s): %s",
            report.source,
            report.severity,
            "; ".join(c.describe() for c in report.changes),
            extra={
                "source": report.source,
                "drift_severity": report.severity,
                "drift_changes": len(report.changes),
                "previous_run_id": report.old_run_id,
            },
        )

    def drift_history(self, source: str) -> list[dict[str, Any]]:
        """Stored drift events for a source, oldest first."""
        cur = self.con.execute(
            "SELECT * FROM bronze._schema_drift WHERE source = ? ORDER BY detected_at, rowid",
            [source],
        )
        cols = [d[0] for d in cur.description]
        return [dict(zip(cols, row, strict=True)) for row in cur.fetchall()]

    def schema_history(self, source: str) -> list[SchemaSnapshot]:
        """All schema snapshots for a source, oldest first."""
        rows = self.con.execute(
            "SELECT record_count, schema, run_id, captured_at FROM bronze._schema_snapshots "
            "WHERE source = ? ORDER BY captured_at, rowid",
            [source],
        ).fetchall()
        return [
            SchemaSnapshot.from_row(source, n, schema, run_id, str(ts))
            for n, schema, run_id, ts in rows
        ]

    def schema_snapshot(self, source: str, run_id: str | None = None) -> SchemaSnapshot | None:
        """The snapshot of a given run, or the most recent one when ``run_id`` is None."""
        history = self.schema_history(source)
        if run_id is not None:
            history = [s for s in history if s.run_id == run_id]
        return history[-1] if history else None

    def latest(self, source: str) -> list[dict[str, Any]]:
        """The latest version of every record, as parsed payload dicts (ordered by key)."""
        rows = self.con.execute(
            f"SELECT payload FROM bronze.{source}{LATEST_SUFFIX} ORDER BY record_key"
        ).fetchall()
        return [json.loads(r[0]) for r in rows]

    def dedupe_stats(self, source: str) -> DedupeStats:
        rows, keys = self.con.execute(
            f"SELECT count(*), count(DISTINCT record_key) FROM bronze.{source}"
        ).fetchone()
        return DedupeStats(source=source, rows=rows, keys=keys)

    def compact(self, source: str) -> int:
        """Physically drop superseded versions, keeping the latest row per key.

        Returns the number of rows removed. Runs in a single transaction.
        """
        before = self.count(source)
        self._ensure_table(source)
        self.con.execute("BEGIN TRANSACTION")
        try:
            self.con.execute(
                "CREATE OR REPLACE TEMP TABLE _bronze_compact AS " + self._latest_select(source)
            )
            self.con.execute(f"DELETE FROM bronze.{source}")
            self.con.execute(
                f"INSERT INTO bronze.{source} BY NAME SELECT * FROM _bronze_compact"
            )
            self.con.execute("DROP TABLE _bronze_compact")
            self.con.execute("COMMIT")
        except Exception:
            self.con.execute("ROLLBACK")
            raise
        self._refresh_latest_view(source)
        removed = before - self.count(source)
        log.info(
            "Compacted bronze.%s: removed %d superseded rows",
            source,
            removed,
            extra={"source": source, "removed": removed},
        )
        return removed

    def last_watermark(self, source: str) -> str | None:
        row = self.con.execute(
            "SELECT max(watermark) FROM bronze._ingestion_log WHERE source = ?", [source]
        ).fetchone()
        return row[0] if row else None

    def count(self, source: str) -> int:
        return self.con.execute(f"SELECT count(*) FROM bronze.{source}").fetchone()[0]

    def close(self) -> None:
        self.con.close()
