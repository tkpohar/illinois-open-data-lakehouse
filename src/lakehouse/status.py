"""Ingestion metrics report: what is in bronze, how fresh it is, how runs went.

One ``SourceStatus`` per source (configured sources plus any other bronze table), built
from ``bronze._ingestion_log``, the bronze tables, ``bronze._schema_snapshots`` and
``bronze._schema_drift``. See ``docs/status.md``.
"""

from __future__ import annotations

import json
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone

from lakehouse.storage.bronze import BronzeStore
from lakehouse.storage.export import bronze_tables

RECENT_RUNS = 7


@dataclass
class SourceStatus:
    source: str
    configured: bool = True
    ingested: bool = False
    rows: int = 0
    keys: int = 0
    runs: int = 0
    empty_runs: int = 0
    last_run_id: str | None = None
    last_run_finished: str | None = None
    last_run_records: int | None = None
    last_run_duration_s: float | None = None
    hours_since_last_run: float | None = None
    stale: bool = False
    watermark: str | None = None
    avg_records_recent: float | None = None
    api_requests: int = 0
    api_retries: int = 0
    rate_limited: int = 0
    waited_s: float = 0.0
    schema_fingerprint: str | None = None
    schema_fields: int | None = None
    last_drift_severity: str | None = None
    last_drift_run_id: str | None = None

    @property
    def duplicates(self) -> int:
        return self.rows - self.keys


@dataclass
class StatusReport:
    generated_at: str
    stale_hours: float
    sources: list[SourceStatus] = field(default_factory=list)

    @property
    def totals(self) -> dict[str, int]:
        return {
            "sources": len(self.sources),
            "ingested": sum(s.ingested for s in self.sources),
            "stale": sum(s.stale for s in self.sources),
            "rows": sum(s.rows for s in self.sources),
            "runs": sum(s.runs for s in self.sources),
            "rate_limited": sum(s.rate_limited for s in self.sources),
        }


def _source_status(
    store: BronzeStore, source: str, configured: bool, now: datetime, stale_hours: float
) -> SourceStatus:
    st = SourceStatus(source=source, configured=configured)
    con = store.con
    agg = con.execute(
        "SELECT count(*), count(*) FILTER (WHERE records = 0), "
        "coalesce(sum(api_requests), 0), coalesce(sum(api_retries), 0), "
        "coalesce(sum(rate_limited), 0), coalesce(sum(waited_s), 0), max(watermark) "
        "FROM bronze._ingestion_log WHERE source = ?",
        [source],
    ).fetchone()
    (st.runs, st.empty_runs, st.api_requests, st.api_retries, st.rate_limited,
     waited, st.watermark) = agg
    st.waited_s = round(float(waited), 3)

    if source in bronze_tables(store):
        st.ingested = True
        st.rows, st.keys = con.execute(
            f"SELECT count(*), count(DISTINCT record_key) FROM bronze.{source}"
        ).fetchone()

    last = con.execute(
        "SELECT run_id, run_finished, records, "
        "epoch(run_finished) - epoch(run_started) FROM bronze._ingestion_log "
        "WHERE source = ? ORDER BY run_finished DESC, rowid DESC LIMIT 1",
        [source],
    ).fetchone()
    if last:
        st.last_run_id, finished, st.last_run_records, duration = last
        st.last_run_finished = finished.isoformat(sep=" ", timespec="seconds")
        st.last_run_duration_s = round(float(duration), 3)
        age_h = (now - finished.replace(tzinfo=timezone.utc)).total_seconds() / 3600
        st.hours_since_last_run = round(max(age_h, 0.0), 2)
        st.stale = age_h > stale_hours
        recent = con.execute(
            "SELECT avg(records) FROM (SELECT records FROM bronze._ingestion_log "
            "WHERE source = ? ORDER BY run_finished DESC, rowid DESC LIMIT ?)",
            [source, RECENT_RUNS],
        ).fetchone()[0]
        st.avg_records_recent = round(float(recent), 1)
    elif configured:
        st.stale = True  # configured but never ingested

    snap = store.schema_snapshot(source)
    if snap is not None:
        st.schema_fingerprint, st.schema_fields = snap.fingerprint, len(snap.fields)

    drift = con.execute(
        "SELECT new_run_id, severity FROM bronze._schema_drift WHERE source = ? "
        "ORDER BY detected_at DESC, rowid DESC",
        [source],
    ).fetchall()
    if drift:
        st.last_drift_run_id = drift[0][0]
        same_run = [sev for run, sev in drift if run == st.last_drift_run_id]
        order = ["info", "warning", "breaking"]
        st.last_drift_severity = max(same_run, key=order.index)
    return st


def collect_status(
    store: BronzeStore,
    sources: Mapping[str, object] | Iterable[str] = (),
    stale_hours: float = 36.0,
    now: datetime | None = None,
) -> StatusReport:
    """Status of every configured source, plus bronze tables not in the config."""
    now = now or datetime.now(timezone.utc)
    configured = list(sources)
    extra = [t for t in bronze_tables(store) if t not in configured]
    report = StatusReport(
        generated_at=now.isoformat(timespec="seconds"), stale_hours=stale_hours
    )
    for name in configured:
        report.sources.append(_source_status(store, name, True, now, stale_hours))
    for name in extra:
        report.sources.append(_source_status(store, name, False, now, stale_hours))
    return report


def status_json(report: StatusReport) -> str:
    return json.dumps(
        {
            "generated_at": report.generated_at,
            "stale_hours": report.stale_hours,
            "totals": report.totals,
            "sources": [{**asdict(s), "duplicates": s.duplicates} for s in report.sources],
        },
        indent=2,
    )


def _age(hours: float | None) -> str:
    if hours is None:
        return "never"
    if hours < 1:
        return f"{hours * 60:.0f}m ago"
    if hours < 48:
        return f"{hours:.1f}h ago"
    return f"{hours / 24:.1f}d ago"


def format_status(report: StatusReport) -> str:
    header = (
        f"{'source':<26} {'rows':>9} {'keys':>9} {'runs':>5} {'last run':>12} "
        f"{'last n':>7} {'429s':>5} {'drift':<9} flags"
    )
    lines = [f"lakehouse status  ({report.generated_at})", header, "-" * len(header)]
    for s in report.sources:
        flags = []
        if s.stale:
            flags.append("STALE" if s.runs else "NEVER INGESTED")
        if not s.configured:
            flags.append("not in config")
        if s.duplicates:
            flags.append(f"{s.duplicates} superseded")
        last_n = "-" if s.last_run_records is None else str(s.last_run_records)
        lines.append(
            f"{s.source:<26} {s.rows:>9} {s.keys:>9} {s.runs:>5} "
            f"{_age(s.hours_since_last_run):>12} {last_n:>7} {s.rate_limited:>5} "
            f"{s.last_drift_severity or '-':<9} {', '.join(flags)}".rstrip()
        )
    t = report.totals
    lines.append("-" * len(header))
    lines.append(
        f"{t['ingested']}/{t['sources']} sources ingested, {t['rows']} rows, {t['runs']} runs, "
        f"{t['stale']} stale (> {report.stale_hours:g}h), {t['rate_limited']} rate-limited requests"
    )
    return "\n".join(lines)
