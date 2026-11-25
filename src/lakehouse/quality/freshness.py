"""Freshness checks per source.

Two questions, each with ``warn`` and ``error`` thresholds from the source's
``freshness`` policy in ``config/sources.yaml``:

- **ingestion** — how long since the last ingestion run finished? A pipeline that
  stopped running goes stale here first.
- **data** — how old is the newest record the source has delivered (the watermark, the
  maximum of its ``incremental_column``)? This catches a source that stopped *publishing*
  while our runs keep succeeding (they simply return nothing new). Only checked for
  sources that set ``data_*_hours``; historical datasets do not.

Statuses: ``ok``, ``warn``, ``error``; a source that has never been ingested is
``error``. See ``docs/freshness.md``.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import asdict, dataclass
from datetime import datetime, timezone

from lakehouse.config import SourceConfig
from lakehouse.storage.bronze import BronzeStore

STATUSES = ("ok", "warn", "error")


@dataclass
class FreshnessResult:
    source: str
    check: str  # ingestion | data
    status: str  # ok | warn | error
    age_hours: float | None
    warn_after_hours: float
    error_after_hours: float
    observed_at: str | None  # the timestamp the age was measured from
    reason: str = ""

    def to_dict(self) -> dict:
        return asdict(self)


def parse_timestamp(value: str | None) -> datetime | None:
    """Parse an ISO-8601 watermark (Socrata floating or ``Z``/offset timestamps) to UTC.

    Floating timestamps have no zone; they are treated as UTC, which is accurate to
    within the Chicago UTC offset (5-6 h) — fine for thresholds measured in days.
    """
    if not value:
        return None
    text = value.strip().replace("Z", "+00:00")
    try:
        ts = datetime.fromisoformat(text)
    except ValueError:
        return None
    if ts.tzinfo is None:
        ts = ts.replace(tzinfo=timezone.utc)
    return ts.astimezone(timezone.utc)


def _grade(age: float, warn: float, error: float) -> str:
    if age > error:
        return "error"
    if age > warn:
        return "warn"
    return "ok"


def _age_hours(ts: datetime, now: datetime) -> float:
    return round(max((now - ts).total_seconds() / 3600, 0.0), 2)


def check_source(
    store: BronzeStore, source: SourceConfig, now: datetime | None = None
) -> list[FreshnessResult]:
    now = now or datetime.now(timezone.utc)
    policy = source.freshness
    row = store.con.execute(
        "SELECT max(run_finished), max(watermark) FROM bronze._ingestion_log WHERE source = ?",
        [source.name],
    ).fetchone()
    last_run, watermark = row if row else (None, None)
    results = []

    warn, error = policy.ingestion_warn_hours, policy.ingestion_error_hours
    if last_run is None:
        results.append(FreshnessResult(source.name, "ingestion", "error", None, warn, error,
                                       None, "never ingested"))
    else:
        ts = last_run.replace(tzinfo=timezone.utc)
        age = _age_hours(ts, now)
        results.append(FreshnessResult(source.name, "ingestion", _grade(age, warn, error), age,
                                       warn, error, ts.isoformat(timespec="seconds"),
                                       f"last run {age:.1f} h ago"))

    if policy.checks_data:
        warn, error = policy.data_warn_hours, policy.data_error_hours
        ts = parse_timestamp(watermark)
        if ts is None:
            reason = "no watermark yet" if watermark is None else f"unparseable {watermark!r}"
            results.append(FreshnessResult(source.name, "data", "error", None, warn, error,
                                           watermark, reason))
        else:
            age = _age_hours(ts, now)
            results.append(FreshnessResult(source.name, "data", _grade(age, warn, error), age,
                                           warn, error, ts.isoformat(timespec="seconds"),
                                           f"newest record {age:.1f} h old"))
    return results


def check_all(
    store: BronzeStore, sources: Mapping[str, SourceConfig], now: datetime | None = None
) -> list[FreshnessResult]:
    now = now or datetime.now(timezone.utc)
    return [r for src in sources.values() for r in check_source(store, src, now)]


def worst(results: list[FreshnessResult]) -> str:
    return max((r.status for r in results), key=STATUSES.index, default="ok")
