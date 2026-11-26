"""Row-count anomaly detection: z-scores on daily volumes.

A **series** is a count per calendar day. Two kinds are built in:

- ``ingestion:<source>`` — records ingested per day (``bronze._ingestion_log``);
- ``events:<silver table>`` — rows per *event* day (when crimes occurred, 311 requests
  were created, taxi trips started).

For a day *d*, the detector compares its count *x* with the trailing window of the
``window`` days before *d* (never including *d* itself, so an anomaly can't hide in its
own baseline). Days inside the window with no rows count as 0, because a day without
data is information. ``z = (x - mean) / std``. ``|z| > threshold`` is an anomaly
(``high`` or ``low``).

With ``seasonal=True`` only the same weekday is compared (the last ``window // 7``
Mondays for a Monday, ...), which absorbs weekly cycles. With fewer than
``min_history`` baseline points the result is ``insufficient_history``. A flat baseline
(std 0) flags any change. See ``docs/anomalies.md``.
"""

from __future__ import annotations

import math
from collections.abc import Iterable, Mapping
from dataclasses import asdict, dataclass
from datetime import date, timedelta

import duckdb

DEFAULT_WINDOW = 28
DEFAULT_THRESHOLD = 3.0
DEFAULT_MIN_HISTORY = 7

#: Silver tables and the column that dates their events.
EVENT_COLUMNS: dict[str, str] = {
    "silver.crimes": "occurred_at",
    "silver.service_requests": "created_at",
    "silver.taxi_trips": "trip_start_at",
}


@dataclass
class AnomalyResult:
    series: str
    day: date
    value: int
    status: str  # ok | anomaly | insufficient_history
    mean: float | None = None
    std: float | None = None
    z: float | None = None
    direction: str | None = None  # high | low (anomalies only)
    history_points: int = 0

    def to_dict(self) -> dict:
        d = asdict(self)
        d["day"] = self.day.isoformat()
        if self.z is not None and math.isinf(self.z):
            d["z"] = "inf" if self.z > 0 else "-inf"
        return d


def baseline(
    counts: Mapping[date, int], day: date, window: int = DEFAULT_WINDOW, seasonal: bool = False
) -> list[int]:
    """Counts for the baseline days before ``day`` (missing days within the data's span = 0)."""
    if not counts:
        return []
    first = min(counts)
    if seasonal:
        days = [day - timedelta(weeks=k) for k in range(1, window // 7 + 1)]
    else:
        days = [day - timedelta(days=k) for k in range(1, window + 1)]
    return [counts.get(d, 0) for d in days if d >= first]


def detect(
    counts: Mapping[date, int],
    day: date,
    series: str = "series",
    window: int = DEFAULT_WINDOW,
    threshold: float = DEFAULT_THRESHOLD,
    min_history: int = DEFAULT_MIN_HISTORY,
    seasonal: bool = False,
) -> AnomalyResult:
    value = int(counts.get(day, 0))
    hist = baseline(counts, day, window, seasonal)
    result = AnomalyResult(series, day, value, "ok", history_points=len(hist))
    if len(hist) < min_history:
        result.status = "insufficient_history"
        return result
    mean = sum(hist) / len(hist)
    std = math.sqrt(sum((h - mean) ** 2 for h in hist) / (len(hist) - 1)) if len(hist) > 1 else 0
    result.mean, result.std = round(mean, 3), round(std, 3)
    if std == 0:
        z = 0.0 if value == mean else math.copysign(math.inf, value - mean)
    else:
        z = (value - mean) / std
    result.z = z if math.isinf(z) else round(z, 3)
    if abs(z) > threshold:
        result.status = "anomaly"
        result.direction = "high" if z > 0 else "low"
    return result


def scan(
    counts: Mapping[date, int], series: str = "series", days: int = 1, **kw
) -> list[AnomalyResult]:
    """Evaluate the last ``days`` days of the series (ending at its latest day)."""
    if not counts:
        return []
    last = max(counts)
    return [detect(counts, last - timedelta(days=k), series, **kw) for k in range(days - 1, -1, -1)]


# ---------------------------------------------------------------------------- series

def _exists(con: duckdb.DuckDBPyConnection, table: str) -> bool:
    schema, name = table.split(".")
    return bool(con.execute(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema = ? AND table_name = ?",
        [schema, name]).fetchone()[0])


def series_names(con: duckdb.DuckDBPyConnection, sources: Iterable[str] = ()) -> list[str]:
    """Series that have data in this database."""
    names = []
    if _exists(con, "bronze._ingestion_log"):
        seen = {r[0] for r in con.execute(
            "SELECT DISTINCT source FROM bronze._ingestion_log").fetchall()}
        names += [f"ingestion:{s}" for s in sorted(set(sources) | seen) if s in seen]
    names += [f"events:{t}" for t in EVENT_COLUMNS if _exists(con, t)]
    return names


def load_series(con: duckdb.DuckDBPyConnection, name: str) -> dict[date, int]:
    kind, _, target = name.partition(":")
    if kind == "ingestion":
        sql = ("SELECT CAST(run_started AS DATE), sum(records) FROM bronze._ingestion_log "
               "WHERE source = ? GROUP BY 1")
        params = [target]
    elif kind == "events" and target in EVENT_COLUMNS:
        col = EVENT_COLUMNS[target]
        sql = (f"SELECT CAST({col} AS DATE), count(*) FROM {target} "
               f"WHERE {col} IS NOT NULL GROUP BY 1")
        params = []
    else:
        raise ValueError(f"Unknown series {name!r} (use ingestion:<source> or "
                         f"events:<{'|'.join(EVENT_COLUMNS)}>)")
    return {d: int(n) for d, n in con.execute(sql, params).fetchall()}


def run_checks(
    con: duckdb.DuckDBPyConnection, names: Iterable[str] | None = None, days: int = 1, **kw
) -> list[AnomalyResult]:
    names = list(names) if names else series_names(con)
    return [r for n in names for r in scan(load_series(con, n), n, days, **kw)]
