"""Self-contained HTML data-quality report built from ``quality.results``.

One file, no JavaScript and no external assets (inline CSS and SVG), so it can be opened
locally, attached to an alert or published as a CI artifact. Every value from the data
is HTML-escaped. See ``docs/dq-report.md``.
"""

from __future__ import annotations

import json
from collections import defaultdict
from datetime import datetime, timezone
from html import escape
from pathlib import Path
from typing import Any

import duckdb

from lakehouse.quality import results as qr

DEFAULT_REPORT = Path("data/reports/dq-report.html")
STATUS_ORDER = ("fail", "warn", "skipped", "pass")

CSS = """
:root{--bg:#fff;--fg:#1d2733;--muted:#5b6b7b;--card:#f4f6f8;--line:#dde3e9;
--pass:#1f7a3f;--warn:#9a6700;--fail:#b42318;--skipped:#5b6b7b}
@media (prefers-color-scheme:dark){:root{--bg:#11161c;--fg:#e6edf3;--muted:#9aa7b4;
--card:#1a222b;--line:#2b3540;--pass:#4ac26b;--warn:#d29922;--fail:#f47067;--skipped:#9aa7b4}}
*{box-sizing:border-box}body{margin:0;background:var(--bg);color:var(--fg);
font:15px/1.5 system-ui,-apple-system,Segoe UI,sans-serif}
main{max-width:1100px;margin:0 auto;padding:24px 16px}h1{font-size:1.6rem;margin:0}
h2{font-size:1.15rem;margin:32px 0 8px}.muted{color:var(--muted)}
.cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(150px,1fr));gap:12px;
margin-top:16px}.card{background:var(--card);border-radius:8px;padding:12px 16px}
.card b{display:block;font-size:1.8rem}.wrap{overflow-x:auto}
table{border-collapse:collapse;width:100%;font-size:.9rem}
th,td{text-align:left;padding:6px 8px;border-bottom:1px solid var(--line);vertical-align:top}
th{color:var(--muted);font-weight:600}td.num{text-align:right;font-variant-numeric:tabular-nums}
.badge{display:inline-block;padding:0 8px;border-radius:10px;font-size:.8rem;font-weight:600;
color:var(--bg)}.pass{background:var(--pass)}.warn{background:var(--warn)}
.fail{background:var(--fail)}.skipped{background:var(--skipped)}
code{font-size:.85em}svg .bar{fill:var(--pass)}svg .low{fill:var(--fail)}
svg text{fill:var(--muted);font-size:10px}
"""


def _badge(status: str) -> str:
    s = escape(status)
    return f'<span class="badge {s}">{s.upper()}</span>'


def _fmt(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:,.2f}".rstrip("0").rstrip(".")
    if isinstance(value, datetime):
        return value.strftime("%Y-%m-%d %H:%M")
    return str(value)


def _latest_details(con: duckdb.DuckDBPyConnection) -> dict[tuple, dict]:
    rows = con.execute(
        """
        SELECT check_type, target, check_name, details FROM (
          SELECT *, row_number() OVER (PARTITION BY check_type, target, check_name
                                       ORDER BY checked_at DESC, rowid DESC) AS rn
          FROM quality.results) WHERE rn = 1
        """
    ).fetchall()
    return {(t, g, n): json.loads(d) if d else {} for t, g, n, d in rows}


def _detail_text(check_type: str, d: dict) -> str:
    if check_type == "expectation":
        if d.get("error"):
            return f"error: {d['error']}"
        sample = ", ".join(d.get("sample") or [])
        text = f"{d.get('failing_rows')} of {d.get('total_rows')} rows"
        if d.get("mostly", 1) < 1:
            text += f" (allowed {1 - d['mostly']:.1%})"
        return text + (f"; e.g. {sample}" if sample else "")
    if check_type == "freshness":
        return (f"{d.get('reason', '')} (warn > {_fmt(d.get('warn_after_hours'))} h, "
                f"error > {_fmt(d.get('error_after_hours'))} h)")
    if check_type == "anomaly":
        return (f"{d.get('value')} rows on {d.get('day')}: z={d.get('z')} vs mean "
                f"{_fmt(d.get('mean'))} ({d.get('direction') or d.get('status')})")
    return ""


def _sparkline(trend: list[dict]) -> str:
    """Bar chart of the overall daily pass rate (oldest left)."""
    by_day: dict = defaultdict(lambda: [0, 0.0])
    for r in trend:
        if r["checks"]:
            by_day[r["day"]][0] += r["checks"]
            by_day[r["day"]][1] += r["checks"] * (r["pass_rate"] or 0)
    days = sorted(by_day)
    if not days:
        return '<p class="muted">No trend yet.</p>'
    w, h, gap = 22, 80, 4
    bars = []
    for i, day in enumerate(days):
        n, passed = by_day[day]
        rate = passed / n
        bh = max(2, round(rate * h))
        cls = "bar" if rate >= 0.95 else "bar low"
        x = i * (w + gap)
        bars.append(
            f'<rect class="{cls}" x="{x}" y="{h - bh}" width="{w}" height="{bh}" rx="2">'
            f"<title>{escape(str(day))}: {rate:.1%} of {n} checks passed</title></rect>"
        )
    width = len(days) * (w + gap)
    labels = (f'<text x="0" y="{h + 14}">{escape(str(days[0]))}</text>'
              f'<text x="{width}" y="{h + 14}" text-anchor="end">{escape(str(days[-1]))}</text>')
    return (f'<svg role="img" aria-label="Daily pass rate" width="{width}" height="{h + 20}" '
            f'viewBox="0 0 {width} {h + 20}">{"".join(bars)}{labels}</svg>')


def render(con: duckdb.DuckDBPyConnection, title: str = "Data-quality report",
           now: datetime | None = None, trend_days: int = 30) -> str:
    now = now or datetime.now(timezone.utc)
    latest = qr.latest(con)
    details = _latest_details(con) if latest else {}
    runs = qr.runs(con, 10)
    trend = qr.pass_rate_by_day(con, trend_days)

    counts = {s: sum(r["status"] == s for r in latest) for s in STATUS_ORDER}
    out = [
        "<!doctype html>",
        '<html lang="en"><head><meta charset="utf-8">',
        '<meta name="viewport" content="width=device-width, initial-scale=1">',
        f"<title>{escape(title)}</title><style>{CSS}</style></head><body><main>",
        f"<h1>{escape(title)}</h1>",
        f'<p class="muted">Generated {now:%Y-%m-%d %H:%M} UTC from <code>quality.results</code>'
        f" · {len(latest)} checks · {len(runs)} recent check runs</p>",
    ]
    if not latest:
        out += ["<p>No stored results yet. Run <code>lakehouse checks run</code> first.</p>",
                "</main></body></html>"]
        return "\n".join(out)

    out.append('<div class="cards">')
    for s in STATUS_ORDER:
        out.append(f'<div class="card"><b>{counts[s]}</b>{_badge(s)}</div>')
    out.append("</div>")

    attention = [r for r in latest if r["status"] in ("fail", "warn")]
    out.append(f"<h2>Needs attention ({len(attention)})</h2>")
    if attention:
        out.append('<div class="wrap"><table><thead><tr><th>Status</th><th>Check</th>'
                   "<th>Target</th><th>Streak</th><th>Last pass</th><th>Details</th>"
                   "</tr></thead><tbody>")
        for r in attention:
            d = details.get((r["check_type"], r["target"], r["check_name"]), {})
            out.append(
                f"<tr><td>{_badge(r['status'])}</td>"
                f"<td>{escape(r['check_type'])}: <code>{escape(r['check_name'])}</code></td>"
                f"<td><code>{escape(r['target'])}</code></td>"
                f"<td class=num>{r['streak']}×</td>"
                f"<td>{escape(_fmt(r['last_pass_at'])) if r['last_pass_at'] else 'never'}</td>"
                f"<td>{escape(_detail_text(r['check_type'], d))}</td></tr>"
            )
        out.append("</tbody></table></div>")
    else:
        out.append('<p class="muted">Everything is passing.</p>')

    out.append("<h2>Daily pass rate</h2>")
    out.append(_sparkline(trend))

    out.append("<h2>By target</h2>")
    by_target: dict = defaultdict(lambda: dict.fromkeys(STATUS_ORDER, 0))
    for r in latest:
        by_target[(r["check_type"], r["target"])][r["status"]] += 1
    out.append('<div class="wrap"><table><thead><tr><th>Type</th><th>Target</th>'
               + "".join(f"<th class=num>{_badge(s)}</th>" for s in STATUS_ORDER)
               + "</tr></thead><tbody>")
    for (ctype, target), c in sorted(by_target.items(),
                                     key=lambda kv: (-kv[1]["fail"], -kv[1]["warn"], kv[0])):
        out.append(f"<tr><td>{escape(ctype)}</td><td><code>{escape(target)}</code></td>"
                   + "".join(f"<td class=num>{c[s]}</td>" for s in STATUS_ORDER) + "</tr>")
    out.append("</tbody></table></div>")

    out.append("<h2>All checks</h2>")
    out.append('<div class="wrap"><table><thead><tr><th>Status</th><th>Type</th><th>Target</th>'
               "<th>Check</th><th class=num>Observed</th><th>Checked</th></tr></thead><tbody>")
    for r in latest:
        out.append(
            f"<tr><td>{_badge(r['status'])}</td><td>{escape(r['check_type'])}</td>"
            f"<td><code>{escape(r['target'])}</code></td>"
            f"<td><code>{escape(r['check_name'])}</code></td>"
            f"<td class=num>{escape(_fmt(r['observed']))}</td>"
            f"<td>{escape(_fmt(r['checked_at']))}</td></tr>"
        )
    out.append("</tbody></table></div>")

    out.append("<h2>Recent check runs</h2>")
    out.append('<div class="wrap"><table><thead><tr><th>Started</th><th>Check run</th>'
               + "".join(f"<th class=num>{_badge(s)}</th>" for s in STATUS_ORDER)
               + "</tr></thead><tbody>")
    for r in runs:
        vals = {"fail": r["failed"], "warn": r["warned"], "skipped": r["skipped"],
                "pass": r["passed"]}
        out.append(f"<tr><td>{escape(_fmt(r['started_at']))}</td>"
                   f"<td><code>{escape(r['check_run_id'])}</code></td>"
                   + "".join(f"<td class=num>{vals[s]}</td>" for s in STATUS_ORDER) + "</tr>")
    out.append("</tbody></table></div>")
    out.append("</main></body></html>")
    return "\n".join(out)


def write(con: duckdb.DuckDBPyConnection, path: Path | str = DEFAULT_REPORT, **kw) -> Path:
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(render(con, **kw), encoding="utf-8")
    return path
