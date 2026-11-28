import json
from datetime import date, datetime, timedelta, timezone
from html.parser import HTMLParser
from pathlib import Path

import pytest

from lakehouse import cli
from lakehouse.quality import report
from lakehouse.quality import results as qr
from lakehouse.quality.anomalies import AnomalyResult
from lakehouse.quality.expectations import in_range, not_null
from lakehouse.quality.freshness import FreshnessResult
from lakehouse.quality.suites import run_suite
from lakehouse.silver import crimes
from lakehouse.storage.bronze import BronzeStore

FIX = Path(__file__).parent / "fixtures"
NOW = datetime(2026, 11, 28, 3, 45, tzinfo=timezone.utc)
VOID = {"meta", "br", "img", "input", "link", "hr"}


class TagChecker(HTMLParser):
    """Checks that non-void tags are balanced and collects text."""

    def __init__(self):
        super().__init__()
        self.stack, self.errors, self.text, self.tags = [], [], [], []

    def handle_starttag(self, tag, attrs):
        self.tags.append(tag)
        if tag not in VOID:
            self.stack.append(tag)

    def handle_endtag(self, tag):
        if not self.stack or self.stack[-1] != tag:
            self.errors.append(f"unexpected </{tag}> (open: {self.stack[-3:]})")
        else:
            self.stack.pop()

    def handle_data(self, data):
        self.text.append(data)


def parse(html):
    p = TagChecker()
    p.feed(html)
    assert not p.errors and not p.stack, (p.errors, p.stack)
    return p


@pytest.fixture
def store():
    s = BronzeStore(":memory:")
    s.write("chicago_crimes", json.loads((FIX / "chicago_crimes_sample.json").read_text()),
            primary_key="id")
    crimes.build(s)
    yield s
    s.close()


def populate(store, days=3):
    for i in range(days):
        failing = i == days - 1
        suite = run_suite(store.con, "silver.crimes", [
            not_null("silver.crimes", "crime_id"),
            in_range("silver.crimes", "community_area", 1, 10 if failing else 77),
        ])
        fresh = FreshnessResult("chicago_311_requests", "data", "warn" if failing else "ok",
                                60.0, 48, 120, None, "newest record 60.0 h old")
        anom = AnomalyResult("events:silver.crimes", date(2026, 11, 20 + i), 5, "ok", 5.0, 0.0, 0)
        run = qr.record(store.con, qr.from_suites([suite]) + qr.from_freshness([fresh])
                        + qr.from_anomalies([anom]))
        ts = (NOW - timedelta(days=days - 1 - i)).replace(tzinfo=None)
        store.con.execute("UPDATE quality.results SET checked_at = ? WHERE check_run_id = ?",
                          [ts, run.check_run_id])
        store.con.execute("UPDATE quality.check_runs SET started_at = ? WHERE check_run_id = ?",
                          [ts, run.check_run_id])


def test_report_structure_and_content(store):
    populate(store)
    html = report.render(store.con, now=NOW)
    p = parse(html)
    text = " ".join(p.text)
    assert html.startswith("<!doctype html>") and "<script" not in html
    assert "Generated 2026-11-28 03:45 UTC" in text and "4 checks" in text
    for heading in ("Needs attention (2)", "Daily pass rate", "By target", "All checks",
                    "Recent check runs"):
        assert heading in text
    # the failing expectation: streak, last pass, sample values
    assert "in_range:community_area" in text and "1×" in text
    assert "2026-11-27 03:45" in text  # last pass
    assert "3 of 5 rows; e.g. 19, 26, 44" in text  # areas 8,26,44,19,6 vs max 10
    assert "newest record 60.0 h old (warn > 48 h, error > 120 h)" in text
    assert "svg" in p.tags and "rect" in p.tags


def test_cards_count_current_statuses(store):
    populate(store)
    html = report.render(store.con, now=NOW)
    for status, n in (("fail", 1), ("warn", 1), ("pass", 2), ("skipped", 0)):
        assert f'<div class="card"><b>{n}</b><span class="badge {status}">' in html


def test_all_passing_message(store):
    populate(store, days=1)
    store.con.execute("UPDATE quality.results SET status = 'pass'")
    html = report.render(store.con, now=NOW)
    parse(html)
    assert "Everything is passing." in html and "Needs attention (0)" in html


def test_empty_report():
    s = BronzeStore(":memory:")
    html = report.render(s.con, now=NOW)
    parse(html)
    assert "No stored results yet" in html
    s.close()


def test_values_from_data_are_escaped(store):
    evil = '<script>alert("x")</script>'
    qr.record(store.con, [qr.ResultRow("expectation", evil, f"in_range:{evil}", "fail", 1.0,
                                       {"failing_rows": 1, "total_rows": 2, "sample": [evil]})])
    html = report.render(store.con, now=NOW)
    parse(html)
    assert "<script" not in html
    assert "&lt;script&gt;alert(&quot;x&quot;)&lt;/script&gt;" in html


def test_sparkline_marks_low_days(store):
    populate(store)
    svg = report._sparkline(qr.pass_rate_by_day(store.con, 30))
    assert svg.count("<rect") == 3
    assert svg.count('class="bar low"') == 1  # the last day had failures
    assert report._sparkline([]) == '<p class="muted">No trend yet.</p>'


def test_detail_text_variants():
    assert report._detail_text("expectation", {"error": "boom"}) == "error: boom"
    assert report._detail_text("expectation", {"failing_rows": 3, "total_rows": 100,
                                               "mostly": 0.99, "sample": []}) == \
        "3 of 100 rows (allowed 1.0%)"
    assert "z=4.2 vs mean 10 (high)" in report._detail_text(
        "anomaly", {"value": 50, "day": "2026-11-27", "z": 4.2, "mean": 10.0,
                    "direction": "high"})
    assert report._detail_text("other", {}) == ""


def test_write_and_cli(tmp_path, store, capsys):
    populate(store, days=1)
    out = report.write(store.con, tmp_path / "nested" / "r.html", now=NOW)
    assert out.exists() and out.read_text().startswith("<!doctype html>")

    db = tmp_path / "lh.duckdb"
    s = BronzeStore(db)
    s.close()
    target = tmp_path / "cli.html"
    assert cli.main(["checks", "report", "--db", str(db), "--out", str(target)]) == 0
    assert f"wrote {target}" in capsys.readouterr().out
    assert "No stored results yet" in target.read_text()
