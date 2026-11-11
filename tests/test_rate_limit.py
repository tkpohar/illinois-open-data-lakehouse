import logging
from datetime import datetime, timedelta, timezone
from email.utils import format_datetime

import pytest
import requests

from lakehouse.config import SourceConfig
from lakehouse.ingestion.socrata import RateLimitError, SocrataClient, parse_retry_after

SRC = SourceConfig("s", "example.org", "abcd-1234", primary_key="id")
NOW = datetime(2026, 11, 11, 3, 41, 0, tzinfo=timezone.utc)


class Resp:
    def __init__(self, status=200, data=None, headers=None):
        self.status_code, self._data, self.headers = status, data or [], headers or {}

    def raise_for_status(self):
        if self.status_code >= 400:
            raise requests.HTTPError(f"HTTP {self.status_code}", response=self)

    def json(self):
        return self._data


class ScriptedSession:
    """Returns the scripted responses (or raises scripted exceptions) in order."""

    def __init__(self, *script):
        self.script, self.calls, self.headers = list(script), 0, {}

    def get(self, url, params, timeout):
        self.calls += 1
        item = self.script.pop(0)
        if isinstance(item, Exception):
            raise item
        return item


def client(session, **kw):
    sleeps = []
    c = SocrataClient(session=session, sleep=sleeps.append, backoff_seconds=1.0, **kw)
    return c, sleeps


@pytest.mark.parametrize(
    "value, expected",
    [
        (None, None),
        ("", None),
        ("   ", None),
        ("0", 0.0),
        ("7", 7.0),
        (" 2.5 ", 2.5),
        ("-3", 0.0),
        ("soon", None),
        (format_datetime(NOW + timedelta(seconds=90), usegmt=True), 90.0),
        (format_datetime(NOW - timedelta(seconds=30), usegmt=True), 0.0),
    ],
)
def test_parse_retry_after(value, expected):
    assert parse_retry_after(value, now=NOW) == expected


def test_429_honours_retry_after_seconds():
    rows = [{"id": "1"}]
    session = ScriptedSession(Resp(429, headers={"Retry-After": "7"}), Resp(200, rows))
    c, sleeps = client(session)
    assert list(c.fetch(SRC)) == rows
    assert sleeps == [7.0]
    assert (c.stats.requests, c.stats.rate_limited, c.stats.retries) == (2, 1, 1)
    assert c.stats.waited_s == 7.0


def test_429_honours_retry_after_http_date(monkeypatch):
    when = datetime.now(timezone.utc) + timedelta(seconds=60)
    session = ScriptedSession(
        Resp(429, headers={"Retry-After": format_datetime(when, usegmt=True)}), Resp(200, [])
    )
    c, sleeps = client(session)
    list(c.fetch(SRC))
    assert len(sleeps) == 1 and 55 <= sleeps[0] <= 60


def test_429_without_header_uses_exponential_backoff():
    session = ScriptedSession(Resp(429), Resp(429), Resp(429), Resp(200, []))
    c, sleeps = client(session)
    list(c.fetch(SRC))
    assert sleeps == [1.0, 2.0, 4.0]


def test_rate_limits_do_not_use_up_error_retries():
    # max_retries=2 would fail on a 2nd generic error, but four 429s are fine.
    session = ScriptedSession(*[Resp(429, headers={"Retry-After": "1"})] * 4, Resp(200, []))
    c, _ = client(session, max_retries=2)
    assert list(c.fetch(SRC)) == []
    assert c.stats.rate_limited == 4


def test_gives_up_after_max_rate_limit_retries():
    session = ScriptedSession(*[Resp(429, headers={"Retry-After": "1"})] * 4)
    c, sleeps = client(session, max_rate_limit_retries=3)
    with pytest.raises(RateLimitError, match="after 3 retries") as err:
        list(c.fetch(SRC))
    assert sleeps == [1.0, 1.0, 1.0]
    assert err.value.retry_after == 1.0
    assert err.value.response.status_code == 429
    assert isinstance(err.value, requests.HTTPError)


def test_refuses_to_wait_longer_than_max_retry_after():
    session = ScriptedSession(Resp(429, headers={"Retry-After": "3600"}))
    c, sleeps = client(session, max_retry_after=120)
    with pytest.raises(RateLimitError, match="3600s"):
        list(c.fetch(SRC))
    assert sleeps == []


def test_backoff_wait_is_capped_by_max_retry_after():
    session = ScriptedSession(*[Resp(429)] * 3, Resp(200, []))
    c, sleeps = client(session, max_retry_after=2.5)
    list(c.fetch(SRC))
    assert sleeps == [1.0, 2.0, 2.5]


def test_503_with_retry_after_is_a_rate_limit():
    session = ScriptedSession(Resp(503, headers={"Retry-After": "3"}), Resp(200, []))
    c, sleeps = client(session, max_retries=1)
    list(c.fetch(SRC))
    assert sleeps == [3.0] and c.stats.rate_limited == 1


def test_503_without_retry_after_is_a_normal_error():
    session = ScriptedSession(Resp(503), Resp(503))
    c, sleeps = client(session, max_retries=2)
    with pytest.raises(requests.HTTPError) as err:
        list(c.fetch(SRC))
    assert not isinstance(err.value, RateLimitError)
    assert sleeps == [1.0] and c.stats.rate_limited == 0


def test_generic_errors_still_back_off_and_raise():
    boom = requests.ConnectionError("down")
    session = ScriptedSession(boom, Resp(500), Resp(200, [{"id": "1"}]))
    c, sleeps = client(session, max_retries=3)
    assert list(c.fetch(SRC)) == [{"id": "1"}]
    assert sleeps == [1.0, 2.0]

    session = ScriptedSession(boom, boom)
    c, _ = client(session, max_retries=2)
    with pytest.raises(requests.ConnectionError):
        list(c.fetch(SRC))


def test_rate_limit_logged_with_structured_fields(caplog):
    session = ScriptedSession(Resp(429, headers={"Retry-After": "2"}), Resp(200, []))
    c, _ = client(session)
    with caplog.at_level(logging.WARNING, logger="lakehouse.ingestion.socrata"):
        list(c.fetch(SRC))
    (rec,) = [r for r in caplog.records if r.getMessage().startswith("Rate limited")]
    assert rec.status == 429 and rec.wait_s == 2.0 and rec.retry_after_header is True


def test_rate_limit_mid_pagination_resumes_same_page():
    rows = [{"id": str(i)} for i in range(3)]
    session = ScriptedSession(
        Resp(200, rows[:2]), Resp(429, headers={"Retry-After": "1"}), Resp(200, rows[2:])
    )
    c, _ = client(session, page_size=2)
    assert list(c.fetch(SRC)) == rows
    assert session.calls == 3
