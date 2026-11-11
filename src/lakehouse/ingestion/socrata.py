"""Paginated client for Socrata (SODA) open-data APIs.

Two kinds of failure are retried separately (see ``docs/rate-limits.md``):

- **Rate limits** (HTTP 429, and 503 with ``Retry-After``): wait exactly as long as the
  server's ``Retry-After`` header asks (seconds or an HTTP date), else exponential backoff;
  up to ``max_rate_limit_retries`` times. A wait longer than ``max_retry_after`` is not
  slept through: ``RateLimitError`` is raised instead.
- **Other errors** (connection errors, timeouts, other 4xx/5xx): exponential backoff,
  up to ``max_retries`` attempts.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterator
from dataclasses import dataclass
from datetime import datetime, timezone
from email.utils import parsedate_to_datetime
from typing import Any

import requests

from lakehouse.config import SourceConfig

log = logging.getLogger(__name__)

RATE_LIMIT_STATUSES = frozenset({429, 503})


class RateLimitError(requests.HTTPError):
    """The server kept rate-limiting, or asked for a wait longer than allowed."""

    def __init__(self, message: str, retry_after: float | None = None, response=None):
        super().__init__(message, response=response)
        self.retry_after = retry_after


def parse_retry_after(value: str | None, now: datetime | None = None) -> float | None:
    """Seconds to wait from a ``Retry-After`` header (delta-seconds or HTTP-date).

    Returns None when the header is missing or unparseable; never negative.
    """
    if value is None:
        return None
    value = value.strip()
    if not value:
        return None
    try:
        return max(0.0, float(value))
    except ValueError:
        pass
    try:
        when = parsedate_to_datetime(value)
    except (TypeError, ValueError, IndexError):
        return None
    if when is None:
        return None
    if when.tzinfo is None:
        when = when.replace(tzinfo=timezone.utc)
    now = now or datetime.now(timezone.utc)
    return max(0.0, (when - now).total_seconds())


@dataclass
class RequestStats:
    """Counters for one client, e.g. for the ingestion metrics report."""

    requests: int = 0
    retries: int = 0
    rate_limited: int = 0
    waited_s: float = 0.0


class SocrataClient:
    """Fetch records page by page with retries, rate-limit handling and optional
    incremental filtering."""

    def __init__(
        self,
        page_size: int = 1000,
        max_retries: int = 3,
        backoff_seconds: float = 2.0,
        app_token: str | None = None,
        session: requests.Session | None = None,
        max_rate_limit_retries: int = 5,
        max_retry_after: float = 300.0,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self.page_size = page_size
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.max_rate_limit_retries = max_rate_limit_retries
        self.max_retry_after = max_retry_after
        self._sleep = sleep
        self.stats = RequestStats()
        self.session = session or requests.Session()
        if app_token:
            self.session.headers["X-App-Token"] = app_token

    def _wait(self, seconds: float) -> None:
        self.stats.waited_s += seconds
        self._sleep(seconds)

    def _get(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        failures = 0  # non-rate-limit failures
        rate_limits = 0
        while True:
            self.stats.requests += 1
            try:
                resp = self.session.get(url, params=params, timeout=30)
            except requests.RequestException as exc:
                failures += 1
                self._retry_or_raise(exc, url, failures)
                continue

            status = getattr(resp, "status_code", 200)
            headers = getattr(resp, "headers", None) or {}
            retry_after = parse_retry_after(headers.get("Retry-After"))
            if status == 429 or (status in RATE_LIMIT_STATUSES and retry_after is not None):
                rate_limits += 1
                self.stats.rate_limited += 1
                self._handle_rate_limit(resp, url, status, retry_after, rate_limits)
                continue

            try:
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                failures += 1
                self._retry_or_raise(exc, url, failures)

    def _handle_rate_limit(
        self, resp, url: str, status: int, retry_after: float | None, count: int
    ) -> None:
        if count > self.max_rate_limit_retries:
            raise RateLimitError(
                f"Still rate-limited (HTTP {status}) after {self.max_rate_limit_retries} "
                f"retries: {url}",
                retry_after,
                resp,
            )
        if retry_after is not None and retry_after > self.max_retry_after:
            raise RateLimitError(
                f"Server asked to wait {retry_after:.0f}s (> max_retry_after "
                f"{self.max_retry_after:.0f}s): {url}",
                retry_after,
                resp,
            )
        wait = retry_after if retry_after is not None else (
            self.backoff_seconds * 2 ** (count - 1)
        )
        wait = min(wait, self.max_retry_after)
        self.stats.retries += 1
        log.warning(
            "Rate limited (HTTP %d); waiting %.1fs before retry %d",
            status,
            wait,
            count,
            extra={
                "url": url,
                "status": status,
                "attempt": count,
                "wait_s": wait,
                "retry_after_header": retry_after is not None,
            },
        )
        self._wait(wait)

    def _retry_or_raise(self, exc: Exception, url: str, attempt: int) -> None:
        if attempt >= self.max_retries:
            raise exc
        wait = self.backoff_seconds * 2 ** (attempt - 1)
        self.stats.retries += 1
        log.warning(
            "Request failed (%s); retry %d in %.1fs",
            exc,
            attempt,
            wait,
            extra={"url": url, "attempt": attempt, "wait_s": wait},
        )
        self._wait(wait)

    def fetch(
        self,
        source: SourceConfig,
        since: str | None = None,
        max_records: int | None = None,
    ) -> Iterator[dict[str, Any]]:
        """Yield records, optionally only those updated after `since` (ISO timestamp)."""
        offset, yielded = 0, 0
        order = source.incremental_column or source.primary_key
        while True:
            limit = self.page_size
            if max_records is not None:
                limit = min(limit, max_records - yielded)
                if limit <= 0:
                    return
            params: dict[str, Any] = {"$limit": limit, "$offset": offset, "$order": order}
            if source.uses_system_fields:
                # System fields (:id, :created_at, :updated_at) are omitted unless selected.
                params["$select"] = ":*, *"
            if since and source.incremental_column:
                params["$where"] = f"{source.incremental_column} > '{since}'"

            page = self._get(source.endpoint, params)
            log.debug(
                "Fetched page of %d records",
                len(page),
                extra={"source": source.name, "offset": offset, "page_records": len(page)},
            )
            if not page:
                return
            for record in page:
                yield record
                yielded += 1
            if len(page) < limit:
                return
            offset += len(page)
