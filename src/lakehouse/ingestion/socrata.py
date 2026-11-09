"""Paginated client for Socrata (SODA) open-data APIs."""

from __future__ import annotations

import logging
import time
from collections.abc import Iterator
from typing import Any

import requests

from lakehouse.config import SourceConfig

log = logging.getLogger(__name__)


class SocrataClient:
    """Fetch records page by page with retries and optional incremental filtering."""

    def __init__(
        self,
        page_size: int = 1000,
        max_retries: int = 3,
        backoff_seconds: float = 2.0,
        app_token: str | None = None,
        session: requests.Session | None = None,
    ) -> None:
        self.page_size = page_size
        self.max_retries = max_retries
        self.backoff_seconds = backoff_seconds
        self.session = session or requests.Session()
        if app_token:
            self.session.headers["X-App-Token"] = app_token

    def _get(self, url: str, params: dict[str, Any]) -> list[dict[str, Any]]:
        for attempt in range(1, self.max_retries + 1):
            try:
                resp = self.session.get(url, params=params, timeout=30)
                resp.raise_for_status()
                return resp.json()
            except requests.RequestException as exc:
                if attempt == self.max_retries:
                    raise
                wait = self.backoff_seconds * 2 ** (attempt - 1)
                log.warning(
                    "Request failed (%s); retry %d in %.1fs",
                    exc,
                    attempt,
                    wait,
                    extra={"url": url, "attempt": attempt, "wait_s": wait},
                )
                time.sleep(wait)
        return []  # unreachable

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
