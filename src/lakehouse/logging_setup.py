"""Structured logging with a run ID attached to every log line of an ingestion run.

Usage::

    configure_logging(json_output=True)
    with run_context() as run_id:
        log.info("Fetched page", extra={"records": 1000})

Every record emitted inside ``run_context`` carries ``run_id``. Fields passed via
``extra=`` are included as top-level keys in JSON output and as ``key=value`` pairs in
text output.
"""

from __future__ import annotations

import contextvars
import json
import logging
import sys
import uuid
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import datetime, timezone
from typing import IO, Any

_RUN_ID: contextvars.ContextVar[str | None] = contextvars.ContextVar("run_id", default=None)

# Attributes every LogRecord has; anything else came from `extra=`.
_STANDARD_ATTRS = set(
    logging.LogRecord("", 0, "", 0, "", None, None).__dict__
) | {"message", "asctime", "run_id", "taskName"}

_HANDLER_MARKER = "_lakehouse_handler"


def new_run_id() -> str:
    """Return a sortable, human-readable run ID: ``YYYYMMDDTHHMMSSZ-<8 hex>``."""
    stamp = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    return f"{stamp}-{uuid.uuid4().hex[:8]}"


def get_run_id() -> str | None:
    """The run ID of the current context, or ``None`` outside a run."""
    return _RUN_ID.get()


@contextmanager
def run_context(run_id: str | None = None) -> Iterator[str]:
    """Bind a run ID (new one if not given) for the duration of the block."""
    rid = run_id or new_run_id()
    token = _RUN_ID.set(rid)
    try:
        yield rid
    finally:
        _RUN_ID.reset(token)


class RunIdFilter(logging.Filter):
    """Stamp each record with the current run ID (``-`` outside a run)."""

    def filter(self, record: logging.LogRecord) -> bool:
        if not getattr(record, "run_id", None):
            record.run_id = get_run_id() or "-"
        return True


def _extras(record: logging.LogRecord) -> dict[str, Any]:
    return {k: v for k, v in record.__dict__.items() if k not in _STANDARD_ATTRS}


class JsonFormatter(logging.Formatter):
    """One JSON object per line: ts, level, logger, run_id, message, extras, exc_info."""

    def format(self, record: logging.LogRecord) -> str:
        payload: dict[str, Any] = {
            "ts": datetime.fromtimestamp(record.created, timezone.utc).isoformat(
                timespec="milliseconds"
            ),
            "level": record.levelname,
            "logger": record.name,
            "run_id": getattr(record, "run_id", None) or get_run_id(),
            "message": record.getMessage(),
        }
        payload.update(_extras(record))
        if record.exc_info:
            payload["exc_info"] = self.formatException(record.exc_info)
        return json.dumps(payload, default=str)


class TextFormatter(logging.Formatter):
    """Human-readable lines with the run ID and ``key=value`` extras."""

    def __init__(self) -> None:
        super().__init__("%(asctime)s %(levelname)s [%(run_id)s] %(name)s: %(message)s")

    def format(self, record: logging.LogRecord) -> str:
        if not hasattr(record, "run_id"):
            record.run_id = get_run_id() or "-"
        line = super().format(record)
        extras = _extras(record)
        if extras:
            line += " " + " ".join(f"{k}={v}" for k, v in extras.items())
        return line


def configure_logging(
    level: int | str = logging.INFO,
    json_output: bool = False,
    stream: IO[str] | None = None,
) -> logging.Handler:
    """Install a single lakehouse handler on the root logger.

    Safe to call repeatedly: a previously installed lakehouse handler is replaced,
    other handlers are left alone.
    """
    root = logging.getLogger()
    for h in list(root.handlers):
        if getattr(h, _HANDLER_MARKER, False):
            root.removeHandler(h)
    handler = logging.StreamHandler(stream or sys.stderr)
    setattr(handler, _HANDLER_MARKER, True)
    handler.addFilter(RunIdFilter())
    handler.setFormatter(JsonFormatter() if json_output else TextFormatter())
    root.addHandler(handler)
    root.setLevel(level)
    return handler
