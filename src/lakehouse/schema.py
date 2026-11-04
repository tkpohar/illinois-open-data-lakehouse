"""Infer a source's schema from the records of one ingestion run.

Socrata returns most values as JSON strings (``"41.8925"``, ``"2026-01-02T15:40:12.000"``),
so each field gets two types:

- ``json_type``: the JSON type actually seen (``string``, ``integer``, ``number``,
  ``boolean``, ``object``, ``array``);
- ``inferred_type``: the type the values *represent* — strings that all look like integers,
  decimals, booleans, dates or timestamps are refined accordingly.

Mixed types widen (``integer`` + ``number`` → ``number``; anything else → ``string``).
Nulls never decide a type; a field that is only ever null has type ``null``.

Numeric strings with a leading zero (``"018"``) are treated as codes and stay ``string``.
"""

from __future__ import annotations

import hashlib
import json
import re
from collections.abc import Iterable
from dataclasses import asdict, dataclass, field
from typing import Any

_INT_RE = re.compile(r"^[+-]?(0|[1-9]\d*)$")
_NUM_RE = re.compile(r"^[+-]?(0|[1-9]\d*)?\.\d+([eE][+-]?\d+)?$|^[+-]?(0|[1-9]\d*)[eE][+-]?\d+$")
_DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_TS_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}[T ]\d{2}:\d{2}(:\d{2}(\.\d+)?)?(Z|[+-]\d{2}:?\d{2})?$"
)
_BOOL_STRINGS = {"true", "false"}

NULL = "null"


def json_type(value: Any) -> str:
    if value is None:
        return NULL
    if isinstance(value, bool):
        return "boolean"
    if isinstance(value, int):
        return "integer"
    if isinstance(value, float):
        return "number"
    if isinstance(value, dict):
        return "object"
    if isinstance(value, list):
        return "array"
    return "string"


def value_type(value: Any) -> str:
    """The type a single value represents (strings refined by their content)."""
    jt = json_type(value)
    if jt != "string":
        return jt
    s = value.strip()
    if s.lower() in _BOOL_STRINGS:
        return "boolean"
    if _INT_RE.match(s):
        return "integer"
    if _NUM_RE.match(s):
        return "number"
    if _TS_RE.match(s):
        return "timestamp"
    if _DATE_RE.match(s):
        return "date"
    return "string"


def widen(a: str, b: str) -> str:
    """The narrowest type that holds values of both types."""
    if a == b or b == NULL:
        return a
    if a == NULL:
        return b
    if {a, b} == {"integer", "number"}:
        return "number"
    if {a, b} == {"date", "timestamp"}:
        return "timestamp"
    return "string"


@dataclass
class FieldSchema:
    name: str
    json_type: str = NULL
    inferred_type: str = NULL
    present: int = 0  # records that contain the key
    nulls: int = 0  # ... of which the value was null

    def nullable(self, record_count: int) -> bool:
        return self.nulls > 0 or self.present < record_count


@dataclass
class SchemaSnapshot:
    source: str
    record_count: int
    fields: dict[str, FieldSchema] = field(default_factory=dict)
    run_id: str | None = None
    captured_at: str | None = None

    @property
    def fingerprint(self) -> str:
        """Hash of (field name, inferred type) pairs: equal iff the shape is equal."""
        shape = sorted((f.name, f.inferred_type) for f in self.fields.values())
        return hashlib.sha256(json.dumps(shape).encode()).hexdigest()[:16]

    def to_json(self) -> str:
        return json.dumps(
            {
                name: {**asdict(f), "nullable": f.nullable(self.record_count)}
                for name, f in sorted(self.fields.items())
            }
        )

    @classmethod
    def from_row(
        cls,
        source: str,
        record_count: int,
        schema_json: str,
        run_id: str | None = None,
        captured_at: str | None = None,
    ) -> SchemaSnapshot:
        fields = {}
        for name, spec in json.loads(schema_json).items():
            spec = {k: v for k, v in spec.items() if k != "nullable"}
            fields[name] = FieldSchema(**spec)
        return cls(source, record_count, fields, run_id, captured_at)


class SchemaInferrer:
    """Accumulates a schema one record at a time, so it fits in a single streaming pass."""

    def __init__(self, source: str) -> None:
        self.source = source
        self.record_count = 0
        self.fields: dict[str, FieldSchema] = {}

    def observe(self, record: dict[str, Any]) -> None:
        self.record_count += 1
        for name, value in record.items():
            f = self.fields.get(name)
            if f is None:
                f = self.fields[name] = FieldSchema(name)
            f.present += 1
            if value is None:
                f.nulls += 1
                continue
            f.json_type = widen(f.json_type, json_type(value))
            f.inferred_type = widen(f.inferred_type, value_type(value))

    def snapshot(self, run_id: str | None = None) -> SchemaSnapshot:
        return SchemaSnapshot(self.source, self.record_count, dict(self.fields), run_id)


def infer_schema(source: str, records: Iterable[dict[str, Any]]) -> SchemaSnapshot:
    inferrer = SchemaInferrer(source)
    for rec in records:
        inferrer.observe(rec)
    return inferrer.snapshot()


def format_schema(snap: SchemaSnapshot) -> str:
    """A fixed-width table for terminal output."""
    header = f"{'field':<28} {'inferred':<10} {'json':<8} {'nullable':<8} {'present':>8}"
    lines = [
        f"{snap.source}  run={snap.run_id or '-'}  records={snap.record_count}  "
        f"fingerprint={snap.fingerprint}",
        header,
        "-" * len(header),
    ]
    for name, f in sorted(snap.fields.items()):
        lines.append(
            f"{name:<28} {f.inferred_type:<10} {f.json_type:<8} "
            f"{'yes' if f.nullable(snap.record_count) else 'no':<8} {f.present:>8}"
        )
    return "\n".join(lines)
