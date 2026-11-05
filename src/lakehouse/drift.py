"""Schema-drift detection: compare two schema snapshots of the same source.

Every change gets a severity:

- ``info``: expected noise or harmless growth — a new field, a field seen only as null,
  a type that *narrowed* (a smaller incremental sample with less variety), a field that
  became nullable;
- ``warning``: worth a look — a field missing from the run (Socrata omits null-valued
  keys, so this can be sparsity rather than removal), or a type that *widened*
  (``integer`` → ``number``, ``date`` → ``timestamp``, anything → ``string``);
- ``breaking``: an incompatible type change (``integer`` → ``boolean``, ``timestamp`` →
  ``number``...) that downstream casts will not survive.

See ``docs/schema-drift.md``.
"""

from __future__ import annotations

import json
from dataclasses import asdict, dataclass, field

from lakehouse.schema import NULL, SchemaSnapshot, widen

SEVERITIES = ("info", "warning", "breaking")


def severity_rank(severity: str) -> int:
    return SEVERITIES.index(severity)


@dataclass(frozen=True)
class SchemaChange:
    field: str
    kind: str  # added | removed | type_widened | type_narrowed | type_changed | ...
    severity: str
    old: str | None = None
    new: str | None = None

    def describe(self) -> str:
        if self.kind == "added":
            return f"+ {self.field} ({self.new})"
        if self.kind == "removed":
            return f"- {self.field} ({self.old})"
        return f"~ {self.field}: {self.kind} {self.old} -> {self.new}"


@dataclass
class DriftReport:
    source: str
    old_run_id: str | None
    new_run_id: str | None
    old_fingerprint: str
    new_fingerprint: str
    changes: list[SchemaChange] = field(default_factory=list)

    @property
    def has_drift(self) -> bool:
        return bool(self.changes)

    @property
    def severity(self) -> str | None:
        """Highest severity among the changes, or None when nothing changed."""
        if not self.changes:
            return None
        return max((c.severity for c in self.changes), key=severity_rank)

    def at_least(self, severity: str) -> list[SchemaChange]:
        return [c for c in self.changes if severity_rank(c.severity) >= severity_rank(severity)]

    def to_dict(self) -> dict:
        return {
            "source": self.source,
            "old_run_id": self.old_run_id,
            "new_run_id": self.new_run_id,
            "old_fingerprint": self.old_fingerprint,
            "new_fingerprint": self.new_fingerprint,
            "severity": self.severity,
            "changes": [asdict(c) for c in self.changes],
        }

    def to_json(self) -> str:
        return json.dumps(self.to_dict(), indent=2)

    def format(self) -> str:
        head = (
            f"{self.source}: {self.old_run_id or '-'} -> {self.new_run_id or '-'}  "
            f"({self.old_fingerprint} -> {self.new_fingerprint})"
        )
        if not self.changes:
            return head + "\nno schema drift"
        lines = [head]
        for c in self.changes:
            lines.append(f"  [{c.severity:<8}] {c.describe()}")
        return "\n".join(lines)


def _type_change(name: str, old: str, new: str) -> SchemaChange | None:
    if old == new:
        return None
    if old == NULL or new == NULL:
        # Only nulls on one side: no type evidence there, nothing to compare.
        return SchemaChange(name, "type_from_nulls", "info", old, new)
    merged = widen(old, new)
    if merged == new:
        return SchemaChange(name, "type_widened", "warning", old, new)
    if merged == old:
        return SchemaChange(name, "type_narrowed", "info", old, new)
    return SchemaChange(name, "type_changed", "breaking", old, new)


def compare(old: SchemaSnapshot, new: SchemaSnapshot) -> DriftReport:
    """Changes from ``old`` to ``new``, ordered by field name."""
    report = DriftReport(
        source=new.source,
        old_run_id=old.run_id,
        new_run_id=new.run_id,
        old_fingerprint=old.fingerprint,
        new_fingerprint=new.fingerprint,
    )
    for name in sorted(set(old.fields) | set(new.fields)):
        o, n = old.fields.get(name), new.fields.get(name)
        if o is None:
            report.changes.append(SchemaChange(name, "added", "info", None, n.inferred_type))
            continue
        if n is None:
            report.changes.append(SchemaChange(name, "removed", "warning", o.inferred_type, None))
            continue
        change = _type_change(name, o.inferred_type, n.inferred_type)
        if change:
            report.changes.append(change)
        if not o.nullable(old.record_count) and n.nullable(new.record_count):
            report.changes.append(SchemaChange(name, "became_nullable", "info", "no", "yes"))
    return report
