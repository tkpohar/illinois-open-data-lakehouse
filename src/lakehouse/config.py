"""Load and validate source configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "sources.yaml"


@dataclass(frozen=True)
class SourceConfig:
    name: str
    domain: str
    dataset_id: str
    primary_key: str
    incremental_column: str | None = None
    description: str = ""

    @property
    def endpoint(self) -> str:
        return f"https://{self.domain}/resource/{self.dataset_id}.json"

    @property
    def uses_system_fields(self) -> bool:
        """True when the key or incremental column is a Socrata system field (``:id``,
        ``:updated_at``, ...). Those are only returned when requested with ``$select``."""
        return any(
            col is not None and col.startswith(":")
            for col in (self.primary_key, self.incremental_column)
        )


def load_sources(path: Path | str = DEFAULT_CONFIG) -> dict[str, SourceConfig]:
    raw = yaml.safe_load(Path(path).read_text()) or {}
    sources = raw.get("sources")
    if not isinstance(sources, dict) or not sources:
        raise ValueError(f"No 'sources' mapping found in {path}")

    result: dict[str, SourceConfig] = {}
    for name, spec in sources.items():
        missing = {"domain", "dataset_id", "primary_key"} - set(spec)
        if missing:
            raise ValueError(f"Source '{name}' is missing required keys: {sorted(missing)}")
        result[name] = SourceConfig(name=name, **spec)
    return result
