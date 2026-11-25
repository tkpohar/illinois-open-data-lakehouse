"""Load and validate source configuration."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import yaml

DEFAULT_CONFIG = Path(__file__).resolve().parents[2] / "config" / "sources.yaml"


DEFAULT_INGESTION_WARN_HOURS = 36.0
DEFAULT_INGESTION_ERROR_HOURS = 72.0


@dataclass(frozen=True)
class FreshnessPolicy:
    """Thresholds (hours) for a source. Data freshness is only checked when configured."""

    ingestion_warn_hours: float = DEFAULT_INGESTION_WARN_HOURS
    ingestion_error_hours: float = DEFAULT_INGESTION_ERROR_HOURS
    data_warn_hours: float | None = None
    data_error_hours: float | None = None

    def __post_init__(self) -> None:
        if self.ingestion_warn_hours > self.ingestion_error_hours:
            raise ValueError("ingestion_warn_hours must not exceed ingestion_error_hours")
        data = (self.data_warn_hours, self.data_error_hours)
        if (data[0] is None) != (data[1] is None):
            raise ValueError("set both data_warn_hours and data_error_hours, or neither")
        if data[0] is not None and data[0] > data[1]:
            raise ValueError("data_warn_hours must not exceed data_error_hours")

    @property
    def checks_data(self) -> bool:
        return self.data_warn_hours is not None


@dataclass(frozen=True)
class SourceConfig:
    name: str
    domain: str
    dataset_id: str
    primary_key: str
    incremental_column: str | None = None
    description: str = ""
    freshness: FreshnessPolicy = FreshnessPolicy()

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
        spec = dict(spec)
        policy = spec.pop("freshness", None) or {}
        unknown = set(policy) - set(FreshnessPolicy.__dataclass_fields__)
        if unknown:
            raise ValueError(f"Source '{name}': unknown freshness keys {sorted(unknown)}")
        try:
            freshness = FreshnessPolicy(**policy)
        except ValueError as exc:
            raise ValueError(f"Source '{name}': {exc}") from exc
        result[name] = SourceConfig(name=name, freshness=freshness, **spec)
    return result
