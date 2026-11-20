"""Silver transform framework: a registry of named steps with dependencies.

Each ``Transform`` is a named step (``run(store, ctx)``) that depends on other steps.
``plan()`` resolves a selection into a dependency-respecting order (Kahn's algorithm,
alphabetical among steps that are ready at the same time, so plans are deterministic),
and ``run()`` executes it:

- a step whose input does not exist yet (``MissingInputError``, e.g. a source that was
  never ingested) is **skipped**, and so is everything downstream of it;
- a step that raises anything else **fails**; its downstream steps are skipped, other
  branches still run;
- each step's duration, status and result are returned as ``StepResult``.

Selecting a step also selects everything **downstream** of it (a rebuilt
``silver.crimes`` drops its geo columns, so ``crimes_geo`` must run again);
``with_upstream=True`` adds its dependencies too. See ``docs/silver-framework.md``.
"""

from __future__ import annotations

import logging
import time
from collections.abc import Callable, Iterable, Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from lakehouse.silver import crimes, geo, service_requests, taxi_trips
from lakehouse.silver.typing import MissingInputError
from lakehouse.storage.bronze import BronzeStore

log = logging.getLogger(__name__)


@dataclass
class Context:
    """Options shared by all steps of a run."""

    areas_path: Path | str | None = None  # community-area GeoJSON for geo steps
    area_property: str | None = None
    _areas: Any = None

    def areas(self) -> geo.AreaIndex | None:
        if self.areas_path is None:
            return None
        if self._areas is None:
            self._areas = geo.load_areas(self.areas_path, self.area_property)
        return self._areas


@dataclass(frozen=True)
class Transform:
    name: str
    run: Callable[[BronzeStore, Context], Any]
    depends_on: tuple[str, ...] = ()
    description: str = ""


@dataclass
class StepResult:
    name: str
    status: str  # ok | skipped | failed
    duration_s: float = 0.0
    result: Any = None
    reason: str | None = None


class CycleError(ValueError):
    """The dependency graph has a cycle."""


@dataclass
class Registry:
    transforms: dict[str, Transform] = field(default_factory=dict)

    def add(self, transform: Transform) -> Transform:
        if transform.name in self.transforms:
            raise ValueError(f"Transform {transform.name!r} is already registered")
        self.transforms[transform.name] = transform
        return transform

    def validate(self) -> None:
        for t in self.transforms.values():
            unknown = [d for d in t.depends_on if d not in self.transforms]
            if unknown:
                raise ValueError(f"{t.name} depends on unknown transform(s): {unknown}")
        self.plan()  # raises CycleError

    def downstream(self, names: Iterable[str]) -> set[str]:
        out, frontier = set(names), list(names)
        while frontier:
            current = frontier.pop()
            for t in self.transforms.values():
                if current in t.depends_on and t.name not in out:
                    out.add(t.name)
                    frontier.append(t.name)
        return out

    def upstream(self, names: Iterable[str]) -> set[str]:
        out, frontier = set(names), list(names)
        while frontier:
            for dep in self.transforms[frontier.pop()].depends_on:
                if dep not in out:
                    out.add(dep)
                    frontier.append(dep)
        return out

    def plan(
        self,
        select: Iterable[str] | None = None,
        with_downstream: bool = True,
        with_upstream: bool = False,
    ) -> list[str]:
        """Names to run, in dependency order."""
        if select is None:
            chosen = set(self.transforms)
        else:
            chosen = set(select)
            unknown = sorted(chosen - set(self.transforms))
            if unknown:
                raise ValueError(f"Unknown transform(s): {unknown}")
            if with_downstream:
                chosen = self.downstream(chosen)
            if with_upstream:
                chosen = self.upstream(chosen)
        indegree = {
            n: sum(d in chosen for d in self.transforms[n].depends_on) for n in chosen
        }
        ready = sorted(n for n, d in indegree.items() if d == 0)
        order: list[str] = []
        while ready:
            current = ready.pop(0)
            order.append(current)
            for n in sorted(chosen):
                if current in self.transforms[n].depends_on:
                    indegree[n] -= 1
                    if indegree[n] == 0:
                        ready.append(n)
            ready.sort()
        if len(order) != len(chosen):
            raise CycleError(f"Dependency cycle among: {sorted(chosen - set(order))}")
        return order

    def run(
        self,
        store: BronzeStore,
        select: Iterable[str] | None = None,
        ctx: Context | None = None,
        with_downstream: bool = True,
        with_upstream: bool = False,
    ) -> list[StepResult]:
        ctx = ctx or Context()
        order = self.plan(select, with_downstream, with_upstream)
        results: dict[str, StepResult] = {}
        for name in order:
            t = self.transforms[name]
            blocked = [
                d for d in t.depends_on if d in results and results[d].status != "ok"
            ]
            if blocked:
                results[name] = StepResult(
                    name, "skipped", reason=f"upstream not ok: {', '.join(blocked)}"
                )
                log.info("Skipped %s (upstream %s)", name, blocked, extra={"step": name})
                continue
            t0 = time.monotonic()
            try:
                out = t.run(store, ctx)
                status, reason = "ok", None
            except MissingInputError as exc:
                out, status, reason = None, "skipped", str(exc)
            except Exception as exc:  # noqa: BLE001 - reported per step
                out, status, reason = None, "failed", f"{type(exc).__name__}: {exc}"
                log.exception("Transform %s failed", name, extra={"step": name})
            results[name] = StepResult(
                name, status, round(time.monotonic() - t0, 3), out, reason
            )
            log.info(
                "Transform %s: %s",
                name,
                status,
                extra={"step": name, "status": status, "duration_s": results[name].duration_s},
            )
        return [results[n] for n in order]


def _geo_step(table: str) -> Callable[[BronzeStore, Context], Any]:
    def run(store: BronzeStore, ctx: Context) -> geo.GeoResult:
        return geo.clean_table(store.con, table, ctx.areas())

    return run


def _seeds(store: BronzeStore, ctx: Context):
    from lakehouse.reference import load_all  # local import: avoids a cycle at import time

    return load_all(store)


def default_registry() -> Registry:
    reg = Registry()
    reg.add(Transform("reference_seeds", _seeds, description="Load seeds/ into reference.*"))
    builds: Mapping[str, Callable] = {
        "crimes": crimes.build,
        "service_requests": service_requests.build,
        "taxi_trips": taxi_trips.build,
    }
    for name, build in builds.items():
        reg.add(
            Transform(name, lambda store, ctx, b=build: b(store), description=f"silver.{name}")
        )
        reg.add(
            Transform(
                f"{name}_geo",
                _geo_step(f"silver.{name}"),
                depends_on=(name,),
                description=f"coordinate validation / community areas for silver.{name}",
            )
        )
    reg.validate()
    return reg


REGISTRY = default_registry()
