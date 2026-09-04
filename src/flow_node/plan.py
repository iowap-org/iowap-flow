"""Plan-Datenmodell für flow.run (T-172). Ein Plan ist ein DAG von Task-Schritten.

SKELETT (Phase 2, Scaffold): Nur Dataclasses/Typen/Docstrings — Logik in Phase 3.
Frozen Contract: §2.2 (Plan-JSON) in .hermes/plans/flow-run-mvp.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class PlanError(ValueError):
    """Plan ist strukturell ungültig (Schema, Duplikate, Zyklen, Unerreichbarkeit)."""


@dataclass(frozen=True)
class PlanTask:
    id: str
    capability: str
    payload: dict[str, Any]
    depends_on: list[str]


@dataclass(frozen=True)
class Plan:
    name: str
    summary: str
    tasks: list[PlanTask]
    by_id: dict[str, PlanTask] = field(init=False)

    def __post_init__(self):
        """Baut den id → PlanTask-Index (§2.2: ids sind eindeutig)."""
        raise NotImplementedError

    def roots(self) -> list[PlanTask]:
        """Alle Plan-Tasks ohne depends_on (DAG-Quellen, §2.2)."""
        raise NotImplementedError


def parse_plan(raw: dict[str, Any]) -> Plan:
    """Validiert Schema + Referenzen + Zyklen + Erreichbarkeit. Wirft PlanError."""
    raise NotImplementedError


def _assert_acyclic_and_reachable(plan: Plan) -> None:
    """Kahn-Topologie: Zyklen → PlanError; nicht erreichte Knoten → PlanError."""
    raise NotImplementedError