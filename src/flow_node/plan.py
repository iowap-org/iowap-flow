"""Plan data model for flow.run (T-172). A plan is a DAG of task steps.

Frozen contract: §2.2 (plan JSON) in .hermes/plans/flow-run-mvp.md.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any


class PlanError(ValueError):
    """Plan is structurally invalid (schema, duplicates, cycles, unreachability)."""


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
        """Builds the id → PlanTask index (§2.2: ids are unique)."""
        object.__setattr__(self, "by_id", {t.id: t for t in self.tasks})

    def roots(self) -> list[PlanTask]:
        """All plan tasks without depends_on (DAG sources, §2.2)."""
        return [t for t in self.tasks if not t.depends_on]


def parse_plan(raw: dict[str, Any]) -> Plan:
    """Validates schema + references + cycles + reachability. Raises PlanError."""
    if not isinstance(raw, dict):
        raise PlanError("plan must be an object")
    if raw.get("version") != 1:
        raise PlanError("plan.version must be 1")
    tasks_raw = raw.get("tasks")
    if not isinstance(tasks_raw, list) or not tasks_raw:
        raise PlanError("plan.no tasks — flow.run does not execute empty plans")
    if any(not isinstance(t, dict) for t in tasks_raw):
        raise PlanError("plan.tasks must be objects")

    ids = [t.get("id") for t in tasks_raw]
    if any(not isinstance(i, str) or not i for i in ids):
        raise PlanError("plan.task id must be a non-empty string")
    if len(set(ids)) != len(ids):
        raise PlanError("plan.duplicate task ids")

    tasks: list[PlanTask] = []
    for t in tasks_raw:
        cap = t.get("capability")
        if not isinstance(cap, str) or not cap:
            raise PlanError(f"plan.task {t['id']!r}: missing capability")
        payload = t.get("payload") or {}
        if not isinstance(payload, dict):
            raise PlanError(f"plan.task {t['id']!r}: payload must be an object")
        deps = t.get("depends_on") or []
        if not isinstance(deps, list):
            raise PlanError(f"plan.task {t['id']!r}: depends_on must be a list")
        unknown = [d for d in deps if d not in ids]
        if unknown:
            raise PlanError(f"plan.task {t['id']!r}: unknown depends_on {unknown}")
        tasks.append(PlanTask(id=t["id"], capability=cap, payload=payload, depends_on=list(deps)))

    plan = Plan(name=str(raw.get("name") or "flow"), summary=str(raw.get("summary") or ""), tasks=tasks)
    _assert_acyclic_and_reachable(plan)
    return plan


def _assert_acyclic_and_reachable(plan: Plan) -> None:
    """Kahn topology: cycles → PlanError; unreachable nodes → PlanError."""
    indeg = {t.id: len(t.depends_on) for t in plan.tasks}
    children: dict[str, list[str]] = {t.id: [] for t in plan.tasks}
    for t in plan.tasks:
        for d in t.depends_on:
            children[d].append(t.id)
    frontier = [tid for tid, d in indeg.items() if d == 0]
    if not frontier:
        raise PlanError("plan.cycle — no root tasks (all tasks have depends_on)")
    seen: set[str] = set()
    while frontier:
        node = frontier.pop()
        seen.add(node)
        for c in children[node]:
            indeg[c] -= 1
            if indeg[c] == 0:
                frontier.append(c)
    unreachable = set(indeg) - seen
    if unreachable:
        raise PlanError(f"plan.unreachable tasks (cycle or dangling edge): {sorted(unreachable)}")