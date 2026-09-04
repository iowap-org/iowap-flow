# tests/test_flow_plan.py — Task 1 (Plan-Datenmodell + Parser), Stubs Phase 2
import json  # noqa: F401  (Phase 3: Plan-JSON-Fixtures)

import pytest

from flow_node.plan import PlanError, parse_plan  # noqa: F401  (Phase 3: Assertions)

VALID = {
    "version": 1,
    "name": "demo",
    "tasks": [
        {"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []},
        {"id": "b", "capability": "tool.y", "payload": {}, "depends_on": ["a"]},
    ],
    "summary": "s",
}


def test_valid_plan_parses():
    assert False  # TODO


@pytest.mark.parametrize("mutate,msg_part", [
    (lambda p: p.update(tasks=[]), "no tasks"),
    (lambda p: p["tasks"][0].update(id="a2", depends_on=["a2"]), "unknown"),   # self-dep unknown
    (lambda p: p["tasks"].append({**p["tasks"][0]}), "duplicate"),             # id doppelt
])
def test_invalid_plans_rejected(mutate, msg_part):
    # Phase 3: import copy; p = copy.deepcopy(VALID); mutate(p)
    # with pytest.raises(PlanError, match=msg_part): parse_plan(p)
    assert False  # TODO


def test_empty_tasks_rejected():
    # §2.2: tasks nicht leer → PlanError "no tasks"
    assert False  # TODO


def test_duplicate_ids_rejected():
    # §2.2: ids eindeutig → PlanError "duplicate"
    assert False  # TODO


def test_unknown_dep_rejected():
    # §2.2: depends_on verweist nur auf existierende ids → PlanError "unknown"
    assert False  # TODO


def test_cycle_rejected():
    # §2.2: zyklenfrei (Kahn) → PlanError "cycle"
    assert False  # TODO


def test_unreachable_task_rejected():
    # §2.2: alle Knoten erreichbar → PlanError "unreachable"
    assert False  # TODO


def test_missing_capability_rejected():
    # §2.2: jede task braucht non-empty capability → PlanError "missing capability"
    assert False  # TODO