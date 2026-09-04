# tests/test_flow_plan.py — Task 1 (Plan-Datenmodell + Parser), §2.2-Contract
import copy
import json

import pytest

from flow_node.plan import PlanError, parse_plan

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
    plan = parse_plan(VALID)
    assert plan.name == "demo" and len(plan.tasks) == 2
    assert plan.summary == "s"
    assert plan.by_id["b"].capability == "tool.y"
    assert [t.id for t in plan.roots()] == ["a"]


def test_valid_plan_from_json_string():
    # Plan kommt als Stage-1-Result (DB-JSON) — muss identisch parsen
    plan = parse_plan(json.loads(json.dumps(VALID)))
    assert plan.name == "demo" and len(plan.tasks) == 2


@pytest.mark.parametrize("mutate,msg_part", [
    (lambda p: p.update(tasks=[]), "no tasks"),
    (lambda p: p["tasks"][0].update(id="a2", depends_on=["a2"]), "unknown"),   # self-dep unknown
    (lambda p: p["tasks"].append({**p["tasks"][0]}), "duplicate"),             # id doppelt
])
def test_invalid_plans_rejected(mutate, msg_part):
    p = copy.deepcopy(VALID)
    mutate(p)
    with pytest.raises(PlanError, match=msg_part):
        parse_plan(p)


def test_empty_tasks_rejected():
    p = copy.deepcopy(VALID)
    p["tasks"] = []
    with pytest.raises(PlanError, match="no tasks"):
        parse_plan(p)


def test_duplicate_ids_rejected():
    p = copy.deepcopy(VALID)
    p["tasks"].append({**p["tasks"][0]})  # id "a" doppelt
    with pytest.raises(PlanError, match="duplicate"):
        parse_plan(p)


def test_unknown_dep_rejected():
    p = copy.deepcopy(VALID)
    p["tasks"][1]["depends_on"] = ["zz"]
    with pytest.raises(PlanError, match="unknown"):
        parse_plan(p)


def test_cycle_rejected():
    p = copy.deepcopy(VALID)
    p["tasks"][0]["depends_on"] = ["b"]  # a ↔ b, keine Quelle
    with pytest.raises(PlanError, match="cycle"):
        parse_plan(p)


def test_unreachable_task_rejected():
    p = copy.deepcopy(VALID)
    p["tasks"][0]["depends_on"] = ["b"]  # a↔b-Zyklus neben Root c → a,b unerreichbar
    p["tasks"].append({"id": "c", "capability": "tool.z", "payload": {}, "depends_on": []})
    with pytest.raises(PlanError, match="unreachable"):
        parse_plan(p)


def test_missing_capability_rejected():
    p = copy.deepcopy(VALID)
    del p["tasks"][0]["capability"]
    with pytest.raises(PlanError, match="missing capability"):
        parse_plan(p)


def test_non_dict_task_rejected():
    # Guard: non-dict Task-Items → PlanError statt AttributeError
    p = copy.deepcopy(VALID)
    p["tasks"].append("not-a-dict")
    with pytest.raises(PlanError, match="must be objects"):
        parse_plan(p)


def test_bad_version_rejected():
    p = copy.deepcopy(VALID)
    p["version"] = 2
    with pytest.raises(PlanError, match="version"):
        parse_plan(p)


def test_non_object_plan_rejected():
    with pytest.raises(PlanError, match="must be an object"):
        parse_plan(["not", "a", "dict"])  # type: ignore[arg-type]