"""T-254: the plan submit talks instruction/task/capabilities (not
systemprompt/taskprompt) — Ronny's contract: the specialist is NOT an
agent, it cannot fetch the capability list, so the runner ships it as a
separate array field ({name, description, input_schema}); the workflow
JSON-schema rides server-side as a const binding rule.

- Split submit key set: instruction, task, capabilities, forward_slot
  (systemprompt/taskprompt are GONE).
- _caps_array: sorted {name, description(+result paths), input_schema,
  available} matching the model.specialist capabilities contract.
- Legacy 1-string submit untouched (env unset).
"""

import os

from flow_node.runner import (
    PLAN_SYSTEM_PROMPT,
    _caps_array,
    build_plan_prompt_parts,
    plan_prompt_split_enabled,
)


def test_split_submit_carries_instruction_task_capabilities(monkeypatch):
    monkeypatch.setenv("FLOW_PLAN_PROMPT_SPLIT", "1")
    instr, taskpart = build_plan_prompt_parts(
        "Erzeuge 3 Bilder und 2 Texte als PDF.",
        "- flow.run (available: True)",
        feedback=None,
    )
    assert instr == PLAN_SYSTEM_PROMPT          # frozen text, verbatim
    assert "Erzeuge 3 Bilder" in taskpart       # the original request
    caps = {
        "flow.run": {"available": True,
                     "input_schema": {"fields": {"original_request": {}}},
                     "result_path_hints": ["answer"]},
        "model.specialist": {"available": True,
                             "input_schema": {"fields": {"task": {}}}},
    }
    arr = _caps_array(caps)
    assert [e["name"] for e in arr] == ["flow.run", "model.specialist"]  # sorted
    assert arr[0]["input_schema"] == {"fields": {"original_request": {}}}
    assert "result paths: answer" in arr[0]["description"]               # T-005c hint rides
    assert arr[1]["available"] is True


def test_caps_array_matches_specialist_contract():
    arr = _caps_array({"fm.chat": {"available": False, "description": "chat"}})
    e = arr[0]
    assert set(e) == {"name", "description", "input_schema", "available"}
    assert e["description"] == "chat"
    assert e["available"] is False


def test_split_env_flag_parse_frozen():
    for v in ("1", "true", "yes", "on", "ON"):
        os.environ["FLOW_PLAN_PROMPT_SPLIT"] = v
        assert plan_prompt_split_enabled() is True
    for v in ("0", "false", "no", "off", ""):
        os.environ["FLOW_PLAN_PROMPT_SPLIT"] = v
        assert plan_prompt_split_enabled() is False
    del os.environ["FLOW_PLAN_PROMPT_SPLIT"]