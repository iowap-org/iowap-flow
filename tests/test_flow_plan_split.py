# tests/test_flow_plan_split.py — T-235: plan-prompt split (systemprompt +
# taskprompt), the submit form + the legacy 1-string compat path (invariants).
# Design: .hermes/pipeline/t235-flow-prompt-split/design.md §1/§2/§4.
# Patterns copied from tests/test_flow_runner.py (MockTransport + fake clock).
import json

import httpx
import pytest

from flow_node.relay_api import RelayApi
from flow_node.runner import (
    PLAN_CAPABILITY,
    PLAN_FORWARD_SLOT,
    PLAN_PROMPT_SPLIT_ENV,
    PLAN_RETRY_FEEDBACK,
    PLAN_SYSTEM_PROMPT,
    PLAN_TASK_PROMPT_TEMPLATE,
    PLAN_TASK_REF,
    POLL_INTERVAL_SECONDS,
    FlowError,
    _caps_snapshot,
    build_plan_prompt_parts,
    plan_prompt_split_enabled,
    run,
)

BASE = "http://relay.test"
TOKEN = "/tmp/flow-test-token"  # does not exist → no Authorization header
ORIGIN_TASK = "task-origin-1"
ORIGIN_STAGE = "stage-origin-1"
NODE = "flow-runner-01"

TASK_TEXT = "Create 3 product images"
CAPS_SNAPSHOT = "- agent.ai (available: True)"

VALID_CAPS = {
    "agent.ai": {"available": True},
    "tool.x": {"available": True},
    "tool.y": {"available": True},
}

PLAN = {
    "version": 1,
    "name": "demo",
    "tasks": [
        {"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []},
        {"id": "b", "capability": "tool.y", "payload": {}, "depends_on": ["a"]},
    ],
    "summary": "s",
}


# ---------------------------------------------------------------------------
# Test transport: no real server, faked TaskView sequences
# ---------------------------------------------------------------------------

def _patch_http(monkeypatch, handler):
    """Redirect httpx.request + httpx.get (module level) to the handler —
    RelayApi and runner call both variants."""
    def patched_request(method, url, **kwargs):
        req = httpx.Request(method, url, headers=kwargs.get("headers") or {},
                            content=kwargs.get("content"))
        resp = handler(req)
        resp._request = req
        return resp

    def patched_get(url, headers=None, timeout=None, **kwargs):
        req = httpx.Request("GET", url, headers=headers or {})
        resp = handler(req)
        resp._request = req
        return resp

    monkeypatch.setattr(httpx, "request", patched_request)
    monkeypatch.setattr(httpx, "get", patched_get)


def _caps_response(caps):
    return httpx.Response(200, json={"capabilities": caps})


def _task_view(task_id, status, result=None, error=None):
    stage = {"status": status}
    if result is not None:
        stage["result"] = result
    if error is not None:
        stage["error"] = error
    return httpx.Response(200, json={"task_id": task_id, "stages": [stage], "status": status})


def _submit_response(task_id):
    return httpx.Response(200, json={"task_id": task_id, "stage_id": f"stage-{task_id}"})


def _ok():
    return httpx.Response(200, json={"ok": True})


def _fake_clock(monkeypatch, step=POLL_INTERVAL_SECONDS):
    """monotonic clock that advances with time.sleep (no real waiting).
    Records all sleeps in state['sleeps'] for backoff assertions."""
    state = {"elapsed": 0.0, "sleeps": []}
    def fake_sleep(seconds):
        state["elapsed"] += seconds
        state["sleeps"].append(seconds)
    monkeypatch.setattr("time.sleep", fake_sleep)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    return state


# ---------------------------------------------------------------------------
# T-235 split constants + pure builders (T1-T4) + env gate (T9)
# ---------------------------------------------------------------------------

def test_split_templates_are_frozen():
    """T-256: the split constants with the sharpened 4-step contract form."""
    # Arrange/Assert: static text checks (nothing to set up)
    assert "You are the planner of an IOWAP task cluster." in PLAN_SYSTEM_PROMPT
    assert "Step 1 - DECOMPOSE" in PLAN_SYSTEM_PROMPT
    assert "Step 2 - MAP" in PLAN_SYSTEM_PROMPT and "Judge by the\nDESCRIPTION" in PLAN_SYSTEM_PROMPT
    assert "Step 3 - COMPOSE" in PLAN_SYSTEM_PROMPT
    assert "Step 4 - ANSWER" in PLAN_SYSTEM_PROMPT
    # T-254-live drift guards (verbatim evidence: fox/dragon prompt-copy case)
    assert "Never copy them into your plan" in PLAN_SYSTEM_PROMPT
    assert "must derive verbatim from the user task" in PLAN_SYSTEM_PROMPT
    assert "Plan minimal: exactly the requested artifacts, no extras." in PLAN_SYSTEM_PROMPT
    assert "the first character of\nyour reply is `{`" in PLAN_SYSTEM_PROMPT
    assert '{"version": 1, "name": "...", "tasks": [' in PLAN_SYSTEM_PROMPT
    assert "Data passing:" in PLAN_SYSTEM_PROMPT
    assert "{task}" not in PLAN_SYSTEM_PROMPT
    assert "{capabilities_snapshot}" not in PLAN_SYSTEM_PROMPT
    assert PLAN_TASK_PROMPT_TEMPLATE.startswith("Aufgabe:\n{task}\n")
    assert "Verfügbare Capabilities (Live-Snapshot, von flow.run beim Planungs-Submit" in PLAN_TASK_PROMPT_TEMPLATE
    assert PLAN_TASK_PROMPT_TEMPLATE.endswith("{capabilities_snapshot}")
    # exactly the two placeholders
    assert PLAN_TASK_PROMPT_TEMPLATE.count("{") == 2
    assert PLAN_TASK_PROMPT_TEMPLATE.count("}") == 2


def test_parts_builder_forms():
    """build_plan_prompt_parts embeds task + caps in the TASK part; the system
    part stays static (design §1.5)."""
    # Arrange
    # (constants above)
    # Act
    system, task = build_plan_prompt_parts(TASK_TEXT, CAPS_SNAPSHOT)
    # Assert
    assert system == PLAN_SYSTEM_PROMPT
    assert "{task}" not in system and "{capabilities_snapshot}" not in system
    assert task != system
    assert TASK_TEXT in task
    assert CAPS_SNAPSHOT in task
    assert CAPS_SNAPSHOT not in system
    assert TASK_TEXT not in system


def test_feedback_goes_to_taskprompt_only():
    """D9 feedback lands in the TASK part; the SYSTEM part byte-stable (§1.3)."""
    # Arrange
    fb = "no plan JSON object"
    # Act
    system_no_fb, task_no_fb = build_plan_prompt_parts(TASK_TEXT, CAPS_SNAPSHOT)
    system, task = build_plan_prompt_parts(TASK_TEXT, CAPS_SNAPSHOT, fb)
    # Assert
    assert system == system_no_fb
    assert task.endswith(PLAN_RETRY_FEEDBACK.replace("{feedback}", fb))
    assert "The last plan attempt was invalid. Error:\n" + fb + "\nFix this error and answer again ONLY with the plan JSON." in task
    assert "The last plan attempt" not in task_no_fb
    assert "The last plan attempt" not in system


def test_replace_not_format_literal_braces():
    """The substitution is .replace() — literal braces in the task text pass
    through (str.format would break or reformat them, design §1.2)."""
    # Arrange
    tricky = 'json {"k": 1, "payload": {"x": "y"}} must stay'
    # Act
    _, task = build_plan_prompt_parts(tricky, CAPS_SNAPSHOT)
    # Assert
    assert 'json {"k": 1, "payload": {"x": "y"}} must stay' in task


def test_flag_parse_matrix(monkeypatch):
    """The env parse is frozen: the truthy/falsey sets + the loud bogus
    value (design §2.2)."""
    # Arrange / Act / Assert per case (monkeypatch.delenv/setenv):
    monkeypatch.delenv(PLAN_PROMPT_SPLIT_ENV, raising=False)
    assert plan_prompt_split_enabled() is False
    for raw in ("1", "true", "yes", "on", "TRUE", "On "):
        monkeypatch.setenv(PLAN_PROMPT_SPLIT_ENV, raw)
        assert plan_prompt_split_enabled() is True
    for raw in ("0", "false", "no", "off", ""):
        monkeypatch.setenv(PLAN_PROMPT_SPLIT_ENV, raw)
        assert plan_prompt_split_enabled() is False
    monkeypatch.setenv(PLAN_PROMPT_SPLIT_ENV, "bogus")
    with pytest.raises(FlowError, match="FLOW_PLAN_PROMPT_SPLIT"):
        plan_prompt_split_enabled()


# ---------------------------------------------------------------------------
# run() — MockTransport scenarios (split ON: T5/T6; legacy invariants: T7/T8)
# ---------------------------------------------------------------------------

def test_split_submit_payload_shape(monkeypatch):
    """T-254: split-mode payload = EXACTLY {instruction, task, capabilities,
    forward_slot} — the specialist is not an agent, so the capability list
    ships as its own array field; systemprompt/taskprompt are gone.
    Idempotency/name as frozen (§2.1)."""
    # Arrange
    monkeypatch.setenv(PLAN_PROMPT_SPLIT_ENV, "1")
    calls = []
    submits = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        calls.append((request.method, url))
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits.append(body)
            if body["capability"] == PLAN_CAPABILITY:
                return _submit_response("plan-child-1")
            return _submit_response(f"child-{body['name']}")
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "plan-child-1":
                return _task_view("plan-child-1", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={"done": task_id})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    # Act
    result = run(api, {"task": TASK_TEXT}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)
    # Assert
    plan_posts = [c for c in calls if c[1] == "/relay/v2/scheduler/task-simple"]
    assert plan_posts, "planning submit happened"
    assert result["status"] == "completed"
    assert len(submits) == 3  # _plan + fan-out a, b
    assert submits[0]["capability"] == PLAN_CAPABILITY
    assert set(submits[0]["payload"]) == {
        "instruction", "task", "capabilities", "forward_slot"}
    assert "systemprompt" not in submits[0]["payload"]
    assert "taskprompt" not in submits[0]["payload"]
    assert submits[0]["payload"]["forward_slot"] == PLAN_FORWARD_SLOT
    assert submits[0]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}"
    assert submits[0]["name"] == "flow:_plan"
    assert PLAN_SYSTEM_PROMPT == submits[0]["payload"]["instruction"]
    assert TASK_TEXT in submits[0]["payload"]["task"]
    # caps text (task field) AND the structued array field ship BOTH:
    caps_text = _caps_snapshot(VALID_CAPS)
    assert caps_text in submits[0]["payload"]["task"]
    caps_arr = submits[0]["payload"]["capabilities"]
    assert isinstance(caps_arr, list) and caps_arr, "live list as array field"
    assert {e["name"] for e in caps_arr} == set(VALID_CAPS)
    assert all(set(e) == {"name", "description", "input_schema", "available"}
               for e in caps_arr)


def test_split_submit_d9_feedback_in_taskprompt(monkeypatch):
    """D9 under the split (T-254 form): attempt-2 task carries the feedback;
    instruction is byte-identical across attempts; capabilities ship both."""
    # Arrange
    _fake_clock(monkeypatch)
    monkeypatch.setenv(PLAN_PROMPT_SPLIT_ENV, "1")
    submits = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits.append(body)
            return _submit_response(f"plan-{len(submits)}")
        if url.startswith("/relay/v2/scheduler/tasks/plan-"):
            n = int(url.rsplit("plan-", 1)[1].split("/")[0])
            if n == 1:
                # no JSON object with tasks
                return _task_view(f"plan-{n}", "completed", result={"result": "no plan here"})
            # 2nd attempt: formally JSON, but empty tasks array → PlanError
            return _task_view(f"plan-{n}", "completed",
                             result={"result": json.dumps({"version": 1, "tasks": []})})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    # Act / Assert: both attempts invalid → the flow fails after the retry (D9)
    with pytest.raises(Exception, match="plan phase failed"):
        run(api, {"task": "test"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert len(submits) == 2  # exactly 1 retry (D9)
    assert submits[0]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}"
    assert submits[1]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}-2"
    assert submits[0]["name"] == "flow:_plan"
    assert submits[1]["name"] == "flow:_plan-2"
    # the retry task carries the validation feedback; the instruction
    # is byte-identical across attempts
    assert "invalid" in submits[1]["payload"]["task"]
    assert "no plan JSON object" in submits[1]["payload"]["task"]
    assert submits[1]["payload"]["instruction"] == submits[0]["payload"]["instruction"] == PLAN_SYSTEM_PROMPT
    assert "systemprompt" not in submits[1]["payload"]
    assert "taskprompt" not in submits[1]["payload"]
    assert isinstance(submits[1]["payload"]["capabilities"], list)


def test_default_is_legacy_1string(monkeypatch):
    """Invariant: without FLOW_PLAN_PROMPT_SPLIT the 1-string form is used —
    the frozen legacy contract (plan C1 fallback; design §2.2)."""
    # Arrange: NO setenv — env unset
    submits = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits.append(body)
            if body["capability"] == PLAN_CAPABILITY:
                return _submit_response("plan-child-1")
            return _submit_response(f"child-{body['name']}")
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "plan-child-1":
                return _task_view("plan-child-1", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={"done": task_id})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    # Act
    result = run(api, {"task": TASK_TEXT}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)
    # Assert
    assert result["status"] == "completed"
    assert set(submits[0]["payload"]) == {"task", "forward_slot"}
    assert TASK_TEXT in submits[0]["payload"]["task"]
    assert _caps_snapshot(VALID_CAPS) in submits[0]["payload"]["task"]
    assert submits[0]["payload"]["forward_slot"] == PLAN_FORWARD_SLOT
    assert submits[0]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}"
    assert submits[0]["name"] == "flow:_plan"


def test_flag_off_env_legacy(monkeypatch):
    """Invariant: FLOW_PLAN_PROMPT_SPLIT="0" keeps the 1-string legacy form —
    same frozen contract as the unset env (§2.2)."""
    # Arrange: flag OFF
    monkeypatch.setenv(PLAN_PROMPT_SPLIT_ENV, "0")
    submits = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits.append(body)
            if body["capability"] == PLAN_CAPABILITY:
                return _submit_response("plan-child-1")
            return _submit_response(f"child-{body['name']}")
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "plan-child-1":
                return _task_view("plan-child-1", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={"done": task_id})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    # Act
    result = run(api, {"task": TASK_TEXT}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)
    # Assert
    assert result["status"] == "completed"
    assert set(submits[0]["payload"]) == {"task", "forward_slot"}
    assert TASK_TEXT in submits[0]["payload"]["task"]
    assert _caps_snapshot(VALID_CAPS) in submits[0]["payload"]["task"]
    assert submits[0]["payload"]["forward_slot"] == PLAN_FORWARD_SLOT
    assert submits[0]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}"
    assert submits[0]["name"] == "flow:_plan"