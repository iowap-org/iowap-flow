# tests/test_flow_templates.py — T-002: template interpolation (data injection
# between flow tasks). Syntax ${ref.result.path}, resolution at submit time from
# the running aggregate — MockTransport pattern like test_flow_runner.py.
import json

import httpx
import pytest

from flow_node.relay_api import RelayApi
from flow_node.runner import FlowError, _resolve_templates, build_plan_prompt, run

BASE = "http://relay.test"
TOKEN = "/tmp/flow-test-token"  # does not exist → no Authorization header
ORIGIN_TASK = "task-origin-1"
ORIGIN_STAGE = "stage-origin-1"
NODE = "flow-runner-01"

VALID_CAPS = {
    "agent.ai": {"available": True},
    "image.generate.mflux": {"available": True},
    "storage.store": {"available": True},
}


# ---------------------------------------------------------------------------
# Test transport: like test_flow_runner.py — faked TaskView sequences
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


def _fake_clock(monkeypatch):
    """monotonic clock that advances with time.sleep (no real waiting)."""
    state = {"elapsed": 0.0}

    def fake_sleep(seconds):
        state["elapsed"] += seconds

    monkeypatch.setattr("time.sleep", fake_sleep)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    return state


# ---------------------------------------------------------------------------
# _resolve_templates — unit (syntax, type preservation, fail-fast D6)
# ---------------------------------------------------------------------------

def test_exact_match_resolves_to_native_type():
    """A string matching EXACTLY one template → native value, type stays
    preserved (str/int/bool/dict, also multi-level dot path)."""
    aggregate = {"img": {"artifact_id": "art-42", "count": 3,
                         "flag": True, "meta": {"w": 512}}}
    resolved = _resolve_templates(
        {
            "artifact_id": "${img.result.artifact_id}",
            "count": "${img.result.count}",
            "flag": "${img.result.flag}",
            "meta": "${img.result.meta}",
            "width": "${img.result.meta.w}",
        },
        aggregate,
    )
    assert resolved["artifact_id"] == "art-42"   # str
    assert resolved["count"] == 3                # int stays int
    assert resolved["flag"] is True              # bool stays bool
    assert resolved["meta"] == {"w": 512}        # dict stays dict
    assert resolved["width"] == 512              # dot path across 2 levels


def test_embedded_templates_substitute_as_str():
    """Template EMBEDDED in a larger string → all matches substituted as str()
    in the text; dict/list via json.dumps, scalars via str()."""
    aggregate = {"img": {"artifact_id": "art-42", "count": 3,
                         "meta": {"w": 512}, "tags": ["a", "b"]}}
    resolved = _resolve_templates(
        {
            "text": "Image ${img.result.artifact_id} stored",
            "two": "${img.result.artifact_id}/${img.result.count}",
            "obj": "meta=${img.result.meta}",
            "lst": "tags=${img.result.tags}",
        },
        aggregate,
    )
    assert resolved["text"] == "Image art-42 stored"
    assert resolved["two"] == "art-42/3"        # int → str()
    assert resolved["obj"] == 'meta={"w": 512}'  # dict → json.dumps
    assert resolved["lst"] == 'tags=["a", "b"]'  # list → json.dumps


def test_nested_payloads_dict_in_dict_and_lists():
    """Recursion over the payload: dict in dict, lists, mixed elements."""
    aggregate = {"img": {"artifact_id": "art-42", "url": "http://x/y.png"}}
    payload = {
        "outer": {"inner": "${img.result.artifact_id}",
                  "deeper": {"x": "${img.result.url}"}},
        "list": ["${img.result.artifact_id}",
                 {"k": "prefix-${img.result.url}"}, 42],
        "plain": "stays as is",
    }
    assert _resolve_templates(payload, aggregate) == {
        "outer": {"inner": "art-42", "deeper": {"x": "http://x/y.png"}},
        "list": ["art-42", {"k": "prefix-http://x/y.png"}, 42],
        "plain": "stays as is",
    }


def test_missing_ref_in_aggregate_raises_flowerror_with_depends_on_hint():
    """Reference to a task missing from the aggregate (usually: missing from depends_on)
    → FlowError with template, referenced task and depends_on hint."""
    aggregate = {"img": {"artifact_id": "art-42"}}
    with pytest.raises(FlowError) as exc_info:
        _resolve_templates({"artifact_id": "${storage.result.path}"}, aggregate)
    msg = str(exc_info.value)
    assert "${storage.result.path}" in msg   # the template
    assert "storage" in msg                  # the referenced task
    assert "depends_on" in msg               # the hint


def test_unresolvable_path_raises_flowerror():
    """Path not resolvable in the result (missing key or hits a
    non-dict value) → FlowError with template, task and depends_on hint."""
    aggregate = {"img": {"artifact_id": "art-42"}}
    for template in ("${img.result.does.not.exist}", "${img.result.artifact_id.sub}"):
        with pytest.raises(FlowError) as exc_info:
            _resolve_templates({"x": template}, aggregate)
        msg = str(exc_info.value)
        assert template in msg
        assert "img" in msg
        assert "depends_on" in msg


def test_non_template_strings_pass_through():
    """Only ${ref.result.path} is a template — everything else stays literal
    (no expressions, no default values, no filters)."""
    aggregate = {"img": {"artifact_id": "art-42"}}
    payload = {
        "a": "plain text",
        "b": "{no} template",
        "c": "${img.artifact_id}",   # without .result. → no template
        "d": "$ref",
        "e": "${img.result}",        # without path → no template
        "f": 7,
    }
    assert _resolve_templates(payload, aggregate) == payload


# ---------------------------------------------------------------------------
# plan prompt: the planner must know the template syntax (T-002 plan D6)
# ---------------------------------------------------------------------------

def test_plan_prompt_documents_template_syntax():
    """build_plan_prompt teaches the planner the template syntax AND the rule:
    every referenced task MUST be in depends_on."""
    prompt = build_plan_prompt("Create 2 images", "- agent.ai (available: True)")
    assert "${ref.result.path}" in prompt
    assert "depends_on" in prompt


# ---------------------------------------------------------------------------
# run() — integration via the MockTransport (pattern: test_flow_runner.py)
# ---------------------------------------------------------------------------

def test_image_storage_flow_resolves_template_at_submit(monkeypatch):
    """2-task flow image → storage: ${img.result.artifact_id} is resolved at
    SUBMIT time from the aggregate (bare value, native type),
    embedded matches as str(); flow metadata comes last."""
    _fake_clock(monkeypatch)
    submits = {}

    plan = {
        "version": 1,
        "name": "paper",
        "tasks": [
            {"id": "img", "capability": "image.generate.mflux",
             "payload": {"prompt": "newspaper page"}, "depends_on": []},
            {"id": "storage", "capability": "storage.store",
             "payload": {"artifact_id": "${img.result.artifact_id}",
                         "label": "art-${img.result.count}"},
             "depends_on": ["img"]},
        ],
        "summary": "s",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits[body["name"]] = body
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed",
                                  result={"result": json.dumps(plan)})
            if task_id == "flow:paper:img":
                return _task_view(task_id, "completed",
                                  result={"artifact_id": "art-7f3", "count": 2})
            return _task_view(task_id, "completed", result={"stored": True})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "newspaper as PDF"}, BASE, TOKEN,
                 ORIGIN_TASK, ORIGIN_STAGE, NODE)

    # template resolved at submit time — NOT only by the storage handler
    storage = submits["flow:paper:storage"]["payload"]
    assert storage["artifact_id"] == "art-7f3"   # exact → native value
    assert storage["label"] == "art-2"           # embedded → str()
    # flow metadata came LAST — template resolution never went through it
    assert storage["flow"] == {"origin_task_id": ORIGIN_TASK, "task_ref": "storage"}
    assert result["status"] == "completed"
    assert result["result"]["aggregate"] == {
        "img": {"artifact_id": "art-7f3", "count": 2},
        "storage": {"stored": True},
    }


def test_flow_fails_when_referenced_task_not_in_aggregate(monkeypatch):
    """A task references a task that is NOT in depends_on → at submit time it
    is missing from the aggregate → FlowError with the affected task_ref,
    template and depends_on hint; the task is never submitted (D6)."""
    _fake_clock(monkeypatch)
    submitted = []

    plan = {
        "version": 1,
        "name": "broken",
        "tasks": [
            {"id": "img", "capability": "image.generate.mflux",
             "payload": {}, "depends_on": []},
            {"id": "storage", "capability": "storage.store",
             "payload": {"artifact_id": "${img.result.artifact_id}"},
             "depends_on": []},  # BUG: img missing from depends_on
        ],
        "summary": "s",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            if body["name"] == "flow:_plan":
                return _submit_response("flow:_plan")
            submitted.append(body["name"])
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed",
                                  result={"result": json.dumps(plan)})
            # the flow fails at the storage submit BEFORE any poll result
            return _task_view(task_id, "pending")
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    with pytest.raises(FlowError) as exc_info:
        run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    msg = str(exc_info.value)
    assert "storage" in msg                       # affected task_ref
    assert "${img.result.artifact_id}" in msg     # the template
    assert "depends_on" in msg                    # the hint
    assert submitted == ["flow:broken:img"]       # storage never submitted (D6)