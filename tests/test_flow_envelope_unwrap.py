# tests/test_flow_envelope_unwrap.py — T-005c: envelope contract, flow side.
#
# _task_result() unwraps the handler result envelope (design.md T-005a §2.2)
# EXACTLY ONCE: {"status":"completed","result":X} → X. Bare results (legacy
# nodes without an envelope) pass through unchanged (tolerant until the fleet
# migration, D7). Thus result_path_hints are paths relative to the inner
# result: ${img.result.artifact_id} → aggregate["img"]["artifact_id"] — for
# envelope- AND bare-handlers alike.
#
# Trigger incident (T-005a): a live flow used ${t1_image.result.answer} exactly
# per the hints, but stage.result contained the whole shell → dot path one
# level too deep → fail-fast D6.
import json

import httpx
import pytest

from flow_node.relay_api import RelayApi
from flow_node.runner import (
    FlowError,
    _caps_snapshot,
    _resolve_templates,
    _task_result,
    run,
)

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
# _task_result — unwrap exactly once (D7: gate on completed + dict)
# ---------------------------------------------------------------------------

def test_task_result_unwraps_completed_envelope():
    """Envelope shape (§2.2 T-005a) → the INNER result dict lands in the aggregate."""
    view = {
        "stages": [
            {"status": "completed",
             "result": {"status": "completed", "result": {"answer": "42"}, "error": None}},
        ],
    }
    assert _task_result(view) == {"answer": "42"}


def test_task_result_passes_bare_result_through():
    """Legacy node without envelope (bare result dict) → passed through unchanged."""
    view = {"stages": [{"status": "completed", "result": {"artifact_id": "art-7f3"}}]}
    assert _task_result(view) == {"artifact_id": "art-7f3"}


def test_task_result_unwrap_exactly_once():
    """An inner result that ITSELF looks like an envelope is NOT unwrapped a
    second time — unwrap is bound to exactly one spot."""
    inner_envelope = {"status": "completed", "result": {"x": 1}, "error": None}
    view = {
        "stages": [
            {"status": "completed",
             "result": {"status": "completed", "result": inner_envelope, "error": None}},
        ],
    }
    assert _task_result(view) is inner_envelope  # one level, not two


def test_task_result_error_envelope_not_unwrapped():
    """The status:"error" shape is never expected here (_stage_status filters
    failed stages beforehand) — and is still left untouched (only completed+dict
    unwraps, D7)."""
    error_shape = {"status": "error", "result": None, "error": "boom"}
    view = {"stages": [{"status": "completed", "result": error_shape}]}
    assert _task_result(view) is error_shape


def test_task_result_completed_without_dict_result_passes_through():
    """status=completed, but result not a dict (e.g. None/list) → no unwrap
    (the gate requires isinstance(result, dict), D7)."""
    bare = {"status": "completed", "result": None, "error": None}
    view = {"stages": [{"status": "completed", "result": bare}]}
    assert _task_result(view) is bare


def test_task_result_takes_first_stage_with_content():
    """Multiple stages: the first stage result with content wins (existing rule)."""
    view = {
        "stages": [
            {"status": "completed", "result": None},
            {"status": "completed",
             "result": {"status": "completed", "result": {"answer": "ja"}, "error": None}},
        ],
    }
    assert _task_result(view) == {"answer": "ja"}


def test_task_result_returns_none_when_no_stage_result():
    """No stage with a result → None (existing contract)."""
    assert _task_result({"stages": [{"status": "pending"}]}) is None
    assert _task_result({}) is None
    assert _task_result({}) is None


def test_task_result_non_dict_result_types_pass_through():
    """Scalars/lists as stage result ( legacy node) → unchanged."""
    for value in ("plain text", [1, 2], 7, True):
        view = {"stages": [{"status": "completed", "result": value}]}
        assert _task_result(view) == value


# ---------------------------------------------------------------------------
# Hints are relative to the inner result — snapshot + template resolution
# ---------------------------------------------------------------------------

def test_caps_snapshot_shows_relative_hints():
    """T-004 fix: hints are paths relative to the inner result — the snapshot
    renders 'answer', not 'result.answer' (planner builds valid paths)."""
    caps = {"chat.ai": {"available": True, "result_path_hints": ["answer"]}}
    snapshot = _caps_snapshot(caps)
    assert "answer" in snapshot
    assert "result.answer" not in snapshot


def test_template_resolution_end_to_end_with_envelope(monkeypatch):
    """The incident shape, now green: the img child answers WITH the handler
    envelope ({"status":"completed","result":{...}}), ${img.result.artifact_id}
    resolves at submit time against the unwrapped aggregate entry
    (MockTransport pattern like test_flow_templates.py)."""
    state = {"elapsed": 0.0}
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    submits = {}

    plan = {
        "version": 1,
        "name": "envelope-chain",
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
            return httpx.Response(200, json={"capabilities": VALID_CAPS})
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits[body["name"]] = body
            return httpx.Response(200, json={"task_id": body["name"],
                                             "stage_id": f"stage-{body['name']}"})
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return httpx.Response(200, json={
                    "task_id": task_id, "status": "completed",
                    "stages": [{"status": "completed",
                                "result": {"result": json.dumps(plan)}}]})
            if task_id == "flow:envelope-chain:img":
                # envelope-emitting handler (new node generation):
                return httpx.Response(200, json={
                    "task_id": task_id, "status": "completed",
                    "stages": [{"status": "completed", "result": {
                        "status": "completed",
                        "result": {"artifact_id": "art-env1", "count": 2},
                        "error": None}}]})
            return httpx.Response(200, json={
                "task_id": task_id, "status": "completed",
                "stages": [{"status": "completed", "result": {"stored": True}}]})
        return httpx.Response(200, json={"ok": True})

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

    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "newspaper as PDF"}, BASE, TOKEN,
                 ORIGIN_TASK, ORIGIN_STAGE, NODE)

    storage = submits["flow:envelope-chain:storage"]["payload"]
    assert storage["artifact_id"] == "art-env1"   # exact → native value
    assert storage["label"] == "art-2"            # embedded → str()
    assert result["status"] == "completed"
    # Aggregate contains the UNWRAPPED results — identical for envelope- and bare-children
    # (that is the normalization effect of unwrap-once):
    assert result["result"]["aggregate"] == {
        "img": {"artifact_id": "art-env1", "count": 2},
        "storage": {"stored": True},
    }


def test_template_resolution_end_to_end_bare_still_works(monkeypatch):
    """Counter-check: bare handler (legacy node, no shell) — identical chain,
    unchanged behavior (tolerant phase)."""
    state = {"elapsed": 0.0}
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    submits = {}

    plan = {
        "version": 1,
        "name": "bare-chain",
        "tasks": [
            {"id": "img", "capability": "image.generate.mflux",
             "payload": {"prompt": "p"}, "depends_on": []},
            {"id": "storage", "capability": "storage.store",
             "payload": {"artifact_id": "${img.result.artifact_id}"},
             "depends_on": ["img"]},
        ],
        "summary": "s",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return httpx.Response(200, json={"capabilities": VALID_CAPS})
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits[body["name"]] = body
            return httpx.Response(200, json={"task_id": body["name"],
                                             "stage_id": f"stage-{body['name']}"})
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return httpx.Response(200, json={
                    "task_id": task_id, "status": "completed",
                    "stages": [{"status": "completed",
                                "result": {"result": json.dumps(plan)}}]})
            if task_id == "flow:bare-chain:img":
                return httpx.Response(200, json={
                    "task_id": task_id, "status": "completed",
                    "stages": [{"status": "completed",
                                "result": {"artifact_id": "art-bare"}}]})
            return httpx.Response(200, json={
                "task_id": task_id, "status": "completed",
                "stages": [{"status": "completed", "result": {"stored": True}}]})
        return httpx.Response(200, json={"ok": True})

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

    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert submits["flow:bare-chain:storage"]["payload"]["artifact_id"] == "art-bare"
    assert result["result"]["aggregate"]["img"] == {"artifact_id": "art-bare"}


def test_unwrap_keeps_fail_fast_for_stale_aggregate_relative_paths():
    """Old hint style (aggregate-relative, 'result.answer') fails loudly against
    the UNWRAPPED aggregate entry (D6 stays intact instead of silently missing):
    after the unwrap the shell is gone — a path segment 'result' exists
    only if the inner result itself carries a 'result' key.
    That is exactly why the hints were switched to result-relative."""
    aggregate = {"img": {"answer": "x"}}  # post-unwrap: envelope removed
    with pytest.raises(FlowError) as exc_info:
        _resolve_templates({"x": "${img.result.result.answer}"}, aggregate)
    msg = str(exc_info.value)
    assert "${img.result.result.answer}" in msg
    assert "depends_on" in msg