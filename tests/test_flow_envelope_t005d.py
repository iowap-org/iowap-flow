"""T-005d (t_b66d8b3f): flow-node Envelope-Contract — Phase B.

Two sides, both T-005d scope (design.md §6):

1. ``run()`` stdin tolerance: the flow capability's handler_runner is the
   node-side contract owner. During the fleet roll-out, flow-runner-01
   (pinned ``iowap-node @ wheel-v2.3.9`` in pyproject.toml) still runs the
   PRE-T-005b runner: stage payload flat on stdin. felix-cyberfox on
   wheel-v2.3.13 sends the strict Request Envelope. The runner accepts all
   three generations via a discriminator (``task_id``+``capability``+dict
   ``input`` → inner payload; else the dict as-is) — mirroring §6 of the
   design's handler-side migration.

2. ``run()`` result shapes: the three final results (list_flows, save_flow,
   classic run completion) move their mode/flow/aggregate/summary payload
   under ``result`` so the T-005b choke-point normalizes them as conforming
   envelopes instead of failing them with ``completed missing result``.

Deviation from design.md §6 (documented, same rationale as T-005a D4/D10):
the wrapped shapes OMIT the ``error`` key entirely. The design text says
``"error": null``, but the daemon counts stage failures via ``"error" in
result`` (key presence, node_daemon.py complete path) — an ``error: null``
key would count every successful flow run as a failure. Omission keeps the
conforming envelope AND the daemon accounting intact.
"""
from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from flow_node.relay_api import RelayApi
from flow_node.runner import run

BASE = "http://relay.test"
TOKEN = "/tmp/flow-test-token"  # existiert nicht → kein Authorization-Header
ORIGIN_TASK = "task-origin-1"
ORIGIN_STAGE = "stage-origin-1"
NODE = "flow-runner-01"

PLAN = {
    "version": 1,
    "name": "demo",
    "summary": "s",
    "tasks": [
        {"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []},
    ],
}

VALID_CAPS = {
    "agent.ai": {"available": True},
    "tool.x": {"available": True},
}


def _ok() -> httpx.Response:
    return httpx.Response(200, json={"ok": True})


def _caps_response(caps: dict) -> httpx.Response:
    return httpx.Response(200, json=caps)


def _submit_response(task_id: str) -> httpx.Response:
    return httpx.Response(
        200,
        json={"task_id": task_id, "stage_id": f"stage-{task_id}"},
    )


def _task_view(
    task_id: str,
    status: str,
    *,
    result: Any = None,
    error: str | None = None,
) -> httpx.Response:
    stage: dict[str, Any] = {"status": status}
    if result is not None:
        stage["result"] = result
    if error is not None:
        stage["error"] = error
    return httpx.Response(
        200,
        json={"task_id": task_id, "stages": [stage], "status": status},
    )


def _patch_http(monkeypatch: pytest.MonkeyPatch, handler) -> None:
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


def _fake_clock(monkeypatch: pytest.MonkeyPatch) -> None:
    import flow_node.runner as runner_mod

    monkeypatch.setattr(runner_mod.time, "sleep", lambda *_: None)


# ---------------------------------------------------------------------------
# 1. stdin tolerance — three daemon generations
# ---------------------------------------------------------------------------


def _plan_child_result() -> dict:
    return {"status": "completed", "result": {"answer": json.dumps(PLAN)}}


def test_stdin_strict_envelope_payload_read_from_input(monkeypatch, tmp_path):
    """Daemon gen ≥2.3.13: {"task_id","capability","input":{...}} → input is
    the payload; task text comes from payload["task"]."""
    seen: list[dict] = []
    completes: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            seen.append(body)
            if body["capability"] == "agent.ai":
                return _submit_response("flow:_plan")
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result=_plan_child_result())
            return _task_view(task_id, "completed", result={"out": task_id})
        if url.endswith("/complete"):
            completes.append(json.loads(request.content))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    _fake_clock(monkeypatch)
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    api = RelayApi(BASE, TOKEN)
    result = run(
        api,
        {"task_id": "task_1", "capability": "flow.run", "input": {"task": "E2E"}},
        BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
    )

    # The submitted plan child carries the task text from the inner payload.
    plan_body = seen[0]
    assert plan_body["capability"] == "agent.ai"
    assert "E2E" in plan_body["payload"]["task"]
    assert result["status"] == "completed"
    assert result["result"]["aggregate"] == {"a": {"out": "flow:demo:a"}}


def test_stdin_mirrored_envelope_inner_payload_preferred(monkeypatch, tmp_path):
    """Daemon gen 2.3.12: mirrored envelope — contract keys win; the runner
    reads the inner payload (strict semantics), not the top-level mirror."""
    seen: list[dict] = []
    completes: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            seen.append(body)
            if body["capability"] == "agent.ai":
                return _submit_response("flow:_plan")
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result=_plan_child_result())
            return _task_view(task_id, "completed", result={"out": task_id})
        if url.endswith("/complete"):
            completes.append(json.loads(request.content))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    _fake_clock(monkeypatch)
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    api = RelayApi(base_url=BASE, token_file=TOKEN)
    result = run(
        api,
        {
            "task": "MIRROR-TOP-LEVEL",  # legacy mirror — must be ignored
            "task_id": "task_1",
            "capability": "flow.run",
            "input": {"task": "INNER"},
        },
        BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
    )

    plan_body = seen[0]
    assert "INNER" in plan_body["payload"]["task"]
    assert "MIRROR-TOP-LEVEL" not in plan_body["payload"]["task"]
    assert result["status"] == "completed"


def test_stdin_flat_payload_still_works(monkeypatch, tmp_path):
    """Daemon gen ≤2.3.9 (flow-runner-01 today): flat payload, no envelope
    keys → runs unchanged (pre-T-005b tolerance)."""
    seen: list[dict] = []
    completes: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            seen.append(body)
            if body["capability"] == "agent.ai":
                return _submit_response("flow:_plan")
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result=_plan_child_result())
            return _task_view(task_id, "completed", result={"out": task_id})
        if url.endswith("/complete"):
            completes.append(json.loads(request.content))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    _fake_clock(monkeypatch)
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    api = RelayApi(base_url=BASE, token_file=TOKEN)
    result = run(
        api,
        {"task": "FLAT-LEGACY"},
        BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
    )

    plan_body = seen[0]
    assert "FLAT-LEGACY" in plan_body["payload"]["task"]
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# 2. Result shapes — conforming envelopes, no error key (deviation, see
#    module docstring)
# ---------------------------------------------------------------------------


def test_classic_run_result_is_conforming_envelope(monkeypatch, tmp_path):
    """run() final result: {"status","result":{flow,aggregate,summary}} —
    normalized conforming, NO top-level flow/aggregate/summary keys, no
    "error" key (daemon counts failures by key presence)."""
    completes: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            if body["capability"] == "agent.ai":
                return _submit_response("flow:_plan")
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result=_plan_child_result())
            return _task_view(task_id, "completed", result={"out": task_id})
        if url.endswith("/complete"):
            completes.append(json.loads(request.content))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    _fake_clock(monkeypatch)
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert result["status"] == "completed"
    assert "error" not in result
    assert result["result"]["flow"] == {"name": "demo", "tasks_done": 1}
    assert result["result"]["aggregate"] == {"a": {"out": "flow:demo:a"}}
    assert result["result"]["summary"] == "s"
    body = completes[0]
    assert body["result"]["status"] == "completed"
    assert body["result"]["result"]["aggregate"] == {"a": {"out": "flow:demo:a"}}
    assert "error" not in body["result"]


def test_list_flows_result_is_conforming_envelope(monkeypatch, tmp_path):
    (tmp_path / "templates").mkdir()
    (tmp_path / "templates" / "demo.json").write_text(json.dumps(PLAN))
    monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    seen_urls: list[str] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        seen_urls.append(url)
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url.endswith("/complete"):
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    result = run(
        api, {"mode": "list_flows"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
    )
    assert result["status"] == "completed"
    assert "error" not in result
    assert result["result"]["mode"] == "list_flows"
    assert result["result"]["flows"][0]["name"] == "demo"


def test_save_flow_result_is_conforming_envelope(monkeypatch, tmp_path):
    (tmp_path / "templates").mkdir()
    (tmp_path / "history").mkdir()
    monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))

    def handler(request: httpx.Request) -> httpx.Response:
        return _ok()

    _patch_http(monkeypatch, handler)
    from flow_node.catalog import record_history
    hid = record_history("demo", PLAN, {"a": 1}, "task-x")

    api = RelayApi(BASE, TOKEN)
    result = run(
        api, {"mode": "save_flow", "history_id": hid},
        BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
    )
    assert result["status"] == "completed"
    assert "error" not in result
    assert result["result"]["mode"] == "save_flow"
    assert result["result"]["flow"] == "demo"


def test_mode_dispatch_inside_envelope_input(monkeypatch, tmp_path):
    """Strict envelope: the dispatcher's full task payload (mode/flow/input)
    arrives inside `input` — run_flow dispatch must work from there."""
    submits: list[dict] = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits.append(body)
            if body["capability"] == "agent.ai":
                return _submit_response("flow:_plan")
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result=_plan_child_result())
            return _task_view(task_id, "completed", result={"r": 1})
        if url.endswith("/complete"):
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    _fake_clock(monkeypatch)
    monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
    (tmp_path / "templates").mkdir()
    (tmp_path / "templates" / "demo.json").write_text(json.dumps(PLAN))
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    api = RelayApi(BASE, TOKEN)
    result = run(
        api,
        {"task_id": "t1", "capability": "flow.run",
         "input": {"mode": "run_flow", "flow": "demo", "input": {"x": 1}}},
        BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
    )
    assert result["status"] == "completed"
    # KEIN Planungs-Kind: genau so viele submits wie Plan-Tasks (a)
    assert len(submits) == 1