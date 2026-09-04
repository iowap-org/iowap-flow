# tests/test_flow_relay_api.py — Task 3 (Relay-Task-Client) via httpx.MockTransport
"""Echte Tests gegen RelayApi mit httpx.MockTransport (kein echter Server nötig).

Abgedeckt laut Plan Task 3:
  - submit setzt idempotency_key, task_name-Präfix "flow:..." (§2.5)
  - get_task parst TaskView (stages mit status/result)
  - add_note sendet kind
  - complete/fail sprechen den richtigen Endpoint mit node_id aus Env-Kontext an
"""
from __future__ import annotations

import json

import httpx
import pytest

from flow_node.relay_api import RelayApi

BASE = "http://relay.test:8788"


class Recorded:
    """Sammelt Requests in .calls und beantwortet sie aus .routes."""

    def __init__(self) -> None:
        self.calls: list[tuple[str, str, dict | None]] = []  # (method, url, body)
        self.routes: dict[tuple[str, str], tuple[int, dict]] = {}
        self.default: tuple[int, dict] = (200, {})

    def add(self, method: str, url: str, response: dict, status: int = 200) -> None:
        self.routes[(method, url)] = (status, response)

    def handler(self, request: httpx.Request) -> httpx.Response:
        body = json.loads(request.content.decode("utf-8")) if request.content else None
        self.calls.append((request.method, str(request.url), body))
        status, payload = self.routes.get((request.method, str(request.url)), self.default)
        return httpx.Response(status, json=payload)


@pytest.fixture
def mock_api(monkeypatch, request):
    """Factory: _install_transport(handler) → RelayApi gegen MockTransport."""
    def factory(handler, token_file: str = "/nonexistent-token-file") -> RelayApi:
        api = RelayApi(f"{BASE}/", token_file, timeout=5.0)  # trailing slash wird gertrimmt
        client = httpx.Client(transport=httpx.MockTransport(handler))

        def fake_request(method, url, **kwargs):
            return client.request(method, url, **kwargs)

        monkeypatch.setattr("flow_node.relay_api.httpx.request", fake_request)
        request.addfinalizer(client.close)
        return api

    return factory


def test_submit_sets_idempotency_key_and_task_name_prefix(mock_api):
    # submit setzt idempotency_key; task_name-Präfix "flow:<flow-name>:<task-id>" (§2.5)
    rec = Recorded()
    rec.add("POST", f"{BASE}/relay/v2/scheduler/task-simple",
            {"task_id": "t-1", "stage_id": "st-1"})
    api = mock_api(rec.handler)

    result = api.submit_simple_task(
        capability="image.generate.mflux",
        payload={"prompt": "cat"},
        name="flow:produktbilder:resize_a",
        idempotency_key="flow-origin-1-resize_a",
        priority=5,
        timeout_seconds=3600,
    )

    assert result == {"task_id": "t-1", "stage_id": "st-1"}
    method, url, body = rec.calls[0]
    assert (method, url) == ("POST", f"{BASE}/relay/v2/scheduler/task-simple")
    assert body["capability"] == "image.generate.mflux"
    assert body["payload"] == {"prompt": "cat"}
    # Name trägt das flow:-Präfix (Kinder im Dashboard gruppierbar, §2.5)
    assert body["name"] == "flow:produktbilder:resize_a"
    assert body["name"].startswith("flow:")
    assert body["idempotency_key"] == "flow-origin-1-resize_a"
    assert body["priority"] == 5
    assert body["timeout_seconds"] == 3600
    # owner_node_id darf NICHT gesetzt sein (Kinder dürfen überall laufen, §2.5)
    assert "owner_node_id" not in body


def test_submit_without_optional_fields_omits_them(mock_api):
    # idempotency_key=None / timeout_seconds=None → Felder fehlen im Body
    rec = Recorded()
    rec.add("POST", f"{BASE}/relay/v2/scheduler/task-simple", {"task_id": "t-2"})
    api = mock_api(rec.handler)

    api.submit_simple_task(capability="agent.ai", payload={"prompt": "plan"}, name="flow:demo:_plan")

    _, _, body = rec.calls[0]
    assert "idempotency_key" not in body
    assert "timeout_seconds" not in body
    assert body["priority"] == 0


def test_get_task_parses_taskview(mock_api):
    # get_task parst TaskView (stages mit status/result)
    taskview = {
        "id": "task-42",
        "name": "flow:demo:a",
        "status": "in_progress",
        "stages": [
            {"stage_id": "st-1", "capability": "tool.x", "status": "completed",
             "result": {"output": "done"}},
            {"stage_id": "st-2", "capability": "tool.y", "status": "in_progress"},
        ],
    }
    rec = Recorded()
    rec.add("GET", f"{BASE}/relay/v2/scheduler/tasks/task-42", taskview)
    api = mock_api(rec.handler)

    view = api.get_task("task-42")

    method, url, _body = rec.calls[0]
    assert (method, url) == ("GET", f"{BASE}/relay/v2/scheduler/tasks/task-42")
    assert view == taskview
    assert view["stages"][0]["status"] == "completed"
    assert view["stages"][0]["result"] == {"output": "done"}
    assert view["stages"][1]["status"] == "in_progress"


def test_add_note_sends_kind(mock_api):
    # add_note sendet {"message": ..., "kind": ...} an /tasks/{id}/notes
    rec = Recorded()
    rec.add("POST", f"{BASE}/relay/v2/scheduler/tasks/task-42/notes", {})
    api = mock_api(rec.handler)

    api.add_note("task-42", "flow progress: 2/3 done", kind="longrun")

    method, url, body = rec.calls[0]
    assert (method, url) == ("POST", f"{BASE}/relay/v2/scheduler/tasks/task-42/notes")
    assert body == {"message": "flow progress: 2/3 done", "kind": "longrun"}


def test_add_note_defaults_to_info_kind(mock_api):
    # kind-Default "info" (Dashboard-Trail, Plan §2.5)
    rec = Recorded()
    rec.add("POST", f"{BASE}/relay/v2/scheduler/tasks/task-42/notes", {})
    api = mock_api(rec.handler)

    api.add_note("task-42", "fan-out: 3 tasks")

    _, _, body = rec.calls[0]
    assert body == {"message": "fan-out: 3 tasks", "kind": "info"}


def test_complete_stage_hits_right_endpoint_with_node_id(mock_api, monkeypatch):
    # POST /relay/v2/scheduler/stages/{stage_id}/complete mit node_id aus Env-Kontext
    monkeypatch.setenv("RELAY_NODE_ID", "flow-runner-01")
    rec = Recorded()
    rec.add("POST", f"{BASE}/relay/v2/scheduler/stages/st-9/complete", {})
    api = mock_api(rec.handler)

    api.complete_stage("task-42", "st-9", {"status": "completed", "aggregate": {"a": {"ok": True}}})

    method, url, body = rec.calls[0]
    assert (method, url) == ("POST", f"{BASE}/relay/v2/scheduler/stages/st-9/complete")
    assert body == {
        "node_id": "flow-runner-01",
        "task_id": "task-42",
        "result": {"status": "completed", "aggregate": {"a": {"ok": True}}},
    }


def test_fail_stage_hits_right_endpoint(mock_api, monkeypatch):
    # fail_stage: same endpoint, {"error": error} statt result
    monkeypatch.setenv("RELAY_NODE_ID", "flow-runner-01")
    rec = Recorded()
    rec.add("POST", f"{BASE}/relay/v2/scheduler/stages/st-9/complete", {})
    api = mock_api(rec.handler)

    api.fail_stage("task-42", "st-9", "flow failed at resize_a (image.generate.mflux): boom")

    method, url, body = rec.calls[0]
    assert (method, url) == ("POST", f"{BASE}/relay/v2/scheduler/stages/st-9/complete")
    assert body == {
        "node_id": "flow-runner-01",
        "task_id": "task-42",
        "error": "flow failed at resize_a (image.generate.mflux): boom",
    }


def test_authorization_header_from_token_file(mock_api, tmp_path):
    # Bearer wird aus token_file gelesen, wenn die Datei existiert (§2.4 Env-Contract)
    token_file = tmp_path / "relay.token"
    token_file.write_text("secret-token-123\n")
    captured = {}

    def handler(request: httpx.Request) -> httpx.Response:
        captured["auth"] = request.headers.get("Authorization")
        return httpx.Response(200, json={"task_id": "t-3"})

    api = mock_api(handler, token_file=str(token_file))
    api.submit_simple_task(capability="tool.x", payload={}, name="flow:x")

    assert captured["auth"] == "Bearer secret-token-123"


def test_http_error_raises(mock_api):
    # raise_for_status: Relay-Fehler → httpx.HTTPStatusError (fail-fast, D6)
    rec = Recorded()
    rec.add("GET", f"{BASE}/relay/v2/scheduler/tasks/missing", {"detail": "not found"}, status=404)
    api = mock_api(rec.handler)

    with pytest.raises(httpx.HTTPStatusError):
        api.get_task("missing")