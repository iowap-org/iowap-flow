# tests/test_flow_runner.py — Task 4 (+4b) (Plan-Phase + Fan-out + Join-Loop, httpx.MockTransport mit gefakten TaskView-Sequenzen)
import itertools
import json

import httpx
import pytest

from flow_node.relay_api import RelayApi
from flow_node.runner import (
    KEEPALIVE_INTERVAL_SECONDS,
    PLAN_TASK_REF,
    POLL_INTERVAL_SECONDS,
    build_plan_prompt,
    extract_plan_json,
    run,
)

BASE = "http://relay.test"
TOKEN = "/tmp/flow-test-token"  # existiert nicht → kein Authorization-Header
ORIGIN_TASK = "task-origin-1"
ORIGIN_STAGE = "stage-origin-1"
NODE = "flow-runner-01"

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
# Test-Transport: kein echter Server, gefakte TaskView-Sequenzen
# ---------------------------------------------------------------------------

def _patch_http(monkeypatch, handler):
    """httpx.request + httpx.get (Modul-Ebene) auf den Handler lenken —
    RelayApi und runner rufen beide Varianten."""
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
    """monotonic-Uhr, die mit time.sleep-Advances mitgeht (kein echtes Warten).
    Records alle sleeps in state['sleeps'] für Backoff-Assertions."""
    state = {"elapsed": 0.0, "sleeps": []}
    def fake_sleep(seconds):
        state["elapsed"] += seconds
        state["sleeps"].append(seconds)
    monkeypatch.setattr("time.sleep", fake_sleep)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    return state


# ---------------------------------------------------------------------------
# build_plan_prompt + extract_plan_json (pure unit tests)
# ---------------------------------------------------------------------------

def test_build_plan_prompt_uses_replace_not_format():
    """str.format() würde an den literalen JSON-Braces brechen — .replace() nicht."""
    prompt = build_plan_prompt("Erstelle 3 Bilder", "- agent.ai (available: True)")
    assert "Erstelle 3 Bilder" in prompt
    assert "- agent.ai (available: True)" in prompt
    assert '{"version": 1, "name": "...", "tasks": [' in prompt  # Literale Braces intakt
    assert "{task}" not in prompt
    assert "{capabilities_snapshot}" not in prompt


def test_extract_plan_json_direct_and_one_level_deep():
    assert extract_plan_json(json.dumps(PLAN))["name"] == "demo"
    assert extract_plan_json(json.dumps({"result": PLAN}))["name"] == "demo"
    assert extract_plan_json(json.dumps({"result": json.dumps(PLAN)}))["name"] == "demo"
    with_text = f"Vorrede\n{json.dumps(PLAN)}\nNachrede"
    assert extract_plan_json(with_text)["name"] == "demo"


def test_extract_plan_json_two_levels_deep_agent_ai():
    """Live-Form (2026-09-06, TTL-300-Test): agent.ai-Antwort steckt zwei Ebenen
    tief — {'status': ..., 'result': {'answer': '<plan-json-als-string>'}}.
    Erwartung: gefunden statt 'no plan JSON object'."""
    wrapped = {"status": "completed", "result": {"answer": json.dumps(PLAN)}}
    assert extract_plan_json(json.dumps(wrapped))["name"] == "demo"


def test_extract_plan_json_rejects_no_tasks_array():
    with pytest.raises(Exception, match="plan"):
        extract_plan_json(json.dumps({"foo": "bar"}))


# ---------------------------------------------------------------------------
# run() — MockTransport-Szenarien
# ---------------------------------------------------------------------------

def test_planning_child_submitted_first_and_plan_extracted_lenient(monkeypatch):
    """(a) Planungs-Kind zuerst submittet (idempotency flow-<origin>-_plan,
    Name flow:_plan); Plan-Extraktion lenient (Result in {'result': ...})."""
    calls = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        calls.append((request.method, url))
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            if body["capability"] == "agent.ai":
                assert body["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}"
                assert body["name"] == "flow:_plan"
                assert "Erstelle 3 Produktbilder" in body["payload"]["task"]
                return _submit_response("plan-child-1")
            return _submit_response(f"child-{body['name']}")
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "plan-child-1":
                # lenient: Plan als JSON-String in {"result": ...}
                return _task_view("plan-child-1", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={"done": task_id})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "Erstelle 3 Produktbilder"}, BASE, TOKEN,
                 ORIGIN_TASK, ORIGIN_STAGE, NODE)

    # Planungs-Kind als ERSTER task-simple-Submit
    simple_posts = [c for c in calls if c[1] == "/relay/v2/scheduler/task-simple"]
    assert simple_posts[0][0] == "POST" and len(simple_posts) == 3  # _plan, a, b
    assert result["status"] == "completed"
    assert result["aggregate"] == {"a": {"done": "child-flow:demo:a"}, "b": {"done": "child-flow:demo:b"}}
    assert result["summary"] == "s"


def test_invalid_plan_json_one_retry_with_feedback_then_fail(monkeypatch):
    """(b) ungültiges Plan-JSON → D9: 1 Retry mit Validerings-Fehler im Prompt,
    2. Versuch failt → Flow failed mit Grund."""
    _fake_clock(monkeypatch)
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
                # kein JSON-Objekt mit tasks
                return _task_view(f"plan-{n}", "completed", result={"result": "kein plan hier"})
            # 2. Versuch: formal JSON, aber leerer tasks-Array → PlanError
            return _task_view(f"plan-{n}", "completed",
                             result={"result": json.dumps({"version": 1, "tasks": []})})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    with pytest.raises(Exception, match="plan phase failed"):
        run(api, {"task": "test"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert len(submits) == 2  # genau 1 Retry (D9)
    # D9-Retry braucht pro Attempt einen NEUEN Idempotency-Key — sonst liefert
    # der Server dasselbe fertig-gefailte Planungs-Kind zurück (Live-Bug
    # 2026-09-06) und der Feedback-Retry wäre tot.
    assert submits[0]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}"
    assert submits[1]["idempotency_key"] == f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}-2"
    assert submits[1]["name"] == "flow:_plan-2"
    # Retry-Prompt enthält das Validerings-Feedback aus Versuch 1
    assert "ungültig" in submits[1]["payload"]["task"]
    assert "no plan JSON object" in submits[1]["payload"]["task"]


def test_parallel_roots_and_join_submit_order_respects_deps(monkeypatch):
    """(c) 2 parallele Roots + 1 Join → Roots zuerst, Join erst wenn Roots done.
    Roots sind bis Poll-Runde 2 pending (t-b-Semantik) — Join-Submit erst danach."""
    _fake_clock(monkeypatch)
    submitted = []
    poll_counts = {}

    plan3 = {
        "version": 1,
        "name": "j",
        "tasks": [
            {"id": "r1", "capability": "tool.x", "payload": {}, "depends_on": []},
            {"id": "r2", "capability": "tool.y", "payload": {}, "depends_on": []},
            {"id": "join", "capability": "agent.ai", "payload": {}, "depends_on": ["r1", "r2"]},
        ],
        "summary": "s3",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            if body["name"] == "flow:_plan":
                return _submit_response("plan-child-1")
            submitted.append(body["name"])
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "plan-child-1":
                return _task_view("plan-child-1", "completed", result={"result": json.dumps(plan3)})
            poll_counts[task_id] = poll_counts.get(task_id, 0) + 1
            if task_id in ("flow:j:r1", "flow:j:r2"):
                done = poll_counts[task_id] >= 2  # Roots pending bis Poll-Runde 2
                return _task_view(task_id, "completed" if done else "pending",
                                  result={"ref": task_id} if done else None)
            if task_id == "flow:j:join":
                done = poll_counts[task_id] >= 1
                return _task_view(task_id, "completed" if done else "pending",
                                  result={"ref": task_id} if done else None)
            return _task_view(task_id, "pending")
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert submitted == ["flow:j:r1", "flow:j:r2", "flow:j:join"]
    assert set(result["aggregate"].keys()) == {"r1", "r2", "join"}


def test_child_failed_is_fail_fast_no_successor_submit(monkeypatch):
    """(d) ein Kind failed (permanent) → fail-fast, kein Submit von Nachfolgern;
    Note 'flow failed at <task-ref> (<capability>): <error>' + fail_stage."""
    _fake_clock(monkeypatch)
    submits, notes, completes = [], [], []

    plan2 = {
        "version": 1,
        "name": "f",
        "tasks": [
            {"id": "a", "capability": "tool.x", "payload": {}, "depends_on": []},
            {"id": "b", "capability": "tool.y", "payload": {}, "depends_on": ["a"]},
        ],
        "summary": "s2",
    }

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            submits.append(body["name"])
            return _submit_response(body["name"])
        if url.endswith("/notes"):
            notes.append(json.loads(request.content))
            return _ok()
        if url.endswith("/complete"):
            completes.append(json.loads(request.content))
            return _ok()
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result={"result": json.dumps(plan2)})
            if task_id == "flow:f:a":
                return _task_view("flow:f:a", "failed", error="tool kaputt")
            return _task_view(task_id, "completed", result={})
        return httpx.Response(404)

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    with pytest.raises(Exception, match="flow failed at a"):
        run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    # Kein Nachfolger-Submit (fail-fast, D6)
    assert "flow:f:b" not in submits
    assert any("flow failed at a (tool.x): tool kaputt" in n["message"] for n in notes)
    assert any(c.get("error") for c in completes)  # fail_stage am Ursprungs-Stage


def test_idempotency_key_stable_on_repeat(monkeypatch):
    """(e) idempotency_key = flow-<origin-task-id>-<task-id> (§2.5) stabil bei Wiederholung."""
    _fake_clock(monkeypatch)
    seen_keys = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            seen_keys.append(body["idempotency_key"])
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)
    run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert f"flow-{ORIGIN_TASK}-a" in seen_keys and f"flow-{ORIGIN_TASK}-b" in seen_keys
    assert f"flow-{ORIGIN_TASK}-{PLAN_TASK_REF}" in seen_keys
    # Keys sind bei Wiederholung identisch → Re-Fanout erzeugt keine Duplikate (§2.5)
    assert seen_keys.count(f"flow-{ORIGIN_TASK}-a") == 2  # beide Läufe, gleicher Key
    assert len(set(seen_keys)) == 3  # _plan + a + b, keine weiteren


def test_join_completes_origin_stage_with_aggregate(monkeypatch):
    """(f) alle done → aggregate {task_ref: result} → complete_stage() am
    Ursprungs-Stage mit {'status': 'completed', 'aggregate': ..., 'summary': plan.summary}."""
    _fake_clock(monkeypatch)
    completes = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={"out": task_id})
        if url.endswith("/complete"):
            assert url == f"/relay/v2/scheduler/stages/{ORIGIN_STAGE}/complete"
            completes.append(json.loads(request.content))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    result = run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert completes, "complete_stage am Ursprungs-Stage wurde nicht gerufen"
    body = completes[0]
    assert body["task_id"] == ORIGIN_TASK
    assert body["result"]["status"] == "completed"
    assert body["result"]["aggregate"] == {"a": {"out": "flow:demo:a"}, "b": {"out": "flow:demo:b"}}
    assert body["result"]["summary"] == "s"
    assert result["status"] == "completed"


# ---------------------------------------------------------------------------
# Task 4b: Poll-Backoff, Keepalive, Budget
# ---------------------------------------------------------------------------

def test_poll_backoff_after_consecutive_http_errors(monkeypatch):
    """Task 4b: HTTP-Fehler beim Pollen → exponentieller Backoff statt hartem
    Busy-Loop; nach Erholung läuft der Flow normal weiter."""
    state = _fake_clock(monkeypatch)
    polls = {"fails_left": 3}

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            return _submit_response(body["name"])
        if url.endswith(("/notes", "/complete")):
            return _ok()  # Fehler-Injektion NUR für Task-GETs, nicht Notes/Complete
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result={"result": json.dumps(PLAN)})
            if polls["fails_left"] > 0:
                polls["fails_left"] -= 1
                return httpx.Response(500)  # simulate HTTP error, raise_for_status greift
            return _task_view(task_id, "completed", result={})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    # Backoff kam vor: nach 3 Fehlern exponentielle Delays > Grundintervall 5s
    assert any(s > POLL_INTERVAL_SECONDS for s in state["sleeps"])
    # Backoff-Delays wachsen exponentiell (5s-Grundtakt → 10s → 20s ...)
    backs = [s for s in state["sleeps"] if s > POLL_INTERVAL_SECONDS]
    assert backs == sorted(backs) and all(b <= 60.0 for b in backs)


def test_keepalive_note_every_600s_independent_timer(monkeypatch):
    """Task 4b: Keepalive-Note alle 600s unabhängig vom Poll-Takt ('flow progress:
    done=K/N') — ein Flow mit langsamen Kindern darf die Lease nicht verlieren."""
    state = _fake_clock(monkeypatch)
    notes = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            return _submit_response(body["name"])
        if url.endswith("/notes"):
            body = json.loads(request.content)
            if "flow progress" in body.get("message", ""):
                notes.append((body["message"], body["kind"], state["elapsed"]))
            return _ok()
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result={"result": json.dumps(PLAN)})
            # Kinder bleiben lange pending: done erst nach >= 1300s (2+ Keepalive-Runden)
            if state["elapsed"] >= 1300:
                return _task_view(task_id, "completed", result={"out": task_id})
            return _task_view(task_id, "pending")
        return _ok()

    _patch_http(monkeypatch, handler)
    monkeypatch.setattr("flow_node.runner.fetch_capabilities",
                        lambda b, t, timeout=10.0: dict(VALID_CAPS))
    api = RelayApi(BASE, TOKEN)
    run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    assert len(notes) >= 2  # mindestens alle 600s eine progress-Note (unabhängiger Timer)
    kinds = {k for _, k, _ in notes}
    assert "longrun" in kinds  # Lease-Refresh (T-154)
    times = [t for _, _, t in notes]
    gaps = [b - a for a, b in itertools.pairwise(times)]
    assert all(g >= KEEPALIVE_INTERVAL_SECONDS * 0.9 for g in gaps)  # ~600s-Abstand
    msgs = [m for m, _, _ in notes]
    assert any("flow progress: done=" in m for m in msgs)  # done=K/N-Format


def test_flow_budget_exceeded_fails_stage(monkeypatch):
    """Task 4b: options.max_flow_seconds überschritten → fail_stage 'flow budget exceeded'."""
    _fake_clock(monkeypatch)
    completes = []

    def handler(request: httpx.Request) -> httpx.Response:
        url = request.url.path
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            return _submit_response(body["name"])
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "flow:_plan":
                return _task_view("flow:_plan", "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "pending")  # Kinder NEVER done
        if url.endswith("/complete"):
            completes.append(json.loads(request.content))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    monkeypatch.setattr("flow_node.runner.fetch_capabilities",
                        lambda b, t, timeout=10.0: dict(VALID_CAPS))
    api = RelayApi(BASE, TOKEN)
    with pytest.raises(Exception, match="flow budget exceeded"):
        run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE,
            options={"max_flow_seconds": 10})

    assert completes and "flow budget exceeded" in completes[0].get("error", "")