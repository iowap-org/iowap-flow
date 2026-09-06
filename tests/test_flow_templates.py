# tests/test_flow_templates.py — T-002: Template-Interpolation (Dateninjektion
# zwischen Flow-Tasks). Syntax ${ref.result.path}, Auflösung beim Submit aus dem
# laufenden Aggregate — MockTransport-Muster wie test_flow_runner.py.
import json

import httpx
import pytest

from flow_node.relay_api import RelayApi
from flow_node.runner import FlowError, _resolve_templates, build_plan_prompt, run

BASE = "http://relay.test"
TOKEN = "/tmp/flow-test-token"  # existiert nicht → kein Authorization-Header
ORIGIN_TASK = "task-origin-1"
ORIGIN_STAGE = "stage-origin-1"
NODE = "flow-runner-01"

VALID_CAPS = {
    "agent.ai": {"available": True},
    "image.generate.mflux": {"available": True},
    "storage.store": {"available": True},
}


# ---------------------------------------------------------------------------
# Test-Transport: wie test_flow_runner.py — gefakte TaskView-Sequenzen
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


def _fake_clock(monkeypatch):
    """monotonic-Uhr, die mit time.sleep-Advances mitgeht (kein echtes Warten)."""
    state = {"elapsed": 0.0}

    def fake_sleep(seconds):
        state["elapsed"] += seconds

    monkeypatch.setattr("time.sleep", fake_sleep)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    return state


# ---------------------------------------------------------------------------
# _resolve_templates — Unit (Syntax, Typ-Erhalt, Fail-fast D6)
# ---------------------------------------------------------------------------

def test_exact_match_resolves_to_native_type():
    """String, der EXAKT einem Template entspricht → nativer Wert, Typ bleibt
    erhalten (str/int/bool/dict, auch mehrstufiger Dot-Path)."""
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
    assert resolved["count"] == 3                # int bleibt int
    assert resolved["flag"] is True              # bool bleibt bool
    assert resolved["meta"] == {"w": 512}        # dict bleibt dict
    assert resolved["width"] == 512              # Dot-Path über 2 Ebenen


def test_embedded_templates_substitute_as_str():
    """Template EINGEBETTET in größeren String → alle Treffer als str() im
    Text substituieren; dict/list via json.dumps, Skalare via str()."""
    aggregate = {"img": {"artifact_id": "art-42", "count": 3,
                         "meta": {"w": 512}, "tags": ["a", "b"]}}
    resolved = _resolve_templates(
        {
            "text": "Bild ${img.result.artifact_id} gespeichert",
            "two": "${img.result.artifact_id}/${img.result.count}",
            "obj": "meta=${img.result.meta}",
            "lst": "tags=${img.result.tags}",
        },
        aggregate,
    )
    assert resolved["text"] == "Bild art-42 gespeichert"
    assert resolved["two"] == "art-42/3"        # int → str()
    assert resolved["obj"] == 'meta={"w": 512}'  # dict → json.dumps
    assert resolved["lst"] == 'tags=["a", "b"]'  # list → json.dumps


def test_nested_payloads_dict_in_dict_and_lists():
    """Rekursion über den Payload: dict in dict, Listen, gemischte Elemente."""
    aggregate = {"img": {"artifact_id": "art-42", "url": "http://x/y.png"}}
    payload = {
        "outer": {"inner": "${img.result.artifact_id}",
                  "deeper": {"x": "${img.result.url}"}},
        "liste": ["${img.result.artifact_id}",
                  {"k": "prefix-${img.result.url}"}, 42],
        "plain": "bleibt wie es ist",
    }
    assert _resolve_templates(payload, aggregate) == {
        "outer": {"inner": "art-42", "deeper": {"x": "http://x/y.png"}},
        "liste": ["art-42", {"k": "prefix-http://x/y.png"}, 42],
        "plain": "bleibt wie es ist",
    }


def test_missing_ref_in_aggregate_raises_flowerror_with_depends_on_hint():
    """Referenz auf Task, der im Aggregate fehlt (meist: fehlt in depends_on)
    → FlowError mit Template, referenziertem Task und depends_on-Hinweis."""
    aggregate = {"img": {"artifact_id": "art-42"}}
    with pytest.raises(FlowError) as exc_info:
        _resolve_templates({"artifact_id": "${storage.result.path}"}, aggregate)
    msg = str(exc_info.value)
    assert "${storage.result.path}" in msg   # das Template
    assert "storage" in msg                  # der referenzierte Task
    assert "depends_on" in msg               # der Hinweis


def test_unresolvable_path_raises_flowerror():
    """Path nicht im Resultat auflösbar (fehlender Key oder trifft auf einen
    Nicht-dict-Wert) → FlowError mit Template, Task und depends_on-Hinweis."""
    aggregate = {"img": {"artifact_id": "art-42"}}
    for template in ("${img.result.does.not.exist}", "${img.result.artifact_id.sub}"):
        with pytest.raises(FlowError) as exc_info:
            _resolve_templates({"x": template}, aggregate)
        msg = str(exc_info.value)
        assert template in msg
        assert "img" in msg
        assert "depends_on" in msg


def test_non_template_strings_pass_through():
    """Nur ${ref.result.path} ist ein Template — alles andere bleibt literal
    (keine Ausdrücke, keine Defaultwerte, keine Filter)."""
    aggregate = {"img": {"artifact_id": "art-42"}}
    payload = {
        "a": "plain text",
        "b": "{kein} template",
        "c": "${img.artifact_id}",   # ohne .result. → kein Template
        "d": "$ref",
        "e": "${img.result}",        # ohne Path → kein Template
        "f": 7,
    }
    assert _resolve_templates(payload, aggregate) == payload


# ---------------------------------------------------------------------------
# Plan-Prompt: Planner muss die Template-Syntax kennen (T-002 Plan D6)
# ---------------------------------------------------------------------------

def test_plan_prompt_documents_template_syntax():
    """build_plan_prompt lehrt den Planner die Template-Syntax UND die Regel:
    jeder referenzierte Task MUSS in depends_on stehen."""
    prompt = build_plan_prompt("Erstelle 2 Bilder", "- agent.ai (available: True)")
    assert "${ref.result.path}" in prompt
    assert "depends_on" in prompt


# ---------------------------------------------------------------------------
# run() — Integration über den MockTransport (Muster: test_flow_runner.py)
# ---------------------------------------------------------------------------

def test_image_storage_flow_resolves_template_at_submit(monkeypatch):
    """2-Task-Flow image → storage: ${img.result.artifact_id} wird zur
    SUBMIT-Zeit aus dem Aggregate aufgelöst (nackter Wert, nativer Typ),
    eingebettete Treffer als str(); flow-Metadaten kommen last."""
    _fake_clock(monkeypatch)
    submits = {}

    plan = {
        "version": 1,
        "name": "paper",
        "tasks": [
            {"id": "img", "capability": "image.generate.mflux",
             "payload": {"prompt": "Zeitungsseite"}, "depends_on": []},
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
    result = run(api, {"task": "Zeitung als PDF"}, BASE, TOKEN,
                 ORIGIN_TASK, ORIGIN_STAGE, NODE)

    # Template zur Submit-Zeit aufgelöst — NICHT erst vom Storage-Handler
    storage = submits["flow:paper:storage"]["payload"]
    assert storage["artifact_id"] == "art-7f3"   # exakt → nativer Wert
    assert storage["label"] == "art-2"           # eingebettet → str()
    # flow-Metadaten kamen LAST — Template-Auflösung lief nie über sie
    assert storage["flow"] == {"origin_task_id": ORIGIN_TASK, "task_ref": "storage"}
    assert result["status"] == "completed"
    assert result["aggregate"] == {
        "img": {"artifact_id": "art-7f3", "count": 2},
        "storage": {"stored": True},
    }


def test_flow_fails_when_referenced_task_not_in_aggregate(monkeypatch):
    """Task referenziert einen Task, der NICHT in depends_on steht → beim
    Submit fehlt er im Aggregate → FlowError mit betroffenem task_ref,
    Template und depends_on-Hinweis; der Task wird nie submittet (D6)."""
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
             "depends_on": []},  # FEHLER: img fehlt in depends_on
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
            # Der Flow failt beim Storage-Submit VOR jedem Poll-Ergebnis
            return _task_view(task_id, "pending")
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    with pytest.raises(FlowError) as exc_info:
        run(api, {"task": "t"}, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)

    msg = str(exc_info.value)
    assert "storage" in msg                       # betroffener task_ref
    assert "${img.result.artifact_id}" in msg     # das Template
    assert "depends_on" in msg                    # der Hinweis
    assert submitted == ["flow:broken:img"]       # storage nie submittet (D6)