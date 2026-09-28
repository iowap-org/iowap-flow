# tests/test_flow_envelope_unwrap.py — T-005c: Envelope-Contract, Flow-Seite.
#
# _task_result() unwrappt den Handler-Result-Envelope (design.md T-005a §2.2)
# EXAKT EINMAL: {"status":"completed","result":X} → X. Bare Results (legacy
# Nodes ohne Envelope) bleiben unverändert (tolerant bis zur Fleet-Migration,
# D7). Damit sind result_path_hints Pfade relativ zum inneren result:
# ${img.result.artifact_id} → aggregate["img"]["artifact_id"] — für Envelope-
# UND Bare-Handler gleichermaßen.
#
# Trigger-Incident (T-005a): Live-Flow nutzte ${t1_bild.result.answer} exakt
# nach den Hints, aber stage.result enthielt die ganze Hülle → Dot-Path eine
# Ebene zu tief → fail-fast D6.
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
# _task_result — Unwrap genau einmal (D7: gate auf completed + dict)
# ---------------------------------------------------------------------------

def test_task_result_unwraps_completed_envelope():
    """Envelope-Form (§2.2 T-005a) → der INNERE result-Dict landet im Aggregate."""
    view = {
        "stages": [
            {"status": "completed",
             "result": {"status": "completed", "result": {"answer": "42"}, "error": None}},
        ],
    }
    assert _task_result(view) == {"answer": "42"}


def test_task_result_passes_bare_result_through():
    """Legacy-Node ohne Envelope (nacktes Result-Dict) → unverändert durchgereicht."""
    view = {"stages": [{"status": "completed", "result": {"artifact_id": "art-7f3"}}]}
    assert _task_result(view) == {"artifact_id": "art-7f3"}


def test_task_result_unwrap_exactly_once():
    """Inneres Result, das SELBST wie ein Envelope aussieht, wird NICHT ein
    zweites Mal unwrappt — Unwrap ist an genau eine Stelle gebunden."""
    inner_envelope = {"status": "completed", "result": {"x": 1}, "error": None}
    view = {
        "stages": [
            {"status": "completed",
             "result": {"status": "completed", "result": inner_envelope, "error": None}},
        ],
    }
    assert _task_result(view) is inner_envelope  # eine Ebene, nicht zwei


def test_task_result_error_envelope_not_unwrapped():
    """status:"error"-Form wird hier nie erwartet (_stage_status filtert failed
    Stages vorher) — und wird trotzdem nicht angetastet (nur completed+dict
    unwrappt, D7)."""
    error_shape = {"status": "error", "result": None, "error": "boom"}
    view = {"stages": [{"status": "completed", "result": error_shape}]}
    assert _task_result(view) is error_shape


def test_task_result_completed_without_dict_result_passes_through():
    """status=completed, aber result kein dict (z. B. None/list) → kein Unwrap
    (Gate verlangt isinstance(result, dict), D7)."""
    bare = {"status": "completed", "result": None, "error": None}
    view = {"stages": [{"status": "completed", "result": bare}]}
    assert _task_result(view) is bare


def test_task_result_takes_first_stage_with_content():
    """Mehrere Stages: erstes Stage-Result mit Inhalt gewinnt (bestehende Regel)."""
    view = {
        "stages": [
            {"status": "completed", "result": None},
            {"status": "completed",
             "result": {"status": "completed", "result": {"answer": "ja"}, "error": None}},
        ],
    }
    assert _task_result(view) == {"answer": "ja"}


def test_task_result_returns_none_when_no_stage_result():
    """Kein Stage mit Result → None (bestehender Vertrag)."""
    assert _task_result({"stages": [{"status": "pending"}]}) is None
    assert _task_result({}) is None
    assert _task_result({}) is None


def test_task_result_non_dict_result_types_pass_through():
    """Skalare/Listen als Stage-Result ( legacy Node) → unverändert."""
    for value in ("plain text", [1, 2], 7, True):
        view = {"stages": [{"status": "completed", "result": value}]}
        assert _task_result(view) == value


# ---------------------------------------------------------------------------
# Hints sind relativ zum inneren result — Snapshot + Template- Auflösung
# ---------------------------------------------------------------------------

def test_caps_snapshot_shows_relative_hints():
    """T-004-Fix: Hints sind Pfade relativ zum inneren result — der Snapshot
    rendert 'answer', nicht 'result.answer' (Planer baut valide Pfade)."""
    caps = {"chat.ai": {"available": True, "result_path_hints": ["answer"]}}
    snapshot = _caps_snapshot(caps)
    assert "answer" in snapshot
    assert "result.answer" not in snapshot


def test_template_resolution_end_to_end_with_envelope(monkeypatch):
    """Der Incident-Shape, jetzt grün: img-Child antwortet MIT Handler-Envelope
    ({"status":"completed","result":{...}}), ${img.result.artifact_id} löst zur
    Submit-Zeit gegen den enthiillten Aggregate-Eintrag auf (MockTransport-
    Muster wie test_flow_templates.py)."""
    state = {"elapsed": 0.0}
    monkeypatch.setattr("time.sleep", lambda s: None)
    monkeypatch.setattr("time.monotonic", lambda: state["elapsed"])
    submits = {}

    plan = {
        "version": 1,
        "name": "envelope-chain",
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
                # Envelope-Emittierender Handler (neue Node-Generation):
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
    result = run(api, {"task": "Zeitung als PDF"}, BASE, TOKEN,
                 ORIGIN_TASK, ORIGIN_STAGE, NODE)

    storage = submits["flow:envelope-chain:storage"]["payload"]
    assert storage["artifact_id"] == "art-env1"   # exakt → nativer Wert
    assert storage["label"] == "art-2"            # eingebettet → str()
    assert result["status"] == "completed"
    # Aggregate enthält die ENTHÜLLTEN Resultate — für Envelope- und Bare-Kinder
    # identisch (das ist der Normalisierungs-Effekt des Unwrap-once):
    assert result["result"]["aggregate"] == {
        "img": {"artifact_id": "art-env1", "count": 2},
        "storage": {"stored": True},
    }


def test_template_resolution_end_to_end_bare_still_works(monkeypatch):
    """Gegenprobe: Bare-Handler (legacy Node, keine Hülle) — identische Kette,
    unverändertes Verhalten (tolerante Phase)."""
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
    """Alter Hint-Stil (aggregate-relativ, 'result.answer') failt laut gegen
    den ENTHÜLLTEN Aggregate-Eintrag (D6 bleibt intakt statt still zu missen):
    nach dem Unwrap ist die Hülle weg — ein Pfad-Segment 'result' existiert
    dann nur noch, wenn das innere Resultat selbst einen 'result'-Key trägt.
    Genau deshalb wurden die Hints auf result-relativ umgestellt."""
    aggregate = {"img": {"answer": "x"}}  # post-unwrap: Envelope ist entfernt
    with pytest.raises(FlowError) as exc_info:
        _resolve_templates({"x": "${img.result.result.answer}"}, aggregate)
    msg = str(exc_info.value)
    assert "${img.result.result.answer}" in msg
    assert "depends_on" in msg