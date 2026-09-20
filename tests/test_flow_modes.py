# tests/test_flow_modes.py — T-003: Modus-Dispatch in run() über MockTransport
import json

import pytest

from flow_node.catalog import load_history_record
from flow_node.relay_api import RelayApi
from flow_node.runner import run
from tests.test_flow_runner import (
    BASE,
    NODE,
    ORIGIN_STAGE,
    ORIGIN_TASK,
    PLAN,
    TOKEN,
    VALID_CAPS,
    _caps_response,
    _fake_clock,
    _ok,
    _patch_http,
    _submit_response,
    _task_view,
)


def _run_flow(api, payload, monkeypatch, tmp_path):
    monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    return run(api=api, payload=payload, base_url=BASE, token_file=TOKEN,
               origin_task_id=ORIGIN_TASK, origin_stage_id=ORIGIN_STAGE,
               node_id=NODE, options={})


def test_list_flows_completes_without_plan_child(monkeypatch, tmp_path):
    (tmp_path / "templates").mkdir()
    (tmp_path / "templates" / "demo.json").write_text(json.dumps(PLAN))
    seen = []

    def handler(req):
        seen.append((req.method, str(req.url)))
        url = str(req.url)
        if url.endswith("/discovery/capabilities"):
            return _caps_response(VALID_CAPS)
        if "/stages/" in url and "complete" in url:
            return _ok()
        pytest.fail(f"unexpected request: {url}")  # kein submit, kein poll

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    result = _run_flow(api, {"mode": "list_flows"}, monkeypatch, tmp_path)
    assert result["status"] == "completed"
    assert result["flows"][0]["name"] == "demo"
    assert result["flows"][0]["available"] is True
    assert not any("tasks" in u and "submit" in u for _, u in seen)  # kein Kind submitted


def test_save_flow_completes_from_history(monkeypatch, tmp_path):
    for d in ("templates", "history"):
        (tmp_path / d).mkdir()
    hid_calls = []

    def handler(req):
        hid_calls.append(str(req.url))
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    from flow_node.catalog import record_history
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    hid = record_history("demo", PLAN, {"a": 1}, "task-x")

    result = _run_flow(api, {"mode": "save_flow", "history_id": hid}, monkeypatch, tmp_path)
    assert result["status"] == "completed"
    assert result["flow"] == "demo"
    assert load_history_record(hid)["history_id"] == hid  # Historie bleibt (Cleanup räumt später)


def test_save_flow_missing_history_fails_stage(monkeypatch, tmp_path):
    failed = []

    def handler(req):
        if "stage" in str(req.url) and req.method == "POST":
            failed.append(str(req.url))
            return _ok()
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    from flow_node.runner import FlowError
    with pytest.raises(FlowError, match="save_flow failed"):
        _run_flow(api, {"mode": "save_flow", "history_id": "nope"}, monkeypatch, tmp_path)
    assert failed  # Stage wurde gefailt


def test_run_flow_skips_plan_phase_and_records_history(monkeypatch, tmp_path):
    (tmp_path / "templates").mkdir()
    (tmp_path / "history").mkdir()
    (tmp_path / "templates" / "demo.json").write_text(json.dumps(PLAN))
    submits = []

    def handler(req):
        url = str(req.url)
        if url.endswith("/discovery/capabilities"):
            return _caps_response(VALID_CAPS)
        if url.endswith("/task-simple") and req.method == "POST":
            body = json.loads(req.content.decode() if isinstance(req.content, bytes) else req.content)
            submits.append(body)
            task_id = f"child-{len(submits)}"
            return _submit_response(task_id)
        if "/tasks/" in url and req.method == "GET":
            task_id = url.rsplit("/", 1)[-1].split("?")[0]
            return _task_view(task_id, "completed", result={"r": 1})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    _fake_clock(monkeypatch)
    result = _run_flow(
        api,
        {"mode": "run_flow", "flow": "demo", "input": {"x": 1}},
        monkeypatch, tmp_path,
    )
    assert result["status"] == "completed"
    # KEIN Planungs-Kind: genau so viele submits wie Plan-Tasks (a, b)
    assert len(submits) == 2
    assert not any(json.dumps(s).find("flow:_plan") != -1 for s in submits)
    # Historie geschrieben
    (tmp_path / "history" / "demo.json").exists()  # dir exists; record:
    import os
    records = [f for f in os.listdir(tmp_path / "history") if f.endswith(".json")]
    assert len(records) == 1
    rec = load_history_record(records[0].removesuffix(".json"))
    assert rec["flow_name"] == "demo"


def test_run_flow_input_templates_resolve(monkeypatch, tmp_path):
    plan_with_input = json.loads(json.dumps(PLAN))
    plan_with_input["tasks"][0]["payload"] = {"city": "${input.stadt}"}
    (tmp_path / "templates").mkdir()
    (tmp_path / "history").mkdir()
    (tmp_path / "templates" / "demo.json").write_text(json.dumps(plan_with_input))
    captured = []

    def handler(req):
        url = str(req.url)
        if url.endswith("/discovery/capabilities"):
            return _caps_response(VALID_CAPS)
        if url.endswith("/task-simple") and req.method == "POST":
            body = json.loads(req.content.decode() if isinstance(req.content, bytes) else req.content)
            captured.append(body)
            return _submit_response(f"child-{len(captured)}")
        if "/tasks/" in url and req.method == "GET":
            task_id = url.rsplit("/", 1)[-1].split("?")[0]
            return _task_view(task_id, "completed", result={"r": 1})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    _fake_clock(monkeypatch)
    result = _run_flow(
        api,
        {"mode": "run_flow", "flow": "demo", "input": {"stadt": "Leipzig"}},
        monkeypatch, tmp_path,
    )
    assert result["status"] == "completed"
    first_submit_payload = captured[0].get("payload") or {}
    assert first_submit_payload.get("city") == "Leipzig"  # ${input.stadt} aufgelöst


def test_classic_run_still_records_history(monkeypatch, tmp_path):
    """Klassischer Plan-Lauf (D9) schreibt bei Erfolg ebenfalls Historie."""
    import os
    (tmp_path / "history").mkdir()
    captured = []

    def handler(req):
        url = str(req.url)
        if url.endswith("/discovery/capabilities"):
            return _caps_response(VALID_CAPS)
        if url.endswith("/task-simple") and req.method == "POST":
            n = len([c for c in captured if c.get("name", "").startswith("flow:")])
            captured.append({})
            return _submit_response("plan-child" if n == 0 else f"child-{n}")
        if "/tasks/plan-child" in url and req.method == "GET":
            return _task_view("plan-child", "completed", result=json.dumps(PLAN))
        if "/tasks/child-" in url and req.method == "GET":
            return _task_view(url.rsplit("/", 1)[-1].split("?")[0], "completed", result={"r": 1})
        return _ok()

    _patch_http(monkeypatch, handler)
    api = RelayApi(BASE, TOKEN)
    _fake_clock(monkeypatch)
    result = _run_flow(api, {"task": "mach demo"}, monkeypatch, tmp_path)
    assert result["status"] == "completed"
    records = [f for f in os.listdir(tmp_path / "history") if f.endswith(".json")]
    assert len(records) == 1