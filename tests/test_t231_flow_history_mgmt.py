"""T-231: Flow-Catalog/History management via flow.run-modes (Features 2+3).

Feature 2 — defined flows as an always-addressable list:
  ``run_flow`` skips the agent.ai plan phase (T-003, existing) and
  ``list_flows`` (existing) exposes the catalog; NEW: ``mode: define_flow``
  writes a catalog entry directly from the payload — no history detour.

Feature 3 — promote from history, query/manage via capabilities:
  NEW ``mode: list_history`` (newest first, slim records), NEW
  ``mode: show_history``, and the ``history_id`` rides in the completed
  run result (previously reachable only as a free-form note string).
  Existing: ``save_flow`` promotion, ``${input.x}`` templates, cleanup.

All new modes return the T-003 conforming shape
``{"status": "completed", "result": {"mode": ..., ...}}`` and follow the
save_flow convention: invalid ids/names raise CatalogError → D6 fail-fast.
"""

from __future__ import annotations

import json

import httpx
import pytest

from flow_node import catalog as catalog_mod
from flow_node.catalog import CatalogError
from flow_node.relay_api import RelayApi
from flow_node.runner import PLAN_CAPABILITY, PLAN_FORWARD_SLOT, FlowError, run
from tests.test_flow_modes import _run_flow
from tests.test_flow_runner import (
    BASE,
    NODE,
    ORIGIN_STAGE,
    ORIGIN_TASK,
    PLAN,
    TOKEN,
    VALID_CAPS,
    _caps_response,
    _ok,
    _patch_http,
    _submit_response,
    _task_view,
)

_SMALL_PLAN = {
    "version": 1,
    "name": "weather-report",
    "summary": "s",
    "tasks": [
        {"id": "a", "capability": "chat.ai",
         "payload": {"prompt": "${input.city}"}, "depends_on": []},
    ],
}


# ── catalog unit helpers ────────────────────────────────────────


class TestCatalogHelpers:
    def test_list_history_records_newest_first_slim_shape(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        for n, ts in enumerate((1700000001, 1700000003, 1700000002)):
            catalog_mod.history_dir().mkdir(parents=True, exist_ok=True)
            (catalog_mod.history_dir() / f"t{n}-demo-{ts}.json").write_text(
                json.dumps({
                    "history_id": f"t{n}-demo-{ts}", "flow_name": "demo",
                    "origin_task_id": f"t{n}", "saved_at": ts,
                    "plan": _SMALL_PLAN, "aggregate": {"x": n},
                })
            )
        records = catalog_mod.list_history_records()
        assert [r["saved_at"] for r in records] == [1700000003, 1700000002, 1700000001], (
            "newest first"
        )
        slim = records[0]
        assert set(slim.keys()) == {
            "history_id", "flow_name", "origin_task_id", "saved_at",
        }, "list view stays slim — no plan/aggregate blobs"
        assert records[0]["history_id"] == "t1-demo-1700000003"

        assert len(catalog_mod.list_history_records(limit=1)) == 1

    def test_list_history_records_fail_soft_on_broken_file(self, tmp_path, monkeypatch):
        monkeypatch.chdir(tmp_path)
        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        h = tmp_path / "history"
        h.mkdir()
        (h / "broken-x-123.json").write_text("{not json")
        assert catalog_mod.list_history_records() == []

    def test_define_flow_writes_catalog_and_normalizes_name(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        name = catalog_mod.define_flow("Weather  Report!", _SMALL_PLAN)
        assert name == "Weather_Report_", "canonical norm is _safe_name (no lowercasing)"
        loaded = catalog_mod.load_flow(name)
        assert loaded["name"] == "weather-report"

    def test_define_flow_rejects_non_dict_plan(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        with pytest.raises(CatalogError, match="plan"):
            catalog_mod.define_flow("demo", ["not", "a", "dict"])

    def test_define_flow_rejects_scrap_plan(self, tmp_path, monkeypatch):
        """T-231c: define validates via parse_plan — no plan poisons the catalog."""
        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        with pytest.raises(CatalogError, match="tasks"):
            catalog_mod.define_flow("scrap1", {"version": 1, "name": "x", "summary": "s"})  # no tasks
        with pytest.raises(CatalogError, match="version"):
            catalog_mod.define_flow("scrap2", {"version": 2, "name": "x", "summary": "s", "tasks": [{"id": "a"}]})
        with pytest.raises(CatalogError, match="duplicate"):
            catalog_mod.define_flow("scrap3", {
                "version": 1, "name": "x", "summary": "s",
                "tasks": [{"id": "a", "capability": "c", "payload": {}}, {"id": "a", "capability": "c", "payload": {}}, {"id": "b", "capability": "c", "payload": {}, "depends_on": ["a"]}],
            })
        assert not (tmp_path / "templates" / "scrap1.json").exists()
        assert not (tmp_path / "templates" / "scrap2.json").exists()

    def test_define_flow_accepts_valid_plan(self, tmp_path, monkeypatch):
        from tests.test_flow_modes import PLAN

        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        name = catalog_mod.define_flow("real-flow", PLAN)
        assert name == "real-flow"
        assert catalog_mod.load_flow(name)["version"] == 1

    def test_define_flow_rejects_schrott_missing_tasks(self, tmp_path, monkeypatch):
        """T-231c: Schrott ablehnen — same validation as run_flow, at upload."""
        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        with pytest.raises(CatalogError, match="tasks"):
            catalog_mod.define_flow("scrap", {"version": 1, "name": "scrap"})

    def test_define_flow_rejects_unknown_dep(self, tmp_path, monkeypatch):
        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        bad = {"version": 1, "name": "bad", "summary": "s",
               "tasks": [{"id": "a", "capability": "chat.ai", "depends_on": ["ghost"]}]}
        with pytest.raises(CatalogError, match="unknown depends_on"):
            catalog_mod.define_flow("bad", bad)


# ── runner modes ────────────────────────────────────────────────


class TestRunnerModes:
    def test_define_flow_mode_writes_catalog(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock

        api = MagicMock()
        _run_flow(api, {
            "mode": "define_flow", "name": "weather-report", "plan": _SMALL_PLAN,
        }, monkeypatch, tmp_path)
        assert (tmp_path / "templates" / "weather-report.json").exists()
        api.fail_stage.assert_not_called()

    def test_list_history_mode_returns_slim_records(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock

        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        api = MagicMock()
        catalog_mod.history_dir().mkdir(parents=True, exist_ok=True)
        (catalog_mod.history_dir() / "t1-demo-1700000003.json").write_text(
            json.dumps({
                "history_id": "t1-demo-1700000003", "flow_name": "demo",
                "origin_task_id": "t1", "saved_at": 1700000003,
                "plan": _SMALL_PLAN, "aggregate": {},
            })
        )
        result = _run_flow(api, {"mode": "list_history", "limit": 10}, monkeypatch, tmp_path)
        assert result["status"] == "completed"
        assert result["result"]["mode"] == "list_history"
        assert result["result"]["records"][0]["history_id"] == "t1-demo-1700000003"
        api.fail_stage.assert_not_called()

    def test_show_history_mode_returns_full_record(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock

        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        api = MagicMock()
        catalog_mod.history_dir().mkdir(parents=True, exist_ok=True)
        rec = {
            "history_id": "t1-demo-1700000003", "flow_name": "demo",
            "origin_task_id": "t1", "saved_at": 1700000003,
            "plan": _SMALL_PLAN, "aggregate": {"a": {"done": 1}},
        }
        (catalog_mod.history_dir() / "t1-demo-1700000003.json").write_text(json.dumps(rec))
        result = _run_flow(api, {"mode": "show_history", "history_id": "t1-demo-1700000003"},
                           monkeypatch, tmp_path)
        assert result["result"]["mode"] == "show_history"
        assert result["result"]["record"]["aggregate"] == {"a": {"done": 1}}

    def test_show_history_unknown_id_fails_flow(self, monkeypatch, tmp_path):
        from unittest.mock import MagicMock

        api = MagicMock()
        with pytest.raises(FlowError):
            _run_flow(api, {"mode": "show_history", "history_id": "nope-xyz-1"},
                      monkeypatch, tmp_path)


# ── classic run reports its history_id ──────────────────────────


def test_successful_classic_run_result_carries_history_id(monkeypatch, tmp_path):
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    complete_bodies = []

    def handler(request: httpx.Request) -> httpx.Response:
        from httpx import Response

        url = request.url.path
        if url == "/relay/v2/discovery/capabilities":
            return _caps_response(VALID_CAPS)
        if url == "/relay/v2/scheduler/task-simple" and request.method == "POST":
            body = json.loads(request.content)
            if body["capability"] == PLAN_CAPABILITY:
                assert body["payload"]["forward_slot"] == PLAN_FORWARD_SLOT
                return _submit_response("plan-child-1")
            return _submit_response(f"child-{body['name']}")
        if url.startswith("/relay/v2/scheduler/tasks/"):
            task_id = url.split("/")[5]
            if task_id == "plan-child-1":
                return _task_view(task_id, "completed", result={"result": json.dumps(PLAN)})
            return _task_view(task_id, "completed", result={"done": task_id})
        if "/stages/" in url and "complete" in url:
            complete_bodies.append(json.loads(request.content))
            return Response(200, json={"ok": True})
        return _ok()

    _patch_http(monkeypatch, handler)
    result = run(RelayApi(BASE, TOKEN), {"task": "Create 3 product images"},
                 BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)
    assert result["status"] == "completed"
    assert len(complete_bodies) == 1
    # T-005d contract: the complete body carries the handler ENVELOPE under
    # "result" — the run's own payload is the nested level.
    envelope = complete_bodies[0]["result"]
    assert envelope.get("status") == "completed"
    hid = envelope.get("result", {}).get("history_id")
    assert hid and f"{ORIGIN_TASK}-demo-" in hid, (
        "T-231 F3: the consumer gets the history_id directly in the result"
    )
    records = catalog_mod.list_history_records()
    assert records and records[0]["history_id"] == hid