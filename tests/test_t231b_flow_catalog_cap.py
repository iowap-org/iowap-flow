"""T-231b: flow.catalog capability — split of management surface from flow.run.

Ronny 2026-10-16: management (history/catalog) gets its own capability so
it is discoverable and callable as a plain task — instead of being buried
inside ``flow.run`` payload modes.

Contract:
- ``flow_node.runner.MANAGEMENT_MODES`` = frozenset({"list_flows", "list_history",
  "show_history", "save_flow", "define_flow"}).
- ``flow_node.runner.manage(payload, api, base_url, token_file,
  origin_task_id, origin_stage_id, node_id)`` — same shape as run(),
  handles ONLY the five management modes; unknown/missing mode → FlowError.
- ``run()`` routes payload modes in MANAGEMENT_MODES to manage() — the
  existing flow.run payload API stays backwards-compatible (T-003 modes
  still work over flow.run).
- ``handlers/flow_catalog.py`` — thin delegate to
  ``flow_node.runner.manage`` with the same stdin/stdout/error contract as
  flow_run.py.
"""

from __future__ import annotations

import json
from unittest.mock import MagicMock

import pytest

from flow_node import catalog as catalog_mod
from flow_node.relay_api import RelayApi
from flow_node.runner import MANAGEMENT_MODES, manage, run
from tests.test_flow_modes import _run_flow
from tests.test_flow_runner import (
    BASE,
    NODE,
    ORIGIN_STAGE,
    ORIGIN_TASK,
    PLAN,
    TOKEN,
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


def _write_history(ts: int) -> str:
    catalog_mod.history_dir().mkdir(parents=True, exist_ok=True)
    hid = f"t1-demo-{ts}"
    (catalog_mod.history_dir() / f"{hid}.json").write_text(json.dumps({
        "history_id": hid, "flow_name": "demo",
        "origin_task_id": "t1", "saved_at": ts,
        "plan": _SMALL_PLAN, "aggregate": {},
    }))
    return hid


def _manage(payload, monkeypatch, tmp_path):
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
    api = MagicMock()
    result = manage(payload, api, BASE, TOKEN, ORIGIN_TASK, ORIGIN_STAGE, NODE)
    return result, api


class TestManagementModesContract:
    def test_management_modes_export_exact_set(self):
        assert MANAGEMENT_MODES == frozenset({
            "list_flows", "list_history", "show_history", "save_flow", "define_flow",
        })

    def test_manage_list_history(self, monkeypatch, tmp_path):
        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        hid = _write_history(1700000003)
        result, api = _manage({"mode": "list_history", "limit": 10}, monkeypatch, tmp_path)
        assert result["status"] == "completed"
        assert result["result"]["mode"] == "list_history"
        assert result["result"]["records"][0]["history_id"] == hid
        api.complete_stage.assert_called_once()

    def test_manage_show_history(self, monkeypatch, tmp_path):
        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        hid = _write_history(1700000003)
        result, api = _manage({"mode": "show_history", "history_id": hid}, monkeypatch, tmp_path)
        assert result["result"]["record"]["history_id"] == hid

    def test_manage_show_history_unknown_id_fails_stage(self, monkeypatch, tmp_path):
        from flow_node.runner import FlowError

        with pytest.raises(FlowError):
            _manage({"mode": "show_history", "history_id": "nope-xyz-1"},
                    monkeypatch, tmp_path)

    def test_manage_define_flow(self, monkeypatch, tmp_path):
        result, api = _manage({"mode": "define_flow", "name": "weather-report",
                               "plan": _SMALL_PLAN}, monkeypatch, tmp_path)
        assert result["result"]["flow"] == "weather-report"
        assert (tmp_path / "templates" / "weather-report.json").exists()
        api.fail_stage.assert_not_called()

    def test_manage_unknown_mode_fails(self, monkeypatch, tmp_path):
        from flow_node.runner import FlowError

        with pytest.raises(FlowError):
            _manage({"mode": "explode_everything"}, monkeypatch, tmp_path)


class TestRunRoutesManagementModes:
    def test_run_still_routes_management_mode_to_manage(self, monkeypatch, tmp_path):
        """Backwards-compat: T-003 modes over flow.run keep working."""
        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        hid = _write_history(1700000003)
        result = _run_flow(
            MagicMock(), {"mode": "list_history", "limit": 5}, monkeypatch, tmp_path
        )
        assert result["result"]["mode"] == "list_history"
        assert result["result"]["records"][0]["history_id"] == hid


class TestFlowCatalogHandler:
    def test_flow_catalog_delegate_exists_and_dispatches_manage(self, monkeypatch, tmp_path):
        """handlers/flow_catalog.py: read stdin envelope → manage() → stdout envelope."""
        import importlib.util
        from pathlib import Path

        spec = importlib.util.spec_from_file_location(
            "flow_catalog", Path(__file__).parents[1] / "handlers" / "flow_catalog.py")
        mod = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(mod)  # noqa: F841 — import proves the delegate exists

        monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
        monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
        hid = _write_history(1700000003)
        payload = {
            "task_id": "t_1", "capability": "flow.catalog",
            "input": {"mode": "show_history", "history_id": hid},
        }
        monkeypatch.setattr("sys.stdin", MagicMock())
        monkeypatch.setattr("sys.stdin", type("S", (), {"read": staticmethod(lambda: json.dumps(payload))})())
        captured = {}

        def fake_run_manage(payload, **kw):
            captured.update(payload)
            return {"status": "completed", "result": {"mode": "show_history"}}

        monkeypatch.setattr(mod, "manage", fake_run_manage)
        monkeypatch.setattr(
            "sys.argv", ["flow_catalog.py"],
        )
        monkeypatch.setenv("RELAY_BASE_URL", BASE)
        monkeypatch.setenv("RELAY_TOKEN_FILE", TOKEN)
        monkeypatch.setenv("RELAY_TASK_ID", "t_1")
        monkeypatch.setenv("RELAY_STAGE_ID", "s_1")
        monkeypatch.setenv("RELAY_NODE_ID", NODE)
        import io

        monkeypatch.setattr("sys.stdin", io.StringIO(json.dumps(payload)))
        try:
            mod.main()
        except SystemExit as exc:
            assert exc.code in (None, 0)
        assert captured["mode"] == "show_history"
        assert captured["history_id"] == hid