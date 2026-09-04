# tests/test_flow_handler.py — Task 5 (Handler-Einstiegspunkt, subprocess-basierter Integrationstest: stdin-JSON rein, stdout-JSON raus)
"""Subprocess-Integrationstests für den flow.run-Handler (Plan Task 5, §2.4).

Der Handler läuft als `python -m flow_node` mit RELAY_*-Env; das Relay wird
nicht gemockt (Subprocess!), stattdessen werden die Fehler-Pfade getestet,
die ohne funktionierendes Relay garantiert eintreten, und der Happy-Path mit
einem minimalen Fake-Relay (python http.server) für die Plan-Phase.
"""
import json
import os
import subprocess
import sys
import threading
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path
from typing import ClassVar

import pytest

REPO_ROOT = Path(__file__).resolve().parent.parent

VALID_PLAN = {
    "version": 1,
    "name": "demo",
    "tasks": [
        {"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []},
    ],
    "summary": "s",
}


def _relay_env(tmp_path: Path, port: int) -> dict[str, str]:
    """RELAY_*-Env für den Handler-Subprocess (handler_runner-Contract §2.4)."""
    token_file = tmp_path / "token"
    token_file.write_text("test-token")
    return {
        "RELAY_BASE_URL": f"http://127.0.0.1:{port}",
        "RELAY_TOKEN_FILE": str(token_file),
        "RELAY_TASK_ID": "origin-task-1",
        "RELAY_STAGE_ID": "origin-stage-1",
        "RELAY_NODE_ID": "flow-runner-01",
    }


def _run_handler(
    stdin_payload: dict,
    env_extra: dict,
) -> subprocess.CompletedProcess:
    """Handler-Subprocess mit RELAY_*-Env + stdin-JSON starten (Plan §2.4)."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        # src/-Layout: Package ohne Install importierbar machen
        "PYTHONPATH": str(REPO_ROOT / "src"),
    }
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, "-m", "flow_node"],
        input=json.dumps(stdin_payload),
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        cwd=REPO_ROOT,
        check=False,
    )


class _FakeRelayHandler(BaseHTTPRequestHandler):
    """Minimal-Fake-Relay: Discovery, task-simple-Submit, TaskView-Poll, Complete.

    Happy-Path-Script: Planungs-Kind (agent.ai) → valides Plan-JSON →
    Fan-out-Kind (tool.x) → completed → Ursprungs-Stage complete.
    """

    plan = VALID_PLAN

    def log_message(self, *args):  # stdout-Noise im Test-Output unterdrücken
        pass

    def _json_body(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        return json.loads(self.rfile.read(length)) if length else {}

    def _reply(self, obj: dict, status: int = 200) -> None:
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_GET(self) -> None:
        if self.path == "/relay/v2/discovery/capabilities":
            self._reply({"capabilities": {"agent.ai": {"available": True}, "tool.x": {"available": True}}})
        elif "/relay/v2/scheduler/tasks/" in self.path:
            self._reply(
                {
                    "task_id": self.path.rsplit("/", 1)[-1],
                    "stages": [{"status": "completed", "result": json.dumps(type(self).plan)}],
                }
            )
        else:
            self._reply({}, status=404)

    def do_POST(self) -> None:
        if self.path == "/relay/v2/scheduler/task-simple":
            self._reply({"task_id": "child-1", "stage_id": "child-stage-1"})
        elif "/notes" in self.path or "/complete" in self.path:
            self._reply({"ok": True})
        else:
            self._reply({}, status=404)


class _UnreachableRelayHandler(_FakeRelayHandler):
    """Plan-Phase-Fail: agent.ai-Planungs-Kind → zweimal failed (D9-Retry ausgeschöpft)."""

    def do_GET(self) -> None:
        if self.path == "/relay/v2/discovery/capabilities":
            self._reply({"capabilities": {"agent.ai": {"available": True}}})
        elif "/relay/v2/scheduler/tasks/" in self.path:
            self._reply({"task_id": self.path.rsplit("/", 1)[-1], "stages": [{"status": "failed", "error": "llm exploded"}]})
        else:
            self._reply({}, status=404)


class _InvalidPlanRelayHandler(_FakeRelayHandler):
    """PlanError-Pfad: agent.ai liefert zweimal ein strukturell ungültiges Plan-JSON."""

    plan: ClassVar[dict] = {"version": 1, "name": "broken", "tasks": [], "summary": "s"}


@pytest.fixture()
def fake_relay():
    """Startet das Fake-Relay in einem Thread; yieldet (port, server)."""
    server = HTTPServer(("127.0.0.1", 0), _FakeRelayHandler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    yield server.server_address[1], server
    server.shutdown()
    thread.join(timeout=5)


def _start_relay(handler_cls) -> HTTPServer:
    server = HTTPServer(("127.0.0.1", 0), handler_cls)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    return server


def test_handler_completes_happy_path(fake_relay, tmp_path):
    # stdin {"task": ..., "options": ...} → stdout {"status": "completed", ...}
    port, _ = fake_relay
    proc = _run_handler(
        {"task": "Erstelle 3 Produktbilder und zippe sie", "options": {}},
        _relay_env(tmp_path, port),
    )
    assert proc.returncode == 0, f"stderr: {proc.stderr}"
    result = json.loads(proc.stdout)
    assert result["status"] == "completed"
    assert result["flow"]["name"] == "demo"
    assert result["flow"]["tasks_done"] == 1
    assert "aggregate" in result and "summary" in result


def test_stdout_contains_only_final_result_json(fake_relay, tmp_path):
    # stdout NUR das finale JSON-Result; kein Logging auf stdout (Plan Task 5, Pitfall 6)
    port, _ = fake_relay
    proc = _run_handler(
        {"task": "demo", "options": {}},
        _relay_env(tmp_path, port),
    )
    assert proc.returncode == 0, f"stderr: {proc.stderr}"
    # stdout ist exakt EINE JSON-Zeile + Trailing-Newline — parsebarer Block, nichts davor/danach
    assert proc.stdout.count("\n") == 1 and proc.stdout.endswith("\n")
    result = json.loads(proc.stdout)
    assert result["status"] == "completed"
    assert set(result) == {"status", "flow", "aggregate", "summary"}


def test_plan_phase_failure_exits_1_with_reason_on_stderr(tmp_path):
    # kein Planungs-Result/kein valides JSON nach Retry → exit 1 + stderr
    # "reason: plan phase failed: <grund>"
    server = _start_relay(_UnreachableRelayHandler)
    try:
        proc = _run_handler(
            {"task": "demo", "options": {}},
            _relay_env(tmp_path, server.server_address[1]),
        )
    finally:
        server.shutdown()
    assert proc.returncode == 1
    assert proc.stdout == ""  # kein halbes Result auf stdout
    assert "reason: plan phase failed:" in proc.stderr
    assert "llm exploded" in proc.stderr


def test_plan_error_exits_1_with_ground_on_stderr(tmp_path):
    # PlanError → stderr mit Grund, exit 1 (Server macht Stage failed)
    server = _start_relay(_InvalidPlanRelayHandler)
    try:
        proc = _run_handler(
            {"task": "demo", "options": {}},
            _relay_env(tmp_path, server.server_address[1]),
        )
    finally:
        server.shutdown()
    assert proc.returncode == 1
    assert proc.stdout == ""
    assert "reason:" in proc.stderr
    assert "no tasks" in proc.stderr  # PlanError-Grund aus parse_plan (§2.2)