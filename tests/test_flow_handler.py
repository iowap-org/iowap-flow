# tests/test_flow_handler.py — task 5 (handler entry point, subprocess-based integration test: stdin JSON in, stdout JSON out)
"""Subprocess integration tests for the flow.run handler (plan task 5, §2.4).

The handler runs as `python -m flow_node` with RELAY_* env; the relay is
not mocked (subprocess!) — instead the error paths are tested that are
guaranteed to occur without a working relay, plus the happy path with
a minimal fake relay (python http.server) for the plan phase.
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
    """RELAY_* env for the handler subprocess (handler_runner contract §2.4)."""
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
    """Start handler subprocess with RELAY_* env + stdin JSON (plan §2.4)."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        # src/-layout: make the package importable without install
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
    """Minimal fake relay: discovery, task-simple submit, TaskView poll, complete.

    Happy-path script: planning child (agent.ai) → valid plan JSON →
    fan-out child (tool.x) → completed → origin stage complete.
    """

    plan = VALID_PLAN

    def log_message(self, *args):  # suppress stdout noise in the test output
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
    """Plan phase fail: agent.ai planning child → failed twice (D9 retry exhausted)."""

    def do_GET(self) -> None:
        if self.path == "/relay/v2/discovery/capabilities":
            self._reply({"capabilities": {"agent.ai": {"available": True}}})
        elif "/relay/v2/scheduler/tasks/" in self.path:
            self._reply({"task_id": self.path.rsplit("/", 1)[-1], "stages": [{"status": "failed", "error": "llm exploded"}]})
        else:
            self._reply({}, status=404)


class _InvalidPlanRelayHandler(_FakeRelayHandler):
    """PlanError path: agent.ai delivers structurally invalid plan JSON twice."""

    plan: ClassVar[dict] = {"version": 1, "name": "broken", "tasks": [], "summary": "s"}


@pytest.fixture()
def fake_relay():
    """Starts the fake relay in a thread; yields (port, server)."""
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
        {"task": "Create 3 product images and zip them", "options": {}},
        _relay_env(tmp_path, port),
    )
    assert proc.returncode == 0, f"stderr: {proc.stderr}"
    result = json.loads(proc.stdout)
    assert result["status"] == "completed"
    assert result["result"]["flow"]["name"] == "demo"
    assert result["result"]["flow"]["tasks_done"] == 1
    assert "aggregate" in result["result"] and "summary" in result["result"]


def test_stdout_contains_only_final_result_json(fake_relay, tmp_path):
    # stdout ONLY the final JSON result; no logging on stdout (plan task 5, pitfall 6)
    port, _ = fake_relay
    proc = _run_handler(
        {"task": "demo", "options": {}},
        _relay_env(tmp_path, port),
    )
    assert proc.returncode == 0, f"stderr: {proc.stderr}"
    # stdout is exactly ONE JSON line + trailing newline — parseable block, nothing before/after
    assert proc.stdout.count("\n") == 1 and proc.stdout.endswith("\n")
    result = json.loads(proc.stdout)
    assert result["status"] == "completed"
    assert set(result) == {"status", "result"}
    assert set(result["result"]) == {"flow", "aggregate", "summary"}


def test_plan_phase_failure_exits_1_with_reason_on_stderr(tmp_path):
    # no planning result/no valid JSON after retry → exit 1 + stderr
    # "reason: plan phase failed: <reason>"
    server = _start_relay(_UnreachableRelayHandler)
    try:
        proc = _run_handler(
            {"task": "demo", "options": {}},
            _relay_env(tmp_path, server.server_address[1]),
        )
    finally:
        server.shutdown()
    assert proc.returncode == 1
    assert proc.stdout == ""  # no half result on stdout
    assert "reason: plan phase failed:" in proc.stderr
    assert "llm exploded" in proc.stderr


def test_plan_error_exits_1_with_ground_on_stderr(tmp_path):
    # PlanError → stderr with reason, exit 1 (server marks the stage failed)
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
    assert "no tasks" in proc.stderr  # PlanError reason from parse_plan (§2.2)