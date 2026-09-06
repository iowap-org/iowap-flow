"""Subprocess-Integrationstests für den agent.ai-Handler (T-001c, §2.2/§4).

Pattern wie tests/test_flow_handler.py: HTTPServer-Fake-Relay (zeichnet notes
auf) + tmp HERMES_BIN-Script + RELAY_*-Env; der Handler läuft als echter
Subprocess mit PYTHONPATH=src. Design source:
.hermes/pipeline/T-001c/design.md — Ausgabe-Shapes sind FROZEN (§2.3).
"""
import json
import os
import subprocess
import sys
import threading
import time  # noqa: F401  (spec-listed; cadence timing happens inside the handler subprocess)
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest  # noqa: F401  (spec-listed; the tmp_path fixture needs no pytest import)

REPO_ROOT = Path(__file__).resolve().parent.parent
HANDLER = REPO_ROOT / "handlers" / "agent_ai.py"

_NOTES_LOCK = threading.Lock()
NOTES: list[dict[str, str]] = []  # recorded note POSTs: path/task_id/message/kind


class _FakeRelay(BaseHTTPRequestHandler):
    """Zeichnet POST /notes auf (kind=longrun zählen), 200/500 steuerbar."""

    fail_notes: bool = False

    def _reply(self, obj: dict, status: int = 200) -> None:
        body = json.dumps(obj).encode()
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def do_POST(self) -> None:
        length = int(self.headers.get("Content-Length") or 0)
        body = json.loads(self.rfile.read(length)) if length else {}
        if "/notes" not in self.path:
            self._reply({}, status=404)
            return
        if type(self).fail_notes:
            self._reply({"error": "relay down"}, status=500)
            return
        task_id = self.path.rsplit("/notes", 1)[0].rsplit("/", 1)[-1]
        with _NOTES_LOCK:
            NOTES.append(
                {
                    "path": self.path,
                    "task_id": task_id,
                    "message": body.get("message", ""),
                    "kind": body.get("kind", ""),
                }
            )
        self._reply({"ok": True})

    def log_message(self, format: str, *args) -> None:  # noqa: A002, RUF100  (spec-verbatim)
        pass


class _FailingNotesRelay(_FakeRelay):
    """500 auf jede Note — Fail-Soft-Pfad (AC2)."""

    fail_notes = True


def _relay_env(tmp_path: Path, port: int) -> dict[str, str]:
    """RELAY_*-Env für den Handler-Subprocess (handler_runner-Contract)."""
    token_file = tmp_path / "token"
    token_file.write_text("test-token")
    return {
        "RELAY_BASE_URL": f"http://127.0.0.1:{port}",
        "RELAY_TOKEN_FILE": str(token_file),
        "RELAY_TASK_ID": "task-42",
        "RELAY_STAGE_ID": "stage-42",
        "RELAY_NODE_ID": "node-42",
        "RELAY_CAPABILITY": "agent.ai",
    }


def _hermes_script(tmp_path: Path, stdout_text: str, exit_code: int, sleep_s: float = 0.0) -> str:
    """Tmp HERMES_BIN: schreibt stdout_text, schläft sleep_s, exit exit_code."""
    out_file = tmp_path / "hermes_stdout.txt"
    out_file.write_text(stdout_text)
    stderr_line = 'echo "hermes exploded" >&2\n' if exit_code != 0 else ""
    script = tmp_path / "hermes.sh"
    script.write_text(
        "#!/bin/sh\n"
        f'cat "{out_file}"\n'
        f"sleep {sleep_s}\n"
        f"{stderr_line}"
        f"exit {exit_code}\n"
    )
    script.chmod(0o755)
    return str(script)


def _run_handler(
    stdin_payload: dict,
    env_extra: dict,
    hermes_bin: str,
) -> subprocess.CompletedProcess:
    """Handler-Subprocess starten (PYTHONPATH=src, stdin-JSON rein)."""
    env = {
        "PATH": os.environ.get("PATH", "/usr/bin:/bin"),
        "HOME": os.environ.get("HOME", "/tmp"),
        "PYTHONPATH": str(REPO_ROOT / "src"),
        "HERMES_BIN": hermes_bin,
    }
    env.update(env_extra)
    return subprocess.run(
        [sys.executable, str(HANDLER)],
        input=json.dumps(stdin_payload),
        capture_output=True,
        text=True,
        env=env,
        timeout=60,
        cwd=REPO_ROOT,
        check=False,
    )


def test_handler_sends_note_on_claim(tmp_path) -> None:
    NOTES.clear()
    server = HTTPServer(("127.0.0.1", 0), _FakeRelay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        hermes = _hermes_script(tmp_path, "planned", 0, sleep_s=0.1)
        proc = _run_handler({"task": "plan this"}, _relay_env(tmp_path, server.server_address[1]), hermes)
        assert proc.returncode == 0, f"stderr: {proc.stderr}"
        result = json.loads(proc.stdout)
        assert result["status"] == "completed"
        assert result["result"]["answer"] == "planned"
        with _NOTES_LOCK:
            notes = list(NOTES)
        assert any(n["kind"] == "longrun" for n in notes), "no kind=longrun note sent on claim"
        assert notes[0]["task_id"] == "task-42"
        assert notes[0]["message"] == "agent.ai: claimed, planning started"
    finally:
        server.shutdown()


def test_handler_keepalive_cadence(tmp_path) -> None:
    NOTES.clear()
    server = HTTPServer(("127.0.0.1", 0), _FakeRelay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        hermes = _hermes_script(tmp_path, "planned", 0, sleep_s=0.2)
        env = _relay_env(tmp_path, server.server_address[1])
        env["AGENT_AI_KEEPALIVE_INTERVAL_SECONDS"] = "0.05"  # D9 test knob
        proc = _run_handler({"task": "plan"}, env, hermes)
        assert proc.returncode == 0, f"stderr: {proc.stderr}"
        with _NOTES_LOCK:
            notes = list(NOTES)
        assert len(notes) >= 3, f"expected >= 3 notes, got {len(notes)}"
        ticks = [n for n in notes if n["message"].startswith("keepalive: still working")]
        assert len(ticks) >= 2, f"expected >= 2 keepalive ticks, got {len(ticks)}"
        assert all(n["kind"] == "longrun" for n in notes)
    finally:
        server.shutdown()


def test_handler_planning_result_delivered(tmp_path) -> None:
    NOTES.clear()
    plan = {
        "version": 1,
        "name": "demo",
        "tasks": [{"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []}],
        "summary": "s",
    }
    server = HTTPServer(("127.0.0.1", 0), _FakeRelay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        hermes = _hermes_script(tmp_path, json.dumps(plan), 0)
        proc = _run_handler({"task": "demo"}, _relay_env(tmp_path, server.server_address[1]), hermes)
        assert proc.returncode == 0, f"stderr: {proc.stderr}"
        result = json.loads(proc.stdout)
        assert result["status"] == "completed"
        # extract_plan_json digs result.answer — the frozen interface for the flow runner
        assert json.loads(result["result"]["answer"]) == plan
        assert result["result"]["task_id"] == "task-42"
        assert result["result"]["stage_id"] == "stage-42"
        assert result["result"]["capability"] == "agent.ai"
        assert isinstance(result["result"]["elapsed_seconds"], float)
        assert "stdout_length" in result["debug"]
    finally:
        server.shutdown()


def test_handler_fail_soft_notes(tmp_path) -> None:
    NOTES.clear()
    server = HTTPServer(("127.0.0.1", 0), _FailingNotesRelay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        hermes = _hermes_script(tmp_path, "planned", 0)
        proc = _run_handler({"task": "plan"}, _relay_env(tmp_path, server.server_address[1]), hermes)
        assert proc.returncode == 0, f"stderr: {proc.stderr}"
        result = json.loads(proc.stdout)
        assert result["status"] == "completed"  # note failures never break planning (AC2)
        assert result["result"]["answer"] == "planned"
        assert "longrun note failed" in proc.stderr  # fail-soft, logged
    finally:
        server.shutdown()


def test_handler_error_path(tmp_path) -> None:
    server = HTTPServer(("127.0.0.1", 0), _FakeRelay)
    threading.Thread(target=server.serve_forever, daemon=True).start()
    try:
        hermes = _hermes_script(tmp_path, "boom", 3)  # exits 3, writes stderr
        proc = _run_handler({"task": "plan"}, _relay_env(tmp_path, server.server_address[1]), hermes)
        assert proc.returncode == 0  # D7: exit 0 even on failure
        result = json.loads(proc.stdout)
        assert result["status"] == "error"
        assert "exited with code 3" in result["error"]
        assert "hermes exploded" in result.get("stderr", "")
    finally:
        server.shutdown()


def test_handler_no_relay_env(tmp_path) -> None:
    hermes = _hermes_script(tmp_path, "planned", 0)
    proc = _run_handler({"task": "plan"}, {}, hermes)  # no RELAY_* env at all
    assert proc.returncode == 0
    result = json.loads(proc.stdout)
    assert result["status"] == "completed"
    assert result["result"]["answer"] == "planned"
    assert "skipping longrun" in proc.stderr  # D5: skip logged, no crash, no keepalive


def test_extract_prompt_order() -> None:
    sys.path.insert(0, str(REPO_ROOT / "handlers"))
    try:
        import agent_ai

        # task wins first (D6 — the flow runner submits payload={"task": PLAN_PROMPT})
        assert agent_ai.extract_prompt({"task": "plan me", "prompt": "p"}) == "plan me"
        # live fallback order preserved
        assert agent_ai.extract_prompt({"prompt": "p", "question": "q"}) == "p"
        assert agent_ai.extract_prompt({"question": "q", "message": "m"}) == "q"
        assert agent_ai.extract_prompt({"message": "m", "input": "i"}) == "m"
        assert agent_ai.extract_prompt({"input": "i"}) == "i"
        # empty-string values fall through to the next key
        assert agent_ai.extract_prompt({"task": "", "prompt": "p"}) == "p"
        # unknown/empty payload → whole payload as JSON envelope (live mirror)
        assert agent_ai.extract_prompt({}) == "{}"
        assert agent_ai.extract_prompt({"other": "x"}) == '{"other": "x"}'
    finally:
        sys.path.pop(0)