"""Subprocess-Integrationstests für den agent.ai-Handler (T-001c, §2.2/§4).

Pattern wie tests/test_flow_handler.py: HTTPServer-Fake-Relay (zeichnet notes
auf) + tmp HERMES_BIN-Script + RELAY_*-Env; der Handler läuft als echter
Subprocess mit PYTHONPATH=src. Design source:
.hermes/pipeline/T-001c/design.md — Ausgabe-Shapes sind FROZEN (§2.3).
"""
import json  # noqa: F401  (Phase 3: stdin-Payload / stdout-JSON)
import os  # noqa: F401  (Phase 3: os.environ für Subprocess-Env)
import subprocess
import sys  # noqa: F401  (Phase 3: sys.executable für den Subprocess)
import threading  # noqa: F401  (Phase 3: FakeRelay note-lock)
import time  # noqa: F401  (Phase 3: cadence assertions)
from http.server import (  # noqa: F401  (Phase 3: Fake-Relay-Server)
    BaseHTTPRequestHandler,
    HTTPServer,
)
from pathlib import Path

import pytest  # noqa: F401  (Phase 3: tmp_path fixtures)

REPO_ROOT = Path(__file__).resolve().parent.parent
HANDLER = REPO_ROOT / "handlers" / "agent_ai.py"


class _FakeRelay(BaseHTTPRequestHandler):
    """Zeichnet POST /notes auf (kind=longrun zählen), 200/500 steuerbar."""

    def do_POST(self) -> None:
        raise NotImplementedError

    def log_message(self, format: str, *args) -> None:  # noqa: A002, RUF100  (spec-verbatim)
        pass


def _relay_env(tmp_path: Path, port: int) -> dict[str, str]:
    """RELAY_*-Env für den Handler-Subprocess (handler_runner-Contract)."""
    raise NotImplementedError


def _hermes_script(tmp_path: Path, stdout_text: str, exit_code: int, sleep_s: float = 0.0) -> str:
    """Tmp HERMES_BIN: schreibt stdout_text, schläft sleep_s, exit exit_code."""
    raise NotImplementedError


def _run_handler(
    stdin_payload: dict,
    env_extra: dict,
    hermes_bin: str,
) -> subprocess.CompletedProcess:
    """Handler-Subprocess starten (PYTHONPATH=src, stdin-JSON rein)."""
    raise NotImplementedError


def test_handler_sends_note_on_claim() -> None:
    raise NotImplementedError


def test_handler_keepalive_cadence() -> None:
    raise NotImplementedError


def test_handler_planning_result_delivered() -> None:
    raise NotImplementedError


def test_handler_fail_soft_notes() -> None:
    raise NotImplementedError


def test_handler_error_path() -> None:
    raise NotImplementedError


def test_handler_no_relay_env() -> None:
    raise NotImplementedError


def test_extract_prompt_order() -> None:
    raise NotImplementedError