# tests/test_flow_handler.py — Task 5 (Handler-Einstiegspunkt, subprocess-basierter Integrationstest: stdin-JSON rein, stdout-JSON raus), Stubs Phase 2
import json  # noqa: F401  (Phase 3: stdin/stdout-JSON)
import os  # noqa: F401  (Phase 3: RELAY_*-Env für Subprocess)
import subprocess
import sys  # noqa: F401  (Phase 3: python executable path)
from pathlib import Path  # noqa: F401  (Phase 3: Handler-Pfad im Repo auflösen)


def _run_handler(
    stdin_payload: dict,
    env_extra: dict,
) -> subprocess.CompletedProcess:
    """Test-Helfer (Phase 3): Handler-Subprocess mit RELAY_*-Env + stdin-JSON starten."""
    raise NotImplementedError


def test_stdout_contains_only_final_result_json():
    # stdout NUR das finale JSON-Result; kein Logging auf stdout (Plan Task 5, Pitfall 6)
    assert False  # TODO


def test_handler_completes_happy_path():
    # stdin {"task": ..., "options": ...} → stdout {"status": "completed", ...}
    assert False  # TODO


def test_plan_phase_failure_exits_1_with_reason_on_stderr():
    # kein Planungs-Result/kein valides JSON nach Retry → exit 1 + stderr
    # "reason: plan phase failed: <grund>"
    assert False  # TODO


def test_plan_error_exits_1_with_ground_on_stderr():
    # PlanError → stderr mit Grund, exit 1 (Server macht Stage failed)
    assert False  # TODO