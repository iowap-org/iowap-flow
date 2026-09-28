"""T-005d follow-up: __main__.main() must accept the strict Request Envelope.

Regression probe (2026-09-28, post-push 8aaa0b8): ``python -m flow_node``
fed the strict envelope {\"task_id\", \"capability\", \"input\": {...}}
failed with ``reason: flow payload missing 'task' (original request)``
because the T-003 payload gate in ``main()`` runs BEFORE the envelope
strip inside ``run()``. The gate must see the stripped payload — same
discriminator the runner already uses (design.md §6, three daemon
generations).
"""
from __future__ import annotations

import io
import json
import sys

import pytest

from flow_node import __main__ as flow_main


def test_main_accepts_strict_envelope_stdin(monkeypatch, capsys):
    """Strict envelope stdin reaches the T-003 gate as the inner payload.

    The gate must NOT fail with "flow payload missing 'task'" — instead the
    handler proceeds until the missing-relay-environment check (no env set).
    """
    envelope = {
        "task_id": "task_test",
        "capability": "flow.run",
        "input": {"original_request": "Erstelle ein Bild und beschreibe es"},
    }
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps(envelope)))
    monkeypatch.delenv("RELAY_BASE_URL", raising=False)
    monkeypatch.delenv("RELAY_TOKEN_FILE", raising=False)
    monkeypatch.delenv("RELAY_TASK_ID", raising=False)
    monkeypatch.delenv("RELAY_STAGE_ID", raising=False)

    with pytest.raises(SystemExit) as excinfo:
        flow_main.main()

    # Missing env fails the stage (exit 1); the T-003 payload gate passed.
    # Discriminator: the gate would say "missing 'task'" instead.
    assert excinfo.value.code == 1
    assert "missing relay environment" in capsys.readouterr().err


def test_main_still_fails_on_flat_payload_without_task(monkeypatch, capsys):
    """Legacy behaviour kept: flat stdin without task/original_request fails."""
    monkeypatch.setattr(sys, "stdin", io.StringIO(json.dumps({"mode": "run"})))
    with pytest.raises(SystemExit) as excinfo:
        flow_main.main()
    assert excinfo.value.code == 1
    assert "missing 'task'" in capsys.readouterr().err