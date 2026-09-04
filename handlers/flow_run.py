"""flow_run.py — Handler-Einstiegspunkt für Capability flow.run (T-172).

SKELETT (Phase 2, Scaffold): Nur Konstanten/Signaturen/Docstrings — Pipeline in Phase 3
(Plan Task 5). Läuft als `python3 /app/handlers/flow_run.py` (profiles/node.yaml).

stdin/stdout-Contract (gemäß handler_runner, Plan Task 5 / §0-Fakt):
  stdin : {"task": "...", "options": {...}}
  env   : RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_STAGE_ID, RELAY_TASK_ID, RELAY_NODE_ID
  stdout: NUR das finale JSON-Result (kein Logging auf stdout — Logging → stderr)
          Erfolg:  {"status": "completed", "flow": {...}, "aggregate": {...}, "summary": "..."}
          Fehler : exit 1 + stderr "reason: ..." (Server-Retry-Pfad greift)
"""
from __future__ import annotations

import json  # noqa: F401  (Phase 3: stdin-JSON lesen)
import os  # noqa: F401  (Phase 3: RELAY_*-Env lesen)
import sys  # noqa: F401  (Phase 3: stderr / exit 1)

from flow_node.discovery import fetch_capabilities  # noqa: F401  (Phase 3)
from flow_node.plan import PlanError  # noqa: F401  (Phase 3: Raises)
from flow_node.relay_api import RelayApi  # noqa: F401  (Phase 3)
from flow_node.runner import (  # noqa: F401  (Phase 3)
    build_plan_prompt,
    extract_plan_json,
    run,
)

# Frozen Konstanten (Plan §2.4/§3 Task 4/5)
PLAN_CAPABILITY = "agent.ai"      # Planungs-Kind (idempotency_key flow-<origin>-_plan)
ORIGIN_PAYLOAD_KEY = "task"       # stdin-Payload-Key für die wörtliche Aufgabe


def main() -> None:
    """Handler-Pipeline (Plan Task 5):
      stdin-JSON lesen → Phase 0 (Planung): fetch_capabilities (Planungs-Snapshot) →
      build_plan_prompt → Planungs-Kind an agent.ai submittet → poll + extract_plan_json →
      parse_plan → validate_capabilities_live (frisch) → runner.run(...).
    Fehler: PlanError → stderr mit Grund, exit 1 (kein Planungs-Result/kein valides JSON
    nach D9-Retry → stderr "reason: plan phase failed: <grund>", exit 1).
    stdout NUR das finale JSON-Result."""
    raise NotImplementedError


if __name__ == "__main__":
    main()