"""flow_run.py — Handler-Einstiegspunkt für Capability flow.run (T-172, Plan Task 5).

Läuft als `python3 /app/handlers/flow_run.py` (profiles/node.yaml — FROZEN-Kontrakt
§2.4, im Node registriert). Dünner Delegat auf die implementierte Pipeline in
`flow_node.__main__` (Phase 3): dort liegen stdin/stdout-Handling, Env-Check,
stdout-only-Contract und exit-1-Pfade — dieser File hält nur die Pfad- und
Konstanten-Verträge, damit Profil und Code synchron bleiben.

Contract (gemäß handler_runner, Plan Task 5 / §0-Fakt):
  stdin : {"task": "...", "options": {...}}
  env   : RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_STAGE_ID, RELAY_TASK_ID, RELAY_NODE_ID
  stdout: NUR das finale JSON-Result (kein Logging auf stdout — Logging → stderr)
          Erfolg:  {"status": "completed", "flow": {...}, "aggregate": {...}, "summary": "..."}
          Fehler : exit 1 + stderr "reason: ..." (Server-Retry-Pfad greift)
"""
from __future__ import annotations

from flow_node.__main__ import main

# Frozen Konstanten (Plan §2.4/§3 Task 4/5) — Quelle der Wahrheit ist
# flow_node.runner (PLAN_CAPABILITY) bzw. __main__ (ORIGIN_PAYLOAD_KEY = "task");
# hier nur gespiegelt als Dokumentation des Profil-Vertrags.
PLAN_CAPABILITY = "agent.ai"      # Planungs-Kind (idempotency_key flow-<origin>-_plan)
ORIGIN_PAYLOAD_KEY = "task"       # stdin-Payload-Key für die wörtliche Aufgabe

if __name__ == "__main__":
    raise SystemExit(main())