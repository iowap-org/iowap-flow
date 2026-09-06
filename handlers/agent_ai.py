"""agent_ai.py — Handler-Einstiegspunkt für Capability agent.ai (T-001c).

Läuft als Shell-Handler (iowap-node handler_runner-Kontrakt), z.B.
  python3 /app/handlers/agent_ai.py  (bzw. mit dem Pfad des Deploy-Hosts).
Braucht das flow_node-Package importierbar (PYTHONPATH=src oder installiert).

Contract (handler_runner, identisch zum Live-Handler ~/.relay/handlers/
hermes-handler.py, modulo die dokumentierten Abweichungen D6/D7 in
.hermes/pipeline/T-001c/design.md):
  stdin : Stage-Payload als JSON-Objekt
  env   : RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_TASK_ID, RELAY_STAGE_ID,
          RELAY_NODE_ID, RELAY_CAPABILITY  (handler_runner setzt immer)
          HERMES_BIN (default /home/felix/.local/bin/hermes)
          HERMES_TIMEOUT (default 280 Sekunden)
          AGENT_AI_KEEPALIVE_INTERVAL_SECONDS (optional float, Tests)
  stdout: NUR das finale JSON-Result (Logging → stderr)
          Erfolg:  {"status": "completed", "result": {"answer": ...,
                   "stage_id": ..., "task_id": ..., "capability": ...,
                   "elapsed_seconds": ...}, "debug": {...}}
          Fehler:  {"status": "error", "error": ...}
  exit  : 0 in beiden Fällen (D7 — der Daemon completed die Stage aus
          dem stdout-JSON; der flow-Runner behandelt error-Results über
          den D9-Retry).

T-001c (Fix a): direkt nach dem Claim eine kind=longrun-Note an den Task
(T-154: claimed -> accepted + 2h-Lease), Keepalive-Notes während des
Hermes-LLM-Calls (Kadenz KEEPALIVE_INTERVAL_SECONDS, per Env für Tests
überschreibbar). Note-Fehler sind fail-soft (AC2): Planning läuft weiter.
"""
from __future__ import annotations

import json  # noqa: F401  (Phase 3: stdin-JSON lesen)
import os  # noqa: F401  (Phase 3: RELAY_*/HERMES_*-Env lesen)
import subprocess  # noqa: F401  (Phase 3: Hermes-Subprocess)
import sys
import time  # noqa: F401  (Phase 3: elapsed_seconds)

from flow_node.keepalive import NoteKeepalive  # noqa: F401  (Phase 3: T-001c keepalive)
from flow_node.relay_api import RelayApi  # noqa: F401  (Phase 3: RelayApi-Instanz)

DEFAULT_HERMES_BIN = "/home/felix/.local/bin/hermes"
DEFAULT_HERMES_TIMEOUT = 280  # live mirror (D8)
PROMPT_KEYS = ("task", "prompt", "question", "message", "input")  # D6 order


def _log(msg: str) -> None:
    """Logging auf stderr — stdout bleibt dem finalen JSON-Result vorbehalten."""
    print(msg, file=sys.stderr, flush=True)


def extract_prompt(payload: dict) -> str:
    """D6: erstes nicht-leeres Feld aus PROMPT_KEYS (task zuerst — der
    flow-Runner submittet payload={"task": PLAN_PROMPT}), sonst
    json.dumps(payload). FROZEN order."""
    raise NotImplementedError


def main() -> int:
    """Handler-Einstiegspunkt: stdin-Payload → longrun-Note am Claim
    (T-001c) → Hermes-Subprocess mit Keepalive → Result-JSON auf stdout.
    Liefert immer 0 (D7)."""
    raise NotImplementedError


if __name__ == "__main__":
    raise SystemExit(main())