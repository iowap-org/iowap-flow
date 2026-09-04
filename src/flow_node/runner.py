"""Plan-Phase + Fan-out + Join-Loop für flow.run (Herzstück, T-172).

SKELETT (Phase 2, Scaffold): Nur Signaturen/Frozen-Konstanten/Docstrings —
Semantik in Phase 3 (Plan Task 4 + 4b: fail-fast D6, D9-Retry, Poll-Backoff,
Lease-Keepalive).
"""
from __future__ import annotations

import time  # noqa: F401  (Phase 3: Poll-Takt / Keepalive-Timer)

from .discovery import fetch_capabilities, validate_capabilities_live  # noqa: F401
from .plan import Plan, PlanError  # noqa: F401
from .relay_api import RelayApi

# ---- Frozen Konstanten (Plan Task 4/4b + §2.4/§2.5) -------------------------

PLAN_CAPABILITY = "agent.ai"          # Planungs-Kind läuft auf agent.ai (Task 4, Step 0)
PLAN_TASK_REF = "_plan"               # idempotency_key: flow-<origin>-_plan
IDEMPOTENCY_PREFIX = "flow"           # idempotency_key: flow-<ursprungs-task-id>-<task-id> (§2.5)
TASK_NAME_PREFIX = "flow:"            # task_name: flow:<flow-name>:<task-id> (§2.5)
POLL_INTERVAL_SECONDS = 5.0           # Poll-Loop 5s (D5)
POLL_BACKOFF_MAX_SECONDS = 60.0       # exponentiell bis 60s (Task 4b)
POLL_BACKOFF_MAX_ERRORS = 3           # nach 3 aufeinanderfolgenden HTTP-Fehlern → Backoff (Task 4b)
KEEPALIVE_INTERVAL_SECONDS = 600.0    # Keepalive-Note alle 600s, unabhängiger Timer (Task 4b, §2.4)
DEFAULT_MAX_FLOW_SECONDS = 14400      # 4h = Stage-Timeout des Submits (Task 4b, §2.1)

# §2.3 PLAN_PROMPT-Template (FROZEN — bytegenau aus dem Plan übernommen). Platzhalter:
# {task} und {capabilities_snapshot}. ACHTUNG: Die Plan-Schema-Zeile enthält LITERALE
# JSON-Braces ({"version": 1, ...}) — eine naive str.format() über das ganze Template
# würde dort brechen; Substitutions-Mechanik entscheidet Phase 3.
PLAN_PROMPT = """\
Du bist der Planner eines IOWAP-Task-Clusters. Erstelle einen Ausführungsplan
für folgende Aufgabe. Antworte NUR mit dem Plan-JSON (Schema siehe unten),
kein weiterer Text.

Aufgabe:
{task}

Verfügbare Capabilities (Live-Snapshot, von flow.run beim Planungs-Submit
eingebettet — plane ausschließlich mit diesen):
{capabilities_snapshot}

Plan-Schema:
{"version": 1, "name": "...", "tasks": [{"id": "...", "capability": "...",
  "payload": {...}, "depends_on": [...]}], "summary": "..."}
Regeln: ids eindeutig; depends_on nur auf existierende ids; keine Zyklen;
Payload-Felder exakt wie im input_schema der Capability; halte die Zahl der
Tasks minimal.
"""


class FlowError(RuntimeError):
    """Flow-Level-Fehler (Plan-Phase, Fan-out, Join) — führt zu exit 1 + stderr-Grund."""


def build_plan_prompt(task: str, capabilities_snapshot: str) -> str:
    """Baut den PLAN_PROMPT (§2.3): task + Cap-Snapshot ins FROZEN Template einbetten."""
    raise NotImplementedError


def extract_plan_json(result_text: str) -> dict:
    """Extrahiert lenient das Plan-JSON aus dem agent.ai-Result: erstes JSON-Objekt
    mit 'tasks'-Array, rekursiv eine Ebene tief — NICHT freetext-Regex (Task 5)."""
    raise NotImplementedError


def run(
    api: RelayApi,
    payload: dict,
    base_url: str,
    token_file: str,
    origin_task_id: str,
    origin_stage_id: str,
    node_id: str,
    options: dict | None = None,
) -> dict:
    """Führt den kompletten Flow aus: Plan-Phase → Validierung → Fan-out → Join → Aggregate.

    Semantik (Plan Task 4, fail-fast D6):
      0. Plan-Phase: Note 'longrun' 'flow started' → Discovery-Snapshot → PLAN_PROMPT →
         Planungs-Kind an agent.ai (idempotency_key flow-<origin>-_plan, Name
         flow:<name>:_plan) → Note 'planning started' → poll bis done → Result lenient
         extrahieren. Kein valides Plan-JSON → D9: 1 Retry mit Validerings-Fehler im
         Prompt, 2. Versuch failt → Flow failed mit Grund.
      1. Validierung: parse_plan + validate_capabilities_live gegen FRISCHEN Snapshot.
      2. Topologisch: alle Tasks mit erfüllten deps gleichzeitig submitten (echtes
         Parallel-Fan-out), idempotency_key flow-<origin-task-id>-<task-id>.
      3. Payload-Injection: 'flow': {origin_task_id, task_ref} — nur wenn input_schema
         es erlaubt, sonst weglassen (§3, Task 4 Step 3).
      4. Poll-Loop 5s pro Kind; Keepalive-Note alle ~10 Min: 'flow progress: done=K/N'.
      5. Kind failed (permanent) → gesamter Flow failt: Note 'flow failed at <task-ref>
         (<capability>): <error>' → fail_stage() → exit 1.
      6. Alle done → aggregate {task_ref: result} → complete_stage() mit
         {'status': 'completed', 'aggregate': ..., 'summary': plan.summary}.
      7. Nach jeder Änderung am Fan-out-Zustand: Note kind=info am Ursprungs-Task.
    """
    raise NotImplementedError