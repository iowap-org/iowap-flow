"""flow_run.py — handler entry point for capability flow.run (T-172, plan task 5).

Runs as `python3 /app/handlers/flow_run.py` (profiles/node.yaml — frozen contract
§2.4, registered on the node). Thin delegate to the implemented pipeline in
`flow_node.__main__` (phase 3): stdin/stdout handling, env check, the
stdout-only contract and the exit-1 paths live there — this file only holds
the path and constant contracts, keeping profile and code in sync.

Contract (per handler_runner, plan task 5 / §0 fact):
  stdin : {"task": "...", "options": {...}}
  env   : RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_STAGE_ID, RELAY_TASK_ID, RELAY_NODE_ID
  stdout: ONLY the final JSON result (no logging on stdout — logging → stderr)
          success:  {"status": "completed", "flow": {...}, "aggregate": {...}, "summary": "..."}
          error : exit 1 + stderr "reason: ..." (server retry path applies)
"""
from __future__ import annotations

from flow_node.__main__ import main

# Frozen constants (plan §2.4/§3 task 4/5) — the source of truth is
# flow_node.runner (PLAN_CAPABILITY) resp. __main__ (ORIGIN_PAYLOAD_KEY = "task");
# mirrored here only as documentation of the profile contract.
PLAN_CAPABILITY = "agent.ai"      # planning child (idempotency_key flow-<origin>-_plan)
ORIGIN_PAYLOAD_KEY = "task"       # stdin payload key for the verbatim task

if __name__ == "__main__":
    raise SystemExit(main())