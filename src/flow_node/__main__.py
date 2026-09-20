#!/usr/bin/env python3
"""Handler-Einstiegspunkt für flow.run (T-172, Plan Task 5).

stdin/stdout-Contract gemäß handler_runner (Plan §2.4):
  stdin:  Payload {"task": "<die Aufgabe, wörtlich>", "options": {...optional}}
  env:    RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_STAGE_ID, RELAY_TASK_ID,
          RELAY_NODE_ID (geliefert von iowap-node handler_runner)
  stdout: NUR das finale JSON-Result (kein Logging auf stdout!)
          Erfolg:  {"status": "completed", "flow": {...}, "aggregate": ..., "summary": ...}
  Fehler:  exit 1 + Grund auf stderr (flow.run failt die Stage mit exit 1 +
          stderr-Grund, damit der Server-Retry-Pfad greift — Plan §2.4).

Pipeline (Plan Task 5):
  read stdin → run() [fetch_caps → submit Planungs-Kind → poll+extract →
  parse_plan → validate_capabilities_live → fan-out → join] → stdout JSON
"""
from __future__ import annotations

import json
import os
import sys

from flow_node.plan import PlanError
from flow_node.relay_api import RelayApi
from flow_node.runner import FlowError, run


def _log(msg: str) -> None:
    """Logging geht auf stderr — stdout bleibt dem finalen JSON-Result vorbehalten."""
    print(msg, file=sys.stderr, flush=True)


def _fail(reason: str) -> SystemExit:
    """Plan Task 5: exit 1 + stderr 'reason: <grund>' (Stage-Fail via exit != 0)."""
    _log(f"reason: {reason}")
    return SystemExit(1)


def main() -> int:
    """Handler-Einstiegspunkt: stdin-Payload → Flow ausführen → stdout JSON."""
    # stdin: {"task": ..., "original_request": ..., "options": {...}} (§2.4)
    # Der Server übergibt das Stage-Payload wörtlich; die Capability-Doku
    # schreibt 'original_request' vor, ältere Clients senden 'task'.
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as e:
        raise _fail(f"invalid stdin JSON: {e}") from e
    if not isinstance(payload, dict):
        raise _fail("stdin payload must be a JSON object")
    # T-003: list_flows/save_flow brauchen kein 'task'; run_flow auch nicht
    # (der Runner setzt task_text selbst). Klassischer Lauf bleibt Pflichtfeld.
    mode = payload.get("mode")
    if mode not in ("list_flows", "run_flow", "save_flow") and not (
        payload.get("task") or payload.get("original_request")
    ):
        raise _fail("flow payload missing 'task' (original request)")

    # env: Relay-Kontext vom handler_runner (§2.4) — Pflichtfelder
    base_url = os.environ.get("RELAY_BASE_URL")
    token_file = os.environ.get("RELAY_TOKEN_FILE")
    origin_task_id = os.environ.get("RELAY_TASK_ID")
    origin_stage_id = os.environ.get("RELAY_STAGE_ID")
    missing = [
        name
        for name, value in (
            ("RELAY_BASE_URL", base_url),
            ("RELAY_TOKEN_FILE", token_file),
            ("RELAY_TASK_ID", origin_task_id),
            ("RELAY_STAGE_ID", origin_stage_id),
        )
        if not value
    ]
    if missing:
        raise _fail(f"missing relay environment: {missing}")

    options = payload.get("options") if isinstance(payload.get("options"), dict) else {}
    api = RelayApi(base_url=base_url, token_file=token_file)

    try:
        result = run(
            api=api,
            payload=payload,
            base_url=base_url,
            token_file=token_file,
            origin_task_id=origin_task_id,
            origin_stage_id=origin_stage_id,
            node_id=os.environ.get("RELAY_NODE_ID", ""),
            options=options,
        )
    except (PlanError, FlowError) as e:
        # Plan Task 5: PlanError → stderr mit Grund, exit 1 (Server macht Stage failed)
        # FlowError → exit 1 mit stderr "reason: plan phase failed: <grund>" bzw. Grund
        raise _fail(str(e)) from e
    except Exception as e:
        # Letzte Verteidigung des stdout-only-Contracts: Relay-Ausfälle (httpx.ConnectError
        # etc.) dürfen NICHT als Traceback auf stderr enden — der Server-Retry-Pfad parst
        # "reason: <grund>". Traceback bleibt trotzdem im Exception-Chaining erhalten.
        raise _fail(f"handler failed: {type(e).__name__}: {e}") from e

    # stdout NUR das finale JSON-Result (kein Logging auf stdout!)
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())