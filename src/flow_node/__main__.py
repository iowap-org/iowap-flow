#!/usr/bin/env python3
"""Handler entry point for flow.run (T-172, plan task 5).

stdin/stdout contract per handler_runner (plan §2.4, T-005d):
  stdin:  request envelope (T-005b) as a JSON string —
          {"task_id": ..., "capability": ..., "input": {...payload...}}
          (older daemons send the payload flat or mirrored — run()
          accepts all three generations). payload = input contains the
          dispatcher payload: "task" or {"mode": "run_flow", "flow": ...}.
  env:    RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_STAGE_ID, RELAY_TASK_ID,
          RELAY_NODE_ID (provided by iowap-node handler_runner)
  stdout: ONLY the final JSON result (no logging on stdout!)
          success:  {"status": "completed", "result": {"flow": {...},
                   "aggregate": ..., "summary": ...}} (response envelope,
                   T-005d; without an "error" key — the daemon counts
                   failures via key presence)
  error:   exit 1 + reason on stderr (flow.run fails the stage with exit 1 +
          stderr reason so the server retry path applies — plan §2.4).

Pipeline (plan task 5):
  read stdin → run() [fetch_caps → submit planning child → poll+extract →
  parse_plan → validate_capabilities_live → fan-out → join] → stdout JSON
"""
from __future__ import annotations

import json
import os
import sys

from flow_node.plan import PlanError
from flow_node.relay_api import RelayApi
from flow_node.runner import FlowError, _envelope_payload, run


def _log(msg: str) -> None:
    """Logging goes to stderr — stdout is reserved for the final JSON result."""
    print(msg, file=sys.stderr, flush=True)


def _fail(reason: str) -> SystemExit:
    """Plan task 5: exit 1 + stderr 'reason: <reason>' (stage fail via exit != 0)."""
    _log(f"reason: {reason}")
    return SystemExit(1)


def main() -> int:
    """Handler entry point: stdin payload → run flow → stdout JSON."""
    # stdin: {"task": ..., "original_request": ..., "options": {...}} (§2.4)
    # The server passes the stage payload verbatim; the capability docs
    # prescribe 'original_request', older clients send 'task'.
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as e:
        raise _fail(f"invalid stdin JSON: {e}") from e
    if not isinstance(payload, dict):
        raise _fail("stdin payload must be a JSON object")
    # T-005d: strip the Request Envelope BEFORE the T-003 gate — the gate
    # inspects the dispatcher's task payload ('task'/'original_request'/
    # 'mode'), which for strict stdin lives inside 'input'. Same three-
    # generation discriminator the runner applies in run() (design.md §6);
    # stripping here keeps run()'s own call idempotent.
    payload = _envelope_payload(payload)
    # T-003: list_flows/save_flow don't need 'task'; run_flow doesn't either
    # (the runner sets task_text itself). The classic run stays mandatory.
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
        # Plan task 5: PlanError → stderr with reason, exit 1 (server marks stage failed)
        # FlowError → exit 1 with stderr "reason: plan phase failed: <reason>" resp. reason
        raise _fail(str(e)) from e
    except Exception as e:
        # Last line of defense for the stdout-only contract: relay outages (httpx.ConnectError
        # etc.) must NOT end up as a traceback on stderr — the server retry path parses
        # "reason: <reason>". The traceback is still preserved via exception chaining.
        raise _fail(f"handler failed: {type(e).__name__}: {e}") from e

    # stdout ONLY the final JSON result (no logging on stdout!)
    json.dump(result, sys.stdout)
    sys.stdout.write("\n")
    sys.stdout.flush()
    return 0


if __name__ == "__main__":
    sys.exit(main())