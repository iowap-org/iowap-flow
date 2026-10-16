#!/usr/bin/env python3
"""Handler entry point for flow.catalog (T-231b).

Companion of flow.run (handlers/flow_run.py): exposes the catalog/history
management surface as its own capability (Ronny 2026-10-16 — the manage
API is callable as a plain task, discoverable separately from execution).

stdin/stdout contract per handler_runner (plan §2.4, T-005d), identical
to flow_run.py:
  stdin:  request envelope (T-005b) as a JSON string —
          {"task_id": ..., "capability": "flow.catalog", "input": {...}}
          input carries payload["mode"] (REQUIRED, one of list_flows |
          list_history | show_history | save_flow | define_flow) plus the
          mode arguments: history_id, limit, name, plan.
  env:    RELAY_BASE_URL, RELAY_TOKEN_FILE, RELAY_TASK_ID, RELAY_STAGE_ID,
          RELAY_NODE_ID (provided by iowap-node handler_runner)
  stdout: ONLY the final JSON result — success:
          {"status": "completed", "result": {"mode": ..., ...}}
          (no "error" key — the daemon counts failures by key presence)
  error:  exit 1 + "reason: ..." on stderr (server marks the stage failed).
"""
from __future__ import annotations

import json
import os
import sys

from flow_node.relay_api import RelayApi
from flow_node.runner import MANAGEMENT_MODES, FlowError, manage


def _log(msg: str) -> None:
    """Logging goes to stderr — stdout is reserved for the final JSON result."""
    print(msg, file=sys.stderr, flush=True)


def _fail(reason: str) -> SystemExit:
    _log(f"reason: {reason}")
    return SystemExit(1)


def main() -> int:
    """Handler entry point: stdin payload → manage() → stdout JSON."""
    try:
        raw = sys.stdin.read()
        payload = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError as e:
        raise _fail(f"invalid stdin JSON: {e}") from e
    if not isinstance(payload, dict):
        raise _fail("stdin payload must be a JSON object")
    # T-005d: strip the Request Envelope before the mode gate — same
    # three-generation discriminator the runner applies (design.md §6).
    from flow_node.runner import _envelope_payload

    payload = _envelope_payload(payload)
    mode = payload.get("mode")
    if mode not in MANAGEMENT_MODES:
        raise _fail(
            "catalog payload missing/unknown 'mode' (one of: "
            + ", ".join(sorted(MANAGEMENT_MODES)) + ")"
        )

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
        result = manage(
            api=api,
            payload=payload,
            base_url=base_url,
            token_file=token_file,
            origin_task_id=origin_task_id,
            origin_stage_id=origin_stage_id,
            node_id=os.environ.get("RELAY_NODE_ID", ""),
            options=options,
        )
    except FlowError as e:
        raise _fail(str(e)) from e
    except Exception as e:  # noqa: BLE001 — stdout-only contract last line of defense
        # Relay outages (httpx.ConnectError etc.) must not end up as a
        # traceback on stderr; exit 1 keeps the server retry path intact.
        _log(f"reason: {type(e).__name__}: {e}")
        return 1
    json.dump(result, sys.stdout, default=str)
    sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())