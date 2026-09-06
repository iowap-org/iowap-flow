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

import json
import os
import subprocess
import sys
import time

from flow_node.keepalive import NoteKeepalive
from flow_node.relay_api import RelayApi

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
    for key in PROMPT_KEYS:
        value = payload.get(key)
        if value:
            return value
    return json.dumps(payload)


def main() -> int:
    """Handler-Einstiegspunkt: stdin-Payload → longrun-Note am Claim
    (T-001c) → Hermes-Subprocess mit Keepalive → Result-JSON auf stdout.
    Liefert immer 0 (D7)."""
    # -- 1. stdin payload (absent/invalid → {}, live mirror) --
    try:
        raw = sys.stdin.read()
    except Exception:  # noqa: BLE001  (absent/closed stdin → empty payload, design.md §2.2 step 1)
        raw = ""
    try:
        parsed = json.loads(raw) if raw.strip() else {}
    except json.JSONDecodeError:
        parsed = {}
    payload = parsed if isinstance(parsed, dict) else {}

    task_id = os.environ.get("RELAY_TASK_ID", "")
    stage_id = os.environ.get("RELAY_STAGE_ID", "")
    capability = os.environ.get("RELAY_CAPABILITY", "")
    base_url = os.environ.get("RELAY_BASE_URL", "")
    token_file = os.environ.get("RELAY_TOKEN_FILE", "")
    hermes_bin = os.environ.get("HERMES_BIN", DEFAULT_HERMES_BIN)
    try:
        hermes_timeout = float(os.environ.get("HERMES_TIMEOUT", "") or DEFAULT_HERMES_TIMEOUT)
    except ValueError:
        hermes_timeout = float(DEFAULT_HERMES_TIMEOUT)

    keepalive: NoteKeepalive | None = None
    output: dict
    started = time.monotonic()
    try:
        # -- 2. prompt + claim log --
        prompt = extract_prompt(payload)
        _log(f"[agent-ai] CLAIMED task={task_id} stage={stage_id}")

        # -- 3. longrun note right after claim (THE fix, T-154; fail-soft D5) --
        if base_url and token_file and task_id:
            interval: float | None = None
            raw_interval = os.environ.get("AGENT_AI_KEEPALIVE_INTERVAL_SECONDS", "")
            if raw_interval:
                try:
                    interval = float(raw_interval)
                except ValueError:
                    interval = None
                    _log(f"[agent-ai] invalid AGENT_AI_KEEPALIVE_INTERVAL_SECONDS={raw_interval!r}")
            api = RelayApi(base_url, token_file)
            keepalive = NoteKeepalive(api, task_id, interval=interval, log=_log)
            keepalive.start("agent.ai: claimed, planning started")
        else:
            _log("[agent-ai] relay env incomplete — skipping longrun notes")

        # -- 4. Hermes subprocess (live mirror, D8) --
        result = subprocess.run(  # noqa: PLW1510  (exit code is handled explicitly below)
            [hermes_bin, "-z", prompt],
            capture_output=True,
            text=True,
            timeout=hermes_timeout,
            env={**os.environ, "HERMES_CLI_MODE": "1"},
        )
        elapsed = time.monotonic() - started
        stdout = result.stdout.strip()
        stderr = result.stderr.strip()

        # -- 5. frozen output shapes (§2.3) --
        if result.returncode == 0:
            output = {
                "status": "completed",
                "result": {
                    "answer": stdout,
                    "stage_id": stage_id,
                    "task_id": task_id,
                    "capability": capability,
                    "elapsed_seconds": round(elapsed, 1),
                },
                "debug": {
                    "stderr": stderr[-2000:] if stderr else "",
                    "stdout_length": len(stdout),
                },
            }
        else:
            output = {
                "status": "error",
                "error": f"Hermes exited with code {result.returncode}",
                "stderr": stderr[-2000:] if stderr else "",
            }
    except subprocess.TimeoutExpired:
        output = {"status": "error", "error": f"Hermes timed out after {hermes_timeout}s"}
    except FileNotFoundError:
        output = {"status": "error", "error": f"Hermes binary not found at {hermes_bin}"}
    except Exception as exc:  # noqa: BLE001  (D7 — never exit non-zero: error JSON instead)
        _log(f"[agent-ai] unexpected error: {exc}")
        output = {"status": "error", "error": str(exc)}
    finally:
        if keepalive is not None:
            keepalive.stop()

    json.dump(output, sys.stdout)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())