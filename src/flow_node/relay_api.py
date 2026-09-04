"""Relay-Task-Client für flow.run: fan-out + notes + status an einer Stelle.

SKELETT (Phase 2, Scaffold): Nur Signaturen/Docstrings (Task 3, Plan §3) —
HTTP-Logik in Phase 3 (httpx, MockTransport-getestet).

Frozen API-Fläche (Plan Task 3 — Signaturen bytegenau übernommen):
  RelayApi(base_url, token_file, timeout=20.0)
    .submit_simple_task(...) -> dict
    .get_task(task_id) -> dict
    .add_note(task_id, message, kind="info") -> None
    .complete_stage(task_id, stage_id, result) -> None
    .fail_stage(task_id, stage_id, error) -> None

Basis-Endpunkte (alles bestehend, Bearer aus RELAY_TOKEN_FILE):
  POST /relay/v2/scheduler/task-simple
  GET  /relay/v2/scheduler/tasks/{id}
  POST /relay/v2/scheduler/tasks/{id}/notes
  POST /relay/v2/scheduler/stages/{stage_id}/complete  (complete + fail)
"""
from __future__ import annotations

import httpx  # noqa: F401  (Phase 3: HTTP-Transport)


class RelayApi:
    def __init__(self, base_url: str, token_file: str, timeout: float = 20.0):
        # Phase 3: base_url/token_file/timeout speichern (token_file lazy lesen)
        raise NotImplementedError

    def submit_simple_task(
        self,
        capability: str,
        payload: dict,
        name: str,
        idempotency_key: str | None = None,
        priority: int = 0,
        timeout_seconds: int | None = None,
    ) -> dict:
        # POST /relay/v2/scheduler/task-simple → {"task_id","stage_id",...}
        raise NotImplementedError

    def get_task(self, task_id: str) -> dict:
        # GET /relay/v2/scheduler/tasks/{id} → TaskView (stages mit status/result)
        raise NotImplementedError

    def add_note(self, task_id: str, message: str, kind: str = "info") -> None:
        # POST /relay/v2/scheduler/tasks/{id}/notes  {"message": ..., "kind": ...}
        raise NotImplementedError

    def complete_stage(self, task_id: str, stage_id: str, result: dict) -> None:
        # POST /relay/v2/scheduler/stages/{stage_id}/complete
        # {"node_id": env RELAY_NODE_ID, "task_id": ..., "result": ...}
        raise NotImplementedError

    def fail_stage(self, task_id: str, stage_id: str, error: str) -> None:
        # same endpoint, {"error": error}
        raise NotImplementedError