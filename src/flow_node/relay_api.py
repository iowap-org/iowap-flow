"""Relay-Task-Client für flow.run: fan-out + notes + status an einer Stelle.

Frozen API-Fläche (Plan Task 3 — Signaturen bytegenau):
  RelayApi(base_url, token_file, timeout=20.0)
    .submit_simple_task(capability, payload, name, idempotency_key=None,
                        priority=0, timeout_seconds=None) -> dict
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

import json
import os
from pathlib import Path
from typing import Any

import httpx


class RelayApi:
    """HTTP-Fassade für alle Relay-Operationen des flow-Handlers (Task 3).

    Der Handler bekommt Relay-Kontext als Env (handler_runner.py: RELAY_BASE_URL,
    RELAY_TOKEN_FILE, ...) — diese Klasse kapselt die zugehörigen Calls
    (submit/get/note/complete) gegen die bestehende Relay-API v2.
    """

    def __init__(self, base_url: str, token_file: str, timeout: float = 20.0):
        self.base_url = base_url.rstrip("/")
        self.token_file = token_file
        self.timeout = timeout

    # -- interne Helfer ---------------------------------------------------

    def _headers(self, with_json: bool = True) -> dict[str, str]:
        """Authorization aus token_file (lazy, Datei darf fehlen); JSON-Header optional."""
        headers: dict[str, str] = {}
        token_path = Path(self.token_file)
        if token_path.exists():
            token = token_path.read_text().strip()
            if token:
                headers["Authorization"] = f"Bearer {token}"
        if with_json:
            headers["Content-Type"] = "application/json"
        return headers

    def _url(self, path: str) -> str:
        return f"{self.base_url}{path}"

    def _request(self, method: str, path: str, **kwargs: Any) -> httpx.Response:
        """Einheitlicher httpx-Call mit Timeout + raise_for_status."""
        kwargs.setdefault("timeout", self.timeout)
        r = httpx.request(method, self._url(path), headers=self._headers(), **kwargs)
        r.raise_for_status()
        return r

    def _json_kwargs(self, body: dict[str, Any]) -> dict[str, Any]:
        return {"content": json.dumps(body).encode("utf-8")}

    # -- öffentliche API (frozen, Plan Task 3) -----------------------------

    def submit_simple_task(
        self,
        capability: str,
        payload: dict,
        name: str,
        idempotency_key: str | None = None,
        priority: int = 0,
        timeout_seconds: int | None = None,
    ) -> dict:
        """POST /relay/v2/scheduler/task-simple → {"task_id","stage_id",...}.

        Felder exakt wie SimpleTaskRequest (Plan §0, api/v2/scheduler.py):
        capability, payload, name, priority, timeout_seconds, owner_node_id,
        idempotency_key. owner_node_id bleibt UNSET (Kinder dürfen überall
        laufen, Plan §2.5).
        """
        body: dict[str, Any] = {
            "capability": capability,
            "payload": payload,
            "name": name,
            "priority": priority,
        }
        if idempotency_key is not None:
            body["idempotency_key"] = idempotency_key
        if timeout_seconds is not None:
            body["timeout_seconds"] = timeout_seconds
        r = self._request("POST", "/relay/v2/scheduler/task-simple", **self._json_kwargs(body))
        return r.json()

    def get_task(self, task_id: str) -> dict:
        """GET /relay/v2/scheduler/tasks/{id} → TaskView (stages mit status/result)."""
        r = self._request("GET", f"/relay/v2/scheduler/tasks/{task_id}")
        return r.json()

    def add_note(self, task_id: str, message: str, kind: str = "info") -> None:
        """POST /relay/v2/scheduler/tasks/{id}/notes  {"message": ..., "kind": ...}.

        kind=longrun hält die Lease am Leben (T-154, Plan §2.4).
        """
        body = {"message": message, "kind": kind}
        self._request("POST", f"/relay/v2/scheduler/tasks/{task_id}/notes", **self._json_kwargs(body))

    def complete_stage(self, task_id: str, stage_id: str, result: dict) -> None:
        """POST /relay/v2/scheduler/stages/{stage_id}/complete.

        Body: {"node_id": env RELAY_NODE_ID, "task_id": ..., "result": ...}
        (node_id aus dem Env-Kontext des Handlers, nicht aus Konstruktor-Args).
        """
        body = {
            "node_id": os.environ.get("RELAY_NODE_ID", ""),
            "task_id": task_id,
            "result": result,
        }
        self._request("POST", f"/relay/v2/scheduler/stages/{stage_id}/complete", **self._json_kwargs(body))

    def fail_stage(self, task_id: str, stage_id: str, error: str) -> None:
        """POST /relay/v2/scheduler/stages/{stage_id}/complete mit {"error": ...}.

        Gleicher Endpoint wie complete_stage, gleiche node_id-Semantik —
        nur der Body unterscheidet sich ({"error": error} statt result).
        """
        body = {
            "node_id": os.environ.get("RELAY_NODE_ID", ""),
            "task_id": task_id,
            "error": error,
        }
        self._request("POST", f"/relay/v2/scheduler/stages/{stage_id}/complete", **self._json_kwargs(body))