"""Discovery-Client für flow.run: holt Live-Capabilities vom Relay.

Frozen Contract: Task 2 in .hermes/plans/flow-run-mvp.md (Pitfall #14:
available=false gilt als „jetzt nicht ausführbar" → fail-fast, D6).
"""
from __future__ import annotations

import httpx

from .plan import Plan, PlanError


def fetch_capabilities(base_url: str, token_file: str, timeout: float = 10.0) -> dict[str, dict]:
    """GET /relay/v2/discovery/capabilities → {cap_name: {available: bool, nodes: [...]}}.
    Endpoint ist offen (kein Bearer nötig); Token-Header wird gesetzt, falls vorhanden."""
    headers = {}
    token_path = __import__("pathlib").Path(token_file)
    if token_path.exists():
        headers["Authorization"] = f"Bearer {token_path.read_text().strip()}"
    r = httpx.get(f"{base_url.rstrip('/')}/relay/v2/discovery/capabilities",
                  headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    caps = {}
    for name, info in (data.get("capabilities") or data).items() if isinstance(data, dict) else []:
        caps[name] = info if isinstance(info, dict) else {"available": bool(info)}
    return caps


def validate_capabilities_live(plan: Plan, caps: dict[str, dict]) -> None:
    """Failt mit eindeutiger Meldung, wenn eine Plan-Cap fehlt oder kein verfügbarer Node existiert."""
    missing, unavailable = [], []
    for t in plan.tasks:
        info = caps.get(t.capability)
        if info is None:
            missing.append(t.capability)
        elif not info.get("available", False):
            unavailable.append(t.capability)
    if missing:
        raise PlanError(f"capabilities not in live discovery: {sorted(set(missing))}")
    if unavailable:
        raise PlanError(f"capabilities have no available node right now: {sorted(set(unavailable))}")
