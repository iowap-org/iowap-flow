"""Discovery-Client für flow.run: holt Live-Capabilities vom Relay.

Frozen Contract: Task 2 in .hermes/plans/flow-run-mvp.md (Pitfall #14:
available=false gilt als „jetzt nicht ausführbar" → fail-fast, D6).
"""
from __future__ import annotations

from pathlib import Path

import httpx

from .plan import Plan, PlanError
from .token_store import load_bearer_token


def fetch_capabilities(base_url: str, token_file: str, timeout: float = 10.0) -> dict[str, dict]:
    """GET /relay/v2/discovery/capabilities → {cap_name: cap_obj}.

    Server-Formate (beide unterstützt):
    - {"capabilities": [{name, available, ...}, ...]}  (Liste, live belegt)
    - {cap_name: {available, ...}, ...}                (altes dict-Format)
    """
    headers = {}
    token_path = Path(token_file)
    if token_path.exists():
        token = load_bearer_token(token_path)
        if token:
            headers["Authorization"] = f"Bearer {token}"
    r = httpx.get(f"{base_url.rstrip('/')}/relay/v2/discovery/capabilities",
                  headers=headers, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    entries = data.get("capabilities", data) if isinstance(data, dict) else data
    caps: dict[str, dict] = {}
    if isinstance(entries, dict):
        # Altes Format: name -> info (bool oder dict).
        for name, info in entries.items():
            caps[name] = info if isinstance(info, dict) else {"available": bool(info)}
    elif isinstance(entries, list):
        # Live-Format: Liste von Capability-Objekten mit "name".
        for item in entries:
            if isinstance(item, dict) and item.get("name"):
                caps[str(item["name"])] = item
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
