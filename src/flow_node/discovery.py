"""Discovery-Client für flow.run: holt Live-Capabilities vom Relay.

SKELETT (Phase 2, Scaffold): Nur Signaturen/Docstrings — Logik in Phase 3.
Frozen Contract: Task 2 in .hermes/plans/flow-run-mvp.md (Pitfall #14:
available=false gilt als „jetzt nicht ausführbar" → fail-fast, D6).
"""
from __future__ import annotations

import httpx  # noqa: F401  (Phase 3: HTTP-GET)

from .plan import Plan, PlanError  # noqa: F401  (Plan/PlanError: Signatur bzw. Raises in Phase 3)


def fetch_capabilities(base_url: str, token_file: str, timeout: float = 10.0) -> dict[str, dict]:
    """GET /relay/v2/discovery/capabilities → {cap_name: {available: bool, nodes: [...]}}.
    Endpoint ist offen (kein Bearer nötig); Token-Header wird gesetzt, falls vorhanden."""
    raise NotImplementedError


def validate_capabilities_live(plan: Plan, caps: dict[str, dict]) -> None:
    """Failt mit eindeutiger Meldung, wenn eine Plan-Cap fehlt oder kein verfügbarer Node existiert."""
    raise NotImplementedError