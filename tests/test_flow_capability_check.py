# tests/test_flow_capability_check.py — Task 2 (Capability-Pre-Check, httpx.MockTransport, kein echter Server), Stubs Phase 2
import httpx  # noqa: F401  (Phase 3: MockTransport)
import pytest  # noqa: F401  (Phase 3: pytest.raises)

from flow_node.discovery import fetch_capabilities, validate_capabilities_live  # noqa: F401
from flow_node.plan import PlanError, parse_plan  # noqa: F401  (Phase 3: Plan bauen + PlanError)


def test_all_capabilities_present_ok():
    # (a) alle Plan-Caps live + available → kein Fehler
    assert False  # TODO


def test_missing_capability_raises_with_name():
    # (b) eine Plan-Cap fehlt im Live-Snapshot → PlanError mit Cap-Namen
    #     ("capabilities not in live discovery: [...]")
    assert False  # TODO


def test_unavailable_capability_raises():
    # (c) Cap da, aber available=false → PlanError
    #     ("capabilities have no available node right now: [...]") — Pitfall #14, fail-fast D6
    assert False  # TODO