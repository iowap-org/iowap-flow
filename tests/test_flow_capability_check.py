# tests/test_flow_capability_check.py — Task 2 (Capability-Pre-Check gegen Discovery).
# Echte Tests via httpx.MockTransport — kein echter Server nötig (Plan Task 2).
# Fälle a/b/c aus dem Plan: (a) alle Caps da → ok, (b) Cap fehlt → PlanError mit
# Cap-Namen, (c) Cap da aber available=false → PlanError (Pitfall #14, fail-fast D6).
import httpx
import pytest

from flow_node.discovery import fetch_capabilities, validate_capabilities_live
from flow_node.plan import PlanError, parse_plan

PLAN_RAW = {
    "version": 1,
    "name": "demo",
    "tasks": [
        {"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []},
        {"id": "b", "capability": "tool.y", "payload": {}, "depends_on": ["a"]},
    ],
    "summary": "s",
}

DISCOVERY_URL = "http://relay.test/relay/v2/discovery/capabilities"


def _mock_relay(monkeypatch, payload, status_code=200, capture=None):
    """Lenkt das Modul-httpx.get auf einen MockTransport-Client um (Produktionscode unverändert)."""
    def handler(request: httpx.Request) -> httpx.Response:
        if capture is not None:
            capture.append(request)
        return httpx.Response(status_code, json=payload)

    client = httpx.Client(transport=httpx.MockTransport(handler))

    def fake_get(url, **kwargs):
        return client.get(url, **kwargs)

    monkeypatch.setattr(httpx, "get", fake_get)


# --- Fall (a): alle Plan-Caps live + available → ok -----------------------------

def test_all_capabilities_present_ok(monkeypatch, tmp_path):
    # (a) alle Plan-Caps im Live-Snapshot vorhanden und available → kein Fehler
    _mock_relay(monkeypatch, {"capabilities": {
        "tool.x": {"available": True, "nodes": [{"node_id": "n1"}]},
        "tool.y": {"available": True, "nodes": [{"node_id": "n2"}]},
    }})
    plan = parse_plan(PLAN_RAW)
    caps = fetch_capabilities("http://relay.test", str(tmp_path / "no-token-file"))
    validate_capabilities_live(plan, caps)  # wirft nicht — das IST die Assertion


# --- Fall (b): eine Plan-Cap fehlt im Live-Snapshot ------------------------------

def test_missing_capability_raises_with_name(monkeypatch, tmp_path):
    # (b) tool.y fehlt komplett in der Discovery-Antwort → PlanError mit Cap-Namen
    _mock_relay(monkeypatch, {"capabilities": {
        "tool.x": {"available": True, "nodes": [{"node_id": "n1"}]},
    }})
    plan = parse_plan(PLAN_RAW)
    caps = fetch_capabilities("http://relay.test", str(tmp_path / "no-token-file"))
    with pytest.raises(PlanError) as excinfo:
        validate_capabilities_live(plan, caps)
    msg = str(excinfo.value)
    assert "capabilities not in live discovery" in msg
    assert "tool.y" in msg  # Cap-Name im Grund (Plan Task 2)


# --- Fall (c): Cap da, aber available=false --------------------------------------

def test_unavailable_capability_raises(monkeypatch, tmp_path):
    # (c) tool.y existiert, aber available=false → PlanError „no available node".
    # Pitfall #14: available kann durch einen unavailable Node auf false geflippt
    # sein, obwohl ein anderer Node die Cap hat — der Plan failt bewusst (fail-fast, D6).
    _mock_relay(monkeypatch, {"capabilities": {
        "tool.x": {"available": True, "nodes": [{"node_id": "n1"}]},
        "tool.y": {"available": False, "nodes": [{"node_id": "n2"}]},
    }})
    plan = parse_plan(PLAN_RAW)
    caps = fetch_capabilities("http://relay.test", str(tmp_path / "no-token-file"))
    with pytest.raises(PlanError) as excinfo:
        validate_capabilities_live(plan, caps)
    msg = str(excinfo.value)
    assert "capabilities have no available node right now" in msg
    assert "tool.y" in msg


# --- fetch_capabilities: Antwort-Shapes + Token-Handling (Plan-Snippet-Zweige) ----

def test_fetch_parses_flat_response_without_capabilities_wrapper(monkeypatch, tmp_path):
    # Discovery-Antwort flach (kein "capabilities"-Wrapper) → gleiche Normalisierung
    _mock_relay(monkeypatch, {
        "tool.x": {"available": True},
    })
    caps = fetch_capabilities("http://relay.test", str(tmp_path / "no-token-file"))
    assert caps == {"tool.x": {"available": True}}


def test_fetch_normalizes_non_dict_info_to_available_flag(monkeypatch, tmp_path):
    # Nicht-dict-Info (z. B. bool) → {"available": <bool>} (Plan-Snippet-Zweig)
    _mock_relay(monkeypatch, {"capabilities": {"tool.x": True, "tool.y": False}})
    caps = fetch_capabilities("http://relay.test", str(tmp_path / "no-token-file"))
    assert caps == {"tool.x": {"available": True}, "tool.y": {"available": False}}


def test_fetch_sends_bearer_token_when_token_file_exists(monkeypatch, tmp_path):
    # Token-File vorhanden → Authorization: Bearer <token> (Endpoint offen, Header optional)
    token_file = tmp_path / "token"
    token_file.write_text("tok123\n")
    captured = []
    _mock_relay(monkeypatch, {"capabilities": {}}, capture=captured)
    fetch_capabilities("http://relay.test", str(token_file))
    assert captured[0].url == DISCOVERY_URL
    assert captured[0].headers["Authorization"] == "Bearer tok123"


def test_fetch_omits_auth_header_when_no_token_file(monkeypatch, tmp_path):
    # Kein Token-File → kein Authorization-Header (Endpoint ist offen, Plan §0)
    captured = []
    _mock_relay(monkeypatch, {"capabilities": {}}, capture=captured)
    fetch_capabilities("http://relay.test", str(tmp_path / "does-not-exist"))
    assert captured[0].url == DISCOVERY_URL
    assert "Authorization" not in captured[0].headers


def test_fetch_raises_on_http_error(monkeypatch, tmp_path):
    # Discovery nicht erreichbar/fehlerhaft (raise_for_status) → HTTPStatusError
    _mock_relay(monkeypatch, {"detail": "boom"}, status_code=500)
    with pytest.raises(httpx.HTTPStatusError):
        fetch_capabilities("http://relay.test", str(tmp_path / "no-token-file"))