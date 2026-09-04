# tests/test_flow_relay_api.py — Task 3 (Relay-Task-Client, httpx.MockTransport), Stubs Phase 2
import httpx  # noqa: F401  (Phase 3: MockTransport)
import pytest  # noqa: F401  (Phase 3: pytest.raises)

from flow_node.relay_api import RelayApi


def _make_api(handler) -> RelayApi:
    """Test-Helfer (Phase 3): RelayApi mit httpx.MockTransport(handler)."""
    raise NotImplementedError


def test_submit_sets_idempotency_key_and_task_name_prefix():
    # submit setzt idempotency_key; task_name-Präfix "flow:<flow-name>:<task-id>" (§2.5)
    assert False  # TODO


def test_get_task_parses_taskview():
    # get_task parst TaskView (stages mit status/result)
    assert False  # TODO


def test_add_note_sends_kind():
    # add_note sendet {"message": ..., "kind": ...} an /tasks/{id}/notes
    assert False  # TODO


def test_complete_stage_hits_right_endpoint_with_node_id():
    # POST /relay/v2/scheduler/stages/{stage_id}/complete mit node_id aus Env-Kontext
    assert False  # TODO


def test_fail_stage_hits_right_endpoint():
    # fail_stage: same endpoint, {"error": error}
    assert False  # TODO