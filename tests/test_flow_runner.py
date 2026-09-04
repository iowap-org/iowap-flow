# tests/test_flow_runner.py — Task 4 (+4b) (Plan-Phase + Fan-out + Join-Loop, httpx.MockTransport mit gefakten TaskView-Sequenzen), Stubs Phase 2
import httpx  # noqa: F401  (Phase 3: MockTransport)
import pytest  # noqa: F401  (Phase 3: pytest.raises)

from flow_node.relay_api import RelayApi
from flow_node.runner import run  # noqa: F401  (Phase 3: run(...))


def _make_api(handler) -> RelayApi:
    """Test-Helfer (Phase 3): RelayApi + gefakte TaskView-Sequenzen via MockTransport."""
    raise NotImplementedError


def test_planning_child_submitted_first_and_plan_extracted_lenient():
    # (a) Planungs-Kind zuerst submittet (idempotency_key flow-<origin>-_plan,
    #     task_name flow:<name>:_plan); Plan-Extraktion lenient
    assert False  # TODO


def test_invalid_plan_json_one_retry_with_feedback_then_fail():
    # (b) ungültiges Plan-JSON → D9: 1 Retry mit Validerings-Fehler im Prompt,
    #     2. Versuch failt → Flow failed mit Grund
    assert False  # TODO


def test_parallel_roots_and_join_submit_order_respects_deps():
    # (c) 2 parallele Roots + 1 Join → Reihenfolge der Submits korrekt
    #     (deps fertig bevor Dependent submitted)
    assert False  # TODO


def test_child_failed_is_fail_fast_no_successor_submit():
    # (d) ein Kind failed (permanent) → fail-fast, kein Submit von Nachfolgern;
    #     Note 'flow failed at <task-ref> (<capability>): <error>' + fail_stage + exit 1
    assert False  # TODO


def test_idempotency_key_stable_on_repeat():
    # (e) idempotency_key = flow-<origin-task-id>-<task-id> (§2.5) stabil bei Wiederholung
    assert False  # TODO


def test_join_completes_origin_stage_with_aggregate():
    # (f) alle done → aggregate {task_ref: result} → complete_stage() am Ursprungs-Stage
    #     mit {'status': 'completed', 'aggregate': ..., 'summary': plan.summary}
    assert False  # TODO


def test_poll_backoff_after_consecutive_http_errors():
    # Task 4b: 3 aufeinanderfolgende HTTP-Fehler → exponentiell bis 60s, danach exit 1
    assert False  # TODO


def test_keepalive_note_every_600s_independent_timer():
    # Task 4b: Keepalive-Note alle 600s unabhängig vom Poll-Takt ('flow progress: done=K/N')
    assert False  # TODO


def test_flow_budget_exceeded_fails_stage():
    # Task 4b: options.max_flow_seconds (default 4h) überschritten → fail_stage
    # 'flow budget exceeded'
    assert False  # TODO