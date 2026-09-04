"""Plan-Phase + Fan-out + Join-Loop für flow.run (Herzstück, T-172).

Implementiert Plan Task 4 + 4b (.hermes/plans/flow-run-mvp.md):
fail-fast D6, D9-Plan-Retry, topologisches Parallel-Fan-out, Poll-Loop 5s
mit exponentiellem Backoff, unabhängiger Lease-Keepalive (600s), Flow-Budget.
"""
from __future__ import annotations

import json
import math
import time
from typing import Any

import httpx

from .discovery import fetch_capabilities, validate_capabilities_live
from .plan import Plan, PlanError, parse_plan
from .relay_api import RelayApi

# ---- Frozen Konstanten (Plan Task 4/4b + §2.4/§2.5) -------------------------

PLAN_CAPABILITY = "agent.ai"          # Planungs-Kind läuft auf agent.ai (Task 4, Step 0)
PLAN_TASK_REF = "_plan"               # idempotency_key: flow-<origin>-_plan
IDEMPOTENCY_PREFIX = "flow"           # idempotency_key: flow-<ursprungs-task-id>-<task-id> (§2.5)
TASK_NAME_PREFIX = "flow:"            # task_name: flow:<flow-name>:<task-id> (§2.5)
POLL_INTERVAL_SECONDS = 5.0           # Poll-Loop 5s (D5)
POLL_BACKOFF_MAX_SECONDS = 60.0       # exponentiell bis 60s (Task 4b)
POLL_BACKOFF_MAX_ERRORS = 3           # nach 3 aufeinanderfolgenden HTTP-Fehlern → Backoff (Task 4b)
KEEPALIVE_INTERVAL_SECONDS = 600.0    # Keepalive-Note alle 600s, unabhängiger Timer (Task 4b, §2.4)
DEFAULT_MAX_FLOW_SECONDS = 14400      # 4h = Stage-Timeout des Submits (Task 4b, §2.1)

# Task 4b: Backoff 5s→10s→20s→40s→60s (Cap), danach Fehler → exit 1
# (Server-Release-Mechanik übernimmt — deshalb FlowError ohne fail_stage).
_BACKOFF_FATAL_ERRORS = POLL_BACKOFF_MAX_ERRORS + math.ceil(
    math.log2(POLL_BACKOFF_MAX_SECONDS / POLL_INTERVAL_SECONDS)
)

# §2.3 PLAN_PROMPT-Template (FROZEN — bytegenau aus dem Plan übernommen). Platzhalter:
# {task} und {capabilities_snapshot}. ACHTUNG: Die Plan-Schema-Zeile enthält LITERALE
# JSON-Braces ({"version": 1, ...}) — eine naive str.format() über das ganze Template
# würde dort brechen; Substitutions-Mechanik entscheidet Phase 3.
PLAN_PROMPT = """\
Du bist der Planner eines IOWAP-Task-Clusters. Erstelle einen Ausführungsplan
für folgende Aufgabe. Antworte NUR mit dem Plan-JSON (Schema siehe unten),
kein weiterer Text.

Aufgabe:
{task}

Verfügbare Capabilities (Live-Snapshot, von flow.run beim Planungs-Submit
eingebettet — plane ausschließlich mit diesen):
{capabilities_snapshot}

Plan-Schema:
{"version": 1, "name": "...", "tasks": [{"id": "...", "capability": "...",
  "payload": {...}, "depends_on": [...]}], "summary": "..."}
Regeln: ids eindeutig; depends_on nur auf existierende ids; keine Zyklen;
Payload-Felder exakt wie im input_schema der Capability; halte die Zahl der
Tasks minimal.
"""


class FlowError(RuntimeError):
    """Flow-Level-Fehler (Plan-Phase, Fan-out, Join) — führt zu exit 1 + stderr-Grund."""


def build_plan_prompt(task: str, capabilities_snapshot: str) -> str:
    """Baut den PLAN_PROMPT (§2.3): task + Cap-Snapshot ins FROZEN Template einbetten.

    .replace() statt str.format(): Das Template enthält LITERALE JSON-Braces
    in der Schema-Zeile ({"version": 1, ...}), an denen str.format() brechen
    würde. .replace() ersetzt ausschließlich die zwei Platzhalter.
    """
    return (
        PLAN_PROMPT
        .replace("{task}", task)
        .replace("{capabilities_snapshot}", capabilities_snapshot)
    )


def extract_plan_json(result_text: str) -> dict:
    """Extrahiert lenient das Plan-JSON aus dem agent.ai-Result: erstes JSON-Objekt
    mit 'tasks'-Array, rekursiv eine Ebene tief — NICHT freetext-Regex (Task 5).

    Eine Ebene tief heißt: der ganze Text als JSON, plus JSON-Objekte in
    Top-Level-Containern (dict-Werte / list-Items), wobei String-Werte noch
    einmal JSON-geparst werden dürfen (z. B. {"result": "{...}"}).
    Fallback ohne Regex: erstes '{' bis letztes '}' des Texts.
    """
    direct = _try_json(result_text)
    if direct is None:
        start, end = result_text.find("{"), result_text.rfind("}")
        if start != -1 and end > start:
            direct = _try_json(result_text[start : end + 1])

    for cand in _candidate_objects(direct):
        if isinstance(cand.get("tasks"), list):
            return cand
    raise FlowError("plan phase failed: no plan JSON object with 'tasks' array in planning result")


def _try_json(text: str) -> Any:
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


def _candidate_objects(obj: Any):
    """Yieldet alle dicts 'eine Ebene tief' (inkl. JSON in String-Werten)."""
    if isinstance(obj, dict):
        yield obj
        values = obj.values()
    elif isinstance(obj, list):
        values = obj
    else:
        return
    for v in values:
        if isinstance(v, dict):
            yield v
        elif isinstance(v, str):
            parsed = _try_json(v)
            if isinstance(parsed, dict):
                yield parsed


def _idem(origin_task_id: str, task_ref: str) -> str:
    """idempotency_key-Formel (§2.5): flow-<ursprungs-task-id>-<task-id>."""
    return f"{IDEMPOTENCY_PREFIX}-{origin_task_id}-{task_ref}"


def _task_result(view: dict) -> Any:
    """Extrahiert das Result aus einem TaskView: erstes Stage-Result mit Inhalt."""
    for stage in view.get("stages") or []:
        if stage.get("result") is not None:
            return stage["result"]
    return None


def _stage_status(view: dict) -> str:
    """Aggregiert den Task-Status aus den Stages (failed schlägt alles)."""
    statuses = [s.get("status") for s in (view.get("stages") or []) if s.get("status")]
    if any(s == "failed" for s in statuses):
        return "failed"
    if statuses and all(s == "completed" for s in statuses):
        return "completed"
    if statuses:
        return statuses[-1]
    return "pending"


def _task_error(view: dict) -> str:
    """Extrahiert die Fehlermeldung aus einem gefailten TaskView."""
    for stage in view.get("stages") or []:
        if stage.get("status") == "failed":
            return str(stage.get("error") or "unknown error")
    return "unknown error"


def _caps_snapshot(caps: dict[str, dict]) -> str:
    """Kompakter, lesbarer Capability-Snapshot für den PLAN_PROMPT."""
    if not caps:
        return "(keine Capabilities verfügbar)"
    return "\n".join(
        f"- {name} (available: {bool((caps.get(name) or {}).get('available', False))})"
        for name in sorted(caps)
    )


def _as_text(value: Any) -> str:
    """Macht ein Result (str|dict|list) lesbar für die JSON-Extraktion."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _backoff_delay(consecutive_errors: int) -> float:
    """Task 4b: 5s Grundtakt, ab POLL_BACKOFF_MAX_ERRORS exponentiell, Cap 60s."""
    factor = 2 ** max(0, consecutive_errors - POLL_BACKOFF_MAX_ERRORS + 1)
    return min(POLL_INTERVAL_SECONDS * factor, POLL_BACKOFF_MAX_SECONDS)


def _inject_flow(payload: dict, cap_info: dict | None, origin_task_id: str, task_ref: str) -> dict:
    """Payload-Injection (Task 4 Step 3): 'flow': {origin_task_id, task_ref} — nur
    wenn das input_schema der Capability kein unbekanntes Feld ablehnt bzw. ein
    'flow'-Feld erlaubt; ohne input_schema im Snapshot wird injiziert
    (Rückverfolgbarkeit, §2.5)."""
    schema = (cap_info or {}).get("input_schema")
    if isinstance(schema, dict):
        fields = schema.get("fields")
        if isinstance(fields, dict) and "flow" not in fields:
            return dict(payload)
    child = dict(payload)
    child["flow"] = {"origin_task_id": origin_task_id, "task_ref": task_ref}
    return child


def _poll_task(api: RelayApi, task_id: str, tick) -> tuple[str, Any]:
    """Pollt EINEN Task bis done/failed (D5). tick() pro Runde: Budget + Keepalive.

    Returns ("completed", result) bzw. ("failed", error). Nach Backoff-Erschöpfung
    (Task 4b) → FlowError OHNE fail_stage: Server-Release-Mechanik übernimmt.
    """
    errors = 0
    while True:
        tick()
        try:
            view = api.get_task(task_id)
            errors = 0
        except httpx.HTTPError:
            errors += 1
            if errors >= _BACKOFF_FATAL_ERRORS:
                raise FlowError(
                    f"relay unreachable: {errors} consecutive poll errors (backoff exhausted)"
                ) from None
            time.sleep(_backoff_delay(errors))
            continue
        status = _stage_status(view)
        if status == "failed":
            return "failed", _task_error(view)
        if status == "completed":
            return "completed", _task_result(view)
        time.sleep(POLL_INTERVAL_SECONDS)


def run(
    api: RelayApi,
    payload: dict,
    base_url: str,
    token_file: str,
    origin_task_id: str,
    origin_stage_id: str,
    node_id: str,
    options: dict | None = None,
) -> dict:
    """Führt den kompletten Flow aus: Plan-Phase → Validierung → Fan-out → Join → Aggregate.

    Semantik (Plan Task 4, fail-fast D6):
      0. Plan-Phase: Note 'longrun' 'flow started' → Discovery-Snapshot → PLAN_PROMPT →
         Planungs-Kind an agent.ai (idempotency_key flow-<origin>-_plan, Name
         flow:_plan) → Note 'planning started' → poll bis done → Result lenient
         extrahieren. Planungs-Kind failed oder kein valides Plan-JSON → D9:
         1 Retry mit Validerings-Fehler im Prompt, 2. Versuch failt → Flow failed.
      1. Validierung: parse_plan + validate_capabilities_live gegen FRISCHEN Snapshot
         (Race Safety-Net).
      2. Topologisch: alle Tasks mit erfüllten deps gleichzeitig submitten (echtes
         Parallel-Fan-out), idempotency_key flow-<origin-task-id>-<task-id>.
      3. Payload-Injection: 'flow': {origin_task_id, task_ref} — nur wenn das
         input_schema der Capability es erlaubt, sonst weglassen.
      4. Poll-Loop 5s pro Kind; Keepalive-Note alle 600s unabhängig vom Poll-Takt:
         'flow progress: done=K/N' (Lease-Refresh am Ursprungs-Task, T-154).
      5. Kind failed (permanent) → gesamter Flow failt: Note 'flow failed at
         <task-ref> (<capability>): <error>' → fail_stage() → FlowError.
      6. Alle done → aggregate {task_ref: result} → complete_stage() mit
         {'status': 'completed', 'aggregate': ..., 'summary': plan.summary}.
      7. Nach jeder Änderung am Fan-out-Zustand: Note kind=info am Ursprungs-Task.
    """
    options = options or {}
    task_text = str(payload.get("task") or payload.get("original_request") or "")
    if not task_text:
        raise FlowError("flow payload missing 'task' (original request)")
    max_flow_seconds = int(options.get("max_flow_seconds") or DEFAULT_MAX_FLOW_SECONDS)

    started = time.monotonic()
    keepalive_due = started + KEEPALIVE_INTERVAL_SECONDS

    def tick(progress_msg: str) -> None:
        """Budget-Check (fail_stage bei Überschreitung) + unabhängiger Keepalive-Timer."""
        nonlocal keepalive_due
        now = time.monotonic()
        if now - started > max_flow_seconds:
            api.fail_stage(origin_task_id, origin_stage_id, "flow budget exceeded")
            raise FlowError("flow budget exceeded")
        if now >= keepalive_due:
            api.add_note(origin_task_id, progress_msg, kind="longrun")
            keepalive_due = now + KEEPALIVE_INTERVAL_SECONDS

    # -- Phase 0: Planung via agent.ai (D2 = A: Prompt + Cap-Snapshot vom Handler) --
    api.add_note(origin_task_id, "flow started", kind="longrun")
    caps = fetch_capabilities(base_url, token_file)
    plan: Plan | None = None
    feedback: str | None = None
    for attempt in (1, 2):  # D9: 1 Retry mit Validerings-Fehler im Prompt
        prompt = build_plan_prompt(task_text, _caps_snapshot(caps))
        if feedback:
            prompt += (
                "\n\nDer letzte Plan-Versuch war ungültig. Fehler:\n"
                f"{feedback}\nBehebe diesen Fehler und antworte erneut NUR mit dem Plan-JSON."
            )
        resp = api.submit_simple_task(
            capability=PLAN_CAPABILITY,
            payload={"task": prompt},
            name=f"{TASK_NAME_PREFIX}{PLAN_TASK_REF}",
            idempotency_key=_idem(origin_task_id, PLAN_TASK_REF),
        )
        planning_task_id = resp.get("task_id") or resp.get("stage_id")
        if not planning_task_id:
            raise FlowError("plan phase failed: planning submit returned no task id")
        api.add_note(origin_task_id, "planning started", kind="longrun")
        status, planning_result = _poll_task(
            api, planning_task_id, lambda: tick("flow progress: planning")
        )
        if status == "failed":
            feedback = f"planning child failed: {planning_result}"
            api.add_note(origin_task_id, f"plan attempt {attempt} invalid: {feedback}", kind="info")
            continue
        try:
            plan = parse_plan(extract_plan_json(_as_text(planning_result)))
            break
        except (PlanError, FlowError) as e:
            feedback = str(e)
            api.add_note(origin_task_id, f"plan attempt {attempt} invalid: {feedback}", kind="info")
    if plan is None:
        raise FlowError(f"plan phase failed: {feedback}")

    # -- Phase 1: Live-Validierung gegen FRISCHEN Snapshot (Race Safety-Net) --
    caps_fresh = fetch_capabilities(base_url, token_file)
    try:
        validate_capabilities_live(plan, caps_fresh)
    except PlanError as e:
        raise FlowError(str(e)) from e

    # -- Phase 2-4: topologisches Fan-out + Join-Loop --
    remaining = {t.id: t for t in plan.tasks}
    open_deps = {t.id: set(t.depends_on) for t in plan.tasks}
    dependents = {t.id: [] for t in plan.tasks}
    for t in plan.tasks:
        for d in t.depends_on:
            dependents[d].append(t.id)
    child_ids: dict[str, str] = {}
    aggregate: dict[str, Any] = {}
    total = len(plan.tasks)
    consecutive_errors = 0

    while remaining or child_ids:
        tick(f"flow progress: done={len(aggregate)}/{total}")
        ready = [tid for tid in remaining if not open_deps[tid]]
        if not ready and not child_ids and remaining:
            raise FlowError(f"plan phase failed: fan-out deadlock on {sorted(remaining)}")
        for tid in ready:
            t = remaining.pop(tid)
            resp = api.submit_simple_task(
                capability=t.capability,
                payload=_inject_flow(t.payload, caps_fresh.get(t.capability), origin_task_id, tid),
                name=f"{TASK_NAME_PREFIX}{plan.name}:{tid}",
                idempotency_key=_idem(origin_task_id, tid),
            )
            child_ids[tid] = resp["task_id"]
        if ready:
            api.add_note(
                origin_task_id,
                f"fan-out: {total - len(remaining)}/{total} tasks submitted",
                kind="info",
            )

        failed: tuple[str, str] | None = None
        for ref in list(child_ids):
            try:
                view = api.get_task(child_ids[ref])
            except httpx.HTTPError:
                consecutive_errors += 1
                if consecutive_errors >= _BACKOFF_FATAL_ERRORS:
                    raise FlowError(
                        f"relay unreachable: {consecutive_errors} consecutive poll errors "
                        "(backoff exhausted)"
                    ) from None
                break  # Backoff-Sleep unten, dann neue Runde
            consecutive_errors = 0
            status = _stage_status(view)
            if status == "failed":
                failed = (ref, _task_error(view))
                break
            if status == "completed":
                aggregate[ref] = _task_result(view)
                del child_ids[ref]
                for c in dependents[ref]:
                    open_deps[c].discard(ref)

        # -- Phase 5: fail-fast D6 — kein Submit von Nachfolgern --
        if failed is not None:
            t = plan.by_id[failed[0]]
            msg = f"flow failed at {t.id} ({t.capability}): {failed[1]}"
            api.add_note(origin_task_id, msg, kind="info")
            api.fail_stage(origin_task_id, origin_stage_id, msg)
            raise FlowError(msg)

        if consecutive_errors:
            time.sleep(_backoff_delay(consecutive_errors))
        elif child_ids or remaining:
            time.sleep(POLL_INTERVAL_SECONDS)

    # -- Phase 6: Aggregate + Complete am Ursprungs-Stage --
    result = {
        "status": "completed",
        "flow": {"name": plan.name, "tasks_done": total},
        "aggregate": aggregate,
        "summary": plan.summary,
    }
    api.complete_stage(origin_task_id, origin_stage_id, result)
    return result