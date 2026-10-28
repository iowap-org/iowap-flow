"""Plan phase + fan-out + join loop for flow.run (core piece, T-172).

Implements plan task 4 + 4b (.hermes/plans/flow-run-mvp.md):
fail-fast D6, D9 plan retry, topological parallel fan-out, poll loop 5s
with exponential backoff, independent lease keepalive (600s), flow budget.
Plus T-002: template interpolation ${ref.result.path} — data injection
between flow tasks, resolved at submit time from the running aggregate.
"""
from __future__ import annotations

import json
import math
import os
import re
import time
from typing import Any

import httpx

from .catalog import (
    CatalogError,
    cleanup_history,
    define_flow,
    list_history_records,
    load_flow,
    load_history_record,
    record_history,
    resolve_input_templates,
    save_flow_from_history,
)
from .catalog import (
    list_flows as list_catalog_flows,
)
from .discovery import fetch_capabilities, validate_capabilities_live
from .plan import Plan, PlanError, parse_plan
from .relay_api import RelayApi

# ---- Frozen constants (plan task 4/4b + §2.4/§2.5) --------------------------

PLAN_CAPABILITY = "forward"          # pseudo-capability: submitter's forward slot resolves to the planner capability
PLAN_FORWARD_SLOT = "plan"           # forward slot for the planning child (needs LLM/decision)
PLAN_TASK_REF = "_plan"               # idempotency_key: flow-<origin>-_plan
IDEMPOTENCY_PREFIX = "flow"           # idempotency_key: flow-<origin-task-id>-<task-id> (§2.5)
TASK_NAME_PREFIX = "flow:"            # task_name: flow:<flow-name>:<task-id> (§2.5)
POLL_INTERVAL_SECONDS = 5.0           # poll loop 5s (D5)
POLL_BACKOFF_MAX_SECONDS = 60.0       # exponential up to 60s (task 4b)
POLL_BACKOFF_MAX_ERRORS = 3           # after 3 consecutive HTTP errors → backoff (task 4b)
KEEPALIVE_INTERVAL_SECONDS = 600.0    # keepalive note every 600s, independent timer (task 4b, §2.4)
DEFAULT_MAX_FLOW_SECONDS = 14400      # 4h = stage timeout of the submit (task 4b, §2.1)

# Task 4b: backoff 5s→10s→20s→40s→60s (cap), then error → exit 1
# (the server release mechanics take over — hence FlowError without fail_stage).
_BACKOFF_FATAL_ERRORS = POLL_BACKOFF_MAX_ERRORS + math.ceil(
    math.log2(POLL_BACKOFF_MAX_SECONDS / POLL_INTERVAL_SECONDS)
)

# §2.3 PLAN_PROMPT template (FROZEN — taken byte-exact from the plan; T-002
# adds the data-passing paragraph). Placeholders:
# {task} and {capabilities_snapshot}. CAREFUL: the plan-schema line contains LITERAL
# JSON braces ({"version": 1, ...}) — a naive str.format() over the whole template
# would break there; the substitution mechanics is decided in phase 3.
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

Datenweitergabe: Payload-Werte dürfen Ergebnisse früherer Tasks über
${ref.result.path} referenzieren (ref = Task-id, path = Dot-Path ins
Resultat des referenzierten Tasks). Die path-Segmente sind die Feldnamen
des Capability-Resultats — pro Capability als 'result paths' im
Capabilities-Snapshot gelistet. Jeder referenzierte Task MUSS in
depends_on stehen, sonst failt der Flow beim Submit.
"""

# ---- T-235: plan-prompt split (systemprompt / taskprompt) -------------------
# The split halves are composed from PLAN_PROMPT's text verbatim (blocks move,
# no sentence is reworded — board T-235 quality contract). Join convention:
# systemprompt first, exactly one blank line between the halves (the handler
# PROMPT_SPLIT_JOIN_MARKER, agent_ai.py) and between blocks inside each half.
# CAREFUL: exactly ONE blank line ("\n\n" between blocks); neither constant
# carries a leading or trailing newline. The substitution mechanics stays
# .replace() (no str.format — the schema line contains LITERAL JSON braces;
# runner.py §0.3 of design.md).
PLAN_SYSTEM_PROMPT = (
    # T-256 (Ronny, 2026-10-27, live findings): the 4-step contract. The
    # live probe proved the model drifts under a 56k-char prompt: it copied
    # field-description EXAMPLES verbatim (fox/dragon instead of the task's
    # coffee subjects), padded the plan (13 tasks: dup generations, pointless
    # paperless.search, plan.compile recursion), stringified payloads and
    # duplicated ids. The steps order the reasoning; HARD CONSTRAINTS kill
    # each proven drift class.
    "You are the planner of an IOWAP task cluster. Work strictly in this order:\n"
    "\n"
    "Step 1 - DECOMPOSE: Break the user task down into the minimal set of work\n"
    "items needed to produce exactly the requested artifacts. Do not add\n"
    "research, discovery, or verification tasks that were not asked for.\n"
    "Step 2 - MAP: For each work item, choose the one fitting capability from\n"
    "the capability list (name : description : input_schema). Judge by the\n"
    "DESCRIPTION, never by the name alone.\n"
    "Step 3 - COMPOSE: Build the execution workflow in the plan schema below.\n"
    "Rules: unique ids; depends_on contains only existing ids and every\n"
    "referenced task; no cycles; payload fields exactly as the capability's\n"
    "input_schema defines; every payload value derives from the user task or\n"
    "from referenced prior results via ${ref.result.path}.\n"
    "Step 4 - ANSWER: Respond with ONLY the plan JSON - the first character of\n"
    "your reply is `{`, the last is `}`. No code fences, no preamble, no\n"
    "commentary, nothing else.\n"
    "\n"
    "Plan schema:\n"
    "{\"version\": 1, \"name\": \"...\", \"tasks\": [{\"id\": \"...\", \"capability\": \"...\",\n"
    "  \"payload\": {...}, \"depends_on\": [...]}], \"summary\": \"...\"}\n"
    "\n"
    "EXAMPLE of the exact shape (structure only, DIFFERENT task - do not copy the content):\n"
    "{\"version\": 1, \"name\": \"example-flow\", \"tasks\": [\n"
    "  {\"id\": \"t1\", \"capability\": \"<cap-from-list>\", \"payload\": {\"<declared-field>\": \"<text derived ONLY from the user task>\"}, \"depends_on\": []},\n"
    "  {\"id\": \"t2\", \"capability\": \"<cap-from-list>\", \"payload\": {\"<field>\": \"${ref.t1.result.<path>\"}, \"depends_on\": [\"t1\"]}],\n"
    "\"summary\": \"one sentence\"\n"
    "}\n"
    "Every artifact the task requests must end at its final destination (storage.store when storage is requested). ids unique, each referenced id in depends_on, no cycles, minimum task count.\n"
    "\n"
    "HARD CONSTRAINTS:\n"
    "- NEVER plan a 'flow.run' task — you ARE the flow planner; flow.run is the orchestrator itself and would recurse forever.\n"
    "- The examples inside input_schema field descriptions are FORMAT\n"
    "  examples. Never copy them into your plan. Prompts, titles and texts\n"
    "  must derive verbatim from the user task (its subjects, its wording).\n"
    "- Plan minimal: exactly the requested artifacts, no extras.\n"
    "- If the task is ambiguous, prefer the most literal reading.\n"
    "\n"
    "Data passing: payload values may reference earlier task results via\n"
    "${ref.result.path} (ref = task id, path = dot-path into the referenced\n"
    "task's result; the path segments are the result-path names listed per\n"
    "capability in the capability snapshot). Every referenced task MUST be in\n"
    "depends_on, otherwise the flow fails at submit."
)

PLAN_TASK_PROMPT_TEMPLATE = (
    "Aufgabe:\n"
    "{task}\n"
    "\n"
    "Verfügbare Capabilities (Live-Snapshot, von flow.run beim Planungs-Submit\n"
    "eingebettet — plane ausschließlich mit diesen):\n"
    "{capabilities_snapshot}"
)

# T-235 §1.3: the D9 retry feedback — extracted from the legacy inline block
# (runner.py:655-658) so legacy and split cannot byte-drift apart. Appended
# with .replace("{feedback}", feedback) — no str.format (house doctrine).
PLAN_RETRY_FEEDBACK = (
    "\n\nThe last plan attempt was invalid. Error:\n"
    "{feedback}\n"
    "Fix this error and answer again ONLY with the plan JSON."
)

# T-235 §2.2: the split mode is env-gated (deploy kill-switch, design §9.9).
# Default OFF — the 1-string legacy form stays the code default so the
# existing suite (env-unset) keeps its frozen 1-string payload assertions.
PLAN_PROMPT_SPLIT_ENV = "FLOW_PLAN_PROMPT_SPLIT"
_SPLIT_ENV_TRUE = frozenset({"1", "true", "yes", "on"})
_SPLIT_ENV_FALSE = frozenset({"0", "false", "no", "off", ""})


def plan_prompt_split_enabled() -> bool:
    """T-235 §2.2: read FLOW_PLAN_PROMPT_SPLIT (frozen parse).

    unset/empty → False (legacy 1-string default). strip().lower() in
    ("1","true","yes","on") → True; in ("0","false","no","off","") → False;
    anything else → FlowError naming the env and the raw value (loud — never
    a silent fallback). Read per call (cheap; testable via monkeypatch).
    """
    raw = os.environ.get(PLAN_PROMPT_SPLIT_ENV, "")
    value = raw.strip().lower()
    if value in _SPLIT_ENV_TRUE:
        return True
    if value in _SPLIT_ENV_FALSE:
        return False
    raise FlowError(f"invalid {PLAN_PROMPT_SPLIT_ENV} value: {raw!r}")


def _caps_compact(caps: dict[str, dict]) -> list[dict]:
    """T-256-live (Ronny 2026-10-28): FULL list, COMPACT entries — every
    capability rides, but each as a compact JSON object: name + usable
    phrase (first description sentences, result-paths appended) +
    result_paths + fields (name/type/required only).

    Why: the live probe proved the 71-entry full-schema form (~56k chars)
    drowns the task — the E2E plan invented pseudo-references
    ("storage.list.path") and dropped images/poems entirely, while the
    same model planned perfectly (A2/A3/A4, 100% task-faithful) from
    compact 3-6 entry lists. Field DESCRIPTIONS are the hallucination
    source (the model copied "A red fox…" example strings verbatim), so
    they are dropped here; type+required carry the contract.

    Pure; no exceptions. Sorted by name (deterministic payloads).
    """
    entries: list[dict] = []
    for name in sorted(caps):
        cap = caps.get(name) or {}
        desc = str(cap.get("description") or "")
        # usable: first ~2 sentences, result-paths appended when hinted
        parts = desc.replace("! ", ".").replace("? ", ".").split(". ")
        usable = ". ".join(p.strip() for p in parts if p.strip())[:220].strip()
        hints = cap.get("result_path_hints")
        if isinstance(hints, list) and hints:
            usable = (usable + f" result paths: {', '.join(str(h) for h in hints)}").strip()
        entry: dict[str, Any] = {"name": name, "usable": usable or "-"}
        schema = cap.get("input_schema") or {}
        fields = schema.get("fields", schema) if isinstance(schema, dict) else {}
        compact: dict[str, Any] = {}
        if isinstance(fields, dict):
            for k, v in fields.items():
                if not isinstance(v, dict):
                    continue
                finfo: dict[str, Any] = {}
                if v.get("type"):
                    finfo["type"] = v.get("type")
                if v.get("required"):
                    finfo["required"] = True
                compact[k] = finfo
        # T-256-e2e2 (live): the A-reihe proved the FIELD DECLARATION must be
        # IN the readable line, not only in the JSON object: the planner read
        # the prose ("Parameter: task") and ignored the fields map next to it
        # (9-task E2E plan: image.gen.ai got "task" instead of "prompt" etc).
        # The line now carries the exact contract form.
        if compact:
            entry["fields"] = compact
            # T-256-e2e2: ".get()" everywhere — live caps carry fields without
            # "type" (enum-only) and the KeyError killed the runner silently
            # after 8 s (handler exited 1, empty result).
            field_line = ", ".join(
                (f"{k} ({v.get('type', '?')}{', required' if v.get('required') else ''})")
                for k, v in compact.items())
            entry["usable"] = (entry["usable"] + f" fields: {field_line}").strip()
        if cap.get("available") is False:
            entry["available"] = False
        entries.append(entry)
    return entries


def _caps_array(caps: dict[str, dict]) -> list[dict]:
    """T-254 (Ronny 2026-10-27): the capability list as an ARRAY field of
    the plan submit — the specialist (qwen one-shot, NO tool use) cannot
    fetch the live capability list itself; the flow runner MUST ship it.
    Form matches the model.specialist ``capabilities`` contract verbatim:
    array of objects {name, description, input_schema} (rendered into the
    user message by the NovaForge handler). ``result_path_hints`` ride as
    the extra key the T-005c contract needs for the data-passing pattern
    ${ref.result.path}; the handler renders unknown keys alongside the
    table, extra keys never break the schema check (name-only D-06).

    Pure; no exceptions. Sorted by name (deterministic payloads).
    """
    entries: list[dict] = []
    for name in sorted(caps):
        cap = caps.get(name) or {}
        desc = str(cap.get("description") or "")
        hints = cap.get("result_path_hints")
        if isinstance(hints, list) and hints:
            desc = (desc + f" result paths: {', '.join(str(h) for h in hints)}").strip()
        entry: dict[str, Any] = {
            "name": name,
            "description": desc,
            "input_schema": cap.get("input_schema"),
        }
        if cap.get("available") is not None:
            entry["available"] = bool(cap.get("available"))
        entries.append(entry)
    return entries


def build_plan_prompt_parts(
    task: str,
    capabilities_snapshot: str,
    feedback: str | None = None,
) -> tuple[str, str]:
    """T-235 §1.5: build the split (systemprompt, taskprompt) pair.

    system = PLAN_SYSTEM_PROMPT verbatim (static — no substitution).
    task = PLAN_TASK_PROMPT_TEMPLATE.replace("{task}", task)
          .replace("{capabilities_snapshot}", capabilities_snapshot)
          + (PLAN_RETRY_FEEDBACK.replace("{feedback}", feedback)
             if feedback else "")
    .replace() instead of str.format(): the same literal-brace rule as
    build_plan_prompt (design §1.2). Pure; no exceptions.
    """
    system = PLAN_SYSTEM_PROMPT
    taskpart = (
        PLAN_TASK_PROMPT_TEMPLATE.replace("{task}", task)
        .replace("{capabilities_snapshot}", capabilities_snapshot)
    )
    if feedback:
        taskpart += PLAN_RETRY_FEEDBACK.replace("{feedback}", feedback)
    return system, taskpart


class FlowError(RuntimeError):
    """Flow-level error (plan phase, fan-out, join) — leads to exit 1 + stderr reason."""


def build_plan_prompt(task: str, capabilities_snapshot: str) -> str:
    """Builds the PLAN_PROMPT (§2.3): embeds task + cap snapshot into the FROZEN template.

    .replace() instead of str.format(): the template contains LITERAL JSON braces
    in the schema line ({"version": 1, ...}) where str.format() would break.
    .replace() replaces only the two placeholders.
    """
    return (
        PLAN_PROMPT
        .replace("{task}", task)
        .replace("{capabilities_snapshot}", capabilities_snapshot)
    )


def extract_plan_json(result_text: str) -> dict:
    """Leniently extracts the plan JSON from the agent.ai result: first JSON object
    with a 'tasks' array, recursively — NOT freetext regex (task 5).

    Recursive means: the whole text as JSON, plus JSON objects in nested
    containers and in string values (string values are JSON-parsed again) —
    e.g. {"result": "{...}"} or the live shape 2026-09-06:
    {"status": "completed", "result": {"answer": "<plan-json>"}} (T-172 test,
    the old code looked only one level deep and never found the plan).
    Regex-free fallback: first '{' to last '}' of the text.
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


def _strip_code_fences(text: str) -> str:
    """Removes markdown code fences (```json ... ``` / ``` ... ```), if
    present — agent.ai sometimes answers fenced (live finding 2026-09-28:
    the plan phase failed because _try_json choked on the fences)."""
    stripped = text.strip()
    if stripped.startswith("```"):
        first_nl = stripped.find("\n")
        if first_nl != -1 and stripped.rstrip().endswith("```"):
            stripped = stripped[first_nl + 1 : stripped.rstrip().rfind("```")]
    return stripped


def _try_json(text: str) -> Any:
    text = _strip_code_fences(text)
    try:
        return json.loads(text)
    except (json.JSONDecodeError, TypeError):
        return None


_MAX_EXTRACT_DEPTH = 8


def _candidate_objects(obj: Any):
    """Yields all dicts in the result (recursive, depth cap, cycle guard) —
    including JSON nested in string values (parsed again)."""
    seen: set[int] = set()

    def walk(node: Any, depth: int):
        if depth > _MAX_EXTRACT_DEPTH or id(node) in seen:
            return
        if isinstance(node, dict):
            seen.add(id(node))
            yield node
            values = node.values()
        elif isinstance(node, list):
            seen.add(id(node))
            values = node
        elif isinstance(node, str):
            parsed = _try_json(node)
            if parsed is not None:
                yield from walk(parsed, depth + 1)
            return
        else:
            return
        for v in values:
            yield from walk(v, depth + 1)

    yield from walk(obj, 0)


def _idem(origin_task_id: str, task_ref: str) -> str:
    """idempotency_key formula (§2.5): flow-<origin-task-id>-<task-id>."""
    return f"{IDEMPOTENCY_PREFIX}-{origin_task_id}-{task_ref}"


def _task_result(view: dict) -> Any:
    """Extracts the result from a TaskView: first stage result with content.

    T-005c (envelope contract, design.md T-005a §2.2/§4.1): the handler result
    envelope is unwrapped EXACTLY ONCE — {"status": "completed", "result": X}
    → X. Bare results (legacy nodes) pass through unchanged (tolerant until the
    whole fleet is migrated). Exactly-once gate (D7): unwraps ONLY for
    dict + status == "completed" + inner result is a dict; everything else
    (bare results, error shapes, bare dicts with status/result keys) goes
    through unchanged. Idempotent by construction — aggregate[task_ref] holds
    in BOTH cases the result object afterwards, i.e. ${ref.result.path}
    navigates relative to the inner result: ${img.result.artifact_id} →
    aggregate["img"]["artifact_id"].
    """
    for stage in view.get("stages") or []:
        if stage.get("result") is not None:
            result = stage["result"]
            if (
                isinstance(result, dict)
                and result.get("status") == "completed"
                and isinstance(result.get("result"), dict)
            ):
                return result["result"]
            return result
    return None


def _stage_status(view: dict) -> str:
    """Aggregates the task status from the stages (failed beats everything)."""
    statuses = [s.get("status") for s in (view.get("stages") or []) if s.get("status")]
    if any(s == "failed" for s in statuses):
        return "failed"
    if statuses and all(s == "completed" for s in statuses):
        return "completed"
    if statuses:
        return statuses[-1]
    return "pending"


def _task_error(view: dict) -> str:
    """Extracts the error message from a failed TaskView."""
    for stage in view.get("stages") or []:
        if stage.get("status") == "failed":
            return str(stage.get("error") or "unknown error")
    return "unknown error"


def _caps_snapshot(caps: dict[str, dict]) -> str:
    """Compact, readable capability snapshot for the PLAN_PROMPT.

    T-004 (iowap-flow): includes `result_path_hints` (dot paths into the result)
    so the planner knows valid paths for the data-passing pattern
    ${ref.result.path} and doesn't have to guess.
    T-005c: hints are paths RELATIVE TO THE INNER result of the handler result
    envelope (design.md T-005a §4.2) — i.e. the field names of what flow
    stores under aggregate[task_ref] after unwrap-once; the planner builds
    valid ${ref.result.<hint>}-templates from them.
    """
    if not caps:
        return "(no capabilities available)"
    lines: list[str] = []
    for name in sorted(caps):
        cap = caps.get(name) or {}
        line = f"- {name} (available: {bool(cap.get('available', False))})"
        hints = cap.get("result_path_hints")
        if isinstance(hints, list) and hints:
            line += f" result paths: {', '.join(str(h) for h in hints)}"
        lines.append(line)
    return "\n".join(lines)


def _as_text(value: Any) -> str:
    """Makes a result (str|dict|list) readable for the JSON extraction."""
    if value is None:
        return ""
    if isinstance(value, str):
        return value
    return json.dumps(value)


def _backoff_delay(consecutive_errors: int) -> float:
    """Task 4b: 5s base interval, exponential from POLL_BACKOFF_MAX_ERRORS, cap 60s."""
    factor = 2 ** max(0, consecutive_errors - POLL_BACKOFF_MAX_ERRORS + 1)
    return min(POLL_INTERVAL_SECONDS * factor, POLL_BACKOFF_MAX_SECONDS)


def _inject_flow(payload: dict, cap_info: dict | None, origin_task_id: str, task_ref: str) -> dict:
    """Payload injection (task 4 step 3): 'flow': {origin_task_id, task_ref} — only
    if the capability's input_schema doesn't reject an unknown field / allows a
    'flow' field; without input_schema in the snapshot it is injected
    (traceability, §2.5)."""
    schema = (cap_info or {}).get("input_schema")
    if isinstance(schema, dict):
        fields = schema.get("fields")
        if isinstance(fields, dict) and "flow" not in fields:
            return dict(payload)
    child = dict(payload)
    child["flow"] = {"origin_task_id": origin_task_id, "task_ref": task_ref}
    return child


# ---- T-002: template interpolation — data injection at submit time ----------

# Only EXACTLY this pattern is a template (no expressions, filters, defaults):
# ${<task_ref>.result.<dot-path>} — 'result' is a literal segment.
_TEMPLATE_PATTERN = r"\$\{([A-Za-z0-9_-]+)\.result\.([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*)\}"
TEMPLATE_RE = re.compile(_TEMPLATE_PATTERN)


def _resolve_templates(payload: dict, aggregate: dict[str, Any]) -> dict:
    """Resolves ${ref.result.path} templates in the payload recursively (T-002):
    dict values, list items, strings.

    Aggregate shape (T-005c, verified against _task_result + the join loop):
    the aggregate stores the BARE stage result under task_ref
    (`aggregate[task_ref] = _task_result(view)`, runner.py join loop) —
    NO wrapper. The handler result envelope (design.md T-005a §2.2) is
    unwrapped EXACTLY ONCE by _task_result (tolerant: bare results from legacy
    nodes pass through unchanged) — the literal segment 'result' addresses
    exactly this aggregate entry, the dot path afterwards navigates in the inner
    result: ${img.result.artifact_id} → aggregate["img"]["artifact_id"].
    result_path_hints are consequently paths relative to the inner result
    (e.g. 'answer', NOT 'result.answer').

    Rules (plan T-002): a string matching exactly ONE template →
    native value (type is preserved); template embedded in a larger
    string → substitute all matches as str() (dict/list via
    json.dumps); missing task in the aggregate or unresolvable path →
    FlowError (fail-fast D6) with template, referenced task and
    depends_on hint. Only ${ref.result.path} is a template — everything
    else stays literal.
    """
    return _resolve_templates_value(payload, aggregate)


def _resolve_templates_value(value: Any, aggregate: dict[str, Any]) -> Any:
    """Recursion over dict values, list items; strings are resolved."""
    if isinstance(value, dict):
        return {k: _resolve_templates_value(v, aggregate) for k, v in value.items()}
    if isinstance(value, list):
        return [_resolve_templates_value(item, aggregate) for item in value]
    if isinstance(value, str):
        return _resolve_templates_string(value, aggregate)
    return value


def _resolve_templates_string(text: str, aggregate: dict[str, Any]) -> Any:
    """One string: exactly ONE template → native value; otherwise all matches as str()."""
    if TEMPLATE_RE.search(text) is None:
        return text
    exact = TEMPLATE_RE.fullmatch(text)
    if exact:
        return _template_lookup(exact.group(1), exact.group(2), text, aggregate)
    return TEMPLATE_RE.sub(
        lambda m: _template_str(_template_lookup(m.group(1), m.group(2), m.group(0), aggregate)),
        text,
    )


def _template_lookup(ref: str, path: str, template: str, aggregate: dict[str, Any]) -> Any:
    """Resolves ref + dot path against the aggregate — fail-fast D6 with a clear
    reason: the template, the referenced task and the depends_on hint."""
    if ref not in aggregate:
        raise FlowError(
            f"template {template} unresolved: task {ref!r} not in aggregate — "
            f"add {ref!r} to depends_on"
        )
    node: Any = aggregate[ref]
    for seg in path.split("."):
        if not isinstance(node, dict) or seg not in node:
            raise FlowError(
                f"template {template} unresolved: path {path!r} not found in result of "
                f"task {ref!r} — check the path and add {ref!r} to depends_on"
            )
        node = node[seg]
    return node


def _template_str(value: Any) -> str:
    """Embedded substitution: scalars via str(), dict/list via json.dumps."""
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _poll_task(api: RelayApi, task_id: str, tick) -> tuple[str, Any]:
    """Polls ONE task until done/failed (D5). tick() per round: budget + keepalive.

    Returns ("completed", result) resp. ("failed", error). After backoff exhaustion
    (task 4b) → FlowError WITHOUT fail_stage: the server release mechanics take over.
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


def _envelope_payload(payload: dict) -> dict:
    """T-005d: strip the Request Envelope (T-005a design.md §3.2) if present.

    The flow capability's stdin comes from the node's handler_runner, which
    has THREE live generations in the fleet:
      - pre-T-005b (iowap-node ≤2.3.9, flow-runner-01 today): flat payload
        dict → returned unchanged;
      - rollout (2.3.12): mirrored envelope — contract keys AND payload keys
        at top level → contract keys win (they cannot occur in a flow
        payload);
      - strict (≥2.3.13): {"task_id", "capability", "input": {...}}.

    Discriminator: "task_id"+"capability" present AND "input" is a mapping.
    Mode bookkeeping ("mode"/"flow"/"history_id"/"input") stays wherever the
    dispatcher put it — for mirrored/strict stdin that is INSIDE `input`
    (the dispatcher wraps its whole task payload), for flat stdin at the
    top level; both are read uniformly after this strip.
    """
    if (
        payload.get("task_id") is not None
        and payload.get("capability") is not None
        and isinstance(payload.get("input"), dict)
    ):
        return payload["input"]
    return payload


MANAGEMENT_MODES = frozenset({
    "list_flows", "list_history", "show_history", "save_flow", "define_flow",
})


def manage(
    payload: dict,
    api: RelayApi,
    base_url: str,
    token_file: str,
    origin_task_id: str,
    origin_stage_id: str,
    node_id: str,
    options: dict | None = None,
) -> dict:
    """T-231b: the flow.catalog surface — catalog/history management.

    Same shape as run(), but no planning child, no fan-out. Handles ONLY
    the modes in MANAGEMENT_MODES; anything else → FlowError (D6).
    """
    payload = _envelope_payload(payload)
    mode = payload.get("mode")
    if mode not in MANAGEMENT_MODES:
        msg = "manage payload missing/unknown 'mode' (one of: " + ", ".join(
            sorted(MANAGEMENT_MODES)) + ")"
        api.fail_stage(origin_task_id, origin_stage_id, msg)
        raise FlowError(msg)
    del node_id  # management needs no node identity beyond origin context
    del options

    if mode == "list_flows":
        caps = fetch_capabilities(base_url, token_file)
        entries = list_catalog_flows(caps)
        # T-005d: conforming Response Envelope — payload under "result"
        # (handler_runner ≥2.3.12 would otherwise fail "completed missing
        # result"); NO "error" key (daemon counts failures by key presence).
        result = {"status": "completed",
                  "result": {"mode": "list_flows", "flows": entries,
                             "summary": f"{len(entries)} flows in catalog"}}
        api.complete_stage(origin_task_id, origin_stage_id, result)
        return result
    if mode == "save_flow":
        try:
            name = save_flow_from_history(str(payload.get("history_id") or ""))
        except CatalogError as e:
            msg = f"save_flow failed: {e}"
            api.fail_stage(origin_task_id, origin_stage_id, msg)
            raise FlowError(msg) from None
        # T-005d: conforming Response Envelope — payload under "result",
        # no "error" key (daemon counts failures by key presence).
        result = {"status": "completed",
                  "result": {"mode": "save_flow", "flow": name,
                             "summary": f"flow {name!r} saved to catalog"}}
        api.complete_stage(origin_task_id, origin_stage_id, result)
        return result
    if mode == "list_history":
        records = list_history_records(payload.get("limit") or 20)
        # T-005d: conforming Response Envelope — payload under "result",
        # no "error" key (daemon counts failures by key presence).
        result = {"status": "completed",
                  "result": {"mode": "list_history", "records": records,
                             "summary": f"{len(records)} history records"}}
        api.complete_stage(origin_task_id, origin_stage_id, result)
        return result
    if mode == "show_history":
        try:
            record = load_history_record(str(payload.get("history_id") or ""))
        except CatalogError as e:
            msg = f"show_history failed: {e}"
            api.fail_stage(origin_task_id, origin_stage_id, msg)
            raise FlowError(msg) from None
        result = {"status": "completed",
                  "result": {"mode": "show_history", "record": record,
                             "summary": f"history {record.get('history_id')!r}"}}
        api.complete_stage(origin_task_id, origin_stage_id, result)
        return result
    if mode == "define_flow":
        # T-231 F2: catalog definition straight from the payload —
        # no history roundtrip needed.
        try:
            name = define_flow(str(payload.get("name") or ""), payload.get("plan"))
        except CatalogError as e:
            msg = f"define_flow failed: {e}"
            api.fail_stage(origin_task_id, origin_stage_id, msg)
            raise FlowError(msg) from None
        result = {"status": "completed",
                  "result": {"mode": "define_flow", "flow": name,
                             "summary": f"flow {name!r} defined in catalog"}}
        api.complete_stage(origin_task_id, origin_stage_id, result)
        return result



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
    """Runs the complete flow: plan phase → validation → fan-out → join → aggregate.

    T-003 modes (payload['mode'], cleanly separated from the classic run):
      - 'list_flows': catalog query (no plan child, no fan-out).
      - 'run_flow':   prebuilt flow from the catalog — skips the plan
        phase, ${input.path} parameters, otherwise identical fan-out/join.
      - 'save_flow':  promotes a history entry permanently into the catalog.
    Without 'mode' (default): classic run (plan phase via agent.ai, FROZEN).

    Semantics (plan task 4, fail-fast D6):
      0. Plan phase: note 'longrun' 'flow started' → discovery snapshot → PLAN_PROMPT →
         planning child to agent.ai (idempotency_key flow-<origin>-_plan, name
         flow:_plan) → note 'planning started' → poll until done → leniently
         extract the result. Planning child failed or no valid plan JSON → D9:
         1 retry with the validation error in the prompt, 2nd attempt fails → flow failed.
      1. Validation: parse_plan + validate_capabilities_live against a FRESH snapshot
         (race safety net).
      2. Topologically: submit all tasks with satisfied deps simultaneously (real
         parallel fan-out), idempotency_key flow-<origin-task-id>-<task-id>.
      3. Template resolution ${ref.result.path} (T-002, data injection from the
         running aggregate) + payload injection: 'flow': {origin_task_id,
         task_ref} — only if the capability's input_schema allows it,
         otherwise omit.
      4. Poll loop 5s per child; keepalive note every 600s independent of the poll cadence:
         'flow progress: done=K/N' (lease refresh on the origin task, T-154).
      5. Child failed (permanent) → the whole flow fails: note 'flow failed at
         <task-ref> (<capability>): <error>' → fail_stage() → FlowError.
      6. All done → aggregate {task_ref: result} → complete_stage() with
         {'status': 'completed', 'aggregate': ..., 'summary': plan.summary}.
      7. After every fan-out state change: note kind=info on the origin task.
    """
    options = options or {}
    # T-005d: accept all three handler_runner generations (strict/mirrored/
    # flat — see _envelope_payload); payload now carries the dispatcher's
    # task payload uniformly.
    payload = _envelope_payload(payload)
    mode = payload.get("mode")

    # T-231b: management modes moved to manage() — still reachable over
    # flow.run payloads (backwards-compatible T-003 modes), and exposed
    # natively via the flow.catalog capability.
    if mode in MANAGEMENT_MODES:
        return manage(
            payload=payload,
            api=api,
            base_url=base_url,
            token_file=token_file,
            origin_task_id=origin_task_id,
            origin_stage_id=origin_stage_id,
            node_id=node_id,
            options=options,
        )
    task_text = str(payload.get("task") or payload.get("original_request") or "")
    flow_input: dict[str, Any] | None = None
    if mode == "run_flow":
        raw_flow = payload.get("flow")
        if not isinstance(raw_flow, str) or not raw_flow:
            raise FlowError("run_flow payload missing 'flow' (catalog name)")
        flow_input = payload.get("input") if isinstance(payload.get("input"), dict) else {}
        # History cleanup fail-soft at flow start (both rules: age + count)
        try:
            cleanup_history()
        except OSError:
            pass
        try:
            raw_plan = load_flow(raw_flow)
        except CatalogError as e:
            api.fail_stage(origin_task_id, origin_stage_id, str(e))
            raise FlowError(str(e)) from None
    elif not task_text:
        raise FlowError("flow payload missing 'task' (original request)")
    if not task_text and mode == "run_flow":
        task_text = f"run flow {raw_flow}"  # basis for notes/naming
    max_flow_seconds = int(options.get("max_flow_seconds") or DEFAULT_MAX_FLOW_SECONDS)

    started = time.monotonic()
    keepalive_due = started + KEEPALIVE_INTERVAL_SECONDS

    def tick(progress_msg: str) -> None:
        """Budget check (fail_stage when exceeded) + independent keepalive timer."""
        nonlocal keepalive_due
        now = time.monotonic()
        if now - started > max_flow_seconds:
            api.fail_stage(origin_task_id, origin_stage_id, "flow budget exceeded")
            raise FlowError("flow budget exceeded")
        if now >= keepalive_due:
            api.add_note(origin_task_id, progress_msg, kind="longrun")
            keepalive_due = now + KEEPALIVE_INTERVAL_SECONDS

    # -- Phase 0: planning via agent.ai (D2 = A: prompt + cap snapshot from the handler) --
    api.add_note(origin_task_id, "flow started", kind="longrun")
    caps = fetch_capabilities(base_url, token_file)
    plan: Plan | None = None
    feedback: str | None = None
    if mode == "run_flow":
        # T-003: prebuilt flow — no plan phase; input templates are resolved
        # into a copy of the payload (the original catalog stays unchanged).
        try:
            plan = parse_plan(raw_plan)
        except PlanError as e:
            api.fail_stage(origin_task_id, origin_stage_id, f"run_flow: invalid catalog plan: {e}")
            raise FlowError(f"run_flow: invalid catalog plan: {e}") from None
        resolved = {}
        for t in plan.tasks:
            try:
                resolved[t.id] = resolve_input_templates(dict(t.payload), flow_input or {})
            except CatalogError as e:
                msg = f"run_flow task {t.id!r}: {e}"
                api.fail_stage(origin_task_id, origin_stage_id, msg)
                raise FlowError(msg) from None
        input_resolved_plan = {
            "version": 1,
            "name": plan.name,
            "summary": plan.summary,
            "tasks": [
                {"id": t.id, "capability": t.capability,
                 "payload": resolved[t.id], "depends_on": list(t.depends_on)}
                for t in plan.tasks
            ],
        }
        plan = parse_plan(input_resolved_plan)
        api.add_note(origin_task_id, f"flow started from catalog: {plan.name}", kind="longrun")
    else:
        for attempt in (1, 2):  # D9: 1 retry with the validation error in the prompt
            if plan_prompt_split_enabled():
                instr, taskpart = build_plan_prompt_parts(
                    task_text, _caps_snapshot(caps), feedback
                )
                # T-254 (Ronny 2026-10-27): the specialist is NOT an agent —
                # no tool use, no access to the live capability list — so the
                # flow runner ships the list itself, as a SEPARATE array field
                # (model.specialist capabilities contract: name/description/
                # input_schema). The plan system prompt rides as "instruction"
                # (verbatim, incl. the inline schema block — frozen text);
                # "task" carries the original request + caps text + retry
                # feedback; the workflow JSON-schema for Ollama structured
                # outputs is added SERVER-SIDE as a const binding rule (the
                # schema lives where the wiring lives — Ronny's variable
                # routing form, not hardcoded in the runner).
                submit_payload = {
                    "instruction": instr,
                    "task": taskpart,
                    # T-256-live: FULL list, COMPACT entries — A2/A3/A4 proved
                    # the compact form plans perfectly while the 56k
                    # full-schema form drowns the task (pseudo-references,
                    # dropped artifacts). _caps_array stays for
                    # model_specialist's raw-form fallback paths.
                    "capabilities": _caps_compact(caps),
                    "forward_slot": PLAN_FORWARD_SLOT,
                }
            else:
                prompt = build_plan_prompt(task_text, _caps_snapshot(caps))
                if feedback:
                    prompt += PLAN_RETRY_FEEDBACK.replace("{feedback}", feedback)
                submit_payload = {"task": prompt, "forward_slot": PLAN_FORWARD_SLOT}
            resp = api.submit_simple_task(
                capability=PLAN_CAPABILITY,
                payload=submit_payload,
                # The D9 retry needs a NEW idempotency key per attempt: with a
                # constant key the server returns the same, already fully
                # failed planning child and the feedback retry would be dead
                # (live bug 2026-09-06). Attempt 1 keeps the §2.5 shape — resume
                # after a handler crash starts again at attempt 1 and finds the
                # finished child via the unchanged key.
                name=f"{TASK_NAME_PREFIX}{PLAN_TASK_REF}{'' if attempt == 1 else f'-{attempt}'}",
                idempotency_key=_idem(origin_task_id, PLAN_TASK_REF) + ("" if attempt == 1 else f"-{attempt}"),
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

    # -- Phase 1: live validation against a FRESH snapshot (race safety net) --
    caps_fresh = fetch_capabilities(base_url, token_file)
    try:
        validate_capabilities_live(plan, caps_fresh)
    except PlanError as e:
        raise FlowError(str(e)) from e

    # -- Phases 2-4: topological fan-out + join loop --
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
            # T-002: template resolution BEFORE _inject_flow — flow metadata comes
            # last and is never interpolated. The FlowError carries the affected
            # task_ref as context (fail-fast D6: no submit, no successors).
            try:
                resolved_payload = _resolve_templates(t.payload, aggregate)
            except FlowError as e:
                raise FlowError(f"task {tid!r}: {e}") from None
            resp = api.submit_simple_task(
                capability=t.capability,
                payload=_inject_flow(
                    resolved_payload,
                    caps_fresh.get(t.capability),
                    origin_task_id,
                    tid,
                ),
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
                break  # backoff sleep below, then a new round
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

        # -- Phase 5: fail-fast D6 — no submit of successors --
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

    # -- Phase 6: aggregate + complete on the origin stage --
    # T-005d: conforming Response Envelope — flow/aggregate/summary move
    # under "result" (design.md §6; the old flat shape collided with the
    # contract → normalize error "completed without result"). The "error"
    # key is deliberately OMITTED: the daemon counts stage failures via
    # "error" in result (key presence) — a literal "error": null would
    # count every successful flow run as a failure. Same rationale as
    # T-005a deviation D4.
    # T-231 F3: record BEFORE complete — the history_id rides in the
    # completed result (the consumer no longer scrapes a note string).
    # Fail-soft stays: a history failure yields no history_id and never
    # fails the completed flow (record errors are OSError-class).
    history_id: str | None = None
    try:
        history_id = record_history(
            flow_name=plan.name,
            plan={
                "version": 1,
                "name": plan.name,
                "summary": plan.summary,
                "tasks": [
                    {"id": t.id, "capability": t.capability,
                     "payload": dict(t.payload), "depends_on": list(t.depends_on)}
                    for t in plan.tasks
                ],
            },
            aggregate=aggregate,
            origin_task_id=origin_task_id,
        )
        api.add_note(origin_task_id, f"flow recorded in history: {history_id}", kind="info")
    except OSError:
        pass
    result_payload: dict[str, Any] = {
        "flow": {"name": plan.name, "tasks_done": total},
        "aggregate": aggregate,
        "summary": plan.summary,
    }
    if history_id:
        result_payload["history_id"] = history_id
    result = {"status": "completed", "result": result_payload}
    api.complete_stage(origin_task_id, origin_stage_id, result)
    return result