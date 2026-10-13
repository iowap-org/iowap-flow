"""Flow catalog + history (T-003).

Cleanly separated from the classic plan run:
- Catalog: prebuilt flows as plan-JSON files in the catalog dir
  (env FLOW_CATALOG_DIR, default 'templates' relative to the CWD).
- History: not persistent. Successful runs land as JSON in the
  history dir (env FLOW_HISTORY_DIR, default 'history'); cleanup at
  flow start with BOTH rules (max-age AND max-count), fail-soft.
- save_flow: explicit promotion history → catalog.
"""
from __future__ import annotations

import json
import os
import re
import time
from pathlib import Path
from typing import Any

# Cleanup rules (both, separate): max-age AND max-count.
HISTORY_MAX_AGE_DAYS = 14
HISTORY_MAX_COUNT = 50

# Template pattern for flow input: ${input.path} (separate from ${ref.result.path}).
INPUT_TEMPLATE_RE = re.compile(
    r"\$\{input\.([A-Za-z0-9_-]+(?:\.[A-Za-z0-9_-]+)*)\}"
)


class CatalogError(RuntimeError):
    """Catalog/history error (missing flow, invalid file)."""


def catalog_dir() -> Path:
    return Path(os.environ.get("FLOW_CATALOG_DIR") or "templates")


def history_dir() -> Path:
    return Path(os.environ.get("FLOW_HISTORY_DIR") or "history")


def list_flows(caps: dict[str, dict]) -> list[dict[str, Any]]:
    """Catalog entries: name, summary, capabilities + live available status.

    Fail-soft: invalid/unreadable catalog files are reported with 'error',
    not raised — the catalog mode should answer, not fail.
    """
    entries: list[dict[str, Any]] = []
    d = catalog_dir()
    if not d.is_dir():
        return entries
    for path in sorted(d.glob("*.json")):
        name = path.stem
        try:
            raw = json.loads(path.read_text(encoding="utf-8"))
            plan_version = raw.get("version")
            tasks = raw.get("tasks") if isinstance(raw.get("tasks"), list) else []
            summary = str(raw.get("summary") or "")
            caps_needed = sorted({str(t.get("capability")) for t in tasks
                                  if isinstance(t, dict) and t.get("capability")})
            availability = {c: bool((caps.get(c) or {}).get("available", False))
                            for c in caps_needed}
        except (OSError, json.JSONDecodeError, AttributeError) as e:
            entries.append({"name": name, "error": str(e), "available": False})
            continue
        entries.append({
            "name": name,
            "summary": summary,
            "plan_version": plan_version,
            "capabilities": caps_needed,
            "capability_available": availability,
            "available": bool(availability) and all(availability.values()),
        })
    return entries


def load_flow(name: str) -> dict[str, Any]:
    """Loads plan JSON from the catalog. CatalogError when missing/invalid."""
    if not re.fullmatch(r"[A-Za-z0-9_-]+", name or ""):
        raise CatalogError(f"invalid flow name: {name!r}")
    path = catalog_dir() / f"{name}.json"
    if not path.is_file():
        raise CatalogError(f"flow {name!r} not in catalog ({catalog_dir()})")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise CatalogError(f"flow {name!r}: unreadable catalog file: {e}") from e
    if not isinstance(raw, dict):
        raise CatalogError(f"flow {name!r}: catalog file must be a JSON object")
    return raw


def save_flow_from_history(history_id: str) -> str:
    """Promotes a history entry into the catalog (explicit, persistent).

    Returns: the flow name in the catalog.
    """
    rec = load_history_record(history_id)
    name = _safe_name(str(rec.get("flow_name") or history_id))
    plan = rec.get("plan")
    if not isinstance(plan, dict):
        raise CatalogError(f"history record {history_id!r} has no plan")
    target = catalog_dir() / f"{name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(json.dumps(plan, indent=2, ensure_ascii=False) + "\n",
                      encoding="utf-8")
    return name


# ---- History (not persistent) ------------------------------------------------

def record_history(flow_name: str, plan: dict[str, Any], aggregate: dict[str, Any],
                   origin_task_id: str) -> str:
    """Writes a history entry for a SUCCESSFUL flow.

    Fail-soft by design: callers must never turn history errors into a flow
    failure (handled that way in runner.run). Returns the history_id.
    """
    history_id = f"{_safe_name(origin_task_id)}-{_safe_name(flow_name)}-{int(time.time())}"
    d = history_dir()
    d.mkdir(parents=True, exist_ok=True)
    record = {
        "history_id": history_id,
        "flow_name": flow_name,
        "origin_task_id": origin_task_id,
        "saved_at": int(time.time()),
        "plan": plan,
        "aggregate": aggregate,
    }
    (d / f"{history_id}.json").write_text(
        json.dumps(record, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    return history_id


def load_history_record(history_id: str) -> dict[str, Any]:
    """Loads a history entry. CatalogError when missing/invalid."""
    if not re.fullmatch(r"[A-Za-z0-9_.-]+", history_id or ""):
        raise CatalogError(f"invalid history_id: {history_id!r}")
    path = history_dir() / f"{history_id}.json"
    if not path.is_file():
        raise CatalogError(f"history record {history_id!r} not found ({history_dir()})")
    try:
        raw = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, json.JSONDecodeError) as e:
        raise CatalogError(f"history record {history_id!r}: unreadable: {e}") from e
    if not isinstance(raw, dict):
        raise CatalogError(f"history record {history_id!r}: must be a JSON object")
    return raw


def cleanup_history(now: float | None = None, max_age_days: int = HISTORY_MAX_AGE_DAYS,
                    max_count: int = HISTORY_MAX_COUNT) -> dict[str, int]:
    """Cleans up the history with BOTH rules: max-age AND max-count.

    Returns {'removed_age': n, 'removed_count': m}. No dir → no-op.
    """
    now = time.time() if now is None else now
    d = history_dir()
    removed = {"removed_age": 0, "removed_count": 0}
    if not d.is_dir():
        return removed
    files = sorted(d.glob("*.json"), key=lambda p: p.stat().st_mtime, reverse=True)
    cutoff = now - max_age_days * 86400
    keep: list[Path] = []
    for path in files:
        try:
            if path.stat().st_mtime < cutoff:
                path.unlink()
                removed["removed_age"] += 1
            elif len(keep) >= max_count:
                path.unlink()
                removed["removed_count"] += 1
            else:
                keep.append(path)
            # Files that are neither old nor surplus stay in keep.
        except OSError:
            continue  # fail-soft: skip busy/stranded file
    return removed


# ---- ${input.path} template resolution (flow input) -------------------------

def resolve_input_templates(value: Any, flow_input: dict[str, Any]) -> Any:
    """Resolves ${input.path} against the flow input object (recursive).

    A string with exactly ONE match → native value; embedded → str() sub-
    stitution. Missing input path → CatalogError (fail-fast; the runner wraps
    it with task context).
    """
    if isinstance(value, dict):
        return {k: resolve_input_templates(v, flow_input) for k, v in value.items()}
    if isinstance(value, list):
        return [resolve_input_templates(v, flow_input) for v in value]
    if isinstance(value, str):
        if INPUT_TEMPLATE_RE.search(value) is None:
            return value
        exact = INPUT_TEMPLATE_RE.fullmatch(value)
        if exact:
            return _input_lookup(exact.group(1), value, flow_input)
        return INPUT_TEMPLATE_RE.sub(
            lambda m: _template_str(_input_lookup(m.group(1), m.group(0), flow_input)),
            value,
        )
    return value


def _input_lookup(path: str, template: str, flow_input: dict[str, Any]) -> Any:
    node: Any = flow_input
    for seg in path.split("."):
        if not isinstance(node, dict) or seg not in node:
            raise CatalogError(
                f"input template {template} unresolved: path {path!r} not in flow input"
            )
        node = node[seg]
    return node


def _template_str(value: Any) -> str:
    if isinstance(value, (dict, list)):
        return json.dumps(value)
    return str(value)


def _safe_name(raw: str) -> str:
    """Compact, filesystem-safe name component."""
    cleaned = re.sub(r"[^A-Za-z0-9_.-]+", "_", raw.strip())
    return cleaned[:60] or "flow"