# tests/test_flow_catalog.py — T-003: Katalog, Historie (Cleanup beide Regeln), ${input}-Templates
import json
import os
import time

import pytest

from flow_node.catalog import (
    CatalogError,
    cleanup_history,
    list_flows,
    load_flow,
    load_history_record,
    record_history,
    resolve_input_templates,
    save_flow_from_history,
)

FLOW_JSON = {
    "version": 1,
    "name": "demo",
    "tasks": [
        {"id": "a", "capability": "tool.x", "payload": {"p": 1}, "depends_on": []},
        {"id": "b", "capability": "tool.y", "payload": {}, "depends_on": ["a"]},
    ],
    "summary": "s",
}


@pytest.fixture
def dirs(tmp_path, monkeypatch):
    monkeypatch.setenv("FLOW_CATALOG_DIR", str(tmp_path / "templates"))
    monkeypatch.setenv("FLOW_HISTORY_DIR", str(tmp_path / "history"))
    (tmp_path / "templates").mkdir()
    (tmp_path / "history").mkdir()
    return tmp_path


# ---- Katalog ---------------------------------------------------------------

def test_list_flows_reports_available_status(dirs):
    (dirs / "templates" / "demo.json").write_text(json.dumps(FLOW_JSON))
    caps = {"tool.x": {"available": True}, "tool.y": {"available": False}}
    entries = list_flows(caps)
    assert len(entries) == 1
    e = entries[0]
    assert e["name"] == "demo"
    assert e["capabilities"] == ["tool.x", "tool.y"]
    assert e["capability_available"] == {"tool.x": True, "tool.y": False}
    assert e["available"] is False  # tool.y unavailable


def test_list_flows_empty_when_no_dir(dirs):
    (dirs / "templates").rmdir()
    assert list_flows({}) == []


def test_list_flows_fail_soft_on_broken_file(dirs):
    (dirs / "templates" / "broken.json").write_text("{not json")
    entries = list_flows({})
    assert entries[0]["name"] == "broken"
    assert "error" in entries[0]


def test_load_flow_validates_name_and_missing(dirs):
    (dirs / "templates" / "demo.json").write_text(json.dumps(FLOW_JSON))
    assert load_flow("demo")["name"] == "demo"
    with pytest.raises(CatalogError, match="not in catalog"):
        load_flow("nope")
    with pytest.raises(CatalogError, match="invalid flow name"):
        load_flow("../etc/passwd")


# ---- Historie + save_flow ---------------------------------------------------

def test_record_and_load_history(dirs):
    hid = record_history("demo", FLOW_JSON, {"a": 1}, "task-9")
    rec = load_history_record(hid)
    assert rec["flow_name"] == "demo"
    assert rec["plan"]["name"] == "demo"
    assert rec["aggregate"] == {"a": 1}
    with pytest.raises(CatalogError, match="not found"):
        load_history_record("missing-id")


def test_cleanup_history_removes_old_and_overcount(dirs):
    d = dirs / "history"
    # alt (30 Tage), jung, plus 55 weitere junge Einträge
    old = d / "old.json"
    old.write_text("{}")
    for i in range(55):
        (d / f"r{i:03d}.json").write_text("{}")
    cutoff = time.time() - 30 * 86400
    os.utime(old, (cutoff, cutoff))
    removed = cleanup_history(max_age_days=14, max_count=50)
    assert removed["removed_age"] == 1  # old.json
    assert removed["removed_count"] == 5  # 55 junge Einträge, Cap 50
    assert len(list(d.glob("*.json"))) == 50


def test_cleanup_history_noop_without_dir(dirs):
    (dirs / "history").rmdir()
    assert cleanup_history() == {"removed_age": 0, "removed_count": 0}


def test_save_flow_promotes_history_to_catalog(dirs):
    hid = record_history("demo", FLOW_JSON, {"a": 1}, "task-9")
    name = save_flow_from_history(hid)
    assert name == "demo"
    assert load_flow("demo")["name"] == "demo"


# ---- ${input.path}-Templates -------------------------------------------------

def test_resolve_input_exact_and_embedded():
    fi = {"stadt": "Leipzig", "n": 3, "opts": {"v": True}}
    assert resolve_input_templates("${input.stadt}", fi) == "Leipzig"
    assert resolve_input_templates("${input.n}", fi) == 3
    assert resolve_input_templates("${input.opts.v}", fi) is True
    assert resolve_input_templates(
        "Wetter für ${input.stadt} mit ${input.n} Tagen", fi
    ) == "Wetter für Leipzig mit 3 Tagen"
    assert resolve_input_templates("kein template", fi) == "kein template"


def test_resolve_input_missing_path_fails():
    with pytest.raises(CatalogError, match="unresolved"):
        resolve_input_templates("${input.nix}", {})


def test_resolve_input_recursive_structures():
    fi = {"a": "x"}
    assert resolve_input_templates({"p": ["${input.a}"]}, fi) == {"p": ["x"]}