# tests/test_flow_caps_snapshot.py — T-004 (iowap-flow): _caps_snapshot shows
# result_path_hints so the planner knows valid ${ref.result.path} paths.
from flow_node.runner import _caps_snapshot


def test_caps_snapshot_without_hints_unchanged():
    caps = {"tool.x": {"available": True}, "tool.y": {"available": False}}
    out = _caps_snapshot(caps)
    assert out == (
        "- tool.x (available: True)\n"
        "- tool.y (available: False)"
    )


def test_caps_snapshot_with_hints_appends_paths():
    caps = {"chat.ai": {"available": True, "result_path_hints": ["result.answer"]}}
    out = _caps_snapshot(caps)
    assert "- chat.ai (available: True) result paths: result.answer" in out


def test_caps_snapshot_hints_multiple_and_empty():
    caps = {
        "a.ai": {"available": True, "result_path_hints": ["result.answer", "result.text"]},
        "b.ai": {"available": True, "result_path_hints": []},
        "c.ai": {"available": True, "result_path_hints": "not-a-list"},
    }
    out = _caps_snapshot(caps)
    assert "a.ai" in out and "result.answer, result.text" in out
    assert "- b.ai (available: True)" in out  # empty hints → no suffix
    assert "- c.ai (available: True)" in out  # not a list type → no suffix


def test_caps_snapshot_empty():
    assert _caps_snapshot({}) == "(no capabilities available)"