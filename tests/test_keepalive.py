"""Unit tests for the T-154 longrun keepalive (T-001c, design.md §4).

In-process fakes: no HTTP, no subprocess. Design source:
.hermes/pipeline/T-001c/design.md — signatures FROZEN.
"""
import threading
import time

import pytest  # noqa: F401  (spec-listed import; no pytest construct needed in these unit tests)

from flow_node.keepalive import NOTE_MAX_LENGTH, NoteKeepalive, send_longrun_note
from flow_node.relay_api import RelayApi  # noqa: F401  (spec-listed import; FakeApi is duck-typed)


class FakeApi:
    """Records add_note calls; can be told to fail (fail-soft tests)."""

    def __init__(self) -> None:
        self.notes: list[tuple[str, str, str]] = []  # (task_id, message, kind)
        self.fail: bool = False
        self.lock = threading.Lock()

    def add_note(self, task_id: str, message: str, kind: str = "info") -> None:
        with self.lock:
            if self.fail:
                raise RuntimeError("relay unavailable")
            self.notes.append((task_id, message, kind))


def test_send_longrun_note_success() -> None:
    api = FakeApi()
    logs: list[str] = []
    assert send_longrun_note(api, "task-1", "planning started", log=logs.append) is True
    assert api.notes == [("task-1", "planning started", "longrun")]
    assert logs == []


def test_send_longrun_note_fail_soft() -> None:
    api = FakeApi()
    api.fail = True
    logs: list[str] = []
    # never raises, returns False, failure is logged (AC2)
    assert send_longrun_note(api, "task-1", "planning started", log=logs.append) is False
    assert api.notes == []
    assert len(logs) == 1


def test_send_longrun_note_truncates() -> None:
    api = FakeApi()
    assert send_longrun_note(api, "task-1", "x" * 3000) is True
    assert len(api.notes[0][1]) == NOTE_MAX_LENGTH


def test_keepalive_start_sends_initial_note() -> None:
    api = FakeApi()
    ka = NoteKeepalive(api, "task-1", interval=3600.0)
    assert ka.start("planning started") is True
    assert len(api.notes) == 1  # exactly one note, synchronously, before any waiting
    assert api.notes[0] == ("task-1", "planning started", "longrun")
    ka.stop()


def test_keepalive_cadence() -> None:
    api = FakeApi()
    ka = NoteKeepalive(api, "task-1", interval=0.05)
    assert ka.start("planning started") is True
    time.sleep(0.25)
    ka.stop()
    assert ka.sent_count >= 2


def test_keepalive_stop_halts() -> None:
    api = FakeApi()
    ka = NoteKeepalive(api, "task-1", interval=0.05)
    ka.start("planning started")
    time.sleep(0.12)
    ka.stop()
    frozen = ka.sent_count
    time.sleep(0.15)  # would fire ~3 more ticks if stop() had not halted the thread
    assert ka.sent_count == frozen


def test_keepalive_survives_note_failure() -> None:
    api = FakeApi()
    api.fail = True
    logs: list[str] = []
    ka = NoteKeepalive(api, "task-1", interval=0.05, log=logs.append)
    assert ka.start("planning started") is False  # initial note fails — fail-soft
    time.sleep(0.15)  # several ticks fail; the cadence thread must survive them
    api.fail = False
    time.sleep(0.12)  # the next tick succeeds — proves the thread lived on
    ka.stop()
    assert ka.sent_count >= 1
    assert any("failed" in line for line in logs)


def test_keepalive_start_idempotent() -> None:
    api = FakeApi()
    ka = NoteKeepalive(api, "task-1", interval=3600.0)
    assert ka.start("planning started") is True
    first_thread = ka._thread  # no public handle exists (frozen API) — pragmatic check
    assert ka.start("again") is True  # no-op: returns the stored initial result
    assert len(api.notes) == 1  # no second initial note
    assert ka._thread is first_thread  # no second thread
    ka.stop()