"""Unit tests for the T-154 longrun keepalive (T-001c, design.md §4).

In-process fakes: no HTTP, no subprocess. Design source:
.hermes/pipeline/T-001c/design.md — signatures FROZEN.
"""
import threading
import time  # noqa: F401  (Phase 3: cadence timing assertions)

import pytest  # noqa: F401  (Phase 3: tmp_path fixtures / raises)

from flow_node.keepalive import (  # noqa: F401  (Phase 3)
    NOTE_MAX_LENGTH,
    NoteKeepalive,
    send_longrun_note,
)
from flow_node.relay_api import RelayApi  # noqa: F401  (Phase 3: FakeApi signature)


class FakeApi:
    """Records add_note calls; can be told to fail (fail-soft tests)."""

    def __init__(self) -> None:
        self.notes: list[tuple[str, str, str]] = []  # (task_id, message, kind)
        self.fail: bool = False
        self.lock = threading.Lock()

    def add_note(self, task_id: str, message: str, kind: str = "info") -> None:
        raise NotImplementedError


def test_send_longrun_note_success() -> None:
    raise NotImplementedError


def test_send_longrun_note_fail_soft() -> None:
    raise NotImplementedError


def test_send_longrun_note_truncates() -> None:
    raise NotImplementedError


def test_keepalive_start_sends_initial_note() -> None:
    raise NotImplementedError


def test_keepalive_cadence() -> None:
    raise NotImplementedError


def test_keepalive_stop_halts() -> None:
    raise NotImplementedError


def test_keepalive_survives_note_failure() -> None:
    raise NotImplementedError


def test_keepalive_start_idempotent() -> None:
    raise NotImplementedError