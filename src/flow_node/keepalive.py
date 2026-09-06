"""Longrun-note keepalive for handler-side long work (T-001c, T-154 pattern).

Sends a kind="longrun" note on the claimed task right after claim — the
server switches the stage claimed -> accepted and starts a 2h TTL (T-154:
iowap-server core/scheduler.py add_note). A daemon thread keeps re-sending
notes on a fixed cadence; every note (any kind) resets the TTL. All note
failures are fail-soft: logged to stderr, never propagated (AC2).
"""
from __future__ import annotations

import threading  # noqa: F401  (Phase 3: cadence thread)
from typing import Callable  # noqa: UP035  (FROZEN design.md import — typing.Callable)

from flow_node.relay_api import RelayApi

NOTE_MAX_LENGTH = 2000  # server NoteRequest.message cap (iowap-server models/task.py)


def send_longrun_note(
    api: RelayApi,
    task_id: str,
    message: str,
    log: Callable[[str], None] | None = None,
) -> bool:
    """Send one kind="longrun" note (T-154) for task_id, fail-soft.

    Message is truncated to NOTE_MAX_LENGTH. Returns True on success,
    False on any failure; never raises. Failures reported via log(msg).
    """
    raise NotImplementedError


class NoteKeepalive:
    """T-154 longrun lease keepalive: initial note at start(), then a
    daemon thread re-sends notes every `interval` seconds. Fail-soft.

    One owner thread (handler main); not a general-purpose primitive.
    """

    def __init__(
        self,
        api: RelayApi,
        task_id: str,
        *,
        interval: float | None = None,
        message_prefix: str = "keepalive",
        log: Callable[[str], None] | None = None,
    ) -> None:
        """interval defaults to flow_node.runner.KEEPALIVE_INTERVAL_SECONDS."""
        raise NotImplementedError

    def start(self, initial_message: str) -> bool:
        """Send the initial longrun note synchronously (fail-soft), then
        start the cadence thread. Returns initial note success. Idempotent."""
        raise NotImplementedError

    def stop(self) -> None:
        """Stop the cadence thread (idempotent, join with grace)."""
        raise NotImplementedError

    @property
    def sent_count(self) -> int:
        """Number of notes actually accepted (200)."""
        raise NotImplementedError