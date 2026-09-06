"""Longrun-note keepalive for handler-side long work (T-001c, T-154 pattern).

Sends a kind="longrun" note on the claimed task right after claim — the
server switches the stage claimed -> accepted and starts a 2h TTL (T-154:
iowap-server core/scheduler.py add_note). A daemon thread keeps re-sending
notes on a fixed cadence; every note (any kind) resets the TTL. All note
failures are fail-soft: logged to stderr, never propagated (AC2).
"""
from __future__ import annotations

import threading
from typing import Callable  # noqa: UP035  (FROZEN design.md §2.1 import — typing.Callable)

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
    if not task_id:
        if log is not None:
            log("longrun note skipped: empty task id")
        return False
    try:
        api.add_note(task_id, message[:NOTE_MAX_LENGTH], kind="longrun")
        return True
    except Exception as exc:  # noqa: BLE001  (D5 fail-soft — "never raises" is frozen design.md §2.1)
        if log is not None:
            log(f"longrun note failed: {exc}")
        return False


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
        self._api = api
        self._task_id = task_id
        if interval is None:
            # Lazy import (stubs.md note): no module-level coupling to flow_node.runner.
            from flow_node.runner import KEEPALIVE_INTERVAL_SECONDS

            interval = KEEPALIVE_INTERVAL_SECONDS
        self._interval = float(interval)
        self._message_prefix = message_prefix
        self._log = log
        self._stop_event = threading.Event()
        self._thread: threading.Thread | None = None
        self._initial_ok = False
        self._count = 0

    def start(self, initial_message: str) -> bool:
        """Send the initial longrun note synchronously (fail-soft), then
        start the cadence thread. Returns initial note success. Idempotent."""
        if self._thread is not None:
            if self._log is not None:
                self._log("keepalive already started — ignoring duplicate start()")
            return self._initial_ok
        ok = send_longrun_note(self._api, self._task_id, initial_message, self._log)
        self._initial_ok = ok
        if ok:
            self._count += 1
        self._stop_event.clear()
        self._thread = threading.Thread(target=self._loop, daemon=True, name="note-keepalive")
        self._thread.start()
        return ok

    def stop(self) -> None:
        """Stop the cadence thread (idempotent, join with grace)."""
        self._stop_event.set()
        thread = self._thread
        if thread is not None and thread.is_alive():
            thread.join(timeout=2.0)

    @property
    def sent_count(self) -> int:
        """Number of notes actually accepted (200)."""
        return self._count

    def _loop(self) -> None:
        """Cadence loop: one fail-soft longrun note per interval until stopped."""
        while not self._stop_event.wait(self._interval):
            ok = send_longrun_note(
                self._api,
                self._task_id,
                f"{self._message_prefix}: still working ({self._count})",
                self._log,
            )
            if ok:
                self._count += 1