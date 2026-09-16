"""Bounded process-lifetime observations, published with the existing bridge meta.

All mutations run on the event loop: the bus library's off-loop reconnect hook
already marshals through call_soon_threadsafe before _note_reconnect. Executor
threads never touch this recorder. Each increment and snapshot is synchronous,
with no await to interleave a read/modify/write; no locks or atomics are needed.
"""

from __future__ import annotations

import time
from collections import deque
from collections.abc import Callable
from typing import Literal

WriteOutcome = Literal[
    "ok",
    "error",
    "timeout_bus",
    "timeout_async",
    "cancelled",
    "detached_late_ok",
    "detached_late_error",
]
SessionRebuildReason = Literal[
    "mqtt_reader_dead",
    "bus_stale",
    "bus_write_stuck",
    "bus_reconnect_storm",
    "mqtt_transport_overload",
    "other",
]
Snapshot = dict[str, int | float | None | dict[str, int]]


class ResponseDiagnostics:
    """One recorder shared by every session; never persists or resets on publish."""

    def __init__(self, *, clock: Callable[[], float] = time.monotonic) -> None:
        self._clock = clock
        self._started_at = clock()
        self._outcomes: dict[WriteOutcome, int] = {
            "ok": 0,
            "error": 0,
            "timeout_bus": 0,
            "timeout_async": 0,
            "cancelled": 0,
            "detached_late_ok": 0,
            "detached_late_error": 0,
        }
        self._hard_cap_total = 0
        self._superseded = 0
        self._bus_reconnect_total = 0
        self._session_rebuild: dict[SessionRebuildReason, int] = {
            "mqtt_reader_dead": 0,
            "bus_stale": 0,
            "bus_write_stuck": 0,
            "bus_reconnect_storm": 0,
            "mqtt_transport_overload": 0,
            "other": 0,
        }
        self._queue_wait_s_sum = 0.0
        self._queue_wait_s_count = 0
        self._rpc_s_sum = 0.0
        self._rpc_s_count = 0
        self._queue_wait_recent: deque[float] = deque(maxlen=64)
        self._rpc_recent: deque[float] = deque(maxlen=64)

    def note_superseded(self) -> None:
        self._superseded += 1

    def note_write_settled(
        self, outcome: WriteOutcome, queue_wait_s: float | None, rpc_s: float | None
    ) -> None:
        """Count one outcome; absent measurements never enter timing populations."""
        self._outcomes[outcome] += 1
        if queue_wait_s is not None:
            self._queue_wait_s_sum += queue_wait_s
            self._queue_wait_s_count += 1
            self._queue_wait_recent.append(queue_wait_s)
        if rpc_s is not None:
            self._rpc_s_sum += rpc_s
            self._rpc_s_count += 1
            self._rpc_recent.append(rpc_s)

    def note_bus_reconnect(self) -> None:
        self._bus_reconnect_total += 1

    def note_session_rebuild(self, reason: SessionRebuildReason) -> None:
        self._session_rebuild[reason] += 1

    def note_hard_cap(self) -> None:
        self._hard_cap_total += 1

    def snapshot(self) -> Snapshot:
        """Copy every mutable container before a publisher can yield the loop."""
        rebuilds: dict[str, int] = {
            reason: count for reason, count in self._session_rebuild.items()
        }
        return {
            "v": 1,
            "uptime_s": self._clock() - self._started_at,
            "write_total": sum(self._outcomes.values()),
            **{f"write_{outcome}": count for outcome, count in self._outcomes.items()},
            "write_hard_cap_total": self._hard_cap_total,
            "superseded_before_dispatch": self._superseded,
            "bus_reconnect_total": self._bus_reconnect_total,
            "session_rebuild": rebuilds,
            "queue_wait_s_sum": self._queue_wait_s_sum,
            "queue_wait_s_count": self._queue_wait_s_count,
            "rpc_s_sum": self._rpc_s_sum,
            "rpc_s_count": self._rpc_s_count,
            "queue_wait_s_recent_max": max(self._queue_wait_recent, default=None),
            "rpc_s_recent_max": max(self._rpc_recent, default=None),
        }
