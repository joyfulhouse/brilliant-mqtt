"""Bus-liveness heartbeat for the independent message-bus watchdog.

Successful reads offer a beat; writes are capped at a ten-second cadence so
the hot poll does not churn tmpfs metadata. The watchdog's stale threshold is
far longer. tmpfs remains the default, so there is no flash wear.
"""

from __future__ import annotations

import logging
import os
import time
from collections.abc import Callable

logger = logging.getLogger(__name__)

_MIN_WRITE_INTERVAL_S = 10.0
_last_attempt: dict[str, float] = {}


def write_heartbeat(
    path: str,
    clock: Callable[[], float],
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> None:
    """Atomically stamp *path* with the current epoch seconds. Best-effort:
    writes are limited to one per ten seconds, an empty path is a no-op, and
    any I/O error is swallowed (a heartbeat failure must never disrupt the
    bridge)."""
    if not path:
        return
    now = monotonic_clock()
    last_attempt = _last_attempt.get(path)
    if last_attempt is not None and now - last_attempt < _MIN_WRITE_INTERVAL_S:
        return
    _last_attempt[path] = now
    try:
        parent = os.path.dirname(path)
        if parent:
            os.makedirs(parent, exist_ok=True)
        tmp = f"{path}.tmp"
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(f"{clock()}")
        os.replace(tmp, path)
    except OSError:
        logger.debug("heartbeat write failed for %s", path, exc_info=True)
