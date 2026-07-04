"""Bus-liveness heartbeat: the bridge stamps this file on every successful bus
read, so the independent bus-watchdog can detect a wedged message_bus session
(the bridge stops stamping) and reboot. tmpfs path by default — no flash wear."""

from __future__ import annotations

import logging
import os
from collections.abc import Callable

logger = logging.getLogger(__name__)


def write_heartbeat(path: str, clock: Callable[[], float]) -> None:
    """Atomically stamp *path* with the current epoch seconds. Best-effort:
    an empty path is a no-op and any I/O error is swallowed (a heartbeat
    failure must never disrupt the bridge)."""
    if not path:
        return
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
