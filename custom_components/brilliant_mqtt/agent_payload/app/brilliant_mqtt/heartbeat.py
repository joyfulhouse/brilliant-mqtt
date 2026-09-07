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
from typing import Literal

logger = logging.getLogger(__name__)

_MIN_WRITE_INTERVAL_S = 10.0
_last_attempt: dict[str, float] = {}

BusPhase = Literal["pre_bus", "bus"]


def _atomic_write(path: str, text: str) -> None:
    """Write *text* into *path* via tmp-file + ``os.replace``.

    Both heartbeat and phase files live under the same runtime directory
    (tmpfs, e.g. ``/run/brilliant-mqtt/``), which may not exist yet on first
    boot — creating the parent here is deliberate for both callers, not an
    accident of one writer. Raises ``OSError`` on failure; callers decide
    whether/how to swallow it.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        f.write(text)
    os.replace(tmp, path)


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
        _atomic_write(path, f"{clock()}")
    except OSError:
        logger.debug("heartbeat write failed for %s", path, exc_info=True)


def write_phase(path: str, phase: BusPhase) -> None:
    """Atomically record the session's bus phase without disrupting startup.

    A failed ``bus`` write is swallowed (logged): a missing stamp reads as
    unconfirmed, which is fail-safe. A failed ``pre_bus`` write is NOT — a
    stale ``bus`` marker left by a prior successful session would still read
    as bus-confirmed, so a broker-only outage could reboot a healthy panel.
    On a failed ``pre_bus`` write we therefore remove the phase file so the
    watchdog fails closed; if that removal itself fails we re-raise, because
    phase tracking is broken and the session must not proceed as if safe.

    Contract: this writer and its reader
    (:func:`brilliant_bus_watchdog.health.bus_confirmed`) must be rolled out
    and rolled back TOGETHER. If only one side is reverted, delete the phase
    file (default ``/run/brilliant-mqtt/bus-phase``) so a stale marker can't
    be misread as bus-confirmed.
    """
    if not path:
        return
    try:
        _atomic_write(path, phase)
    except OSError:
        if phase != "pre_bus":
            logger.debug("bus phase write failed for %s", path, exc_info=True)
            return
        # A failed pre_bus stamp is fail-unsafe: clear any stale marker so the
        # watchdog cannot read a prior session's "bus" as today's truth. If the
        # file is already gone we are safe; any other removal failure propagates
        # so the caller learns phase tracking is broken.
        logger.debug("pre_bus phase write failed for %s; clearing phase file", path, exc_info=True)
        try:
            os.unlink(path)
        except FileNotFoundError:
            pass
