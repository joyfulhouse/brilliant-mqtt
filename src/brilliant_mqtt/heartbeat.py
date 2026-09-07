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
    whether/how to swallow it. On a failed write the tmp scratch file is
    removed best-effort first, so a failed ``os.replace`` (e.g. the destination
    is a directory) never leaks ``<path>.tmp``.
    """
    parent = os.path.dirname(path)
    if parent:
        os.makedirs(parent, exist_ok=True)
    tmp = f"{path}.tmp"
    try:
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(text)
        os.replace(tmp, path)
    except OSError:
        try:
            os.unlink(tmp)
        except OSError:
            pass
        raise


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

    Writes ``"<phase> <pid>"`` — the writer's own pid lets the reader
    (:func:`brilliant_bus_watchdog.health.bus_confirmed`) verify the writer is
    still alive, so a stale ``bus`` marker from a dead/reverted process does not
    read as a live confirmed bus.

    A failed write is best-effort for EVERY phase, ``pre_bus`` included: on any
    failure the phase file is cleared best-effort and the error is swallowed,
    never re-raised. Two things force this:

    * The ``pre_bus`` stamp runs at :mod:`brilliant_mqtt.__main__` *before* the
      session ``try``, so re-raising would make the supervisor back off and
      retry forever and the bridge would never connect to anything.
    * :func:`brilliant_mqtt.__main__.run` retries ``_run_session`` in the SAME
      process while teardown deliberately KEEPS the ``bus`` marker, so a
      leftover ``bus <pid>`` names THIS still-live process. Merely swallowing a
      failed re-stamp would leave that live-pid marker readable, so
      :func:`brilliant_bus_watchdog.health.bus_confirmed` would return True and
      — with a stale heartbeat during a broker-only outage — reboot a healthy
      panel in a loop (issue #87). The pid liveness check does NOT save us here
      (the pid is alive), so the failed stamp must ACTIVELY clear the marker.

    Removing the file disables the reboot guard (a cleared/absent marker reads
    as unconfirmed), which is fail-safe. Every error is swallowed — including
    the unlink's (``FileNotFoundError``, ``IsADirectoryError``,
    ``NotADirectoryError``, ``PermissionError``, …): phase tracking degrading to
    "off" is safe, a re-raise is not. ``_atomic_write`` already removes its own
    ``.tmp`` scratch file, so a failed replace never leaks it.

    Contract: this writer and its reader
    (:func:`brilliant_bus_watchdog.health.bus_confirmed`) must be rolled out
    and rolled back TOGETHER. If only one side is reverted, delete the phase
    file (default ``/run/brilliant-mqtt/bus-phase``) so a stale marker can't
    be misread as bus-confirmed.
    """
    if not path:
        return
    try:
        _atomic_write(path, f"{phase} {os.getpid()}")
    except OSError:
        # A failed stamp must not take the bridge down AND must not leave a
        # stale (same-pid) marker readable. Log, clear the marker best-effort so
        # the watchdog fails closed (reboot guard disabled), and swallow every
        # error — the unlink's included.
        logger.warning("bus phase write failed for %s; reboot guard disabled", path, exc_info=True)
        try:
            os.unlink(path)
        except OSError:
            pass
