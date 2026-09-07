"""Watchdog health reads — fail closed and never raise. ``heartbeat_age`` is a
pure single read; ``bus_confirmed`` also checks that the writer is still alive."""

from __future__ import annotations

import os


def _writer_alive(pid: int) -> bool:
    """Whether *pid* names a live process, via stdlib-native ``os.kill(pid, 0)``.

    Signal 0 performs the existence/permission check without delivering a
    signal: ``ProcessLookupError`` means the writer is gone, ``PermissionError``
    means it is alive but owned by another user (treat as alive). A pid <= 1 is
    rejected: 0 and negative pids would target a process group rather than an
    individual writer, and pid 1 (init) always exists yet never names our
    writer, so a torn/truncated marker like ``bus 1`` must not read as a live
    writer (which would suppress reboots forever, fail-open). Any other error
    fails closed (unconfirmed) — including ``OverflowError`` (an
    ``ArithmeticError``, not an ``OSError``), which ``os.kill`` raises for a pid
    beyond the C ``pid_t``/``long`` range, e.g. a torn/corrupt phase file like
    ``bus 2147483648``.

    Best-effort by nature: PID reuse means a stale marker whose pid was recycled
    by an unrelated process would read as alive. The real guard against a stale
    marker is the ``pre_bus`` re-stamp at session entry (see
    :func:`brilliant_mqtt.heartbeat.write_phase`), not this check alone.
    """
    if pid <= 1:
        return False
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return False
    except PermissionError:
        return True
    except (OSError, OverflowError):
        return False
    return True


def bus_confirmed(path: str) -> bool:
    """Whether the bridge reached the local-bus phase AND the writer that
    stamped it is still alive.

    The bridge writes ``"<phase> <pid>"`` (see
    :func:`brilliant_mqtt.heartbeat.write_phase`); this reads as confirmed only
    when the phase token is exactly ``bus`` and that pid is a live process. So a
    stale ``bus`` marker left in tmpfs by a dead or reverted writer — which
    survives a service restart and is only cleared when a reboot wipes ``/run``
    — reads as unconfirmed rather than as a live confirmed bus.

    Fails closed and never raises: a missing/unreadable file (OSError), bytes
    that are not valid UTF-8 (UnicodeError), a wrong/absent phase token, a
    non-integer or out-of-range pid, or a dead writer all read as unconfirmed.

    The pid liveness check is best-effort (PID reuse could make a recycled pid
    read as alive); the ``pre_bus`` re-stamp at session entry is the real guard
    against a stale marker.
    """
    try:
        with open(path, encoding="utf-8") as f:
            parts = f.read().split()
    except (OSError, UnicodeError):
        return False
    if len(parts) != 2 or parts[0] != "bus":
        return False
    try:
        pid = int(parts[1])
    except ValueError:
        return False
    return _writer_alive(pid)


def heartbeat_age(path: str, *, now: float, started_at: float) -> float:
    """Seconds since the bridge last stamped *path*. If the file is absent or
    unparsable, age is measured from *started_at* (the watchdog's own start) so
    a never-seen heartbeat can't read as infinitely stale right after boot."""
    try:
        with open(path, encoding="utf-8") as f:
            stamped = float(f.read().strip())
    except (OSError, ValueError):
        return now - started_at
    return now - stamped
