"""Watchdog health reads: fail closed and never raise."""

from __future__ import annotations

import fcntl
import os
import time
from collections.abc import Callable

from brilliant_mqtt.heartbeat import PhaseRecord, current_boot_id, process_generation


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
    """Whether a live, generation-matched writer holds the bus-phase lease."""
    record, leased = _read_phase_record_and_lease(path)
    return bool(
        record is not None
        and record.phase == "bus"
        and record.boot_id == current_boot_id()
        and _writer_alive(record.pid)
        and process_generation(record.pid) == record.process_generation
        and leased
    )


def _read_phase_record_and_lease(path: str) -> tuple[PhaseRecord | None, bool]:
    try:
        with open(path, encoding="utf-8") as stream:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                leased = True
            except OSError:
                return None, False
            else:
                leased = False
                fcntl.flock(stream.fileno(), fcntl.LOCK_UN)
            return PhaseRecord.parse(stream.read()), leased
    except (OSError, UnicodeError):
        return None, False


def bus_failure_age(
    path: str,
    *,
    now: float | None = None,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> float | None:
    """Age of attributable local-bus failure, including bounded crash evidence."""
    sampled_at = monotonic_clock() if now is None else now
    record, leased = _read_phase_record_and_lease(path)
    if (
        record is None
        or record.phase != "bus"
        or record.boot_id != current_boot_id()
        or record.failure_started_at is None
        or record.bus_updated_at is None
        or record.retain_until is None
        or record.failure_started_at > sampled_at
        or record.bus_updated_at > sampled_at
    ):
        return None
    if _writer_alive(record.pid):
        if process_generation(record.pid) != record.process_generation or not leased:
            return None
    elif leased or sampled_at > record.retain_until:
        return None
    return sampled_at - record.failure_started_at


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
