"""Watchdog health reads: fail closed and never raise."""

from __future__ import annotations

import fcntl
import time

from .phase_record import (
    DEAD_WRITER_RETENTION_S,
    PhaseRecord,
    current_boot_id,
    process_generation,
    process_is_absent,
)


def _read_phase_record_and_lease(path: str) -> tuple[PhaseRecord | None, bool]:
    try:
        with open(path, encoding="utf-8") as stream:
            try:
                fcntl.flock(stream.fileno(), fcntl.LOCK_SH | fcntl.LOCK_NB)
            except BlockingIOError:
                leased = True
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
    service_started_at: float | None = None,
) -> float | None:
    """Age of attributable local-bus failure, including bounded crash evidence."""
    sampled_at = time.monotonic() if now is None else now
    record, leased = _read_phase_record_and_lease(path)
    if (
        record is None
        or record.phase != "bus"
        or record.boot_id != current_boot_id()
        or record.failure_started_at is None
        or record.bus_updated_at is None
        or record.bus_read_succeeded is None
        or (service_started_at is not None and record.bus_updated_at < service_started_at)
        or record.failure_started_at > sampled_at
        or record.bus_updated_at > sampled_at
    ):
        return None
    live_generation = process_generation(record.pid)
    if live_generation is not None:
        if live_generation != record.process_generation or not leased:
            return None
    # A read-only filesystem can hide a recovered bus before this writer dies;
    # its bounded attempt is intentionally indistinguishable from #133. See #143.
    elif not process_is_absent(record.pid) or (
        leased
        or record.bus_read_succeeded
        or sampled_at > record.bus_updated_at + DEAD_WRITER_RETENTION_S
    ):
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
