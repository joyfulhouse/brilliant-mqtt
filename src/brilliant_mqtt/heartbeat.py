"""Bus-liveness heartbeat for the independent message-bus watchdog.

Successful reads offer a beat; writes are capped at a ten-second cadence so
the hot poll does not churn tmpfs metadata. The watchdog's stale threshold is
far longer. tmpfs remains the default, so there is no flash wear.
"""

from __future__ import annotations

import fcntl
import logging
import math
import os
import time
from collections.abc import Callable
from dataclasses import dataclass
from typing import Literal, TextIO

logger = logging.getLogger(__name__)

_MIN_WRITE_INTERVAL_S = 10.0
DEAD_WRITER_RETENTION_S = 300.0
_last_attempt: dict[str, float] = {}

BusPhase = Literal["pre_bus", "bus"]


@dataclass(frozen=True, slots=True)
class PhaseRecord:
    phase: BusPhase
    pid: int
    process_generation: str
    boot_id: str
    failure_started_at: float | None
    bus_updated_at: float | None
    retain_until: float | None

    def encode(self) -> str:
        timing = (
            ("-", "-", "-")
            if self.failure_started_at is None
            else (
                repr(self.failure_started_at),
                repr(self.bus_updated_at),
                repr(self.retain_until),
            )
        )
        return " ".join(
            (
                "v2",
                self.phase,
                str(self.pid),
                self.process_generation,
                self.boot_id,
                *timing,
            )
        )

    @classmethod
    def parse(cls, text: str) -> PhaseRecord | None:
        parts = text.split()
        if len(parts) != 8 or parts[0] != "v2" or parts[1] not in ("pre_bus", "bus"):
            return None
        try:
            pid = int(parts[2])
        except ValueError:
            return None
        generation = parts[3]
        boot_id = parts[4]
        if pid <= 1 or not generation.isdecimal() or not boot_id or len(boot_id) > 128:
            return None
        timing_parts = parts[5:]
        if timing_parts == ["-", "-", "-"]:
            if parts[1] == "bus":
                return None
            timing: tuple[float | None, float | None, float | None] = (None, None, None)
        else:
            try:
                parsed = tuple(float(value) for value in timing_parts)
            except ValueError:
                return None
            if (
                len(parsed) != 3
                or not all(math.isfinite(value) and value >= 0.0 for value in parsed)
                or not parsed[0] <= parsed[1] <= parsed[2]
            ):
                return None
            timing = parsed
        phase: BusPhase = "pre_bus" if parts[1] == "pre_bus" else "bus"
        return cls(
            phase=phase,
            pid=pid,
            process_generation=generation,
            boot_id=boot_id,
            failure_started_at=timing[0],
            bus_updated_at=timing[1],
            retain_until=timing[2],
        )


@dataclass(slots=True)
class _PhaseLease:
    stream: TextIO
    last_success_write: float | None


_phase_leases: dict[str, _PhaseLease] = {}


def current_boot_id() -> str | None:
    try:
        with open("/proc/sys/kernel/random/boot_id", encoding="utf-8") as stream:
            boot_id = stream.read().strip()
    except (OSError, UnicodeError):
        return None
    return boot_id or None


def process_generation(pid: int) -> str | None:
    """Return Linux's per-process start tick, which disambiguates PID reuse."""
    try:
        with open(f"/proc/{pid}/stat", encoding="utf-8") as stream:
            stat = stream.read()
    except (OSError, UnicodeError):
        return None
    command_end = stat.rfind(")")
    if command_end < 0:
        return None
    fields_after_command = stat[command_end + 1 :].split()
    if len(fields_after_command) <= 19:
        return None
    start_tick = fields_after_command[19]
    return start_tick if start_tick.isdecimal() else None


def read_phase_record(path: str) -> PhaseRecord | None:
    try:
        with open(path, encoding="utf-8") as stream:
            return PhaseRecord.parse(stream.read())
    except (OSError, UnicodeError):
        return None


def _release_phase_lease(path: str) -> None:
    lease = _phase_leases.pop(path, None)
    if lease is not None:
        lease.stream.close()


def _acquire_phase_lease(path: str, last_success_write: float | None) -> None:
    stream = open(path, encoding="utf-8")
    try:
        fcntl.flock(stream.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
    except BaseException:
        stream.close()
        raise
    _phase_leases[path] = _PhaseLease(stream, last_success_write)


def _history_is_current(record: PhaseRecord | None, boot_id: str, now: float) -> bool:
    return bool(
        record is not None
        and record.boot_id == boot_id
        and record.failure_started_at is not None
        and record.bus_updated_at is not None
        and record.retain_until is not None
        and record.bus_updated_at <= now <= record.retain_until
    )


def _invalidate_failed_write(path: str, write_error: BaseException) -> None:
    try:
        os.unlink(path)
    except FileNotFoundError:
        logger.warning(
            "bus phase write failed for %s; marker absent and reboot guard disabled",
            path,
            exc_info=(type(write_error), write_error, write_error.__traceback__),
        )
    except OSError as invalidation_error:
        logger.error(
            "bus phase write failed for %s and marker invalidation failed: %s; "
            "stale marker remains but its live-writer lease was released",
            path,
            invalidation_error,
            exc_info=(type(write_error), write_error, write_error.__traceback__),
        )
    else:
        logger.warning(
            "bus phase write failed for %s; marker cleared and reboot guard disabled",
            path,
            exc_info=(type(write_error), write_error, write_error.__traceback__),
        )


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


def write_phase(
    path: str,
    phase: BusPhase,
    *,
    bus_read_succeeded: bool = False,
    monotonic_clock: Callable[[], float] = time.monotonic,
) -> None:
    """Record timed, boot-aware local-bus attribution without disrupting startup.

    A live ``bus`` writer holds an exclusive lock on the marker inode. Moving to
    ``pre_bus`` releases that lease before any filesystem write, so even a
    failed replacement plus failed unlink cannot leave an old live-PID marker
    authoritative. Recent records from dead writers remain bounded evidence for
    native-crash loops; boot IDs, process start ticks, and retention reject PID
    reuse and ancient records.
    """
    if not path:
        return
    now = monotonic_clock()
    lease = _phase_leases.get(path)
    if (
        phase == "bus"
        and bus_read_succeeded
        and lease is not None
        and lease.last_success_write is not None
        and now - lease.last_success_write < _MIN_WRITE_INTERVAL_S
    ):
        return
    previous = read_phase_record(path)
    _release_phase_lease(path)
    try:
        boot_id = current_boot_id()
        pid = os.getpid()
        generation = process_generation(pid)
        if boot_id is None or generation is None:
            raise OSError("cannot establish boot/process generation")
        history = previous if _history_is_current(previous, boot_id, now) else None
        if phase == "pre_bus":
            failure_started_at = history.failure_started_at if history is not None else None
            bus_updated_at = history.bus_updated_at if history is not None else None
            retain_until = history.retain_until if history is not None else None
        else:
            failure_started_at = (
                history.failure_started_at
                if history is not None and not bus_read_succeeded
                else now
            )
            bus_updated_at = now
            retain_until = now + DEAD_WRITER_RETENTION_S
        record = PhaseRecord(
            phase=phase,
            pid=pid,
            process_generation=generation,
            boot_id=boot_id,
            failure_started_at=failure_started_at,
            bus_updated_at=bus_updated_at,
            retain_until=retain_until,
        )
        _atomic_write(path, record.encode())
        if phase == "bus":
            _acquire_phase_lease(path, now if bus_read_succeeded else None)
    except (OSError, UnicodeError) as error:
        _release_phase_lease(path)
        _invalidate_failed_write(path, error)
