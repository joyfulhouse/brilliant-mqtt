"""Reader for the bridge/watchdog bus-attribution file contract."""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Literal

PHASE_RECORD_VERSION = "v2"
PHASE_ATTEMPT = "attempt"
PHASE_SUCCESS = "success"
DEAD_WRITER_RETENTION_S = 300.0
_MAX_PID = 2**31 - 1

BusPhase = Literal["pre_bus", "bus"]


@dataclass(frozen=True, slots=True)
class PhaseRecord:
    phase: BusPhase
    pid: int
    process_generation: str
    boot_id: str
    failure_started_at: float | None
    bus_updated_at: float | None
    bus_read_succeeded: bool | None

    @classmethod
    def parse(cls, text: str) -> PhaseRecord | None:
        parts = text.split()
        if (
            len(parts) != 8
            or parts[0] != PHASE_RECORD_VERSION
            or parts[1] not in ("pre_bus", "bus")
        ):
            return None
        try:
            pid = int(parts[2])
        except ValueError:
            return None
        generation = parts[3]
        boot_id = parts[4]
        if (
            not 1 < pid <= _MAX_PID
            or not generation.isdecimal()
            or not boot_id
            or len(boot_id) > 128
        ):
            return None

        timing_parts = parts[5:]
        if timing_parts == ["-", "-", "-"]:
            if parts[1] == "bus":
                return None
            timing: tuple[float | None, float | None] = (None, None)
            read_succeeded: bool | None = None
        else:
            if timing_parts[2] not in (PHASE_ATTEMPT, PHASE_SUCCESS):
                return None
            try:
                parsed = (float(timing_parts[0]), float(timing_parts[1]))
            except ValueError:
                return None
            if (
                not all(math.isfinite(value) and value >= 0.0 for value in parsed)
                or parsed[0] > parsed[1]
            ):
                return None
            timing = parsed
            read_succeeded = timing_parts[2] == PHASE_SUCCESS

        phase: BusPhase = "pre_bus" if parts[1] == "pre_bus" else "bus"
        return cls(
            phase=phase,
            pid=pid,
            process_generation=generation,
            boot_id=boot_id,
            failure_started_at=timing[0],
            bus_updated_at=timing[1],
            bus_read_succeeded=read_succeeded,
        )


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
