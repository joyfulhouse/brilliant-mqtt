"""Shared test harness.

The boot/process attribution helpers in :mod:`brilliant_mqtt.heartbeat` read
Linux-only ``/proc`` files. On the panel (and on Linux CI) the real helpers run
untouched; on any other developer machine they would return ``None`` and fail
every phase-marker test. The suite must run on any machine, so when ``/proc``
is absent the helpers are replaced with a deterministic stand-in that mirrors
the Linux contract: a stable boot id, one constant generation for every live
pid, and ``None`` for a pid that no longer exists (as ``/proc/<pid>/stat``
would be missing).
"""

from __future__ import annotations

import os
from collections.abc import Iterator

import pytest

from brilliant_bus_watchdog import health as bus_health
from brilliant_mqtt import heartbeat

_LINUX_BOOT_ID_PATH = "/proc/sys/kernel/random/boot_id"
_STAND_IN_BOOT_ID = "00000000-0000-4000-8000-000000000001"
_STAND_IN_GENERATION = "1"


def _stand_in_process_generation(pid: int) -> str | None:
    if pid <= 0:
        return None
    try:
        os.kill(pid, 0)
    except ProcessLookupError:
        return None
    except PermissionError:
        return _STAND_IN_GENERATION
    except (OverflowError, OSError):
        return None
    return _STAND_IN_GENERATION


@pytest.fixture(autouse=True)
def portable_process_attribution(monkeypatch: pytest.MonkeyPatch) -> Iterator[None]:
    """Use the real ``/proc`` helpers on Linux; a faithful stand-in elsewhere."""
    if os.path.exists(_LINUX_BOOT_ID_PATH):
        yield
        return
    for module in (heartbeat, bus_health):
        monkeypatch.setattr(module, "current_boot_id", lambda: _STAND_IN_BOOT_ID)
        monkeypatch.setattr(module, "process_generation", _stand_in_process_generation)
    yield
