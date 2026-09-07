"""Reboot the panel — the only recovery that clears a wedged message_bus."""

from __future__ import annotations

from collections.abc import Sequence
from typing import Any

from . import bounded

# `systemctl reboot` normally queues and returns fast, but bound it anyway so a
# wedged systemd cannot leave the watchdog stuck; a timed-out child is killed
# and reaped.
_REBOOT_TIMEOUT = 30.0


def _run(argv: Sequence[str]) -> None:
    bounded.run_bounded(argv, timeout=_REBOOT_TIMEOUT)


def reboot(run: Any = _run) -> None:
    run(["systemctl", "reboot"])
