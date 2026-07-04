"""Reboot the panel — the only recovery that clears a wedged message_bus."""

from __future__ import annotations

import subprocess
from collections.abc import Sequence
from typing import Any


def _run(argv: Sequence[str]) -> None:
    subprocess.run(list(argv), check=False)


def reboot(run: Any = _run) -> None:
    run(["systemctl", "reboot"])
