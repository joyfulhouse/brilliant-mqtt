"""Hue-coordinator control boundary. The coordinator is a uWSGI emperor vassal;
its presence is signalled by the vassal control file, and touching that file
triggers an emperor reload (restart). This panel hosts Hue only when the file
exists, so is_running() doubles as "am I the current Hue host"."""

from __future__ import annotations

import os
from typing import Protocol


class Coordinator(Protocol):
    def is_running(self) -> bool: ...
    def restart(self) -> None: ...


class RealCoordinator:
    def __init__(self, vassal_ini_path: str) -> None:
        self._ini = vassal_ini_path

    def is_running(self) -> bool:
        return os.path.exists(self._ini)

    def restart(self) -> None:
        os.utime(self._ini, None)
