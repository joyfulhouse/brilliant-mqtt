"""Operator desired-state for the motion subsystem.

The Brilliant firmware reverts the motion *enable* flags to defaults within
minutes (NVM thresholds persist; runtime enables reset). This module persists
the last value the operator commanded for a fixed set of motion vars so the
bridge can re-assert them on drift. Pure data + JSON persistence — no bus/mqtt
deps, so it is unit-tested off-panel.
"""

from __future__ import annotations

import json
import logging
import os
from pathlib import Path

logger = logging.getLogger(__name__)

# Vars whose last-commanded value the bridge re-asserts on drift. The enable
# flags revert on the device; thresholds are included so a factory-reset device
# self-heals (re-asserting a matching value is a cheap no-op).
RECONCILED_VARS: frozenset[str] = frozenset(
    {
        "enable_motion_score",
        "motion_high_threshold",
        "motion_low_threshold",
        "enable_pir_motion_score",
        "enable_screen_motion_detection",
        "enable_light_motion_detection",
        "pir_motion_detection_high_threshold",
        "pir_motion_detection_low_threshold",
    }
)


class DesiredState:
    """peripheral_id -> {var: desired bus-string value}, persisted as JSON.

    Stores whatever it is told (the caller gates on RECONCILED_VARS). Values are
    the bus strings produced by ``translate_aux`` ("1"/"0"/"30"), matching the
    ``Variable.value`` strings read back from the bus so comparisons are exact.
    """

    def __init__(self, path: Path) -> None:
        self._path = path
        self._state: dict[str, dict[str, str]] = {}

    def record(self, peripheral_id: str, var: str, value: str) -> None:
        """Set the desired value for (peripheral_id, var) and persist."""
        self._state.setdefault(peripheral_id, {})[var] = value
        self.save()

    def wanted(self, peripheral_id: str) -> dict[str, str]:
        """Desired vars for a peripheral (copy; empty if none)."""
        return dict(self._state.get(peripheral_id, {}))

    def load(self) -> None:
        """Load from disk; a missing or unreadable file yields empty state."""
        try:
            raw = json.loads(self._path.read_text())
        except (OSError, ValueError) as e:
            if not isinstance(e, FileNotFoundError):
                logger.warning(
                    "could not load desired-state from %s (%s); starting empty",
                    self._path,
                    e,
                )
            self._state = {}
            return
        if not isinstance(raw, dict):
            logger.warning("desired-state file %s is not a JSON object; starting empty", self._path)
            self._state = {}
            return
        self._state = {
            str(pid): {str(k): str(v) for k, v in vars_.items()}
            for pid, vars_ in raw.items()
            if isinstance(vars_, dict)
        }

    def save(self) -> None:
        """Atomically persist state (write temp + os.replace)."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        tmp.write_text(json.dumps(self._state))
        os.replace(tmp, self._path)
