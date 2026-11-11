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

_TRUTHY = {"true", "on", "yes", "1"}
_FALSY = {"false", "off", "no", "0"}


def _normalize_value(v: object) -> str:
    """Normalize a loaded JSON value to a canonical bus string.

    JSON booleans (``true``/``false``) and common string synonyms
    (``"on"``/``"off"``/``"yes"``/``"no"``) map to the canonical bus strings
    ``"1"``/``"0"``, and integral numerics (``30``, ``30.0``, ``"030"``) map to
    their canonical integer string (``"30"``) — the bus only ever returns those
    canonical forms, so a hand-edited value in any other spelling would never
    compare equal and would re-write every min-interval forever. Non-integral
    and non-numeric values pass through unchanged.
    """
    s = str(v).strip().lower()
    if s in _TRUTHY:
        return "1"
    if s in _FALSY:
        return "0"
    try:
        f = float(s)
    except ValueError:
        return str(v)
    if f.is_integer():
        return str(int(f))
    return str(v)


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
        """Set the desired value for (peripheral_id, var) and persist.

        Updates in-memory state first so a disk error never prevents the
        command from reaching the device on the same call stack.
        """
        self._state.setdefault(peripheral_id, {})[var] = value
        try:
            self.save()
        except Exception as e:
            logger.warning(
                "could not persist desired-state to %s (%s); keeping in memory",
                self._path,
                e,
            )

    def wanted(self, peripheral_id: str) -> dict[str, str]:
        """Desired vars for a peripheral (copy; empty if none)."""
        return dict(self._state.get(peripheral_id, {}))

    def load(self) -> None:
        """Load from disk; a missing or unreadable file yields empty state.

        Only RECONCILED_VARS are kept: stale or hand-edited files containing
        non-motion vars (e.g. "on", "intensity") will not cause the reconciler
        to re-assert them. Peripherals with no surviving vars are dropped.
        """
        self._state = {}
        try:
            raw = json.loads(self._path.read_text())
        except (OSError, ValueError) as e:
            if isinstance(e, FileNotFoundError):
                # First boot looks identical to a vanished file without this line.
                logger.info("no desired-state file at %s; starting empty", self._path)
            else:
                logger.warning(
                    "could not load desired-state from %s (%s); starting empty",
                    self._path,
                    e,
                )
            return
        if not isinstance(raw, dict):
            logger.warning("desired-state file %s is not a JSON object; starting empty", self._path)
            return
        for pid, vars_ in raw.items():
            if not isinstance(vars_, dict):
                logger.warning(
                    "desired-state %s: entry for %r is not an object; dropping it",
                    self._path,
                    pid,
                )
                continue
            filtered = {
                str(k): _normalize_value(v) for k, v in vars_.items() if str(k) in RECONCILED_VARS
            }
            if filtered:
                self._state[str(pid)] = filtered
        logger.info(
            "loaded desired-state from %s: %d peripheral(s), %d var(s)",
            self._path,
            len(self._state),
            sum(len(v) for v in self._state.values()),
        )

    def save(self) -> None:
        """Durably persist state: write temp, fsync, atomic rename, fsync dir.

        ``os.replace`` alone gives atomicity but not durability — without the
        fsyncs, a breaker power-cut shortly after a command could leave a
        truncated file (ext4 delayed allocation) and silently drop the whole
        desired config at next boot. Saves only happen on operator commands,
        so the extra syncs cost nothing in steady state.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        tmp = self._path.with_name(self._path.name + ".tmp")
        with open(tmp, "w", encoding="utf-8") as f:
            f.write(json.dumps(self._state))
            f.flush()
            os.fsync(f.fileno())
        os.replace(tmp, self._path)
        dir_fd = os.open(self._path.parent, os.O_RDONLY)
        try:
            os.fsync(dir_fd)
        finally:
            os.close(dir_fd)
