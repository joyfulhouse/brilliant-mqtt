"""Score-derived motion for BLE-mesh loads.

The firmware ``movement_detected`` latch on mesh loads never fires
(live-verified 2026-07-02 on the dining pilot: ``motion_score`` reached 255
with ``enable_motion_score=1`` / ``motion_high_threshold=45`` on-device and
the latch stayed 0 across 48 h). The bridge therefore derives the Motion
binary_sensor from the score stream instead: motion turns ON the moment
``motion_score >= motion_high_threshold`` and stays on until no qualifying
spike has been seen for ``hold_s`` seconds (inclusive window). Validated
offline against aiosense ground truth (11 h, dining): ~85% episode recall,
94–100% time precision, ~0 false triggers/h at threshold 45–50 / hold 60 s.

Pure stdlib + project model types; no bus or MQTT imports.
"""

from __future__ import annotations

import time
from collections.abc import Callable
from dataclasses import replace

from brilliant_mqtt.model import BrilliantDevice, Variable

_MOTION_VAR = "movement_detected"
_SCORE_VAR = "motion_score"
_ENABLE_VAR = "enable_motion_score"
_THRESHOLD_VAR = "motion_high_threshold"


class MotionDeriver:
    """Derives ``movement_detected`` from ``motion_score`` with a hold window.

    Peripherals that lack any of the motion-subsystem variables (panel loads,
    faceplates) pass through untouched. State is keyed by peripheral_id, so
    one instance can safely serve both Bridge scopes in a process.
    """

    def __init__(self, hold_s: float, clock: Callable[[], float] = time.monotonic) -> None:
        self._hold_s = hold_s
        self._clock = clock
        # peripheral_id -> monotonic time of the last score >= threshold.
        self._last_hot: dict[str, float] = {}

    def apply(self, device: BrilliantDevice) -> BrilliantDevice:
        """Return *device* with ``movement_detected`` rewritten to the derived value.

        Returns the SAME object when the device lacks the motion subsystem or
        when the derived value already matches the snapshot (cheap no-op for
        the hot poll). Never raises: unparsable score/threshold derive "0".
        """
        vars_ = device.variables
        motion = vars_.get(_MOTION_VAR)
        if motion is None or _SCORE_VAR not in vars_ or _ENABLE_VAR not in vars_:
            return device

        pid = device.peripheral_id
        if not vars_[_ENABLE_VAR].as_bool():
            # Scoring off: no live data. Drop the hold so a later re-enable
            # starts cold instead of resurrecting a stale window.
            self._last_hot.pop(pid, None)
            return self._with_motion(device, motion, "0")

        score = vars_[_SCORE_VAR].as_int()
        threshold_var = vars_.get(_THRESHOLD_VAR)
        threshold = threshold_var.as_int() if threshold_var is not None else None
        if score is None or threshold is None:
            return self._with_motion(device, motion, "0")

        now = self._clock()
        if score >= threshold:
            self._last_hot[pid] = now
        last = self._last_hot.get(pid)
        derived = "1" if last is not None and (now - last) <= self._hold_s else "0"
        return self._with_motion(device, motion, derived)

    def forget(self, peripheral_id: str) -> None:
        """Drop the hold state for one peripheral."""
        self._last_hot.pop(peripheral_id, None)

    def clear(self) -> None:
        """Drop all hold state (mesh-leadership step-down)."""
        self._last_hot.clear()

    @staticmethod
    def _with_motion(device: BrilliantDevice, motion: Variable, value: str) -> BrilliantDevice:
        if motion.value == value:
            return device
        new_vars = dict(device.variables)
        new_vars[_MOTION_VAR] = Variable(
            _MOTION_VAR, value, externally_settable=motion.externally_settable
        )
        return replace(device, variables=new_vars)
