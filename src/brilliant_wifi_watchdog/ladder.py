"""Pure escalation state machine — no I/O, fully unit-testable."""

from __future__ import annotations

import enum
from dataclasses import dataclass


class Action(enum.Enum):
    NONE = "none"
    SOFT_RECONNECT = "soft_reconnect"
    RESTART_SERVICES = "restart_services"
    GPIO_RESET_REBOOT = "gpio_reset_reboot"
    ESCALATE_NOTIFY = "escalate_notify"  # reboot wanted but blocked by the guard


@dataclass(frozen=True)
class Thresholds:
    fail_debounce: int = 3
    soft_after: float = 90.0
    restart_after: float = 180.0
    reboot_after: float = 360.0


# Rung order by elapsed-seconds threshold.
_RUNGS = (
    ("soft", Action.SOFT_RECONNECT),
    ("restart", Action.RESTART_SERVICES),
    ("reboot", Action.GPIO_RESET_REBOOT),
)


class Ladder:
    def __init__(self, thresholds: Thresholds) -> None:
        self._t = thresholds
        self._fails = 0
        self._down_since: float | None = None
        self._fired: set[str] = set()
        self._reboot_deferred = False

    def reset(self) -> None:
        self._fails = 0
        self._down_since = None
        self._fired.clear()
        self._reboot_deferred = False

    def _threshold(self, name: str) -> float:
        return {
            "soft": self._t.soft_after,
            "restart": self._t.restart_after,
            "reboot": self._t.reboot_after,
        }[name]

    def observe(self, *, gateway_up: bool, now: float, reboot_eligible: bool = True) -> Action:
        if gateway_up:
            self.reset()
            return Action.NONE
        self._fails += 1
        if self._down_since is None:
            self._down_since = now
        if self._fails < self._t.fail_debounce:
            return Action.NONE
        elapsed = now - self._down_since
        # Fire the highest-threshold rung whose time has passed and which hasn't fired.
        for name, action in reversed(_RUNGS):
            if elapsed < self._threshold(name) or name in self._fired:
                continue
            if name == "reboot" and not reboot_eligible:
                # Reboot is due but the guard blocks it.  Never mark it fired, so a
                # later eligible poll re-arms it (issue #91); notify once, then keep
                # walking to the cheaper rungs so a poll gap can't starve soft/restart.
                if not self._reboot_deferred:
                    self._reboot_deferred = True
                    return Action.ESCALATE_NOTIFY
                continue
            self._fired.add(name)
            if name == "reboot":
                self._reboot_deferred = False
            return action
        return Action.NONE
