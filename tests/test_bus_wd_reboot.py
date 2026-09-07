"""The panel-reboot child must be bounded too: a wedged `systemctl reboot`
should not leave the watchdog stuck forever."""

from __future__ import annotations

from typing import Any

import pytest

from brilliant_bus_watchdog import bounded, reboot


def test_reboot_default_runner_is_bounded(monkeypatch: pytest.MonkeyPatch) -> None:
    seen: list[tuple[list[str], float]] = []

    def spy(
        argv: Any, *, timeout: float, capture: bool = False, popen: Any = None
    ) -> bounded.Completed:
        seen.append((list(argv), timeout))
        return bounded.Completed(returncode=0, stdout="", timed_out=False)

    monkeypatch.setattr(bounded, "run_bounded", spy)
    reboot.reboot()
    assert seen[0][0] == ["systemctl", "reboot"]
    assert seen[0][1] > 0


def test_reboot_injectable_runner_still_used() -> None:
    calls: list[list[str]] = []
    reboot.reboot(run=lambda argv: calls.append(list(argv)))
    assert calls == [["systemctl", "reboot"]]
