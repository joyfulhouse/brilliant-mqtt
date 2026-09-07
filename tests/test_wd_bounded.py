"""Bounded child-process execution: a hung probe/recovery child must be killed,
reaped, and reported as a bounded failure so the watchdog loop keeps running."""

from __future__ import annotations

import subprocess

from brilliant_wifi_watchdog import bounded


class FakePopen:
    """A stand-in for :class:`subprocess.Popen` that records kill/reap calls.

    ``communicate`` raises :class:`subprocess.TimeoutExpired` on its first call
    when ``timeout_first`` is set (the hung-child case); the caller must then
    ``kill`` and ``communicate`` again to reap, which we count.
    """

    def __init__(self, *, timeout_first: bool = False, returncode: int = 0, out: str = "") -> None:
        self._timeout_first = timeout_first
        self._out = out
        self.returncode = returncode
        self.kill_calls = 0
        self.communicate_calls = 0

    def communicate(
        self, input: str | None = None, timeout: float | None = None
    ) -> tuple[str, str]:
        self.communicate_calls += 1
        if self._timeout_first and self.communicate_calls == 1:
            raise subprocess.TimeoutExpired(cmd="fake", timeout=timeout or 0.0)
        return (self._out, "")

    def kill(self) -> None:
        self.kill_calls += 1


def test_run_bounded_success_returns_rc_and_stdout() -> None:
    fake = FakePopen(returncode=0, out="hello\n")
    result = bounded.run_bounded(
        ["echo", "hi"], timeout=5.0, capture=True, popen=lambda *a, **k: fake
    )
    assert result.timed_out is False
    assert result.returncode == 0
    assert result.stdout == "hello\n"


def test_run_bounded_nonzero_rc_is_not_a_timeout() -> None:
    fake = FakePopen(returncode=1)
    result = bounded.run_bounded(["false"], timeout=5.0, popen=lambda *a, **k: fake)
    assert result.timed_out is False
    assert result.returncode == 1


def test_run_bounded_timeout_kills_and_reaps_child() -> None:
    """The hung-child + cleanup case: kill the child, then reap it (a second
    communicate) so no zombie or lingering process remains."""
    fake = FakePopen(timeout_first=True)
    result = bounded.run_bounded(["sleep", "999"], timeout=0.01, popen=lambda *a, **k: fake)
    assert result.timed_out is True
    assert result.returncode == bounded.TIMEOUT_RC
    assert fake.kill_calls == 1  # child was killed
    assert fake.communicate_calls == 2  # first timed out, second reaped the killed child


def test_run_bounded_timeout_does_not_raise_so_loop_continues() -> None:
    """A timeout is reported, never raised, so the watchdog proceeds to its next
    check instead of dying on an unhandled TimeoutExpired."""
    fake = FakePopen(timeout_first=True)
    # Must not raise:
    result = bounded.run_bounded(["hang"], timeout=0.01, popen=lambda *a, **k: fake)
    assert result.timed_out is True


def test_run_bounded_passes_timeout_through_to_communicate() -> None:
    seen: list[float | None] = []

    class RecordingPopen(FakePopen):
        def communicate(
            self, input: str | None = None, timeout: float | None = None
        ) -> tuple[str, str]:
            seen.append(timeout)
            return super().communicate(timeout=timeout)

    rec = RecordingPopen(returncode=0)
    bounded.run_bounded(["x"], timeout=2.5, popen=lambda *a, **k: rec)
    assert seen == [2.5]
