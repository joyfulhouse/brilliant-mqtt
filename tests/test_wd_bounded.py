"""Bounded child-process execution, proven against REAL child processes: a hung
child (and any grandchild holding its stdout pipe) is killed, reaped, and
reported as a bounded failure so the watchdog loop keeps running."""

from __future__ import annotations

import logging
import os
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import pytest

from brilliant_wifi_watchdog import bounded

# Process-group kill and /proc liveness are Linux-specific; the watchdog is a
# Linux on-panel agent, so gate the kill/reap tests there. The success/rc tests
# below are portable and run everywhere.
_LINUX = pytest.mark.skipif(
    not sys.platform.startswith("linux"),
    reason="process-group kill + /proc liveness are Linux-only",
)


def _proc_state(pid: int) -> str | None:
    """The /proc state char for *pid* ('R','S','D' alive; 'Z' zombie), or None if
    the process is gone. comm (field 2) is parenthesized and may contain spaces,
    so read the state as the char two positions after the final ')'."""
    try:
        with open(f"/proc/{pid}/stat", encoding="ascii") as f:
            data = f.read()
    except (FileNotFoundError, ProcessLookupError):
        return None
    return data[data.rfind(")") + 2]


def _wait_killed(pid: int, timeout: float) -> bool:
    """True once *pid* is gone or a zombie (i.e. killed), False if it stays alive."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if _proc_state(pid) in (None, "Z"):
            return True
        time.sleep(0.02)
    return _proc_state(pid) in (None, "Z")


def test_run_bounded_success_zero_rc() -> None:
    result = bounded.run_bounded([sys.executable, "-c", "pass"], timeout=10.0)
    assert result.timed_out is False
    assert result.returncode == 0


def test_run_bounded_captures_stdout() -> None:
    result = bounded.run_bounded([sys.executable, "-c", "print('hi')"], timeout=10.0, capture=True)
    assert result.timed_out is False
    assert result.returncode == 0
    assert result.stdout == "hi\n"


def test_run_bounded_nonzero_rc_is_not_a_timeout() -> None:
    result = bounded.run_bounded([sys.executable, "-c", "import sys; sys.exit(3)"], timeout=10.0)
    assert result.timed_out is False
    assert result.returncode == 3


@_LINUX
def test_run_bounded_hung_child_is_bounded_and_reaped() -> None:
    start = time.monotonic()
    result = bounded.run_bounded([sys.executable, "-c", "import time; time.sleep(30)"], timeout=0.3)
    elapsed = time.monotonic() - start
    assert result.timed_out is True
    assert result.returncode == bounded.TIMEOUT_RC
    assert elapsed < 10.0  # bounded near the 0.3s deadline, not the 30s sleep


@_LINUX
def test_run_bounded_kills_grandchild_holding_the_stdout_pipe(tmp_path: Path) -> None:
    """The HIGH case: with capture=True a grandchild inheriting the stdout pipe
    would block a read-to-EOF reap for its whole lifetime. Killing the process
    group (not just the direct child) and reaping with wait() bounds it and
    leaves no lingering grandchild."""
    pidfile = tmp_path / "gc.pid"
    script = (
        "import subprocess, sys, time\n"
        # grandchild inherits our stdout (the capture pipe) and holds it open
        "gc = subprocess.Popen([sys.executable, '-c', 'import time; time.sleep(30)'])\n"
        "open(sys.argv[1], 'w').write(str(gc.pid))\n"
        "time.sleep(30)\n"
    )
    start = time.monotonic()
    result = bounded.run_bounded(
        [sys.executable, "-c", script, str(pidfile)], timeout=0.3, capture=True
    )
    elapsed = time.monotonic() - start
    assert result.timed_out is True
    assert elapsed < 10.0  # NOT ~30s: the grandchild no longer wedges the reap
    gc_pid = int(pidfile.read_text())
    assert _wait_killed(gc_pid, timeout=5.0)  # grandchild killed, not lingering


@_LINUX
def test_run_bounded_sigkills_child_that_ignores_sigterm() -> None:
    """A child that ignores catchable termination signals — as glibc's C resolver
    effectively ignores a Python-level alarm — is still bounded: the group is sent
    the uncatchable SIGKILL."""
    script = "import signal, time; signal.signal(signal.SIGTERM, signal.SIG_IGN); time.sleep(30)"
    start = time.monotonic()
    result = bounded.run_bounded([sys.executable, "-c", script], timeout=0.3)
    elapsed = time.monotonic() - start
    assert result.timed_out is True
    assert elapsed < 10.0


# ---------------------------------------------------------------------------
# Cleanup edge cases with a fake process (deterministic, no real child):
# a D-state child that survives SIGKILL, and a kill() that races a reap.
# ---------------------------------------------------------------------------


def _raise_lookup(*args: Any, **kwargs: Any) -> int:
    raise ProcessLookupError


def test_run_bounded_logs_and_bounds_when_child_survives_sigkill(
    monkeypatch: pytest.MonkeyPatch, caplog: pytest.LogCaptureFixture
) -> None:
    """A child stuck in uninterruptible (D-state) kernel sleep survives even
    SIGKILL until its syscall returns. Rather than a silent lingering process, the
    reap timeout is logged; the call still reports timed_out."""

    class FakeProc:
        pid = 4321
        stdout = None

        def communicate(
            self, input: str | None = None, timeout: float | None = None
        ) -> tuple[str, str]:
            raise subprocess.TimeoutExpired(cmd="x", timeout=timeout or 0.0)

        def kill(self) -> None:
            pass

        def wait(self, timeout: float | None = None) -> int:
            raise subprocess.TimeoutExpired(cmd="x", timeout=timeout or 0.0)  # D-state: never dies

    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: FakeProc())
    monkeypatch.setattr(os, "getpgid", lambda pid: pid)
    monkeypatch.setattr(os, "killpg", lambda pgid, sig: None)
    with caplog.at_level(logging.WARNING, logger="brilliant_wifi_watchdog.bounded"):
        result = bounded.run_bounded(["x"], timeout=0.01)
    assert result.timed_out is True
    assert result.returncode == bounded.TIMEOUT_RC
    assert any("D-state" in r.getMessage() for r in caplog.records)


def test_run_bounded_reaps_and_closes_even_if_kill_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    """If the group signal fails and the fallback proc.kill() itself raises
    (child already reaped), cleanup must still wait() and close the pipe — no fd
    leak, no exception out of run_bounded."""
    closed: list[bool] = []

    class FakeStdout:
        def close(self) -> None:
            closed.append(True)

    class FakeProc:
        pid = 4321

        def __init__(self) -> None:
            self.stdout = FakeStdout()
            self.waited = False

        def communicate(
            self, input: str | None = None, timeout: float | None = None
        ) -> tuple[str, str]:
            raise subprocess.TimeoutExpired(cmd="x", timeout=timeout or 0.0)

        def kill(self) -> None:
            raise ProcessLookupError  # already reaped

        def wait(self, timeout: float | None = None) -> int:
            self.waited = True
            return 0

    fp = FakeProc()
    monkeypatch.setattr(subprocess, "Popen", lambda *a, **k: fp)
    monkeypatch.setattr(os, "getpgid", _raise_lookup)  # force the fallback kill() path
    result = bounded.run_bounded(["x"], timeout=0.01, capture=True)
    assert result.timed_out is True
    assert fp.waited is True  # reaped despite kill() raising
    assert closed == [True]  # stdout closed — no fd leak
