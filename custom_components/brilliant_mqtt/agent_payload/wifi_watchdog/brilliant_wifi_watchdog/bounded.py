"""Bounded child-process execution for the watchdog.

A watchdog must never be wedged by a hung child. A probe against an unhealthy
network service, or a recovery command against a stuck daemon, can block
forever; without a bound that would leave the watchdog alive but unable to run
another check — exactly the failure this module prevents. Every probe/recovery
command therefore runs through :func:`run_bounded`, which enforces a wall-clock
deadline and, on expiry, kills the child and reaps it (no zombie and no
lingering process piling up on the panel's constrained hardware) before
returning a timeout result. The caller treats a timeout as a bounded failure
and the watchdog loop proceeds to its next check.

Stdlib only (:mod:`subprocess`), so the module deploys self-contained on the
panel interpreter (Python 3.10) with only this package on ``PYTHONPATH``.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable, Sequence
from dataclasses import dataclass
from typing import Protocol

# Exit status reported for a timed-out child, mirroring the shell's timeout(1).
TIMEOUT_RC = 124


class _Process(Protocol):
    """The slice of :class:`subprocess.Popen` this module drives — kept minimal
    so a test fake can satisfy it structurally without a real child process."""

    @property
    def returncode(self) -> int | None: ...

    def communicate(
        self, input: str | None = ..., timeout: float | None = ...
    ) -> tuple[str, str]: ...

    def kill(self) -> None: ...


@dataclass(frozen=True)
class Completed:
    """Outcome of a bounded child process."""

    returncode: int
    stdout: str
    timed_out: bool


def run_bounded(
    argv: Sequence[str],
    *,
    timeout: float,
    capture: bool = False,
    popen: Callable[..., _Process] | None = None,
) -> Completed:
    """Run *argv* with a wall-clock *timeout*; kill and reap on expiry.

    Returns a :class:`Completed`. On timeout the child is sent ``SIGKILL`` and
    then waited for (a second ``communicate`` reaps it), so no zombie or
    lingering process is left behind; the result carries ``timed_out=True`` with
    ``returncode=TIMEOUT_RC`` and no :class:`subprocess.TimeoutExpired` is
    raised, so the watchdog loop keeps running.

    ``capture`` decides whether stdout is captured (probes that parse output) or
    discarded (recovery commands run for their side effect). ``popen`` is
    injectable for tests; it defaults to :class:`subprocess.Popen`.
    """
    out = subprocess.PIPE if capture else subprocess.DEVNULL
    proc: _Process
    if popen is not None:
        proc = popen(list(argv), stdout=out, stderr=subprocess.DEVNULL, text=True)
    else:
        proc = subprocess.Popen(list(argv), stdout=out, stderr=subprocess.DEVNULL, text=True)
    try:
        stdout, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.communicate()  # reap the killed child so no zombie/lingering process remains
        return Completed(returncode=TIMEOUT_RC, stdout="", timed_out=True)
    rc = proc.returncode
    # communicate() returned, so the child has exited and its status is set; the
    # None fallback is defensive (a stub that never sets it) and never crashes.
    return Completed(
        returncode=rc if rc is not None else TIMEOUT_RC, stdout=stdout or "", timed_out=False
    )
