"""Bounded child-process execution for the watchdog.

A watchdog must never be wedged by a hung child. A probe against an unhealthy
network service, or a recovery command against a stuck daemon, can block
forever; without a bound that would leave the watchdog alive but unable to run
another check — exactly the failure this module prevents. Every probe/recovery
command therefore runs through :func:`run_bounded`, which enforces a wall-clock
deadline and, on expiry, kills the child's whole process group and reaps it (no
zombie and no lingering process — not even a grandchild — piling up on the
panel's constrained hardware) before returning a timeout result. The caller
treats a timeout as a bounded failure and the watchdog loop proceeds.

Stdlib only (:mod:`subprocess`/:mod:`os`/:mod:`signal`), so the module deploys
self-contained on the panel interpreter (Python 3.10) with only this package on
``PYTHONPATH``.
"""

from __future__ import annotations

import logging
import os
import signal
import subprocess
from collections.abc import Sequence
from dataclasses import dataclass

_LOG = logging.getLogger(__name__)

# Exit status reported for a timed-out child, mirroring the shell's timeout(1).
TIMEOUT_RC = 124
# Bound on reaping a SIGKILLed group; it dies promptly, so this is only reached
# if the child sits in uninterruptible (D-state) kernel sleep — logged, not silent.
_REAP_TIMEOUT = 5.0


@dataclass(frozen=True)
class Completed:
    """Outcome of a bounded child process."""

    returncode: int
    stdout: str
    timed_out: bool


def _kill_and_reap(proc: subprocess.Popen[str]) -> None:
    """Kill the child's whole process group with ``SIGKILL``, then reap it.

    The child is started in its own session (``start_new_session=True``), so
    signalling the group takes down any grandchild too — including one still
    holding the stdout pipe open, which a plain ``communicate()`` (read-to-EOF)
    would block on until that grandchild exited. Reaping uses ``wait()`` (which
    ignores the pipe) rather than a second ``communicate()``, and the pipe is
    closed explicitly, so a full or grandchild-held pipe can never wedge cleanup.
    """
    try:
        os.killpg(os.getpgid(proc.pid), signal.SIGKILL)
    except OSError:  # no group, or already gone — at least kill the child
        try:
            proc.kill()
        except OSError:
            pass  # child already reaped; wait()+close() below must still run
    try:
        proc.wait(timeout=_REAP_TIMEOUT)
    except subprocess.TimeoutExpired:
        # A child in uninterruptible (D-state) kernel sleep — plausible for a
        # probe against a wedged Wi-Fi driver — survives even SIGKILL until the
        # syscall returns. Surface it (one per occurrence) rather than leaving a
        # process to linger invisibly; the call still reports timed_out.
        _LOG.warning("child pid %s did not die after SIGKILL (D-state?)", proc.pid)
    finally:
        if proc.stdout is not None:
            proc.stdout.close()


def run_bounded(argv: Sequence[str], *, timeout: float, capture: bool = False) -> Completed:
    """Run *argv* with a wall-clock *timeout*; kill its group and reap on expiry.

    Returns a :class:`Completed`. On timeout the child's process group is sent
    ``SIGKILL`` and reaped, so no zombie or lingering process (grandchildren
    included) is left behind; the result carries ``timed_out=True`` with
    ``returncode=TIMEOUT_RC`` and no :class:`subprocess.TimeoutExpired` is raised,
    so the watchdog loop keeps running.

    ``capture`` decides whether stdout is captured (probes that parse output) or
    discarded (recovery commands run for their side effect).
    """
    out = subprocess.PIPE if capture else subprocess.DEVNULL
    proc = subprocess.Popen(
        list(argv), stdout=out, stderr=subprocess.DEVNULL, text=True, start_new_session=True
    )
    try:
        stdout, _ = proc.communicate(timeout=timeout)
    except subprocess.TimeoutExpired:
        _kill_and_reap(proc)
        return Completed(returncode=TIMEOUT_RC, stdout="", timed_out=True)
    rc = proc.returncode
    # communicate() returned, so the child has exited and its status is set; the
    # None fallback is defensive (never observed) and keeps the watchdog alive.
    return Completed(
        returncode=rc if rc is not None else TIMEOUT_RC, stdout=stdout or "", timed_out=False
    )
