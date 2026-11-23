"""Shared bounded-retry policy for panel SSH connects (#219).

Used by the agent update's pre-mutation connect (manager) and by the managed
update's fresh-session rollback reconnect (panel_provisioner). Backoff before
retry n is ``min(cap, base * 2**(n-1))`` scaled by a jitter factor in
``[JITTER_MIN, 1.0]``.

Only network faults are transient. Authentication, host-key, key-exchange and
protocol failures are permanent: password-only root SSH must never be retried
into a lockout, and a changed host key must never be retried around. These
classes can still occur AFTER asyncssh began user authentication (connect_timeout
spans the handshake and auth), so callers additionally refuse any failure whose
shell reports ``connect_reached_auth()``: only a connect that never offered the
password repeats.
"""

from __future__ import annotations

import errno
import random
import socket

import asyncssh

MAX_ATTEMPTS = 5
BASE_DELAY_S = 5.0
MAX_DELAY_S = 30.0
DEADLINE_S = 120.0
JITTER_MIN = 0.5

_TRANSIENT_CONNECT_ERRNOS: dict[int, str] = {
    errno.EHOSTUNREACH: "host_unreachable",
    errno.EHOSTDOWN: "host_unreachable",
    errno.ENETUNREACH: "network_unreachable",
    errno.ENETDOWN: "network_unreachable",
    errno.ECONNREFUSED: "connection_refused",
    errno.ECONNRESET: "connection_reset",
    errno.ECONNABORTED: "connection_reset",
    errno.ETIMEDOUT: "timeout",
}

# Every reason classify_transient_connect_failure can return.
TRANSIENT_REASONS = frozenset(
    {*_TRANSIENT_CONNECT_ERRNOS.values(), "dns_temporary_failure", "connection_lost"}
)


def jitter() -> float:
    """Return the backoff scale factor: the single seam tests pin."""
    return random.uniform(JITTER_MIN, 1.0)


def classify_transient_connect_failure(error: BaseException) -> str | None:
    """Return a fixed retry reason for a transient connect failure, else None."""
    if isinstance(error, socket.gaierror):
        return "dns_temporary_failure" if error.errno == socket.EAI_AGAIN else None
    if isinstance(error, TimeoutError):
        return "timeout"
    if isinstance(error, OSError):
        return _TRANSIENT_CONNECT_ERRNOS.get(error.errno) if error.errno is not None else None
    if isinstance(error, asyncssh.ConnectionLost):
        return "connection_lost"
    return None


def backoff_delay(attempt: int, jitter_factor: float | None = None) -> float:
    """Capped exponential backoff after failed *attempt* (1-based), jittered.

    *jitter_factor* defaults to a fresh ``jitter()`` draw (looked up at call time).
    """
    factor = jitter() if jitter_factor is None else jitter_factor
    ceiling = min(MAX_DELAY_S, BASE_DELAY_S * 2.0 ** (attempt - 1))
    return round(ceiling * factor, 1)
