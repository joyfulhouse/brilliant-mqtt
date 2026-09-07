"""Connectivity probes (stdlib). Gateway derived from the routing table, never hardcoded."""

from __future__ import annotations

from collections.abc import Callable

from . import bounded

# Wall-clock bound for a single probe child. `ip route`/`ping` are local and
# fast; the bound guards the pathological case where the child never returns
# (a hung network stack), which would otherwise wedge the watchdog loop.
_PROBE_TIMEOUT = 5.0


def _run_out(argv: list[str]) -> tuple[int, str]:
    r = bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT, capture=True)
    return r.returncode, r.stdout


def _run_rc(argv: list[str]) -> int:
    return bounded.run_bounded(argv, timeout=_PROBE_TIMEOUT).returncode


def default_gateway(run: Callable[[list[str]], tuple[int, str]] = _run_out) -> str | None:
    rc, out = run(["ip", "route", "show", "default"])
    if rc != 0:
        return None
    for line in out.splitlines():
        parts = line.split()
        if "via" in parts:
            idx = parts.index("via") + 1
            if idx < len(parts):  # guard: "default via" with no following token
                return parts[idx]
    return None


def ping(host: str, run: Callable[[list[str]], int] = _run_rc) -> bool:
    return run(["ping", "-c", "1", "-W", "2", host]) == 0
