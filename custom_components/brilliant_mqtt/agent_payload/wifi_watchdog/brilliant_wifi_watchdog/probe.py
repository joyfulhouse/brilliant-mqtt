"""Connectivity probes (stdlib). Gateway derived from the routing table, never hardcoded."""

from __future__ import annotations

import socket
import subprocess
from collections.abc import Callable


def _run_out(argv: list[str]) -> tuple[int, str]:
    p = subprocess.run(argv, check=False, capture_output=True, text=True)
    return p.returncode, p.stdout


def _run_rc(argv: list[str]) -> int:
    return subprocess.run(argv, check=False, capture_output=True).returncode


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


def tcp_open(host: str, port: int, timeout: float = 3.0) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False
