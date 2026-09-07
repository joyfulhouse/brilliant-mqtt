"""Bus-watchdog daemon: reboot only after sustained, attributable bus failure.

Heartbeat staleness and timed local-bus attribution must both exceed the
threshold while the bridge is active and its gateway is reachable. Broker-only
startup failures and a newly entered bus handshake therefore cannot borrow an
older heartbeat's age. Logic lives in should_reboot()/handle(); main() is thin.
"""

from __future__ import annotations

import logging
import logging.handlers
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from . import bounded, probe
from .health import bus_failure_age, heartbeat_age
from .reboot import reboot as _reboot
from .reboot_guard import GuardPolicy, RebootGuard

_LOG = logging.getLogger("brilliant_bus_watchdog")

# Wall-clock bound for the `systemctl is-active` probe; a hung systemd query must
# not read as active (which would wrongly let the watchdog believe the bridge is
# up) and must not wedge the loop.
_SERVICE_TIMEOUT = 5.0


class _GuardLike(Protocol):
    def can_reboot(self, now: float) -> bool: ...
    def record(self, now: float) -> None: ...


@dataclass(frozen=True)
class Config:
    interval: float
    stale_after: float
    heartbeat_path: str
    phase_path: str
    state_path: str
    log_path: str
    bridge_service: str
    gateway: str | None
    policy: GuardPolicy


def load_config(environ: Mapping[str, str]) -> Config:
    def f(key: str, default: float) -> float:
        try:
            return float(environ[key])
        except (KeyError, ValueError):
            return default

    return Config(
        interval=f("BUS_WATCHDOG_INTERVAL", 60.0),
        stale_after=f("BUS_WATCHDOG_STALE_AFTER", 1800.0),
        heartbeat_path=environ.get("BUS_HEARTBEAT_FILE", "/run/brilliant-mqtt/bus-heartbeat"),
        phase_path=environ.get("BUS_PHASE_FILE", "/run/brilliant-mqtt/bus-phase"),
        state_path=environ.get("BUS_WATCHDOG_STATE", "/var/brilliant-mqtt/bus-watchdog.state"),
        log_path=environ.get("BUS_WATCHDOG_LOG", "/var/brilliant-mqtt/bus-watchdog.log"),
        bridge_service=environ.get("BRIDGE_SERVICE", "brilliant-mqtt"),
        gateway=environ.get("BUS_WATCHDOG_GATEWAY") or None,
        policy=GuardPolicy(
            cooldown=f("BUS_WATCHDOG_REBOOT_COOLDOWN", 3600.0),
            cap=int(f("BUS_WATCHDOG_REBOOT_CAP", 3)),
            window=f("BUS_WATCHDOG_REBOOT_WINDOW", 21600.0),
        ),
    )


def should_reboot(
    *,
    age: float,
    bridge_active: bool,
    gateway_up: bool,
    bus_failure_age: float | None,
    stale_after: float,
) -> bool:
    return (
        age >= stale_after
        and bridge_active
        and gateway_up
        and bus_failure_age is not None
        and bus_failure_age >= stale_after
    )


def handle(*, should: bool, guard: _GuardLike, now: float, reboot_fn: Any = _reboot) -> None:
    if not should:
        return
    if guard.can_reboot(now):
        _LOG.error("bus session dead past threshold, network up, bridge active — rebooting")
        guard.record(now)
        reboot_fn()
    else:
        _LOG.error("bus session dead but reboot guard blocked (cooldown/cap) — waiting")


def _service_active(service: str, run: Any = None) -> bool:
    runner = run or (lambda argv: bounded.run_bounded(argv, timeout=_SERVICE_TIMEOUT, capture=True))
    try:
        r = runner(["systemctl", "is-active", service])
        return (r.stdout or "").strip() in {"active", "activating"}
    except OSError:
        return False


def _service_started_at(service: str, run: Any = None) -> float | None:
    runner = run or (lambda argv: bounded.run_bounded(argv, timeout=_SERVICE_TIMEOUT, capture=True))
    try:
        result = runner(
            [
                "systemctl",
                "show",
                "--property=ExecMainStartTimestampMonotonic",
                "--value",
                service,
            ]
        )
        started_at_us = int((result.stdout or "").strip())
    except (OSError, ValueError):
        return None
    return started_at_us / 1_000_000 if started_at_us > 0 else None


def _configure_logging(path: str) -> None:
    handler = logging.handlers.RotatingFileHandler(path, maxBytes=512_000, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _LOG.addHandler(handler)
    _LOG.setLevel(logging.INFO)


def main() -> None:  # pragma: no cover - thin loop
    cfg = load_config(os.environ)
    _configure_logging(cfg.log_path)
    guard = RebootGuard(cfg.state_path, cfg.policy)
    started_at = time.time()
    while True:
        now = time.time()
        age = heartbeat_age(cfg.heartbeat_path, now=now, started_at=started_at)
        gw = cfg.gateway or probe.default_gateway()
        gateway_up = probe.ping(gw) if gw else False
        active = _service_active(cfg.bridge_service)
        service_started_at = _service_started_at(cfg.bridge_service)
        failure_age = (
            bus_failure_age(
                cfg.phase_path,
                now=time.monotonic(),
                service_started_at=service_started_at,
            )
            if service_started_at is not None
            else None
        )
        should = should_reboot(
            age=age,
            bridge_active=active,
            gateway_up=gateway_up,
            bus_failure_age=failure_age,
            stale_after=cfg.stale_after,
        )
        _LOG.info(
            "age=%.0fs bridge_active=%s gateway_up=%s bus_failure_age=%s should=%s",
            age,
            active,
            gateway_up,
            "none" if failure_age is None else f"{failure_age:.0f}s",
            should,
        )
        handle(should=should, guard=guard, now=now)
        time.sleep(cfg.interval)


if __name__ == "__main__":  # pragma: no cover
    main()
