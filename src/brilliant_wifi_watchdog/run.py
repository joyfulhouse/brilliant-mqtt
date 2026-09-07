"""Watchdog daemon: probe loop + action dispatch. Logic lives in handle(); main() is thin."""

from __future__ import annotations

import logging
import logging.handlers
import os
import time
from collections.abc import Mapping
from dataclasses import dataclass
from typing import Any, Protocol

from . import probe, recovery
from .ladder import Action, Ladder, Thresholds
from .reboot_guard import GuardPolicy, RebootGuard

_LOG = logging.getLogger("brilliant_wifi_watchdog")


class _GuardLike(Protocol):
    def can_reboot(self, now: float) -> bool: ...

    def record(self, now: float) -> None: ...


class _LadderLike(Protocol):
    def observe(self, *, gateway_up: bool, now: float, reboot_eligible: bool) -> Action: ...


def _can_request(guard: _GuardLike, now: float) -> bool:
    request_check = getattr(guard, "can_request", None)
    if callable(request_check):
        return bool(request_check(now))
    return guard.can_reboot(now)


def _record_request(guard: _GuardLike, now: float) -> None:
    request_record = getattr(guard, "record_request", None)
    if callable(request_record):
        request_record(now)
    else:
        guard.record(now)


@dataclass(frozen=True)
class Config:
    interval: float
    gateway: str | None
    broker_host: str | None
    broker_port: int
    log_path: str
    state_path: str
    thresholds: Thresholds
    policy: GuardPolicy


def load_config(environ: Mapping[str, str]) -> Config:
    def f(key: str, default: float) -> float:
        try:
            return float(environ[key])
        except (KeyError, ValueError):
            return default

    return Config(
        interval=f("WIFI_WATCHDOG_INTERVAL", 30.0),
        gateway=environ.get("WIFI_WATCHDOG_GATEWAY") or None,
        broker_host=environ.get("MQTT_HOST") or None,
        broker_port=int(f("MQTT_PORT", 1883)),
        log_path=environ.get("WIFI_WATCHDOG_LOG", "/var/brilliant-mqtt/wifi-watchdog.log"),
        state_path=environ.get("WIFI_WATCHDOG_STATE", "/var/brilliant-mqtt/wifi-watchdog.state"),
        thresholds=Thresholds(
            soft_after=f("WIFI_WATCHDOG_SOFT_AFTER", 90.0),
            restart_after=f("WIFI_WATCHDOG_RESTART_AFTER", 180.0),
            reboot_after=f("WIFI_WATCHDOG_REBOOT_AFTER", 360.0),
        ),
        policy=GuardPolicy(
            cooldown=f("WIFI_WATCHDOG_REBOOT_COOLDOWN", 3600.0),
            cap=int(f("WIFI_WATCHDOG_REBOOT_CAP", 3)),
            window=f("WIFI_WATCHDOG_REBOOT_WINDOW", 21600.0),
        ),
    )


def handle(
    action: Action,
    *,
    guard: _GuardLike,
    now: float,
    reboot_eligible: bool = False,
    recovery_mod: Any = recovery,
) -> int | None:
    if action == Action.SOFT_RECONNECT:
        _LOG.warning("gateway down ~90s: connman reconnect")
        recovery_mod.soft_reconnect()
    elif action == Action.RESTART_SERVICES:
        _LOG.warning("gateway down ~180s: restarting connman + wpa_supplicant")
        recovery_mod.restart_services()
    elif action == Action.ESCALATE_NOTIFY:
        # Reboot wanted but the ladder saw the guard block it (cooldown/cap).  It
        # stays pending and re-arms automatically once the guard clears; log once.
        _LOG.error(
            "gateway down ~360s: reboot deferred by guard (cooldown/cap) "
            "— will retry automatically once eligible"
        )
    elif action == Action.GPIO_RESET_REBOOT:
        # Act on the SAME eligibility the ladder used this poll (passed in) — a
        # second, independent guard read here could disagree with the ladder's
        # pending-request latch. Defense in depth: never reboot unless that shared
        # read said eligible; otherwise log the blocked path (never silent).
        if reboot_eligible:
            _LOG.error("gateway down ~360s: GPIO/SDIO reset + reboot")
            _record_request(guard, now)
            result = recovery_mod.gpio_reset_and_reboot()
            if isinstance(result, int):
                if result == 0:
                    _LOG.error("reboot requested; waiting for a new boot identity")
                else:
                    _LOG.error(
                        "reboot request failed with status %d; recovery remains pending", result
                    )
                return result
        else:
            _LOG.error("gateway down ~360s but reboot guard blocked (cooldown/cap) — notify only")
    return None


def _configure_logging(path: str) -> None:
    handler = logging.handlers.RotatingFileHandler(path, maxBytes=512_000, backupCount=3)
    handler.setFormatter(logging.Formatter("%(asctime)s %(levelname)s %(message)s"))
    _LOG.addHandler(handler)
    _LOG.setLevel(logging.INFO)


def _poll_once(cfg: Config, *, guard: _GuardLike, ladder: _LadderLike) -> None:
    """One watchdog cycle: probe, decide, dispatch.

    The recovery ladder's health signal is the gateway ping alone. The broker TCP
    check is a bounded, log-only diagnostic — being bounded it can never delay
    reaching the ladder, and its tri-state result never feeds the decision, so a
    timed-out (INCONCLUSIVE) diagnostic can neither block nor force recovery.
    """
    gw = cfg.gateway or probe.default_gateway()
    gateway_up = probe.ping(gw) if gw else False
    if cfg.broker_host:
        broker = probe.tcp_open(cfg.broker_host, cfg.broker_port)
        _LOG.info("gateway=%s up=%s broker=%s", gw, gateway_up, broker.value)
    # One guard read per poll, shared by the ladder and handle() so they can
    # never act on disagreeing eligibility (rung-elapsed math stays monotonic;
    # only the guard-facing timestamp is wall-clock).
    wall = time.time()
    eligible = _can_request(guard, wall)
    action = ladder.observe(gateway_up=gateway_up, now=time.monotonic(), reboot_eligible=eligible)
    if action != Action.NONE:
        handle(action, guard=guard, now=wall, reboot_eligible=eligible)


def main() -> None:  # pragma: no cover - thin loop
    cfg = load_config(os.environ)
    _configure_logging(cfg.log_path)
    guard = RebootGuard(cfg.state_path, cfg.policy)
    ladder = Ladder(cfg.thresholds)
    while True:
        _poll_once(cfg, guard=guard, ladder=ladder)
        time.sleep(cfg.interval)


if __name__ == "__main__":  # pragma: no cover
    main()
