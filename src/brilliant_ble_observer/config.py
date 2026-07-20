"""Strict environment configuration for the standalone BLE observer."""

from __future__ import annotations

import json
import math
import os
import re
from collections.abc import Mapping
from dataclasses import dataclass

from .model import AllowlistEntry, normalize_panel, parse_allowlist

MAX_EVENTS_PER_SECOND = 100.0

_ENV_TRUTHY = frozenset({"1", "true", "on", "yes"})
_ENV_FALSY = frozenset({"0", "false", "off", "no"})
_ADAPTER_PATTERN = re.compile(r"hci(?:0|[1-9][0-9]{0,2})")
_LOG_LEVELS = frozenset({"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"})


def normalize_adapter(value: object) -> str:
    """Return one validated BlueZ adapter name."""
    if not isinstance(value, str):
        raise ValueError("BLE_OBSERVER_ADAPTER must be hci followed by a numeric index")
    normalized = value.strip()
    if _ADAPTER_PATTERN.fullmatch(normalized) is None:
        raise ValueError("BLE_OBSERVER_ADAPTER must be hci followed by a numeric index")
    return normalized


@dataclass(frozen=True)
class Settings:
    """Immutable observer settings sourced from the process environment."""

    panel: str
    mqtt_host: str
    mqtt_username: str
    mqtt_password: str
    mqtt_port: int = 1883
    enabled: bool = False
    allowlist: tuple[AllowlistEntry, ...] = ()
    adapter: str = "hci0"
    max_events_per_second: float = 10.0
    log_level: str = "INFO"

    @classmethod
    def from_env(cls, env: Mapping[str, str] | None = None) -> Settings:
        """Parse settings, raising immediately on missing or malformed input."""
        source = os.environ if env is None else env
        panel = _required(source, "BRILLIANT_PANEL", strip=True)
        try:
            panel = normalize_panel(panel)
        except ValueError as error:
            raise ValueError(f"BRILLIANT_PANEL: {error}") from error

        mqtt_host = _required(source, "MQTT_HOST", strip=True)
        mqtt_username = _required(source, "MQTT_USERNAME", strip=True)
        mqtt_password = _required(source, "MQTT_PASSWORD", strip=False)
        mqtt_port = _parse_port(source.get("MQTT_PORT", "1883"))
        enabled = _parse_bool(source.get("BLE_OBSERVER_ENABLED", "false"))
        allowlist = _parse_allowlist_json(source.get("BLE_OBSERVER_ALLOWLIST_JSON", "[]"))

        adapter = normalize_adapter(source.get("BLE_OBSERVER_ADAPTER", "hci0"))

        rate = _parse_rate(source.get("BLE_OBSERVER_MAX_EVENTS_PER_SECOND", "10"))
        log_level = source.get("BLE_OBSERVER_LOG_LEVEL", "INFO").strip().upper()
        if log_level not in _LOG_LEVELS:
            raise ValueError(
                "BLE_OBSERVER_LOG_LEVEL must be DEBUG, INFO, WARNING, ERROR, or CRITICAL"
            )

        return cls(
            panel=panel,
            mqtt_host=mqtt_host,
            mqtt_username=mqtt_username,
            mqtt_password=mqtt_password,
            mqtt_port=mqtt_port,
            enabled=enabled,
            allowlist=allowlist,
            adapter=adapter,
            max_events_per_second=rate,
            log_level=log_level,
        )


def _required(env: Mapping[str, str], key: str, *, strip: bool) -> str:
    value = env[key]
    if not isinstance(value, str) or not value.strip():
        raise ValueError(f"{key} must be non-empty")
    return value.strip() if strip else value


def _parse_bool(raw: str) -> bool:
    value = raw.strip().lower()
    if value in _ENV_TRUTHY:
        return True
    if value in _ENV_FALSY:
        return False
    raise ValueError(
        f"BLE_OBSERVER_ENABLED must be one of {sorted(_ENV_TRUTHY)} / {sorted(_ENV_FALSY)}"
    )


def _parse_port(raw: str) -> int:
    try:
        value = int(raw)
    except ValueError as error:
        raise ValueError("MQTT_PORT must be an integer from 1 through 65535") from error
    if not 1 <= value <= 65_535:
        raise ValueError("MQTT_PORT must be an integer from 1 through 65535")
    return value


def _parse_rate(raw: str) -> float:
    try:
        value = float(raw)
    except ValueError as error:
        raise ValueError(
            f"BLE_OBSERVER_MAX_EVENTS_PER_SECOND must be greater than 0 and at most "
            f"{MAX_EVENTS_PER_SECOND:g}"
        ) from error
    if not math.isfinite(value) or not 0 < value <= MAX_EVENTS_PER_SECOND:
        raise ValueError(
            f"BLE_OBSERVER_MAX_EVENTS_PER_SECOND must be greater than 0 and at most "
            f"{MAX_EVENTS_PER_SECOND:g}"
        )
    return value


def _reject_duplicate_keys(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key {key!r}")
        result[key] = value
    return result


def _parse_allowlist_json(raw: str) -> tuple[AllowlistEntry, ...]:
    try:
        decoded = json.loads(raw, object_pairs_hook=_reject_duplicate_keys)
        return parse_allowlist(decoded)
    except (json.JSONDecodeError, TypeError, ValueError) as error:
        raise ValueError(f"BLE_OBSERVER_ALLOWLIST_JSON is invalid: {error}") from error
