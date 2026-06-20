"""Redacted diagnostics for one panel entry."""

from __future__ import annotations

from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant

from . import BrilliantMqttConfigEntry
from .const import CONF_MQTT_PASSWORD, CONF_ROOT_PASSWORD

_TO_REDACT = {CONF_ROOT_PASSWORD, CONF_MQTT_PASSWORD}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: BrilliantMqttConfigEntry
) -> dict[str, Any]:
    """Entry data (secrets redacted) + options + the manager's live panel state."""
    manager = entry.runtime_data
    return {
        "entry": async_redact_data(dict(entry.data), _TO_REDACT),
        "options": dict(entry.options),
        "availability": manager.availability,
        "meta": manager.meta,
        "problem": manager.problem,
        "problem_reason": manager.problem_reason,
    }
