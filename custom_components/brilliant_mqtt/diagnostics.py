"""Redacted diagnostics for one panel entry."""

from __future__ import annotations

from collections.abc import Mapping, Sequence
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr

from . import BrilliantMqttConfigEntry
from .ble_scanner import disabled_scanner_diagnostics
from .const import (
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_TOKEN,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MQTT_PASSWORD,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    DATA_CONTROL_PLANE,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
)
from .ha_control import HaControlPlane

_TO_REDACT = {CONF_ROOT_PASSWORD, CONF_MQTT_PASSWORD, CONF_HA_MIRROR_TOKEN}


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: BrilliantMqttConfigEntry
) -> dict[str, Any]:
    """Entry data (secrets redacted) + options + the manager's live panel state."""
    manager = entry.runtime_data
    data = dict(entry.data)
    raw_overrides = data.pop(CONF_ROOM_OVERRIDES, {})
    raw_actions = data.pop(CONF_SCENE_ACTIONS, {})
    label = data.get(CONF_HA_CONTROL_LABEL)
    if not isinstance(label, str) or not label:
        legacy_label = data.get(CONF_HA_MIRROR_LABEL)
        label = (
            legacy_label
            if isinstance(legacy_label, str) and legacy_label
            else DEFAULT_HA_CONTROL_LABEL
        )
    raw_domains = data.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)
    domains = (
        list(raw_domains)
        if isinstance(raw_domains, Sequence) and not isinstance(raw_domains, str)
        else list(DEFAULT_HA_CONTROL_DOMAINS)
    )
    maximum = data.get(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES)
    maximum = maximum if type(maximum) is int else DEFAULT_MAX_MIRRORED_ENTITIES
    scene_panel = data.get(CONF_SCENE_PANEL)
    scene_panel = scene_panel if isinstance(scene_panel, str) else None

    domain_data = hass.data.get(DOMAIN, {})
    candidate = domain_data.get(DATA_CONTROL_PLANE) if isinstance(domain_data, Mapping) else None
    plane = candidate if isinstance(candidate, HaControlPlane) else None
    scene_control = plane.scene_control if plane is not None else None
    scene_status = (
        scene_control.transport_status("scene", scene_panel)
        if scene_control is not None and scene_panel is not None
        else None
    )
    label_entry = lr.async_get(hass).async_get_label_by_name(label)
    selected_entity_count = (
        sum(
            label_entry.label_id in registry_entry.labels
            for registry_entry in er.async_get(hass).entities.values()
        )
        if label_entry is not None
        else 0
    )
    ble_scanner_bridge = getattr(manager, "ble_scanner_bridge", None)
    return {
        "entry": async_redact_data(data, _TO_REDACT),
        "options": dict(entry.options),
        "availability": manager.availability,
        "meta": manager.meta,
        "problem": manager.problem,
        "problem_reason": manager.problem_reason,
        "ble_scanner": (
            ble_scanner_bridge.diagnostics
            if ble_scanner_bridge is not None
            else disabled_scanner_diagnostics()
        ),
        "ha_control": {
            "enabled": data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED) is True,
            "label": label,
            "room_override_count": len(raw_overrides) if isinstance(raw_overrides, Mapping) else 0,
            "scene_action_count": len(raw_actions) if isinstance(raw_actions, Mapping) else 0,
            "domains": domains,
            "maximum_entities": maximum,
            "selected_entity_count": selected_entity_count,
            "manifest_revision": plane.manifest_revision if plane is not None else None,
            "manifest_entity_count": plane.manifest_entity_count if plane is not None else None,
            "scene_panel": scene_panel,
            "scene_catalog_revision": scene_control.catalog_revision("scene", scene_panel)
            if scene_control is not None and scene_panel is not None
            else None,
            "scene_last_event_timestamp_ms": scene_control.last_event_timestamp_ms(
                "scene", scene_panel
            )
            if scene_control is not None and scene_panel is not None
            else None,
            "scene_status": "online"
            if scene_status is True
            else "offline"
            if scene_status is False
            else None,
            "native_tiles": {"status": "blocked", "validated": False},
        },
    }
