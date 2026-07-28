"""Redacted diagnostics for a Brilliant MQTT fleet entry."""

from __future__ import annotations

import re
from collections.abc import Mapping, Sequence
from typing import Any

from homeassistant.components.diagnostics import async_redact_data
from homeassistant.core import HomeAssistant
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr

from . import BrilliantMqttConfigEntry
from .const import (
    AVAILABILITY_OFFLINE,
    AVAILABILITY_ONLINE,
    CONF_BROKER_KIND,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_TOKEN,
    CONF_HOST,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SCHEMA_VERSION,
    DATA_CONTROL_PLANE,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DOMAIN,
)
from .ha_control import HaControlPlane

_TO_REDACT = {
    CONF_ROOT_PASSWORD,
    CONF_MQTT_USERNAME,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_TLS_CA,
    CONF_HA_MIRROR_TOKEN,
}
_SAFE_ENTRY_KEYS = {
    CONF_ENTRY_KIND,
    CONF_BROKER_KIND,
    CONF_SCHEMA_VERSION,
    CONF_HOST,
    CONF_PANEL,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_ENABLED,
}
_SAFE_META_KEYS = ("agent_version", "panel_firmware")
_SAFE_VERSION = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._+-]{0,127}$")


def _entry_diagnostics(data: Mapping[str, Any]) -> dict[str, Any]:
    """Allowlist persisted values so unknown secret-bearing mappings stay private."""
    included = _SAFE_ENTRY_KEYS | _TO_REDACT
    return async_redact_data(
        {key: value for key, value in data.items() if key in included},
        _TO_REDACT,
    )


def _meta_diagnostics(meta: Mapping[str, Any] | None) -> dict[str, str] | None:
    """Expose only bounded version identifiers, never the raw MQTT metadata object."""
    if meta is None:
        return None
    safe = {
        key: value
        for key in _SAFE_META_KEYS
        if isinstance((value := meta.get(key)), str) and _SAFE_VERSION.fullmatch(value) is not None
    }
    return safe or None


async def async_get_config_entry_diagnostics(
    hass: HomeAssistant, entry: BrilliantMqttConfigEntry
) -> dict[str, Any]:
    """Return allowlisted config and live state for every panel in the fleet."""
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
    return {
        "entry": _entry_diagnostics(data),
        "options": {"configured": bool(entry.options)},
        "broker_available": manager.broker_available,
        "panels": {
            panel_id: {
                "availability": panel.availability
                if panel.availability in {AVAILABILITY_ONLINE, AVAILABILITY_OFFLINE}
                else None,
                "meta": _meta_diagnostics(panel.meta),
                "problem": panel.problem,
                "problem_reason": panel.problem_reason,
            }
            for panel_id, panel in manager.panels.items()
        },
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
