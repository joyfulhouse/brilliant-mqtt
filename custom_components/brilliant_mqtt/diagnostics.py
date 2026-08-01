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
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_TOKEN,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
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
    ENTRY_KIND_FLEET,
)
from .entry_data import EntryDataError, PanelConfig
from .ha_control import HaControlPlane
from .provisioning_journal import ProvisioningJournal

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
_SAFE_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_SAFE_COMPONENTS = frozenset(
    {
        COMPONENT_BRIDGE,
        COMPONENT_BUS_WATCHDOG,
        COMPONENT_HA_MIRROR,
        COMPONENT_HUE_CA,
        COMPONENT_VOICE,
        COMPONENT_WIFI_WATCHDOG,
    }
)


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


def _safe_bounded_text(value: object, *, maximum: int) -> str | None:
    """Return one bounded printable identifier, never arbitrary persisted text."""
    if (
        not isinstance(value, str)
        or not value
        or len(value) > maximum
        or not value.isascii()
        or not value.isprintable()
        or any(char.isspace() for char in value)
    ):
        return None
    return value


def _problem_reason_diagnostics(value: object) -> str | None:
    """Classify runtime problems without exporting panel- or exception-controlled text."""
    if not isinstance(value, str) or not value:
        return None
    if value.startswith("runtime setup failed"):
        return "runtime_setup_failed"
    if "host key changed" in value:
        return "host_key_changed"
    for prefix, code in (
        ("bridge offline past grace period", "bridge_offline"),
        ("bridge offline again within", "repair_cooldown"),
        ("panel unreachable", "panel_unreachable"),
        ("repair step failed", "repair_step_failed"),
        ("panel repair step failed", "repair_step_failed"),
        ("agent update", "agent_update_failed"),
        ("agent uninstall", "agent_uninstall_failed"),
        ("repair ran but the bridge did not come back", "repair_recovery_timeout"),
        ("agent retained-topic ownership ledger", "retained_ledger"),
    ):
        if value.startswith(prefix):
            return code
    return "needs_attention"


def _panel_configuration(panel: object) -> dict[str, Any] | None:
    """Expose only validated, non-secret panel configuration fields."""
    store = getattr(panel, "store", None)
    data = getattr(store, "data", None)
    if not isinstance(data, Mapping):
        return None

    raw_components = data.get(CONF_COMPONENTS)
    if isinstance(raw_components, Mapping):
        enabled_components = sorted(
            component for component in _SAFE_COMPONENTS if raw_components.get(component) is True
        )
    else:
        enabled_components = []
    fingerprint = _safe_bounded_text(data.get(CONF_IDENTITY_FINGERPRINT), maximum=128)
    if fingerprint is not None and not fingerprint.startswith("SHA256:"):
        fingerprint = None
    panel_slug = _safe_bounded_text(data.get(CONF_PANEL), maximum=64)
    if panel_slug is not None and _SAFE_PANEL_SLUG.fullmatch(panel_slug) is None:
        panel_slug = None
    mesh_priority = data.get(CONF_MESH_PRIORITY)
    if type(mesh_priority) is not int or not 0 <= mesh_priority <= 99:
        mesh_priority = None
    return {
        "address": _safe_bounded_text(data.get(CONF_HOST), maximum=253),
        "enabled_components": enabled_components,
        "identity_fingerprint": fingerprint,
        "mesh_priority": mesh_priority,
        "panel_slug": panel_slug,
    }


def _scene_panel_slug(
    entry: BrilliantMqttConfigEntry,
    data: Mapping[str, Any],
) -> str | None:
    """Resolve a fleet's config-subentry owner to its MQTT panel slug."""
    scene_owner = data.get(CONF_SCENE_PANEL)
    if not isinstance(scene_owner, str):
        return None
    if data.get(CONF_ENTRY_KIND) != ENTRY_KIND_FLEET:
        return scene_owner
    subentry = entry.subentries.get(scene_owner)
    if subentry is None:
        return None
    try:
        return PanelConfig.from_subentry(subentry).panel
    except EntryDataError:
        return None


async def _async_provisioning_diagnostics(
    hass: HomeAssistant,
) -> dict[str, int | str | None]:
    """Return only count/phase, degrading safely when private storage is unreadable."""
    try:
        return await ProvisioningJournal(hass).async_diagnostics()
    except Exception:
        return {"count": None, "phase": None}


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
    scene_panel = _scene_panel_slug(entry, data)

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
    panels = dict(manager.panels)
    panel_diagnostics = {
        panel_id: {
            "availability": panel.availability
            if panel.availability in {AVAILABILITY_ONLINE, AVAILABILITY_OFFLINE}
            else None,
            "configuration": _panel_configuration(panel),
            "meta": _meta_diagnostics(panel.meta),
            "problem": panel.problem,
            "problem_reason": _problem_reason_diagnostics(panel.problem_reason),
        }
        for panel_id, panel in panels.items()
    }
    fleet_diagnostics = {
        "panel_count": len(panel_diagnostics),
        "online_panel_count": sum(
            panel["availability"] == AVAILABILITY_ONLINE for panel in panel_diagnostics.values()
        ),
        "offline_panel_count": sum(
            panel["availability"] == AVAILABILITY_OFFLINE for panel in panel_diagnostics.values()
        ),
        "unknown_panel_count": sum(
            panel["availability"] not in {AVAILABILITY_ONLINE, AVAILABILITY_OFFLINE}
            for panel in panel_diagnostics.values()
        ),
        "problem_panel_count": sum(
            panel["problem"] is True for panel in panel_diagnostics.values()
        ),
    }
    ha_control_diagnostics = {
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
        "scene_last_event_timestamp_ms": scene_control.last_event_timestamp_ms("scene", scene_panel)
        if scene_control is not None and scene_panel is not None
        else None,
        "scene_status": "online"
        if scene_status is True
        else "offline"
        if scene_status is False
        else None,
        "native_tiles": {"status": "blocked", "validated": False},
    }
    broker_available = manager.broker_available
    provisioning_diagnostics = await _async_provisioning_diagnostics(hass)
    return {
        "entry": _entry_diagnostics(data),
        "options": {"configured": bool(entry.options)},
        "broker_available": broker_available,
        "fleet": fleet_diagnostics,
        "provisioning": provisioning_diagnostics,
        "panels": panel_diagnostics,
        "ha_control": ha_control_diagnostics,
    }
