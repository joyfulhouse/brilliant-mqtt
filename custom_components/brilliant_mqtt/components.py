"""Per-panel component registry: id -> present/install/remove over panel_ops.

Keeps each component's existing panel_ops recipes; this is only the selection +
orchestration seam the config flow / reconfigure / manager drive. New component =
one REGISTRY row (+ its panel_ops recipes).
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from typing import Any

from homeassistant.core import HomeAssistant

from . import manager as _mgr
from . import panel_ops
from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_LEADER_PRIORITY,
    CONF_HA_MIRROR_TOKEN,
    CONF_HA_MIRROR_WS_URL,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DEFAULT_HA_MIRROR_LABEL,
    DEFAULT_HA_MIRROR_LEADER_PRIORITY,
    DEFAULT_VOICE_WAKE_WORD,
    VOICE_PAYLOAD_VERSION,
    panel_device_name,
)
from .shell import PanelShell
from .voice_payload import async_fetch_voice_payload


@dataclass(frozen=True)
class Component:
    id: str
    label: str
    locked: bool
    default_enabled: bool
    present: Callable[[PanelShell], Awaitable[bool]]
    install: Callable[[HomeAssistant, PanelShell, Mapping[str, Any]], Awaitable[None]]
    remove: Callable[[PanelShell], Awaitable[None]]


async def _bridge_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_panel(shell)).payload_present


async def _bridge_install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
    payload_dir = _mgr._payload_dir()
    unit = await hass.async_add_executor_job((payload_dir / "brilliant-mqtt.service").read_text)
    version = (await hass.async_add_executor_job((payload_dir / "VERSION").read_text)).strip()
    env = panel_ops.render_env(
        panel=data[CONF_PANEL],
        mesh_priority=data[CONF_MESH_PRIORITY],
        mqtt_host=data[CONF_MQTT_HOST],
        mqtt_port=data[CONF_MQTT_PORT],
        mqtt_username=data[CONF_MQTT_USERNAME],
        mqtt_password=data[CONF_MQTT_PASSWORD],
    )
    await panel_ops.deploy_payload(shell, str(payload_dir), version)
    await panel_ops.ensure_configs(shell, unit, env)
    await panel_ops.enable_now(shell)


async def _voice_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_voice(shell)).payload_present


async def _voice_install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
    tarball = await async_fetch_voice_payload(hass)
    env = panel_ops.render_voice_env(
        panel=data[CONF_PANEL],
        name=panel_device_name(data[CONF_PANEL]),
        api_port=6053,
        wake_word=data.get(CONF_VOICE_WAKE_WORD, DEFAULT_VOICE_WAKE_WORD),
        ha_host=data.get(CONF_VOICE_HA_HOST, ""),
        enable_aec=False,
    )
    await panel_ops.deploy_voice_payload(shell, tarball, VOICE_PAYLOAD_VERSION)
    await panel_ops.ensure_voice_config(shell, env)
    await panel_ops.enable_voice(shell)


async def _wd_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_wifi_watchdog(shell)).payload_present


async def _wd_install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
    payload_dir = _mgr._payload_dir()
    unit = await hass.async_add_executor_job(
        (payload_dir / "brilliant-wifi-watchdog.service").read_text
    )
    await panel_ops.deploy_wifi_watchdog(shell, str(payload_dir / "wifi_watchdog"))
    await panel_ops.ensure_wifi_watchdog_unit(shell, unit)
    await panel_ops.enable_wifi_watchdog(shell)


async def _bus_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_bus_watchdog(shell)).payload_present


async def _bus_install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
    payload_dir = _mgr._payload_dir()
    unit = await hass.async_add_executor_job(
        (payload_dir / "brilliant-bus-watchdog.service").read_text
    )
    await panel_ops.deploy_bus_watchdog(shell, str(payload_dir / "bus_watchdog"))
    await panel_ops.ensure_bus_watchdog_unit(shell, unit)
    await panel_ops.enable_bus_watchdog(shell)


async def _hamirror_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_ha_mirror(shell)).payload_present


async def _hamirror_install(
    hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]
) -> None:
    payload_dir = _mgr._payload_dir()
    unit = await hass.async_add_executor_job(
        (payload_dir / "brilliant-ha-mirror.service").read_text
    )
    env = panel_ops.render_ha_mirror_env(
        panel=data[CONF_PANEL],
        ha_ws_url=data[CONF_HA_MIRROR_WS_URL],
        ha_token=data[CONF_HA_MIRROR_TOKEN],
        mirror_label=data.get(CONF_HA_MIRROR_LABEL, DEFAULT_HA_MIRROR_LABEL),
        leader_priority=data.get(CONF_HA_MIRROR_LEADER_PRIORITY, DEFAULT_HA_MIRROR_LEADER_PRIORITY),
        mqtt_host=data[CONF_MQTT_HOST],
        mqtt_port=data[CONF_MQTT_PORT],
        mqtt_username=data[CONF_MQTT_USERNAME],
        mqtt_password=data[CONF_MQTT_PASSWORD],
    )
    await panel_ops.deploy_ha_mirror(shell, str(payload_dir / "ha_mirror"))
    await panel_ops.ensure_ha_mirror_config(shell, unit, env)
    await panel_ops.enable_ha_mirror(shell)


REGISTRY: dict[str, Component] = {
    COMPONENT_BRIDGE: Component(
        id=COMPONENT_BRIDGE,
        label="MQTT bridge",
        locked=True,
        default_enabled=True,
        present=_bridge_present,
        install=_bridge_install,
        remove=panel_ops.uninstall,
    ),
    COMPONENT_VOICE: Component(
        id=COMPONENT_VOICE,
        label="Voice satellite",
        locked=False,
        default_enabled=False,
        present=_voice_present,
        install=_voice_install,
        remove=panel_ops.uninstall_voice,
    ),
    COMPONENT_WIFI_WATCHDOG: Component(
        id=COMPONENT_WIFI_WATCHDOG,
        label="Wi-Fi watchdog",
        locked=False,
        default_enabled=True,
        present=_wd_present,
        install=_wd_install,
        remove=panel_ops.uninstall_wifi_watchdog,
    ),
    COMPONENT_BUS_WATCHDOG: Component(
        id=COMPONENT_BUS_WATCHDOG,
        label="Bus watchdog",
        locked=False,
        default_enabled=True,
        present=_bus_present,
        install=_bus_install,
        remove=panel_ops.uninstall_bus_watchdog,
    ),
    COMPONENT_HA_MIRROR: Component(
        id=COMPONENT_HA_MIRROR,
        label="HA mirror",
        locked=False,
        default_enabled=False,
        present=_hamirror_present,
        install=_hamirror_install,
        remove=panel_ops.uninstall_ha_mirror,
    ),
}


def optional() -> list[Component]:
    """Non-locked components in stable display order."""
    return [c for c in REGISTRY.values() if not c.locked]


def default_components() -> dict[str, bool]:
    return {c.id: (True if c.locked else c.default_enabled) for c in REGISTRY.values()}


def selected_ids(entry_data: Mapping[str, Any]) -> list[str]:
    chosen = dict(entry_data.get(CONF_COMPONENTS, {}))
    return [c.id for c in REGISTRY.values() if c.locked or chosen.get(c.id, False)]
