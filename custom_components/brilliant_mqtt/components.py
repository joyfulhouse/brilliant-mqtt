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
from .component_payloads import SINGLE_UNIT_PAYLOAD_BY_ID, SingleUnitPayloadSpec
from .const import (
    COMPONENT_BLE_OBSERVER,
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HUE_CA_CERT,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_VOICE_WAKE_WORD,
    VOICE_PAYLOAD_VERSION,
    ble_observer_allowlist_json,
    panel_device_name,
)
from .panel_ops import PanelOpError
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
    deprecated: bool = False


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
        scene_bridge_enabled=data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED) is True,
        ble_observer_enabled=bool(data.get(CONF_COMPONENTS, {}).get(COMPONENT_BLE_OBSERVER, False)),
        ble_observer_allowlist_json=ble_observer_allowlist_json(data),
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


def _single_unit_present(
    spec: SingleUnitPayloadSpec,
) -> Callable[[PanelShell], Awaitable[bool]]:
    async def present(shell: PanelShell) -> bool:
        return (await spec.inspect(shell)).payload_present

    return present


async def _install_single_unit_payload(
    hass: HomeAssistant,
    shell: PanelShell,
    *,
    spec: SingleUnitPayloadSpec,
) -> None:
    """Deploy and enable one single-unit component from the bundled payload."""
    payload_dir = _mgr._payload_dir()
    unit = await hass.async_add_executor_job((payload_dir / spec.service_filename).read_text)
    await spec.deploy(shell, str(payload_dir / spec.payload_subdir))
    await spec.ensure_unit(shell, unit)
    await spec.enable(shell)


def _single_unit_installer(
    spec: SingleUnitPayloadSpec,
) -> Callable[[HomeAssistant, PanelShell, Mapping[str, Any]], Awaitable[None]]:
    async def install(
        hass: HomeAssistant,
        shell: PanelShell,
        data: Mapping[str, Any],
    ) -> None:
        del data
        await _install_single_unit_payload(hass, shell, spec=spec)

    return install


async def _hue_ca_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_hue_ca(shell)).payload_present


async def _hue_ca_install(hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]) -> None:
    ca_pem = str(data.get(CONF_HUE_CA_CERT, "")).strip()
    if not ca_pem:
        raise PanelOpError("Hue CA recovery needs the diyHue CA certificate (PEM)")
    payload_dir = _mgr._payload_dir()
    service = await hass.async_add_executor_job(
        (payload_dir / "brilliant-hue-ca.service").read_text
    )
    timer = await hass.async_add_executor_job((payload_dir / "brilliant-hue-ca.timer").read_text)
    await panel_ops.deploy_hue_ca(shell, str(payload_dir / "hue_ca"), ca_pem)
    await panel_ops.ensure_hue_ca_units(shell, service, timer)
    await panel_ops.enable_hue_ca(shell)


async def _hamirror_present(shell: PanelShell) -> bool:
    return (await panel_ops.inspect_ha_mirror(shell)).payload_present


async def _hamirror_install(
    hass: HomeAssistant, shell: PanelShell, data: Mapping[str, Any]
) -> None:
    del hass, shell, data
    raise PanelOpError("HA mirror is deprecated and cannot be installed")


async def _apply_ble_observer(
    hass: HomeAssistant,
    shell: PanelShell,
    data: Mapping[str, Any],
    *,
    activate: Callable[[PanelShell], Awaitable[None]],
) -> None:
    """Rebuild/configure the observer, then use one proof-bearing activation path."""
    spec = SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BLE_OBSERVER]
    payload_dir = _mgr._payload_dir()
    unit = await hass.async_add_executor_job((payload_dir / spec.service_filename).read_text)
    await spec.deploy(shell, str(payload_dir / spec.payload_subdir))
    await panel_ops.configure_ble_observer_env(
        shell,
        enabled=True,
        allowlist_json=ble_observer_allowlist_json(data),
    )
    await spec.ensure_unit(shell, unit)
    await activate(shell)


async def _ble_observer_install(
    hass: HomeAssistant,
    shell: PanelShell,
    data: Mapping[str, Any],
) -> None:
    """Deploy/configure/start the observer without controlling bridge or BlueZ."""
    spec = SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BLE_OBSERVER]
    await _apply_ble_observer(hass, shell, data, activate=spec.enable)


async def refresh_ble_observer(
    hass: HomeAssistant,
    shell: PanelShell,
    data: Mapping[str, Any],
) -> None:
    """Self-heal all observer assets and restart onto the desired runtime config."""
    activate = SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BLE_OBSERVER].activate_updated
    if activate is None:
        raise PanelOpError("BLE observer update activation is not configured")
    await _apply_ble_observer(hass, shell, data, activate=activate)


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
        label=SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_WIFI_WATCHDOG].label,
        locked=False,
        default_enabled=True,
        present=_single_unit_present(SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_WIFI_WATCHDOG]),
        install=_single_unit_installer(SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_WIFI_WATCHDOG]),
        remove=panel_ops.uninstall_wifi_watchdog,
    ),
    COMPONENT_BUS_WATCHDOG: Component(
        id=COMPONENT_BUS_WATCHDOG,
        label=SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BUS_WATCHDOG].label,
        locked=False,
        default_enabled=True,
        present=_single_unit_present(SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BUS_WATCHDOG]),
        install=_single_unit_installer(SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BUS_WATCHDOG]),
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
        deprecated=True,
    ),
    COMPONENT_HUE_CA: Component(
        id=COMPONENT_HUE_CA,
        label="Hue CA recovery",
        locked=False,
        default_enabled=False,
        present=_hue_ca_present,
        install=_hue_ca_install,
        remove=panel_ops.uninstall_hue_ca,
    ),
    COMPONENT_BLE_OBSERVER: Component(
        id=COMPONENT_BLE_OBSERVER,
        label=SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BLE_OBSERVER].label,
        locked=False,
        default_enabled=False,
        present=_single_unit_present(SINGLE_UNIT_PAYLOAD_BY_ID[COMPONENT_BLE_OBSERVER]),
        install=_ble_observer_install,
        remove=panel_ops.uninstall_ble_observer,
    ),
}


def optional() -> list[Component]:
    """Non-locked components in stable display order."""
    return [c for c in REGISTRY.values() if not c.locked and not c.deprecated]


def default_components() -> dict[str, bool]:
    return {
        c.id: (True if c.locked else c.default_enabled)
        for c in REGISTRY.values()
        if not c.deprecated
    }


def selected_ids(entry_data: Mapping[str, Any]) -> list[str]:
    chosen = dict(entry_data.get(CONF_COMPONENTS, {}))
    return [
        c.id
        for c in REGISTRY.values()
        if not c.deprecated and (c.locked or chosen.get(c.id, False))
    ]
