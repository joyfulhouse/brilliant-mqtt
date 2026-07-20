"""Canonical metadata for optional components backed by one payload and one unit."""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping
from dataclasses import dataclass
from types import MappingProxyType
from typing import Protocol

from . import panel_ops
from .const import (
    COMPONENT_BLE_OBSERVER,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
)
from .shell import PanelShell


class PayloadState(Protocol):
    """Minimal state proof shared by the three single-unit payloads."""

    @property
    def payload_present(self) -> bool: ...


@dataclass(frozen=True)
class SingleUnitPayloadSpec:
    """Immutable install/repair metadata for one isolated systemd payload."""

    component_id: str
    label: str
    service_filename: str
    payload_subdir: str
    inspect: Callable[[PanelShell], Awaitable[PayloadState]]
    deploy: Callable[[PanelShell, str], Awaitable[None]]
    ensure_unit: Callable[[PanelShell, str], Awaitable[None]]
    enable: Callable[[PanelShell], Awaitable[None]]
    activate_updated: Callable[[PanelShell], Awaitable[None]] | None = None


SINGLE_UNIT_PAYLOAD_SPECS = (
    SingleUnitPayloadSpec(
        component_id=COMPONENT_WIFI_WATCHDOG,
        label="Wi-Fi watchdog",
        service_filename="brilliant-wifi-watchdog.service",
        payload_subdir="wifi_watchdog",
        inspect=panel_ops.inspect_wifi_watchdog,
        deploy=panel_ops.deploy_wifi_watchdog,
        ensure_unit=panel_ops.ensure_wifi_watchdog_unit,
        enable=panel_ops.enable_wifi_watchdog,
    ),
    SingleUnitPayloadSpec(
        component_id=COMPONENT_BUS_WATCHDOG,
        label="Bus watchdog",
        service_filename="brilliant-bus-watchdog.service",
        payload_subdir="bus_watchdog",
        inspect=panel_ops.inspect_bus_watchdog,
        deploy=panel_ops.deploy_bus_watchdog,
        ensure_unit=panel_ops.ensure_bus_watchdog_unit,
        enable=panel_ops.enable_bus_watchdog,
    ),
    SingleUnitPayloadSpec(
        component_id=COMPONENT_BLE_OBSERVER,
        label="BLE observer",
        service_filename="brilliant-ble-observer.service",
        payload_subdir="ble_observer",
        inspect=panel_ops.inspect_ble_observer,
        deploy=panel_ops.deploy_ble_observer,
        ensure_unit=panel_ops.ensure_ble_observer_unit,
        enable=panel_ops.enable_ble_observer,
        activate_updated=panel_ops.activate_updated_ble_observer,
    ),
)

SINGLE_UNIT_PAYLOAD_BY_ID: Mapping[str, SingleUnitPayloadSpec] = MappingProxyType(
    {spec.component_id: spec for spec in SINGLE_UNIT_PAYLOAD_SPECS}
)
