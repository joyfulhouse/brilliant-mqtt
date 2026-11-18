"""Every panel action resolves HA targets to the right panel on both registry models.

The registry is the production one: a real HA 2026.6 disk (shared panel devices)
loaded by the harness's HA - which on 2026.10+ splits each shared device into a
twin owned by the MQTT entry and a twin owned by the Brilliant entry - and then
consolidated into one fleet entry, exactly as on the operator's instance.
"""

from __future__ import annotations

import copy
import json
from collections.abc import AsyncIterator, Callable
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from unittest.mock import patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import DOMAIN, panel_device_identifier
from custom_components.brilliant_mqtt.migration_coordinator import SINGLE_OWNER_DEVICES
from tests.fakes import FakeShell
from tests.registry_helpers import panel_device
from tests.test_migration_coordinator import _restart_from_disk
from tests.test_migration_coordinator import _run as _run_migration

_FIXTURE = Path(__file__).with_name("fixtures") / "legacy_fleet_ha_2026_6_disk.json"
_MANAGER = "custom_components.brilliant_mqtt.PanelManager"
_SCENES = "custom_components.brilliant_mqtt.scene_control.SceneControl"

# service -> (patched class, method, extra call data, needs exactly one panel)
SERVICES: dict[str, tuple[str, str, dict[str, object], bool]] = {
    "repair": (_MANAGER, "async_repair", {}, False),
    "redeploy": (_MANAGER, "async_update_agent", {}, False),
    "uninstall": (_MANAGER, "async_uninstall", {}, False),
    "reboot": (_MANAGER, "async_reboot", {"collect_diagnostics": False}, False),
    "canary_rollback": (_MANAGER, "async_canary_operation", {"name": "baseline-1"}, True),
    "canary_finalize": (_MANAGER, "async_canary_operation", {"name": "baseline-1"}, True),
    "run_scene": (_SCENES, "async_run_scene", {"scene_id": "all_off"}, True),
    "set_mode": (_SCENES, "async_set_mode", {"mode_id": "away"}, True),
}


@dataclass(frozen=True)
class Fleet:
    hass: HomeAssistant
    fleet_id: str
    mqtt_id: str
    pre_split_ids: dict[str, str]

    def management_twin(self, slug: str) -> dr.DeviceEntry:
        return panel_device(self.hass, slug, self.fleet_id)

    def mqtt_twin(self, slug: str) -> dr.DeviceEntry:
        return panel_device(self.hass, slug, self.mqtt_id)

    def area_on(self, device: dr.DeviceEntry) -> dict[str, object]:
        area = ar.async_get(self.hass).async_get_or_create(f"Area {device.id}")
        dr.async_get(self.hass).async_update_device(device.id, area_id=area.id)
        return {"area_id": area.id}

    def foreign_device(self, slug: str) -> dr.DeviceEntry:
        """Another integration's device reusing the panel's exact identifier."""
        foreign = MockConfigEntry(domain="demo", title="Foreign")
        foreign.add_to_hass(self.hass)
        return dr.async_get(self.hass).async_get_or_create(
            config_entry_id=foreign.entry_id,
            identifiers={panel_device_identifier(slug)},
            name="Foreign",
        )


# target kind -> builds the call's HA target (expected panels: see _expected)
TARGETS: dict[str, Callable[[Fleet], dict[str, object]]] = {
    "management-entity": lambda f: {"entity_id": "binary_sensor.brilliant_office_bridge_health"},
    "mqtt-entity": lambda f: {"entity_id": "light.brilliant_office"},
    "management-twin": lambda f: {"device_id": f.management_twin("office").id},
    "mqtt-twin": lambda f: {"device_id": f.mqtt_twin("office").id},
    "both-twins": lambda f: {
        "device_id": [f.mqtt_twin("office").id, f.management_twin("office").id]
    },
    "area-on-management-twin": lambda f: f.area_on(f.management_twin("office")),
    "area-on-mqtt-twin": lambda f: f.area_on(f.mqtt_twin("office")),
    "pre-split-id": lambda f: {"device_id": f.pre_split_ids["office"]},
    "foreign-identical-identifier": lambda f: {"device_id": f.foreign_device("office").id},
    "foreign-identical-identifier-area": lambda f: f.area_on(f.foreign_device("office")),
    "two-panels": lambda f: {
        "device_id": [f.management_twin("office").id, f.management_twin("kitchen").id]
    },
}


def _expected(target_kind: str) -> list[str]:
    if target_kind == "two-panels":
        return ["kitchen", "office"]
    if target_kind.startswith("foreign-"):
        # Pre-2026.10 identifiers are global: the foreign registration merges into
        # the panel's own (Brilliant-owned) device. On 2026.10+ it is a separate
        # device that no Brilliant or MQTT entry owns, so it selects nothing.
        return [] if SINGLE_OWNER_DEVICES else ["office"]
    return ["office"]


@pytest.fixture
async def production_fleet(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
    hass_storage: dict[str, Any],
) -> AsyncIterator[Fleet]:
    del mqtt_mock, fake_shell, payload_dir
    mqtt_id = hass.config_entries.async_entries("mqtt")[0].entry_id
    raw = await hass.async_add_executor_job(_FIXTURE.read_text)
    disk = {**copy.deepcopy(hass_storage), **json.loads(raw.replace("MQTT_ENTRY_ID", mqtt_id))}
    pre_split_ids = {
        device["identifiers"][0][1].removeprefix("brilliant_panel_"): device["id"]
        for device in disk["core.device_registry"]["data"]["devices"]
        if len(device["config_entries"]) > 1
    }
    await _restart_from_disk(hass, hass_storage, disk)
    assert (await _run_migration(hass, dry_run=False))["status"] == "migrated"
    await hass.async_block_till_done()
    (fleet,) = hass.config_entries.async_entries(DOMAIN)
    yield Fleet(hass, fleet.entry_id, mqtt_id, pre_split_ids)
    for entry in hass.config_entries.async_entries(DOMAIN):
        assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.parametrize("target_kind", list(TARGETS))
@pytest.mark.parametrize("service", list(SERVICES))
@pytest.mark.allow_lingering_timers
async def test_service_target_matrix(
    production_fleet: Fleet,
    caplog: pytest.LogCaptureFixture,
    service: str,
    target_kind: str,
) -> None:
    hass = production_fleet.hass
    owner, method, data, exactly_one = SERVICES[service]
    target = TARGETS[target_kind](production_fleet)
    expected = _expected(target_kind)
    caplog.clear()

    with patch(f"{owner}.{method}", autospec=True) as operation:
        call = hass.services.async_call(DOMAIN, service, {**target, **data}, blocking=True)
        if exactly_one and len(expected) != 1:
            with pytest.raises(HomeAssistantError, match="exactly one"):
                await call
        else:
            await call

    if owner == _SCENES:
        panels = [awaited.args[1] for awaited in operation.await_args_list]
    else:
        panels = [awaited.args[0].panel for awaited in operation.await_args_list]
    assert sorted(panels) == ([] if exactly_one and len(expected) != 1 else expected)
    assert "deprecated" not in caplog.text


@pytest.mark.parametrize("service", ["run_scene", "set_mode"])
@pytest.mark.allow_lingering_timers
async def test_scene_services_reconcile_panel_slug_with_target(
    production_fleet: Fleet, service: str
) -> None:
    """A slug that agrees with the target is accepted; one that disagrees is rejected."""
    hass = production_fleet.hass
    owner, method, data, _ = SERVICES[service]
    device_id = production_fleet.mqtt_twin("office").id

    with patch(f"{owner}.{method}", autospec=True) as operation:
        await hass.services.async_call(
            DOMAIN, service, {"device_id": device_id, "panel": "office", **data}, blocking=True
        )
        with pytest.raises(HomeAssistantError, match="does not match"):
            await hass.services.async_call(
                DOMAIN,
                service,
                {"device_id": device_id, "panel": "kitchen", **data},
                blocking=True,
            )
        # Without a target the slug (or the configured default) still applies.
        await hass.services.async_call(DOMAIN, service, {"panel": "kitchen", **data}, blocking=True)
        await hass.services.async_call(DOMAIN, service, dict(data), blocking=True)
        # An explicit target that resolves nothing never falls back to the default.
        with pytest.raises(HomeAssistantError, match="exactly one"):
            await hass.services.async_call(
                DOMAIN, service, {"entity_id": "none", **data}, blocking=True
            )

    assert [awaited.args[1] for awaited in operation.await_args_list] == ["office", "kitchen", None]


@pytest.mark.skipif(not SINGLE_OWNER_DEVICES, reason="single-owner devices are HA 2026.10+")
@pytest.mark.allow_lingering_timers
async def test_mqtt_device_naming_several_panels_fails_closed(production_fleet: Fleet) -> None:
    """An MQTT device carrying two panels' identifiers is ambiguous, never a fan-out."""
    hass = production_fleet.hass
    registry = dr.async_get(hass)
    registry.async_remove_device(production_fleet.mqtt_twin("kitchen").id)
    device = production_fleet.mqtt_twin("office")
    registry.async_update_device(
        device.id, new_identifiers=device.identifiers | {panel_device_identifier("kitchen")}
    )

    with (
        patch(f"{_MANAGER}.async_repair", autospec=True) as repair,
        pytest.raises(HomeAssistantError, match="ambiguous"),
    ):
        await hass.services.async_call(DOMAIN, "repair", {"device_id": device.id}, blocking=True)
    repair.assert_not_awaited()
