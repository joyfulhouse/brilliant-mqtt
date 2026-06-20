"""Domain services resolve their target device to the right PanelManager."""

from __future__ import annotations

from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import CONF_PANEL, DOMAIN
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


@pytest.mark.allow_lingering_timers
async def test_repair_and_uninstall_services(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    device = dr.async_get(hass).async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    assert device is not None

    await hass.services.async_call(DOMAIN, "repair", {"device_id": device.id}, blocking=True)
    assert "systemctl enable --now brilliant-mqtt" in fake_shell.commands

    await hass.services.async_call(DOMAIN, "uninstall", {"device_id": device.id}, blocking=True)
    assert any(c.startswith("systemctl disable --now") for c in fake_shell.commands)
    assert (
        "rm -f /etc/systemd/system/brilliant-mqtt.service /etc/brilliant-mqtt.env"
        in fake_shell.commands
    )

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_repair_service_accepts_entity_target(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    """A UI service call with an ENTITY target must reach the manager.

    services.yaml targets these services by ``entity`` (required by hassfest), so HA
    merges ``entity_id`` — not ``device_id`` — into the call data. The schema must
    therefore accept an entity-only call; the handler resolves the config entry via
    ``async_extract_config_entry_ids`` (entity/device/area alike). Exercised through
    the real service registry so the schema is actually validated (the other tests
    pass ``device_id`` and so never hit the entity path the reviewer flagged).
    """
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    # The panel's health binary_sensor is registered against this config entry; use it
    # as the entity target a user would pick in the UI.
    registry = er.async_get(hass)
    entity_id = registry.async_get_entity_id(
        "binary_sensor", DOMAIN, f"{entry.entry_id}_bridge_health"
    )
    assert entity_id is not None

    with patch(
        "custom_components.brilliant_mqtt.PanelManager.async_repair",
        autospec=True,
    ) as repair:
        await hass.services.async_call(DOMAIN, "repair", {"entity_id": entity_id}, blocking=True)

    repair.assert_awaited_once()

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_uninstall_aggregates_multi_target_failures(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    """A multi-panel uninstall must attempt EVERY target and report all failures.

    Each manager's async_uninstall raises HomeAssistantError on failure, so a naive
    `for m in targets: await m.async_uninstall()` aborts on the FIRST failing panel
    and silently skips the rest (M8 "uninstall the fleet" stops halfway with no
    signal). Both panels fail here — regardless of the (set-derived) iteration order
    the second must still be attempted, and ONE aggregated error must name both.
    """
    office = MockConfigEntry(
        domain=DOMAIN, unique_id="office", data={**ENTRY_DATA, CONF_PANEL: "office"}
    )
    kitchen = MockConfigEntry(
        domain=DOMAIN, unique_id="kitchen", data={**ENTRY_DATA, CONF_PANEL: "kitchen"}
    )
    office.add_to_hass(hass)
    assert await hass.config_entries.async_setup(office.entry_id)
    await hass.async_block_till_done()
    kitchen.add_to_hass(hass)
    assert await hass.config_entries.async_setup(kitchen.entry_id)
    await hass.async_block_till_done()

    registry = dr.async_get(hass)
    office_device = registry.async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    kitchen_device = registry.async_get_device(identifiers={("mqtt", "brilliant_panel_kitchen")})
    assert office_device is not None and kitchen_device is not None

    # BOTH targets fail: a naive loop attempts only whichever the set yields first,
    # leaving the other's mock un-awaited — order-independently red against the bug.
    office.runtime_data.async_uninstall = AsyncMock(side_effect=HomeAssistantError("office boom"))
    kitchen.runtime_data.async_uninstall = AsyncMock(side_effect=HomeAssistantError("kitchen boom"))

    with pytest.raises(HomeAssistantError) as err:
        await hass.services.async_call(
            DOMAIN,
            "uninstall",
            {"device_id": [office_device.id, kitchen_device.id]},
            blocking=True,
        )

    # Every target attempted (no early abort) and ONE error names BOTH failed panels.
    office.runtime_data.async_uninstall.assert_awaited_once()
    kitchen.runtime_data.async_uninstall.assert_awaited_once()
    assert "office" in str(err.value) and "kitchen" in str(err.value)

    assert await hass.config_entries.async_unload(office.entry_id)
    assert await hass.config_entries.async_unload(kitchen.entry_id)
