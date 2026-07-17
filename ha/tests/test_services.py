"""Domain services resolve their target device to the right PanelManager."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
import voluptuous as vol
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import (
    CONF_HA_CONTROL_ENABLED,
    CONF_PANEL,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    DOMAIN,
)
from custom_components.brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    mode_result_topic,
)
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


async def _setup_control_entry(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office-control-services",
        data={
            **ENTRY_DATA,
            CONF_HA_CONTROL_ENABLED: True,
            CONF_SCENE_PANEL: "office",
            CONF_SCENE_ACTIONS: {},
        },
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


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


@pytest.mark.allow_lingering_timers
async def test_scene_services_validate_required_ids_and_attached_panels(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup_control_entry(hass)
    assert hass.services.has_service(DOMAIN, "run_scene")
    assert hass.services.has_service(DOMAIN, "set_mode")

    with pytest.raises(vol.Invalid):
        await hass.services.async_call(DOMAIN, "run_scene", {}, blocking=True)
    with pytest.raises(HomeAssistantError, match="panel"):
        await hass.services.async_call(
            DOMAIN,
            "run_scene",
            {"panel": "garage", "scene_id": "all_off"},
            blocking=True,
        )

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_set_mode_service_uses_selected_panel_and_waits_for_confirmation(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup_control_entry(hass)
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/mode/catalog/office",
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "panel": "office",
                "generated_at_ms": 100,
                "modes": [{"mode_id": "away", "display_name": "Away"}],
            }
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/mode/office",
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "transport": "mode",
                "panel": "office",
                "available": True,
                "reason": None,
                "timestamp_ms": 101,
            }
        ),
        retain=True,
    )
    await hass.async_block_till_done()
    mqtt_mock.async_publish.reset_mock()

    call_task = hass.async_create_task(
        hass.services.async_call(
            DOMAIN,
            "set_mode",
            {"mode_id": "away"},
            blocking=True,
        )
    )
    await asyncio.sleep(0)
    command_calls = [
        call
        for call in mqtt_mock.async_publish.call_args_list
        if call.args[0].startswith("brilliant/ha-control/v1/mode/command/")
    ]
    assert len(command_calls) == 1
    command = json.loads(command_calls[0].args[1])
    assert command["panel"] == "office"
    assert command["mode_id"] == "away"
    assert command_calls[0].args[3] is False
    async_fire_mqtt_message(
        hass,
        mode_result_topic(command["command_id"]),
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "command_id": command["command_id"],
                "panel": "office",
                "mode_id": "away",
                "accepted": True,
                "timestamp_ms": command["issued_at_ms"] + 1,
            }
        ),
    )
    await call_task

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_reboot_service_reaches_manager_with_options(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    """The reboot service resolves its target to the panel manager and forwards the
    diagnostics options (the operator's overnight automation targets one panel each)."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    assert hass.services.has_service(DOMAIN, "reboot")

    device = dr.async_get(hass).async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    assert device is not None
    with patch(
        "custom_components.brilliant_mqtt.PanelManager.async_reboot", autospec=True
    ) as reboot:
        await hass.services.async_call(
            DOMAIN,
            "reboot",
            {"device_id": device.id, "collect_diagnostics": False, "journal_lines": 250},
            blocking=True,
        )
    reboot.assert_awaited_once()
    assert reboot.await_args is not None
    assert reboot.await_args.kwargs == {"collect_diagnostics": False, "journal_lines": 250}

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_reboot_service_rejects_out_of_range_journal_lines(
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
    with pytest.raises(vol.Invalid):
        await hass.services.async_call(
            DOMAIN,
            "reboot",
            {"device_id": device.id, "journal_lines": 5000},
            blocking=True,
        )

    assert await hass.config_entries.async_unload(entry.entry_id)


def test_reboot_service_and_button_descriptions_and_translations_are_complete() -> None:
    root = Path(__file__).parents[2]
    services = (root / "custom_components/brilliant_mqtt/services.yaml").read_text()
    strings = json.loads((root / "custom_components/brilliant_mqtt/strings.json").read_text())
    translations = json.loads(
        (root / "custom_components/brilliant_mqtt/translations/en.json").read_text()
    )

    assert "reboot:" in services
    for field in ("collect_diagnostics", "journal_lines"):
        assert f"{field}:" in services
    for document in (strings, translations):
        assert document["services"]["reboot"]["name"]
        assert document["services"]["reboot"]["description"]
        for field in ("collect_diagnostics", "journal_lines"):
            assert document["services"]["reboot"]["fields"][field]["description"]
        assert document["entity"]["button"]["reboot_panel"]["name"]
        assert document["exceptions"]["reboot_failed"]["message"]


def test_scene_service_descriptions_and_translations_are_complete() -> None:
    root = Path(__file__).parents[2]
    services = (root / "custom_components/brilliant_mqtt/services.yaml").read_text()
    strings = json.loads((root / "custom_components/brilliant_mqtt/strings.json").read_text())
    translations = json.loads(
        (root / "custom_components/brilliant_mqtt/translations/en.json").read_text()
    )

    for service, field in (("run_scene", "scene_id"), ("set_mode", "mode_id")):
        assert f"{service}:" in services
        assert "panel:" in services
        assert f"{field}:" in services
        for document in (strings, translations):
            assert document["services"][service]["name"]
            assert document["services"][service]["fields"]["panel"]["description"]
            assert document["services"][service]["fields"][field]["description"]

    for document in (strings, translations):
        assert document["entity"]["select"]["scene"]["name"]
        assert document["entity"]["button"]["run_selected_scene"]["name"]
