"""Entities attach to the existing MQTT device and drive manager actions."""

from __future__ import annotations

import asyncio
import json
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant, State
from homeassistant.helpers import device_registry as dr
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_mqtt_message,
)
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import (
    COMPONENT_VOICE,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_ENABLED,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_VOICE_WAKE_WORD,
    DEFAULT_VOICE_WAKE_WORD,
    DOMAIN,
    VOICE_WAKE_WORDS,
)
from custom_components.brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    scene_result_topic,
)
from custom_components.brilliant_mqtt.manager import PanelManager
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA

HEALTH = "binary_sensor.brilliant_office_bridge_health"
REPAIR = "button.brilliant_office_repair_bridge"
UPDATE = "update.brilliant_office_bridge"
VOICE_SWITCH = "switch.brilliant_office_voice_satellite"
WAKE_WORD_SELECT = "select.brilliant_office_wake_word"
SCENE_SELECT = "select.brilliant_office_scene"
RUN_SCENE = "button.brilliant_office_run_selected_scene"


async def _setup(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    return entry


async def _setup_scene_control(hass: HomeAssistant) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office-scenes",
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


def _catalog(generated_at_ms: int, scenes: list[dict[str, object]]) -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": "office",
            "generated_at_ms": generated_at_ms,
            "scenes": scenes,
        }
    )


def _status(timestamp_ms: int, available: bool = True) -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "transport": "scene",
            "panel": "office",
            "available": available,
            "reason": None if available else "execution_unavailable",
            "timestamp_ms": timestamp_ms,
        }
    )


def _state(hass: HomeAssistant, entity_id: str) -> State:
    state = hass.states.get(entity_id)
    assert state is not None
    return state


@pytest.mark.allow_lingering_timers
async def test_entities_exist_and_reflect_manager_state(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    entry = await _setup(hass)
    health_state = hass.states.get(HEALTH)
    assert health_state is not None
    assert health_state.state == "off"
    assert hass.states.get(REPAIR) is not None

    async_fire_mqtt_message(
        hass, "brilliant/office/bridge", '{"agent_version": "0.1.0", "panel_firmware": "v1"}'
    )
    await hass.async_block_till_done()
    update_state = hass.states.get(UPDATE)
    assert update_state is not None
    assert update_state.attributes["installed_version"] == "0.1.0"
    assert update_state.attributes["latest_version"] == "0.2.0"  # payload fixture VERSION
    assert update_state.state == "on"  # update available

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_entities_attach_to_the_mqtt_discovery_device(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    # Simulate the device MQTT discovery already created for this panel.
    mqtt_entry = hass.config_entries.async_entries("mqtt")[0]
    registry = dr.async_get(hass)
    existing = registry.async_get_or_create(
        config_entry_id=mqtt_entry.entry_id,
        identifiers={("mqtt", "brilliant_panel_office")},
        name="Brilliant Office",
    )
    entry = await _setup(hass)
    merged = registry.async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    assert merged is not None
    assert merged.id == existing.id  # ONE device page: MQTT entities + management entities

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_entities_attach_when_discovery_arrives_after_entry(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """The riskier ordering: OUR entry sets up first (creating the device under the
    ("mqtt", ...) identifier), THEN the agent's MQTT discovery claims the same
    identifier. Both must land on ONE device — our management entities and the
    discovery device sharing it — not split into two cards.
    """
    entry = await _setup(hass)
    registry = dr.async_get(hass)
    ours = registry.async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    assert ours is not None  # our entity created the device under the mqtt identifier

    # Now MQTT discovery for the panel arrives and claims the same identifier.
    mqtt_entry = hass.config_entries.async_entries("mqtt")[0]
    discovered = registry.async_get_or_create(
        config_entry_id=mqtt_entry.entry_id,
        identifiers={("mqtt", "brilliant_panel_office")},
        name="Brilliant Office",
    )
    assert discovered.id == ours.id  # merged onto the same device, not a second one
    # And the device now carries BOTH config entries (ours + mqtt) on one page.
    assert mqtt_entry.entry_id in discovered.config_entries
    assert entry.entry_id in discovered.config_entries

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_repair_button_runs_manual_repair(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    entry = await _setup(hass)
    await hass.services.async_call("button", "press", {"entity_id": REPAIR}, blocking=True)
    await hass.async_block_till_done()
    assert "systemctl enable --now brilliant-mqtt" in fake_shell.commands

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_update_install_deploys_payload_and_restarts(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    entry = await _setup(hass)
    async_fire_mqtt_message(
        hass, "brilliant/office/bridge", '{"agent_version": "0.1.0", "panel_firmware": "v1"}'
    )
    await hass.async_block_till_done()
    await hass.services.async_call("update", "install", {"entity_id": UPDATE}, blocking=True)
    await hass.async_block_till_done()
    assert fake_shell.dir_uploads  # payload tree uploaded
    assert "systemctl restart brilliant-mqtt" in fake_shell.commands

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_voice_switch_exists_and_reflects_entry_data(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """Voice satellite switch reflects components dict from entry.data."""
    entry = await _setup(hass)
    manager = entry.runtime_data

    # Default: voice_enabled not set → switch is off.
    state = hass.states.get(VOICE_SWITCH)
    assert state is not None
    assert state.state == "off"

    # Update entry data to enable voice; fire manager notify to push state refresh.
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_COMPONENTS: {COMPONENT_VOICE: True}}
    )
    manager._notify()
    await hass.async_block_till_done()
    state = hass.states.get(VOICE_SWITCH)
    assert state is not None
    assert state.state == "on"

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_voice_switch_turn_on_calls_manager(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """Turning the switch on delegates to manager.async_set_voice_enabled(True)."""
    await _setup(hass)

    with patch.object(PanelManager, "async_set_voice_enabled", new_callable=AsyncMock) as mock_set:
        await hass.services.async_call(
            "switch", "turn_on", {"entity_id": VOICE_SWITCH}, blocking=True
        )
        await hass.async_block_till_done()
        mock_set.assert_awaited_once_with(True)


@pytest.mark.allow_lingering_timers
async def test_voice_switch_turn_off_calls_manager(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """Turning the switch off delegates to manager.async_set_voice_enabled(False)."""
    await _setup(hass)

    with patch.object(PanelManager, "async_set_voice_enabled", new_callable=AsyncMock) as mock_set:
        await hass.services.async_call(
            "switch", "turn_off", {"entity_id": VOICE_SWITCH}, blocking=True
        )
        await hass.async_block_till_done()
        mock_set.assert_awaited_once_with(False)


@pytest.mark.allow_lingering_timers
async def test_wake_word_select_exists_and_reflects_entry_data(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """Wake-word select reflects CONF_VOICE_WAKE_WORD from entry.data."""
    entry = await _setup(hass)
    manager = entry.runtime_data

    # Default: wake word not set → select falls back to DEFAULT_VOICE_WAKE_WORD.
    state = hass.states.get(WAKE_WORD_SELECT)
    assert state is not None
    assert state.state == DEFAULT_VOICE_WAKE_WORD

    # Verify options match VOICE_WAKE_WORDS.
    assert state.attributes.get("options") == list(VOICE_WAKE_WORDS)

    # Update entry data with a specific wake word; fire notify to push state refresh.
    non_default = next(w for w in VOICE_WAKE_WORDS if w != DEFAULT_VOICE_WAKE_WORD)
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_VOICE_WAKE_WORD: non_default}
    )
    manager._notify()
    await hass.async_block_till_done()
    state = hass.states.get(WAKE_WORD_SELECT)
    assert state is not None
    assert state.state == non_default

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_wake_word_select_option_calls_manager(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """Selecting an option delegates to manager.async_set_voice_wake_word(option)."""
    await _setup(hass)

    target_word = "hey_jarvis"
    with patch.object(
        PanelManager, "async_set_voice_wake_word", new_callable=AsyncMock
    ) as mock_set:
        await hass.services.async_call(
            "select",
            "select_option",
            {"entity_id": WAKE_WORD_SELECT, "option": target_word},
            blocking=True,
        )
        await hass.async_block_till_done()
        mock_set.assert_awaited_once_with(target_word)


@pytest.mark.allow_lingering_timers
async def test_voice_entities_attach_to_panel_device(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, fake_shell: FakeShell, payload_dir: Path
) -> None:
    """Voice switch and wake-word select attach to the panel's MQTT-discovery device."""
    mqtt_entry = hass.config_entries.async_entries("mqtt")[0]
    registry = dr.async_get(hass)
    existing = registry.async_get_or_create(
        config_entry_id=mqtt_entry.entry_id,
        identifiers={("mqtt", "brilliant_panel_office")},
        name="Brilliant Office",
    )
    await _setup(hass)

    # Both voice entities must be on the same merged device page.
    switch_state = hass.states.get(VOICE_SWITCH)
    select_state = hass.states.get(WAKE_WORD_SELECT)
    assert switch_state is not None
    assert select_state is not None

    merged = registry.async_get_device(identifiers={("mqtt", "brilliant_panel_office")})
    assert merged is not None
    assert merged.id == existing.id


@pytest.mark.allow_lingering_timers
async def test_scene_entities_map_display_names_to_stable_ids_and_confirm_button(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup_scene_control(hass)
    assert _state(hass, SCENE_SELECT).state == "unavailable"
    assert _state(hass, RUN_SCENE).state == "unavailable"

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _catalog(
            100,
            [
                {"scene_id": "all_off", "display_name": "All Lights Off", "icon": None},
                {"scene_id": "all_on", "display_name": "All Lights On", "icon": "light"},
            ],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _status(101),
        retain=True,
    )
    await hass.async_block_till_done()

    select_state = hass.states.get(SCENE_SELECT)
    assert select_state is not None
    assert select_state.attributes["options"] == ["All Lights Off", "All Lights On"]
    assert select_state.state == "unknown"
    assert _state(hass, RUN_SCENE).state == "unavailable"
    mqtt_mock.async_publish.reset_mock()

    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": SCENE_SELECT, "option": "All Lights On"},
        blocking=True,
    )
    await hass.async_block_till_done()
    assert _state(hass, SCENE_SELECT).state == "All Lights On"
    assert _state(hass, RUN_SCENE).state != "unavailable"
    assert not [
        call
        for call in mqtt_mock.async_publish.call_args_list
        if call.args[0].startswith("brilliant/ha-control/v1/scene/command/")
    ]

    press = hass.async_create_task(
        hass.services.async_call("button", "press", {"entity_id": RUN_SCENE}, blocking=True)
    )
    await asyncio.sleep(0)
    command_calls = [
        call
        for call in mqtt_mock.async_publish.call_args_list
        if call.args[0].startswith("brilliant/ha-control/v1/scene/command/")
    ]
    assert len(command_calls) == 1
    command = json.loads(command_calls[0].args[1])
    assert command["panel"] == "office"
    assert command["scene_id"] == "all_on"
    assert command_calls[0].args[3] is False
    async_fire_mqtt_message(
        hass,
        scene_result_topic(command["command_id"]),
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "command_id": command["command_id"],
                "panel": "office",
                "scene_id": "all_on",
                "accepted": True,
                "timestamp_ms": command["issued_at_ms"] + 1,
            }
        ),
    )
    await press

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _status(102, available=False),
    )
    await hass.async_block_till_done()
    assert _state(hass, SCENE_SELECT).state == "unavailable"
    assert _state(hass, RUN_SCENE).state == "unavailable"

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_scene_catalog_clear_makes_both_entities_unavailable(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    entry = await _setup_scene_control(hass)
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _catalog(
            100,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _status(101),
        retain=True,
    )
    await hass.async_block_till_done()
    await hass.services.async_call(
        "select",
        "select_option",
        {"entity_id": SCENE_SELECT, "option": "All Lights Off"},
        blocking=True,
    )

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _catalog(102, []),
    )
    await hass.async_block_till_done()
    assert _state(hass, SCENE_SELECT).state == "unavailable"
    assert _state(hass, RUN_SCENE).state == "unavailable"

    assert await hass.config_entries.async_unload(entry.entry_id)
