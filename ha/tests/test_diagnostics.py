"""Diagnostics never leak the root password or broker password."""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import (
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_TOKEN,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_ROOM_OVERRIDES,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    DOMAIN,
)
from custom_components.brilliant_mqtt.diagnostics import async_get_config_entry_diagnostics
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


@pytest.mark.allow_lingering_timers
async def test_diagnostics_redact_secrets(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    fake_shell: FakeShell,
    payload_dir: Path,
) -> None:
    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers import label_registry as lr

    mirror_token = "diagnostics-must-never-expose-this-token"
    action_secret = "diagnostics-must-never-expose-action-data"
    room_secret = "diagnostics-must-never-expose-room-mapping"
    label = lr.async_get(hass).async_create("controlled")
    entity = er.async_get(hass).async_get_or_create(
        "switch", "test", "diagnostic", original_name="Diagnostic"
    )
    er.async_get(hass).async_update_entity(entity.entity_id, labels={label.label_id})
    hass.states.async_set(entity.entity_id, "off")
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        version=3,
        data={
            **ENTRY_DATA,
            CONF_HA_MIRROR_TOKEN: mirror_token,
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_CONTROL_LABEL: "controlled",
            CONF_ROOM_OVERRIDES: {"Secret area": room_secret},
            CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
            CONF_MAX_MIRRORED_ENTITIES: 50,
            CONF_SCENE_PANEL: "office",
            CONF_SCENE_ACTIONS: {
                "office:private": {
                    "domain": "script",
                    "service": "turn_on",
                    "target": {"entity_id": ["script.secret_target"]},
                    "data": {"secret": action_secret},
                }
            },
        },
    )
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()
    # Successful lifecycle retirement removes old credentials while control is
    # enabled. Reinsert a synthetic token to exercise diagnostics redaction itself.
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_HA_MIRROR_TOKEN: mirror_token}
    )

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        '{"schema_version":1,"mapping_version":1,"panel":"office",'
        '"generated_at_ms":1,"scenes":[{"scene_id":"all_off",'
        '"display_name":"All Off","icon":null}]}',
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        '{"schema_version":1,"mapping_version":1,"transport":"scene",'
        '"panel":"office","available":true,"reason":null,"timestamp_ms":2}',
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        '{"schema_version":1,"mapping_version":1,"panel":"office",'
        '"scene_id":"all_off","executed_at_ms":3,'
        '"deduplication_key":"office:all_off:3"}',
    )
    await hass.async_block_till_done()

    diag = await async_get_config_entry_diagnostics(hass, entry)
    assert diag["entry"]["root_password"] == "**REDACTED**"
    assert diag["entry"]["mqtt_password"] == "**REDACTED**"
    assert diag["entry"][CONF_HA_MIRROR_TOKEN] == "**REDACTED**"
    assert mirror_token not in repr(diag)
    assert "**REDACTED**" in repr(diag)
    assert diag["entry"]["host"] == "192.168.1.10"  # non-secrets stay visible
    assert CONF_ROOM_OVERRIDES not in diag["entry"]
    assert CONF_SCENE_ACTIONS not in diag["entry"]
    assert room_secret not in repr(diag)
    assert action_secret not in repr(diag)
    assert "script.secret_target" not in repr(diag)

    control = diag["ha_control"]
    assert control["enabled"] is True
    assert control["label"] == "controlled"
    assert control["room_override_count"] == 1
    assert control["scene_action_count"] == 1
    assert control["domains"] == ["light", "switch"]
    assert control["maximum_entities"] == 50
    assert control["selected_entity_count"] == 1
    assert control["manifest_revision"] == 1
    assert control["manifest_entity_count"] == 1
    assert control["scene_panel"] == "office"
    assert control["scene_catalog_revision"] == 1
    assert control["scene_last_event_timestamp_ms"] == 3
    assert control["scene_status"] == "online"
    assert control["native_tiles"] == {"status": "blocked", "validated": False}

    assert await hass.config_entries.async_unload(entry.entry_id)


async def test_diagnostics_missing_control_runtime_values_are_none(
    hass: HomeAssistant,
) -> None:
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA, version=3)
    entry.add_to_hass(hass)
    # A minimal stand-in is enough: diagnostics must tolerate no singleton runtime data.
    entry.runtime_data = type(
        "Manager",
        (),
        {"availability": None, "meta": None, "problem": False, "problem_reason": None},
    )()
    diag = await async_get_config_entry_diagnostics(hass, entry)
    control = diag["ha_control"]
    assert control["manifest_revision"] is None
    assert control["manifest_entity_count"] is None
    assert control["scene_catalog_revision"] is None
    assert control["scene_last_event_timestamp_ms"] is None
    assert control["scene_status"] is None
