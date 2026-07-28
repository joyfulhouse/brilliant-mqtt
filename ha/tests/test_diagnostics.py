"""Diagnostics never export fleet, panel, or MQTT-carried secrets."""

from __future__ import annotations

import json
from pathlib import Path
from types import MappingProxyType

import pytest
from homeassistant.config_entries import ConfigSubentry
from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.broker import BrokerKind
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_TOKEN,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_NEXT_MESH_PRIORITY,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SCHEMA_VERSION,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    SUBENTRY_TYPE_PANEL,
    availability_topic,
    meta_topic,
)
from custom_components.brilliant_mqtt.diagnostics import async_get_config_entry_diagnostics
from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from tests.fakes import FakeShell
from tests.test_init import ENTRY_DATA


@pytest.mark.allow_lingering_timers
async def test_fleet_diagnostics_are_keyed_by_runtime_id_and_never_export_secrets(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A fleet diagnostic cannot serialize any persisted or MQTT-carried secret."""
    username = "diagnostics-fleet-username"
    mqtt_password = "diagnostics-fleet-password"
    ca_pem = (
        "-----BEGIN CERTIFICATE-----\ndiagnostics-ca-body-must-not-leak\n-----END CERTIFICATE-----"
    )
    environment = "MQTT_PASSWORD=diagnostics-env-body-must-not-leak\n"
    identities = {
        "office": (
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIKIykuTed7zNwJwn20eCelcKcHKJ9c/pGFfvulRWazuC",
            "SHA256:JfCon51dCgE/yWGkyroh3Ne+ONLMm6QmHMQnEoPSLx0",
        ),
        "kitchen": (
            "ssh-ed25519 AAAAC3NzaC1lZDI1NTE5AAAAIG/koBYdTnHujqIpcXlQkQqzGBoZJ6Y4rm22iGIdAu4B",
            "SHA256:8mIRtm2GlHfcML0pUZInHQk3nT+hlkTq4k2FGR/Y0KM",
        ),
    }

    def panel(slug: str, runtime_id: str) -> ConfigSubentry:
        public_key, fingerprint = identities[slug]
        return ConfigSubentry(
            data=MappingProxyType(
                {
                    CONF_IDENTITY_FINGERPRINT: fingerprint,
                    CONF_SSH_HOST_KEY: public_key,
                    CONF_HOST: f"{slug}.example.com",
                    CONF_SSH_USERNAME: "root",
                    CONF_ROOT_PASSWORD: f"diagnostics-{slug}-root-password",
                    CONF_NAME: slug.title(),
                    CONF_PANEL: slug,
                    CONF_MANAGEMENT_ID: fingerprint,
                    CONF_COMPONENTS: {
                        COMPONENT_BRIDGE: True,
                        COMPONENT_WIFI_WATCHDOG: True,
                        COMPONENT_BUS_WATCHDOG: True,
                    },
                    CONF_FEATURE_OVERRIDES: {"environment": environment},
                    CONF_MESH_PRIORITY: 1,
                }
            ),
            subentry_id=runtime_id,
            subentry_type=SUBENTRY_TYPE_PANEL,
            title=slug.title(),
            unique_id=fingerprint,
        )

    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data={
            CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
            CONF_BROKER_KIND: BrokerKind.EXISTING_BROKER.value,
            CONF_MQTT_HOST: "mqtt.example.com",
            CONF_MQTT_PORT: 8883,
            CONF_MQTT_USERNAME: username,
            CONF_MQTT_PASSWORD: mqtt_password,
            CONF_MQTT_TLS_ENABLED: True,
            CONF_MQTT_TLS_CA: ca_pem,
            CONF_NEXT_MESH_PRIORITY: 3,
            CONF_HA_CONTROL_ENABLED: False,
            CONF_HA_CONTROL_LABEL: "brilliant",
            CONF_ROOM_OVERRIDES: {},
            CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
            CONF_MAX_MIRRORED_ENTITIES: 50,
            CONF_SCENE_PANEL: "panel-office",
            CONF_SCENE_ACTIONS: {},
            CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
        },
        options={"environment": environment},
        subentries_data=[
            panel("office", "panel-office").as_dict(),
            panel("kitchen", "panel-kitchen").as_dict(),
        ],
    )
    runtime = FleetManager(hass, entry)
    entry.runtime_data = runtime
    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.fleet_manager.mqtt.is_connected",
        lambda _hass: False,
    )

    await runtime.async_setup()
    try:
        async_fire_mqtt_message(hass, availability_topic("office"), "online")
        async_fire_mqtt_message(
            hass,
            meta_topic("office"),
            json.dumps(
                {
                    "agent_version": "0.7.0",
                    "panel_firmware": "v26.07.28.1",
                    "environment": environment,
                }
            ),
        )
        await hass.async_block_till_done()
        runtime.panels["panel-kitchen"].mark_runtime_degraded("runtime setup failed (RuntimeError)")

        diag = await async_get_config_entry_diagnostics(hass, entry)
    finally:
        await runtime.async_shutdown()

    assert diag["broker_available"] is False
    assert diag["panels"] == {
        "panel-office": {
            "availability": "online",
            "meta": {
                "agent_version": "0.7.0",
                "panel_firmware": "v26.07.28.1",
            },
            "problem": False,
            "problem_reason": None,
        },
        "panel-kitchen": {
            "availability": None,
            "meta": None,
            "problem": True,
            "problem_reason": "runtime setup failed (RuntimeError)",
        },
    }
    diagnostic_text = repr(diag)
    for secret in (
        username,
        mqtt_password,
        ca_pem,
        "diagnostics-ca-body-must-not-leak",
        environment,
        "diagnostics-env-body-must-not-leak",
        "diagnostics-office-root-password",
        "diagnostics-kitchen-root-password",
    ):
        assert secret not in diagnostic_text


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
    assert set(diag["panels"]) == {entry.entry_id}
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
    # A minimal one-panel FleetManager stand-in isolates the absent control-plane case.
    panel = type(
        "PanelManager",
        (),
        {"availability": None, "meta": None, "problem": False, "problem_reason": None},
    )()
    entry.runtime_data = type(
        "FleetManager",
        (),
        {"broker_available": None, "panels": {entry.entry_id: panel}},
    )()
    diag = await async_get_config_entry_diagnostics(hass, entry)
    assert diag["broker_available"] is None
    assert diag["panels"] == {
        entry.entry_id: {
            "availability": None,
            "meta": None,
            "problem": False,
            "problem_reason": None,
        }
    }
    control = diag["ha_control"]
    assert control["manifest_revision"] is None
    assert control["manifest_entity_count"] is None
    assert control["scene_catalog_revision"] is None
    assert control["scene_last_event_timestamp_ms"] is None
    assert control["scene_status"] is None
