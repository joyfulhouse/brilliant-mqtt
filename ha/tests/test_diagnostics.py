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
from custom_components.brilliant_mqtt.ha_control import get_control_plane
from custom_components.brilliant_mqtt.provisioning_journal import (
    ProvisioningJournal,
    ProvisioningJournalError,
)
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
    unknown_component = "diagnostics_unknown_component_must_not_leak"
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

    def panel(slug: str, runtime_id: str, mesh_priority: int) -> ConfigSubentry:
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
                        unknown_component: True,
                    },
                    CONF_FEATURE_OVERRIDES: {"environment": environment},
                    CONF_MESH_PRIORITY: mesh_priority,
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
            panel("office", "panel-office", 1).as_dict(),
            panel("kitchen", "panel-kitchen", 2).as_dict(),
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
    assert diag["fleet"] == {
        "panel_count": 2,
        "online_panel_count": 1,
        "offline_panel_count": 0,
        "unknown_panel_count": 1,
        "problem_panel_count": 1,
    }
    assert diag["provisioning"] == {"count": 0, "phase": None}
    assert diag["panels"] == {
        "panel-office": {
            "availability": "online",
            "configuration": {
                "address": "office.example.com",
                "enabled_components": [
                    COMPONENT_BRIDGE,
                    COMPONENT_BUS_WATCHDOG,
                    COMPONENT_WIFI_WATCHDOG,
                ],
                "identity_fingerprint": identities["office"][1],
                "mesh_priority": 1,
                "panel_slug": "office",
            },
            "meta": {
                "agent_version": "0.7.0",
                "panel_firmware": "v26.07.28.1",
            },
            "problem": False,
            "problem_reason": None,
        },
        "panel-kitchen": {
            "availability": None,
            "configuration": {
                "address": "kitchen.example.com",
                "enabled_components": [
                    COMPONENT_BRIDGE,
                    COMPONENT_BUS_WATCHDOG,
                    COMPONENT_WIFI_WATCHDOG,
                ],
                "identity_fingerprint": identities["kitchen"][1],
                "mesh_priority": 2,
                "panel_slug": "kitchen",
            },
            "meta": None,
            "problem": True,
            "problem_reason": "runtime_setup_failed",
        },
    }
    diagnostic_text = repr(diag)
    assert all(
        CONF_SSH_USERNAME not in panel_diagnostics["configuration"]
        for panel_diagnostics in diag["panels"].values()
    )
    for secret in (
        username,
        mqtt_password,
        ca_pem,
        "diagnostics-ca-body-must-not-leak",
        environment,
        "diagnostics-env-body-must-not-leak",
        "diagnostics-office-root-password",
        "diagnostics-kitchen-root-password",
        identities["office"][0],
        identities["kitchen"][0],
        unknown_component,
    ):
        assert secret not in diagnostic_text


@pytest.mark.allow_lingering_timers
async def test_fleet_diagnostics_resolve_scene_owner_id_to_panel_slug(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
) -> None:
    """Fleet scene diagnostics use the MQTT slug, not the config-subentry id."""
    fingerprint = "SHA256:diagnostics-office"
    panel = ConfigSubentry(
        data=MappingProxyType(
            {
                CONF_IDENTITY_FINGERPRINT: fingerprint,
                CONF_SSH_HOST_KEY: "ssh-ed25519 AAAA-diagnostics-office",
                CONF_HOST: "office.example.com",
                CONF_SSH_USERNAME: "root",
                CONF_ROOT_PASSWORD: "diagnostics-office-root-password",
                CONF_NAME: "Office",
                CONF_PANEL: "office",
                CONF_MANAGEMENT_ID: fingerprint,
                CONF_COMPONENTS: {COMPONENT_BRIDGE: True},
                CONF_FEATURE_OVERRIDES: {},
                CONF_MESH_PRIORITY: 1,
            }
        ),
        subentry_id="panel-office",
        subentry_type=SUBENTRY_TYPE_PANEL,
        title="Office",
        unique_id=fingerprint,
    )
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=CONFIG_ENTRY_VERSION,
        data={
            CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
            CONF_BROKER_KIND: BrokerKind.EXISTING_BROKER.value,
            CONF_MQTT_HOST: "mqtt.example.com",
            CONF_MQTT_PORT: 1883,
            CONF_MQTT_USERNAME: "brilliant",
            CONF_MQTT_PASSWORD: "diagnostics-fleet-password",
            CONF_MQTT_TLS_ENABLED: False,
            CONF_NEXT_MESH_PRIORITY: 2,
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_CONTROL_LABEL: "brilliant",
            CONF_ROOM_OVERRIDES: {},
            CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
            CONF_MAX_MIRRORED_ENTITIES: 50,
            CONF_SCENE_PANEL: panel.subentry_id,
            CONF_SCENE_ACTIONS: {},
            CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
        },
        subentries_data=[panel.as_dict()],
    )
    entry.add_to_hass(hass)
    runtime_panel = type(
        "PanelManager",
        (),
        {"availability": None, "meta": None, "problem": False, "problem_reason": None},
    )()
    entry.runtime_data = type(
        "FleetManager",
        (),
        {"broker_available": True, "panels": {panel.subentry_id: runtime_panel}},
    )()
    plane = get_control_plane(hass)

    await plane.async_attach(entry)
    try:
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

        control = (await async_get_config_entry_diagnostics(hass, entry))["ha_control"]
    finally:
        await plane.async_detach(entry.entry_id)

    assert control["scene_panel"] == "office"
    assert control["scene_catalog_revision"] == 1
    assert control["scene_last_event_timestamp_ms"] == 3
    assert control["scene_status"] == "online"


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
    assert diag["fleet"] == {
        "panel_count": 1,
        "online_panel_count": 0,
        "offline_panel_count": 0,
        "unknown_panel_count": 1,
        "problem_panel_count": 0,
    }
    assert diag["provisioning"] == {"count": 0, "phase": None}
    assert diag["panels"] == {
        entry.entry_id: {
            "availability": None,
            "configuration": None,
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


@pytest.mark.parametrize(
    ("reason_prefix", "expected_code"),
    [
        ("repair step failed", "repair_step_failed"),
        ("panel repair step failed", "repair_step_failed"),
        ("agent update could not connect", "agent_update_failed"),
        ("agent uninstall completed but close was unverified", "agent_uninstall_failed"),
    ],
)
async def test_diagnostics_never_export_raw_panel_problem_text(
    hass: HomeAssistant,
    reason_prefix: str,
    expected_code: str,
) -> None:
    """Remote command stderr cannot cross the diagnostics redaction boundary."""
    canary = "MQTT_PASSWORD=diagnostics-remote-stderr-secret"
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA, version=3)
    entry.add_to_hass(hass)
    panel = type(
        "PanelManager",
        (),
        {
            "availability": None,
            "meta": None,
            "problem": True,
            "problem_reason": f"{reason_prefix}: {canary}",
        },
    )()
    entry.runtime_data = type(
        "FleetManager",
        (),
        {"broker_available": True, "panels": {entry.entry_id: panel}},
    )()

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["panels"][entry.entry_id]["problem_reason"] == expected_code
    assert canary not in repr(diag)


async def test_provisioning_journal_failure_does_not_block_diagnostics(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A private journal read failure yields a fixed unavailable summary."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA, version=3)
    entry.add_to_hass(hass)
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

    async def fail_closed(_journal: ProvisioningJournal) -> dict[str, int | str | None]:
        raise ProvisioningJournalError("journal_load_failed")

    monkeypatch.setattr(ProvisioningJournal, "async_diagnostics", fail_closed)

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["provisioning"] == {"count": None, "phase": None}


async def test_unexpected_provisioning_diagnostics_failure_degrades_safely(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """Support-bundle generation survives coordinator and storage corruption."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA, version=3)
    entry.add_to_hass(hass)
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

    async def fail_unexpectedly(
        _journal: ProvisioningJournal,
    ) -> dict[str, int | str | None]:
        raise RuntimeError("corrupt coordinator")

    monkeypatch.setattr(ProvisioningJournal, "async_diagnostics", fail_unexpectedly)

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["provisioning"] == {"count": None, "phase": None}


async def test_diagnostics_use_one_panel_snapshot_across_journal_await(
    hass: HomeAssistant,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    """A concurrent reconciliation cannot produce mismatched fleet and panel views."""
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA, version=3)
    entry.add_to_hass(hass)
    initial = type(
        "PanelManager",
        (),
        {"availability": None, "meta": None, "problem": False, "problem_reason": None},
    )()
    newcomer = type(
        "PanelManager",
        (),
        {"availability": "online", "meta": None, "problem": False, "problem_reason": None},
    )()
    runtime = type(
        "FleetManager",
        (),
        {"broker_available": True, "panels": {"initial": initial}},
    )()
    entry.runtime_data = runtime

    async def reconcile_during_read(
        _journal: ProvisioningJournal,
    ) -> dict[str, int | str | None]:
        runtime.panels["newcomer"] = newcomer
        initial.availability = "online"
        return {"count": 0, "phase": None}

    monkeypatch.setattr(ProvisioningJournal, "async_diagnostics", reconcile_during_read)

    diag = await async_get_config_entry_diagnostics(hass, entry)

    assert diag["fleet"]["panel_count"] == 1
    assert set(diag["panels"]) == {"initial"}
    assert diag["fleet"]["unknown_panel_count"] == 1
    assert diag["panels"]["initial"]["availability"] is None
