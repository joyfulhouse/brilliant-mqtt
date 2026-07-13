"""The integration is discoverable and its manifest is coherent."""

from __future__ import annotations

import asyncio
import json
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.components import mqtt
from homeassistant.config_entries import ConfigEntryState
from homeassistant.core import HomeAssistant
from homeassistant.loader import async_get_integration
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt import (
    async_migrate_entry,
    async_setup_entry,
    async_unload_entry,
)
from custom_components.brilliant_mqtt.const import (
    COMPONENT_BRIDGE,
    COMPONENT_HA_MIRROR,
    COMPONENT_VOICE,
    CONF_COMPONENTS,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_USERNAME,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_VOICE_ENABLED,
    CONFIG_ENTRY_VERSION,
    DATA_SSH_HOST_KEY,
    DOMAIN,
    availability_topic,
    meta_topic,
)
from custom_components.brilliant_mqtt.ha_control import get_control_plane
from custom_components.brilliant_mqtt.ha_control_protocol import (
    manifest_topic,
    stable_id,
    state_topic,
)
from custom_components.brilliant_mqtt.manager import PanelManager


async def test_integration_discoverable(hass: HomeAssistant) -> None:
    """The HA loader resolves the integration and the manifest carries the contract."""
    integration = await async_get_integration(hass, DOMAIN)
    assert integration.domain == DOMAIN
    assert integration.integration_type == "device"
    assert "mqtt" in (integration.dependencies or [])
    assert any(r.startswith("asyncssh==") for r in integration.requirements or [])


ENTRY_DATA = {
    CONF_HOST: "192.168.1.10",
    CONF_ROOT_PASSWORD: "panelpass",
    CONF_PANEL: "office",
    CONF_MESH_PRIORITY: 1,
    CONF_MQTT_HOST: "192.168.1.250",
    CONF_MQTT_PORT: 1883,
    CONF_MQTT_USERNAME: "brilliant",
    CONF_MQTT_PASSWORD: "mqttpass",
    DATA_SSH_HOST_KEY: "ssh-ed25519 PINNED",
}


@pytest.mark.allow_lingering_timers
async def test_entry_sets_up_and_tracks_availability(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    from pytest_homeassistant_custom_component.common import async_fire_mqtt_message

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    manager = entry.runtime_data
    assert manager.availability is None

    async_fire_mqtt_message(hass, "brilliant/office/availability", "online")
    await hass.async_block_till_done()
    assert manager.availability == "online"

    async_fire_mqtt_message(
        hass,
        "brilliant/office/bridge",
        '{"agent_version": "0.2.0", "panel_firmware": "v26.05.20.2"}',
    )
    await hass.async_block_till_done()
    assert manager.meta == {"agent_version": "0.2.0", "panel_firmware": "v26.05.20.2"}

    assert await hass.config_entries.async_unload(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_non_object_meta_is_ignored(hass: HomeAssistant, mqtt_mock: MqttMockHAClient) -> None:
    """Valid JSON that isn't an object must not be stored (Task 9 entities do .get())."""
    from pytest_homeassistant_custom_component.common import async_fire_mqtt_message

    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)
    assert await hass.config_entries.async_setup(entry.entry_id)
    await hass.async_block_till_done()

    manager = entry.runtime_data
    assert manager.meta is None

    async_fire_mqtt_message(hass, "brilliant/office/bridge", "42")
    await hass.async_block_till_done()
    assert manager.meta is None  # non-object payload left meta unchanged

    assert await hass.config_entries.async_unload(entry.entry_id)


# HA parks the entry in SETUP_RETRY with its own internal retry timer (not ours).
@pytest.mark.allow_lingering_timers
async def test_setup_retries_when_mqtt_unavailable(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    """If the MQTT client isn't ready, setup must raise ConfigEntryNotReady.

    The config-entries machinery catches ConfigEntryNotReady and parks the entry in
    SETUP_RETRY, so assert on that terminal state (test-before-setup quality rule).
    """
    entry = MockConfigEntry(domain=DOMAIN, unique_id="office", data=ENTRY_DATA)
    entry.add_to_hass(hass)

    with patch(
        "homeassistant.components.mqtt.async_wait_for_mqtt_client",
        return_value=False,
    ):
        assert not await hass.config_entries.async_setup(entry.entry_id)
        await hass.async_block_till_done()

    assert entry.state is ConfigEntryState.SETUP_RETRY


@pytest.mark.allow_lingering_timers
async def test_two_entries_share_control_plane_through_setup_and_unload(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    """Entry lifecycle publishes and tears down one singleton control plane."""
    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers import label_registry as lr

    label = lr.async_get(hass).async_create("brilliant")
    entity = er.async_get(hass).async_get_or_create("switch", "test", "desk", original_name="Desk")
    er.async_get(hass).async_update_entity(entity.entity_id, labels={label.label_id})
    hass.states.async_set(entity.entity_id, "off")
    control_data = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "brilliant",
        CONF_HA_CONTROL_DOMAINS: ("light", "switch"),
        CONF_MAX_MIRRORED_ENTITIES: 50,
        CONF_ROOM_OVERRIDES: {},
    }
    zulu = MockConfigEntry(
        domain=DOMAIN,
        unique_id="zulu",
        data={**ENTRY_DATA, **control_data, CONF_PANEL: "zulu"},
    )
    alpha = MockConfigEntry(
        domain=DOMAIN,
        unique_id="alpha",
        data={**ENTRY_DATA, **control_data, CONF_PANEL: "alpha"},
    )
    zulu.add_to_hass(hass)
    alpha.add_to_hass(hass)

    assert await hass.config_entries.async_setup(zulu.entry_id)
    await hass.async_block_till_done()
    assert zulu.state is ConfigEntryState.LOADED
    assert alpha.state is ConfigEntryState.LOADED

    plane = get_control_plane(hass)
    control_subscriptions = [
        call
        for call in mqtt_mock.async_subscribe.call_args_list
        if call.args[0] == "brilliant/ha-control/v1/command/+"
    ]
    assert len(control_subscriptions) == 1
    assert plane.owner_entry_id == alpha.entry_id
    assert (
        len(
            [
                call
                for call in mqtt_mock.async_publish.call_args_list
                if call.args[0] == manifest_topic() and call.args[3] is True
            ]
        )
        == 1
    )
    assert (
        len(
            [
                call
                for call in mqtt_mock.async_publish.call_args_list
                if call.args[0] == state_topic(stable_id(entity.entity_id)) and call.args[3] is True
            ]
        )
        == 1
    )

    assert await hass.config_entries.async_unload(alpha.entry_id)
    assert get_control_plane(hass) is plane
    assert plane.started is True
    assert mqtt_mock.is_active_subscription("brilliant/ha-control/v1/command/+")

    assert await hass.config_entries.async_unload(zulu.entry_id)
    assert plane.started is False
    assert not mqtt_mock.is_active_subscription("brilliant/ha-control/v1/command/+")


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(
    "detach_failure",
    [RuntimeError("alternate owner publish failed"), asyncio.CancelledError()],
    ids=["error", "cancelled"],
)
async def test_unload_always_shuts_manager_down_when_alternate_owner_reload_fails(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    detach_failure: BaseException,
) -> None:
    """Detach failures must not leave per-entry MQTT subscriptions or timers."""
    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers import label_registry as lr

    label = lr.async_get(hass).async_create("brilliant")
    entity = er.async_get(hass).async_get_or_create(
        "switch", "test", "cleanup", original_name="Cleanup"
    )
    er.async_get(hass).async_update_entity(entity.entity_id, labels={label.label_id})
    hass.states.async_set(entity.entity_id, "off")
    control_data = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_DOMAINS: ("light", "switch"),
        CONF_MAX_MIRRORED_ENTITIES: 50,
        CONF_ROOM_OVERRIDES: {},
    }
    zulu = MockConfigEntry(
        domain=DOMAIN,
        unique_id="zulu-cleanup",
        data={
            **ENTRY_DATA,
            **control_data,
            CONF_PANEL: "zulu",
            CONF_HA_CONTROL_LABEL: "unused",
        },
    )
    alpha = MockConfigEntry(
        domain=DOMAIN,
        unique_id="alpha-cleanup",
        data={
            **ENTRY_DATA,
            **control_data,
            CONF_PANEL: "alpha",
            CONF_HA_CONTROL_LABEL: "brilliant",
        },
    )
    zulu.add_to_hass(hass)
    alpha.add_to_hass(hass)
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    manager = PanelManager(hass, alpha, asyncio.Lock())
    alpha.runtime_data = manager
    await manager.async_setup()
    await plane.async_attach(alpha)
    async_fire_mqtt_message(hass, availability_topic("alpha"), "offline")
    await hass.async_block_till_done()
    assert manager._grace_cancel is not None

    with (
        patch.object(
            hass.config_entries, "async_unload_platforms", new=AsyncMock(return_value=True)
        ),
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=detach_failure,
        ),
        pytest.raises(type(detach_failure)),
    ):
        await async_unload_entry(hass, alpha)

    assert manager._grace_cancel is None
    assert not mqtt_mock.is_active_subscription(availability_topic("alpha"))
    assert not mqtt_mock.is_active_subscription(meta_topic("alpha"))
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
async def test_setup_failure_preserves_original_error_when_detach_cleanup_fails(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    """A cleanup publish failure must not replace the platform setup error."""
    from homeassistant.helpers import entity_registry as er
    from homeassistant.helpers import label_registry as lr

    label = lr.async_get(hass).async_create("brilliant")
    entity = er.async_get(hass).async_get_or_create(
        "switch", "test", "setup_cleanup", original_name="Setup Cleanup"
    )
    er.async_get(hass).async_update_entity(entity.entity_id, labels={label.label_id})
    hass.states.async_set(entity.entity_id, "off")
    zulu = MockConfigEntry(
        domain=DOMAIN,
        unique_id="zulu-setup-cleanup",
        data={
            **ENTRY_DATA,
            CONF_PANEL: "zulu",
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_CONTROL_LABEL: "unused",
        },
    )
    alpha = MockConfigEntry(
        domain=DOMAIN,
        unique_id="alpha-setup-cleanup",
        data={
            **ENTRY_DATA,
            CONF_PANEL: "alpha",
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_CONTROL_LABEL: "brilliant",
        },
    )
    zulu.add_to_hass(hass)
    alpha.add_to_hass(hass)
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    real_publish = mqtt.async_publish

    async def fail_empty_manifest(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if topic == manifest_topic() and json.loads(str(payload))["entities"] == []:
            raise RuntimeError("detach cleanup failed")
        await real_publish(
            hass,
            topic,
            payload,
            qos,
            retain,
            encoding,
            message_expiry_interval=message_expiry_interval,
        )

    with (
        patch.object(
            hass.config_entries,
            "async_forward_entry_setups",
            new=AsyncMock(side_effect=ValueError("platform setup failed")),
        ),
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=fail_empty_manifest,
        ),
        pytest.raises(ValueError, match="platform setup failed"),
    ):
        await async_setup_entry(hass, alpha)

    assert not mqtt_mock.is_active_subscription(availability_topic("alpha"))
    assert not mqtt_mock.is_active_subscription(meta_topic("alpha"))
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(
    "setup_failure",
    [RuntimeError("manager setup failed"), asyncio.CancelledError()],
    ids=["error", "cancelled"],
)
async def test_partial_manager_setup_failure_is_cleaned_up(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    setup_failure: BaseException,
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="partial-setup",
        data={**ENTRY_DATA, CONF_PANEL: "partial-setup"},
    )
    entry.add_to_hass(hass)
    real_subscribe = mqtt.async_subscribe
    subscriptions = 0

    async def fail_second_subscribe(*args: object, **kwargs: object) -> object:
        nonlocal subscriptions
        subscriptions += 1
        if subscriptions == 2:
            raise setup_failure
        return await real_subscribe(*args, **kwargs)  # type: ignore[arg-type]

    with (
        patch(
            "custom_components.brilliant_mqtt.manager.mqtt.async_subscribe",
            side_effect=fail_second_subscribe,
        ),
        pytest.raises(type(setup_failure)),
    ):
        await async_setup_entry(hass, entry)

    assert not mqtt_mock.is_active_subscription(availability_topic("partial-setup"))


@pytest.mark.allow_lingering_timers
async def test_external_unload_cancellation_is_drained_before_manager_shutdown(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="cancel-cleanup",
        data={**ENTRY_DATA, CONF_PANEL: "cancel-cleanup"},
    )
    entry.add_to_hass(hass)
    manager = PanelManager(hass, entry, asyncio.Lock())
    entry.runtime_data = manager
    await manager.async_setup()
    plane = get_control_plane(hass)
    entered = asyncio.Event()
    release = asyncio.Event()

    async def slow_detach(_entry_id: str) -> None:
        entered.set()
        await release.wait()

    with (
        patch.object(
            hass.config_entries, "async_unload_platforms", new=AsyncMock(return_value=True)
        ),
        patch.object(plane, "async_detach", side_effect=slow_detach),
    ):
        unload = hass.async_create_task(async_unload_entry(hass, entry))
        await entered.wait()
        unload.cancel()
        release.set()
        with pytest.raises(asyncio.CancelledError):
            await unload

    assert not mqtt_mock.is_active_subscription(availability_topic("cancel-cleanup"))
    assert not mqtt_mock.is_active_subscription(meta_topic("cancel-cleanup"))


async def test_migrate_v1_folds_voice_enabled_into_components(hass: HomeAssistant) -> None:
    """v1 voice selection survives migration to the current safe component map."""
    entry = MockConfigEntry(
        domain=DOMAIN,
        version=1,
        data={"panel": "office", CONF_VOICE_ENABLED: True},
    )
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.version == CONFIG_ENTRY_VERSION
    assert entry.data[CONF_COMPONENTS][COMPONENT_BRIDGE] is True
    assert entry.data[CONF_COMPONENTS][COMPONENT_VOICE] is True
    assert entry.data[CONF_COMPONENTS][COMPONENT_HA_MIRROR] is False


async def test_migrate_v1_no_voice_defaults_components_off(hass: HomeAssistant) -> None:
    """v1 entry defaults optional components off, including the retired mirror."""
    entry = MockConfigEntry(domain=DOMAIN, version=1, data={"panel": "kitchen"})
    entry.add_to_hass(hass)
    assert await async_migrate_entry(hass, entry) is True
    assert entry.data[CONF_COMPONENTS] == {
        COMPONENT_BRIDGE: True,
        COMPONENT_VOICE: False,
        COMPONENT_HA_MIRROR: False,
    }
