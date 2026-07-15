"""Tests for the singleton Home Assistant MQTT control plane."""

from __future__ import annotations

import asyncio
import json
import time
from collections.abc import Callable, Mapping
from datetime import timedelta
from typing import Any
from unittest.mock import patch
from uuid import UUID, uuid4

import pytest
from homeassistant.components import mqtt
from homeassistant.components.cover import CoverEntityFeature
from homeassistant.components.light import ATTR_SUPPORTED_COLOR_MODES
from homeassistant.const import ATTR_SUPPORTED_FEATURES
from homeassistant.core import HomeAssistant, ServiceCall
from homeassistant.helpers import area_registry as ar
from homeassistant.helpers import device_registry as dr
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers import label_registry as lr
from homeassistant.util import dt as dt_util
from pytest_homeassistant_custom_component.common import (
    MockConfigEntry,
    async_fire_mqtt_message,
    async_fire_time_changed,
)
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt.const import (
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_PANEL,
    CONF_ROOM_OVERRIDES,
    DOMAIN,
)
from custom_components.brilliant_mqtt.ha_control import get_control_plane
from custom_components.brilliant_mqtt.ha_control_manifest import build_manifest
from custom_components.brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    command_topic,
    encode_json,
    manifest_topic,
    result_topic,
    stable_id,
    state_topic,
)

COMMAND_CASES: tuple[tuple[str, object, str, str, dict[str, int]], ...] = (
    ("turn_on", None, "light", "turn_on", {}),
    ("set_brightness", 128, "light", "turn_on", {"brightness": 128}),
    ("turn_off", None, "switch", "turn_off", {}),
    ("lock", None, "lock", "lock", {}),
    ("unlock", None, "lock", "unlock", {}),
    ("open", None, "cover", "open_cover", {}),
    ("close", None, "cover", "close_cover", {}),
    ("set_position", 42, "cover", "set_cover_position", {"position": 42}),
    ("set_tilt", 25, "cover", "set_cover_tilt_position", {"tilt_position": 25}),
)

ALL_DOMAINS = ("light", "switch", "lock", "cover")


def _entry(
    hass: HomeAssistant,
    panel: str,
    *,
    enabled: bool = True,
    label: str = "brilliant",
    domains: tuple[str, ...] = ALL_DOMAINS,
    maximum: int = 50,
    overrides: Mapping[str, str] | None = None,
) -> MockConfigEntry:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id=panel,
        data={
            CONF_PANEL: panel,
            CONF_HA_CONTROL_ENABLED: enabled,
            CONF_HA_CONTROL_LABEL: label,
            CONF_HA_CONTROL_DOMAINS: domains,
            CONF_MAX_MIRRORED_ENTITIES: maximum,
            CONF_ROOM_OVERRIDES: dict(overrides or {}),
        },
    )
    entry.add_to_hass(hass)
    return entry


def _selected_entity(
    hass: HomeAssistant,
    entity_id: str,
    *,
    label: str = "brilliant",
    supported_features: int = 0,
    state: str = "off",
    attributes: Mapping[str, Any] | None = None,
) -> er.RegistryEntry:
    labels = lr.async_get(hass)
    label_entry = labels.async_get_label_by_name(label) or labels.async_create(label)
    domain, object_id = entity_id.split(".", maxsplit=1)
    registry = er.async_get(hass)
    entry = registry.async_get_or_create(
        domain,
        "test",
        object_id,
        original_name=object_id.replace("_", " ").title(),
        supported_features=supported_features,
    )
    entry = registry.async_update_entity(entry.entity_id, labels={label_entry.label_id})
    hass.states.async_set(entry.entity_id, state, dict(attributes or {}))
    return entry


def _entity_for_domain(hass: HomeAssistant, domain: str) -> er.RegistryEntry:
    attributes: dict[str, Any] = {}
    supported_features = 0
    if domain == "light":
        attributes[ATTR_SUPPORTED_COLOR_MODES] = ["brightness"]
    elif domain == "cover":
        supported_features = int(
            CoverEntityFeature.OPEN
            | CoverEntityFeature.CLOSE
            | CoverEntityFeature.SET_POSITION
            | CoverEntityFeature.SET_TILT_POSITION
        )
        attributes[ATTR_SUPPORTED_FEATURES] = supported_features
    return _selected_entity(
        hass,
        f"{domain}.controlled",
        supported_features=supported_features,
        attributes=attributes,
    )


def _published(mqtt_mock: MqttMockHAClient, topic: str) -> list[Any]:
    return [call for call in mqtt_mock.async_publish.call_args_list if call.args[0] == topic]


def _payload(call: Any) -> dict[str, Any]:
    value = json.loads(call.args[1])
    assert isinstance(value, dict)
    return value


def _command_payload(
    entity_id: str,
    kind: str,
    value: object,
    *,
    command_id: str | None = None,
    issued_at_ms: int | None = None,
    payload_stable_id: str | None = None,
    observed_sequence: int = 1,
) -> tuple[str, str]:
    command_id = command_id or str(uuid4())
    return command_id, encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": command_id,
            "stable_id": payload_stable_id or stable_id(entity_id),
            "kind": kind,
            "value": value,
            "observed_sequence": observed_sequence,
            "issued_at_ms": issued_at_ms or time.time_ns() // 1_000_000,
        }
    )


def _current_sequence(mqtt_mock: MqttMockHAClient, entity_id: str) -> int:
    calls = _published(mqtt_mock, state_topic(stable_id(entity_id)))
    assert calls, f"no state has been published yet for {entity_id}"
    sequence = _payload(calls[-1])["sequence"]
    assert isinstance(sequence, int)
    return sequence


@pytest.mark.allow_lingering_timers
async def test_disabled_entries_keep_a_dormant_singleton(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    plane = get_control_plane(hass)
    disabled = _entry(hass, "office", enabled=False)

    await plane.async_attach(disabled)

    assert get_control_plane(hass) is plane
    assert plane.started is False
    assert not [
        call
        for call in mqtt_mock.async_subscribe.call_args_list
        if call.args[0] == "brilliant/ha-control/v1/command/+"
    ]
    assert _published(mqtt_mock, manifest_topic()) == []

    await plane.async_detach(disabled.entry_id)
    assert plane.started is False


@pytest.mark.allow_lingering_timers
async def test_default_settings_use_legacy_label_and_fail_closed_domains(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    selected_switch = _selected_entity(hass, "switch.legacy", label="legacy")
    _selected_entity(hass, "lock.legacy", label="legacy")
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="office",
        data={
            CONF_PANEL: "office",
            CONF_HA_CONTROL_ENABLED: True,
            CONF_HA_MIRROR_LABEL: "legacy",
        },
    )
    entry.add_to_hass(hass)
    plane = get_control_plane(hass)

    await plane.async_attach(entry)

    manifest = _payload(_published(mqtt_mock, manifest_topic())[0])
    assert [item["entity_id"] for item in manifest["entities"]] == [selected_switch.entity_id]
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_cancelled_initial_publish_rolls_back_all_listeners(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    _selected_entity(hass, "switch.office")
    entry = _entry(hass, "office")
    plane = get_control_plane(hass)

    with (
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=asyncio.CancelledError,
        ),
        pytest.raises(asyncio.CancelledError),
    ):
        await plane.async_attach(entry)

    assert plane.started is False
    assert not mqtt_mock.is_active_subscription("brilliant/ha-control/v1/command/+")
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_failed_manifest_publish_remains_retryable(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    _selected_entity(hass, "switch.office")
    zulu = _entry(hass, "zulu", label="unused")
    alpha = _entry(hass, "alpha", label="brilliant")
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)

    with (
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=RuntimeError("broker unavailable"),
        ),
        pytest.raises(RuntimeError, match="broker unavailable"),
    ):
        await plane.async_attach(alpha)
    mqtt_mock.async_publish.reset_mock()

    await plane.async_reload_settings()

    manifest_calls = _published(mqtt_mock, manifest_topic())
    assert len(manifest_calls) == 1
    assert len(_payload(manifest_calls[0])["entities"]) == 1
    await plane.async_detach(alpha.entry_id)
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
async def test_smallest_enabled_panel_owns_settings_without_restarting(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    selected = _selected_entity(hass, "switch.office")
    plane = get_control_plane(hass)
    zulu = _entry(hass, "zulu", label="unused")
    alpha = _entry(hass, "alpha", label="brilliant")

    await plane.async_attach(zulu)
    await plane.async_attach(alpha)

    subscriptions = [
        call
        for call in mqtt_mock.async_subscribe.call_args_list
        if call.args[0] == "brilliant/ha-control/v1/command/+"
    ]
    assert len(subscriptions) == 1
    assert plane.owner_entry_id == alpha.entry_id
    manifests = _published(mqtt_mock, manifest_topic())
    assert [_payload(call)["revision"] for call in manifests] == [1, 2]
    assert (
        _payload(_published(mqtt_mock, manifest_topic())[-1])["entities"][0]["entity_id"]
        == selected.entity_id
    )

    await plane.async_detach(alpha.entry_id)
    assert plane.started is True
    assert plane.owner_entry_id == zulu.entry_id
    assert mqtt_mock.is_active_subscription("brilliant/ha-control/v1/command/+")

    await plane.async_detach(zulu.entry_id)
    assert plane.started is False
    assert not mqtt_mock.is_active_subscription("brilliant/ha-control/v1/command/+")


@pytest.mark.allow_lingering_timers
async def test_successful_alternate_owner_reload_reopens_command_acceptance(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.office")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    zulu = _entry(hass, "zulu")
    alpha = _entry(hass, "alpha")
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    await plane.async_attach(alpha)

    await plane.async_detach(alpha.entry_id)
    command_id, payload = _command_payload(entity.entity_id, "turn_on", None)
    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert len(calls) == 1
    assert _payload(_published(mqtt_mock, result_topic(command_id))[-1])["accepted"] is True
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
async def test_registry_events_coalesce_into_one_real_bus_rebuild(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    _selected_entity(hass, "switch.office")
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()

    with patch(
        "custom_components.brilliant_mqtt.ha_control.build_manifest", wraps=build_manifest
    ) as builder:
        for event_type in (
            er.EVENT_ENTITY_REGISTRY_UPDATED,
            dr.EVENT_DEVICE_REGISTRY_UPDATED,
            ar.EVENT_AREA_REGISTRY_UPDATED,
            lr.EVENT_LABEL_REGISTRY_UPDATED,
        ):
            hass.bus.async_fire(str(event_type))
        await hass.async_block_till_done()

        assert builder.call_count == 0
        async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=1))
        await hass.async_block_till_done()

    assert builder.call_count == 1
    assert _published(mqtt_mock, manifest_topic()) == []
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_state_changes_publish_immediately_with_monotonic_sequence(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.office")
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()

    hass.states.async_set(entity.entity_id, "on")
    await hass.async_block_till_done()

    calls = _published(mqtt_mock, state_topic(stable_id(entity.entity_id)))
    assert len(calls) == 1
    assert calls[0].args[3] is True
    assert _payload(calls[0])["sequence"] == 2
    assert _published(mqtt_mock, manifest_topic()) == []
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_failed_state_publish_does_not_consume_sequence(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    current_sequence = _current_sequence(mqtt_mock, entity.entity_id)
    manifest_entity = plane._manifest_entity(stable_id(entity.entity_id))
    assert manifest_entity is not None
    mqtt_mock.async_publish.reset_mock()

    with (
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=RuntimeError("state publish failed"),
        ),
        pytest.raises(RuntimeError, match="state publish failed"),
    ):
        await plane._async_publish_state(manifest_entity)

    await plane._async_publish_state(manifest_entity)

    calls = _published(mqtt_mock, state_topic(stable_id(entity.entity_id)))
    assert len(calls) == 1
    assert _payload(calls[0])["sequence"] == current_sequence + 1
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_command_result_carries_sequence_published_by_command_state_change(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")

    async def turn_on(_call: ServiceCall) -> None:
        hass.states.async_set(entity.entity_id, "on")

    hass.services.async_register("switch", "turn_on", turn_on)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    current_sequence = _current_sequence(mqtt_mock, entity.entity_id)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(
        entity.entity_id,
        "turn_on",
        None,
        observed_sequence=current_sequence,
    )

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    state = _payload(_published(mqtt_mock, state_topic(stable_id(entity.entity_id)))[-1])
    result = _payload(_published(mqtt_mock, result_topic(command_id))[-1])
    assert state["sequence"] == current_sequence + 1
    assert result["accepted"] is True
    assert result["resulting_sequence"] == state["sequence"]
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_service_waiting_for_its_state_callback_does_not_deadlock(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    state_published = asyncio.Event()

    async def turn_on(_call: ServiceCall) -> None:
        hass.states.async_set(entity.entity_id, "on")
        await asyncio.wait_for(state_published.wait(), timeout=0.1)

    hass.services.async_register("switch", "turn_on", turn_on)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    current_sequence = _current_sequence(mqtt_mock, entity.entity_id)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(
        entity.entity_id,
        "turn_on",
        None,
        observed_sequence=current_sequence,
    )
    real_publish = mqtt.async_publish

    async def publish_and_signal_state(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        await real_publish(
            hass,
            topic,
            payload,
            qos,
            retain,
            encoding,
            message_expiry_interval=message_expiry_interval,
        )
        if topic == state_topic(stable_id(entity.entity_id)):
            state_published.set()

    with patch(
        "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
        side_effect=publish_and_signal_state,
    ):
        async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
        await hass.async_block_till_done()

    result = _payload(_published(mqtt_mock, result_topic(command_id))[-1])
    assert state_published.is_set()
    assert result["accepted"] is True
    assert result["error"] is None
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(("kind", "value", "domain", "service", "extra_data"), COMMAND_CASES)
async def test_valid_commands_call_exact_home_assistant_service(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    kind: str,
    value: object,
    domain: str,
    service: str,
    extra_data: dict[str, int],
) -> None:
    entity = _entity_for_domain(hass, domain)
    calls: list[ServiceCall] = []

    async def handler(call: ServiceCall) -> None:
        calls.append(call)

    hass.services.async_register(domain, service, handler)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(entity.entity_id, kind, value)

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert len(calls) == 1
    assert calls[0].domain == domain
    assert calls[0].service == service
    assert calls[0].data == {"entity_id": entity.entity_id, **extra_data}
    result_calls = _published(mqtt_mock, result_topic(command_id))
    assert len(result_calls) == 1
    assert result_calls[0].args[3] is False
    result = _payload(result_calls[0])
    assert result == {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "command_id": command_id,
        "stable_id": stable_id(entity.entity_id),
        "accepted": True,
        "resulting_sequence": 1,
        "timestamp_ms": result["timestamp_ms"],
        "error": None,
        "elapsed_ms": result["elapsed_ms"],
    }
    assert "state_sequence" not in result
    assert isinstance(result["timestamp_ms"], int)
    assert isinstance(result["elapsed_ms"], int) and result["elapsed_ms"] >= 0
    await plane.async_detach(entry.entry_id)


InvalidCommand = Callable[[str, str], tuple[str, str]]


def _expired(entity_id: str, kind: str) -> tuple[str, str]:
    return _command_payload(
        entity_id,
        kind,
        None,
        issued_at_ms=time.time_ns() // 1_000_000 - 15_001,
    )


def _mismatched(entity_id: str, kind: str) -> tuple[str, str]:
    return _command_payload(
        entity_id,
        kind,
        None,
        payload_stable_id="aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
    )


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(
    ("make_command", "topic_stable_id"),
    [
        (_expired, lambda entity_id: stable_id(entity_id)),
        (_mismatched, lambda entity_id: stable_id(entity_id)),
    ],
    ids=["expired", "stable-id-mismatch"],
)
async def test_invalid_command_context_never_calls_a_service(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    make_command: InvalidCommand,
    topic_stable_id: Callable[[str], str],
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = make_command(entity.entity_id, "turn_on")

    async_fire_mqtt_message(hass, command_topic(topic_stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert calls == []
    result = _payload(_published(mqtt_mock, result_topic(command_id))[0])
    assert result["accepted"] is False
    assert isinstance(result["error"], str) and result["error"]
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_command_absent_from_manifest_is_rejected(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(
        hass,
        "light.on_off",
        attributes={ATTR_SUPPORTED_COLOR_MODES: ["onoff"]},
    )
    calls: list[ServiceCall] = []
    hass.services.async_register("light", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(entity.entity_id, "set_brightness", 128)

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert calls == []
    result = _payload(_published(mqtt_mock, result_topic(command_id))[0])
    assert result["accepted"] is False
    assert result["error"] == "command is not allowed by the current manifest"
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_current_observed_sequence_executes_the_command(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    current_sequence = _current_sequence(mqtt_mock, entity.entity_id)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(
        entity.entity_id, "turn_on", None, observed_sequence=current_sequence
    )

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert len(calls) == 1
    result = _payload(_published(mqtt_mock, result_topic(command_id))[0])
    assert result["accepted"] is True
    assert result["error"] is None
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_stale_observed_sequence_is_rejected_with_a_conflict_result(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    current_sequence = _current_sequence(mqtt_mock, entity.entity_id)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(
        entity.entity_id, "turn_on", None, observed_sequence=current_sequence + 1
    )

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert calls == []
    result = _payload(_published(mqtt_mock, result_topic(command_id))[0])
    assert result["accepted"] is False
    assert result["error"] == "observed_sequence is stale"
    assert result["resulting_sequence"] == current_sequence
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_stale_observed_sequence_rejection_does_not_advance_the_sequence(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    current_sequence = _current_sequence(mqtt_mock, entity.entity_id)
    mqtt_mock.async_publish.reset_mock()
    stale_id, stale_payload = _command_payload(
        entity.entity_id, "turn_on", None, observed_sequence=current_sequence + 1
    )
    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), stale_payload)
    await hass.async_block_till_done()
    assert calls == []
    assert _payload(_published(mqtt_mock, result_topic(stale_id))[0])["accepted"] is False

    current_id, current_payload = _command_payload(
        entity.entity_id, "turn_on", None, observed_sequence=current_sequence
    )
    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), current_payload)
    await hass.async_block_till_done()

    assert len(calls) == 1
    assert _payload(_published(mqtt_mock, result_topic(current_id))[0])["accepted"] is True
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_dormant_restart_republishes_state_changed_while_stopped(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    entry = _entry(hass, "office")
    plane = get_control_plane(hass)
    await plane.async_attach(entry)
    sequence_before_stop = _current_sequence(mqtt_mock, entity.entity_id)

    await plane.async_detach(entry.entry_id)
    mqtt_mock.async_publish.reset_mock()
    hass.states.async_set(entity.entity_id, "on")
    await hass.async_block_till_done()
    assert _published(mqtt_mock, state_topic(stable_id(entity.entity_id))) == []

    await plane.async_attach(entry)

    calls = _published(mqtt_mock, state_topic(stable_id(entity.entity_id)))
    assert len(calls) == 1
    state = _payload(calls[0])
    assert state["state"] == "on"
    assert state["sequence"] == sequence_before_stop + 1
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_dormant_restart_preserves_revision_sequence_and_duplicate_cache(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    first = _selected_entity(hass, "switch.first")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    entry = _entry(hass, "office")
    plane = get_control_plane(hass)
    await plane.async_attach(entry)

    _selected_entity(hass, "switch.second")
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=1))
    await hass.async_block_till_done()
    assert _payload(_published(mqtt_mock, manifest_topic())[-1])["revision"] == 2
    sequence_before_dormancy = _payload(
        _published(mqtt_mock, state_topic(stable_id(first.entity_id)))[-1]
    )["sequence"]
    assert sequence_before_dormancy > 1

    hass.states.async_set(first.entity_id, "on")
    await hass.async_block_till_done()
    sequence_before_dormancy += 1
    assert (
        _payload(_published(mqtt_mock, state_topic(stable_id(first.entity_id)))[-1])["sequence"]
        == sequence_before_dormancy
    )
    command_id, command_payload = _command_payload(
        first.entity_id, "turn_on", None, observed_sequence=sequence_before_dormancy
    )
    async_fire_mqtt_message(hass, command_topic(stable_id(first.entity_id)), command_payload)
    await hass.async_block_till_done()
    first_result = _published(mqtt_mock, result_topic(command_id))[-1].args[1]
    assert len(calls) == 1

    await plane.async_detach(entry.entry_id)
    assert plane.started is False
    assert plane.result_cache_size == 1
    mqtt_mock.async_publish.reset_mock()
    await plane.async_attach(entry)
    assert (
        _payload(_published(mqtt_mock, state_topic(stable_id(first.entity_id)))[-1])["sequence"]
        == sequence_before_dormancy + 1
    )

    async_fire_mqtt_message(hass, command_topic(stable_id(first.entity_id)), command_payload)
    await hass.async_block_till_done()
    assert len(calls) == 1
    assert _published(mqtt_mock, result_topic(command_id))[0].args[1] == first_result

    hass.states.async_set(first.entity_id, "off")
    await hass.async_block_till_done()
    assert (
        _payload(_published(mqtt_mock, state_topic(stable_id(first.entity_id)))[-1])["sequence"]
        == sequence_before_dormancy + 2
    )
    _selected_entity(hass, "switch.third")
    async_fire_time_changed(hass, dt_util.utcnow() + timedelta(seconds=1))
    await hass.async_block_till_done()
    assert _payload(_published(mqtt_mock, manifest_topic())[-1])["revision"] == 3
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_partial_state_failure_retries_full_snapshot_before_authorizing(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    first = _selected_entity(hass, "switch.first")
    second = _selected_entity(hass, "switch.second")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    zulu = _entry(hass, "zulu", label="unused")
    alpha = _entry(hass, "alpha")
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    real_publish = mqtt.async_publish
    failed = False

    async def fail_second_state(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        nonlocal failed
        if topic == state_topic(stable_id(second.entity_id)) and not failed:
            failed = True
            raise RuntimeError("state publish failed")
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
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=fail_second_state,
        ),
        pytest.raises(RuntimeError, match="state publish failed"),
    ):
        await plane.async_attach(alpha)

    rejected_id, rejected_payload = _command_payload(first.entity_id, "turn_on", None)
    async_fire_mqtt_message(hass, command_topic(stable_id(first.entity_id)), rejected_payload)
    await hass.async_block_till_done()
    assert calls == []
    assert _payload(_published(mqtt_mock, result_topic(rejected_id))[-1])["accepted"] is False
    mqtt_mock.async_publish.reset_mock()

    await plane.async_reload_settings()

    assert len(_published(mqtt_mock, manifest_topic())) == 1
    assert len(_published(mqtt_mock, state_topic(stable_id(first.entity_id)))) == 1
    assert len(_published(mqtt_mock, state_topic(stable_id(second.entity_id)))) == 1
    accepted_id, accepted_payload = _command_payload(
        first.entity_id,
        "turn_on",
        None,
        observed_sequence=_current_sequence(mqtt_mock, first.entity_id),
    )
    async_fire_mqtt_message(hass, command_topic(stable_id(first.entity_id)), accepted_payload)
    await hass.async_block_till_done()
    assert len(calls) == 1
    assert _payload(_published(mqtt_mock, result_topic(accepted_id))[-1])["accepted"] is True
    await plane.async_detach(alpha.entry_id)
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
async def test_failed_candidate_state_never_replaces_broker_manifest_a(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity_a = _selected_entity(hass, "switch.a", label="label-a")
    _selected_entity(hass, "switch.b_first", label="label-b")
    entity_b_second = _selected_entity(hass, "switch.b_second", label="label-b")
    entry = _entry(hass, "office", label="label-a")
    plane = get_control_plane(hass)
    await plane.async_attach(entry)
    manifest_a = _payload(_published(mqtt_mock, manifest_topic())[-1])
    real_publish = mqtt.async_publish

    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_HA_CONTROL_LABEL: "label-b"}
    )

    async def fail_b_state(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if topic == state_topic(stable_id(entity_b_second.entity_id)):
            raise RuntimeError("candidate B state failed")
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
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=fail_b_state,
        ),
        pytest.raises(RuntimeError, match="candidate B state failed"),
    ):
        await plane.async_reload_settings()

    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_HA_CONTROL_LABEL: "label-a"}
    )
    await plane.async_reload_settings()

    manifests = [_payload(call) for call in _published(mqtt_mock, manifest_topic())]
    assert manifests == [manifest_a]
    assert manifests[0]["entities"][0]["entity_id"] == entity_a.entity_id
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_failed_b_then_c_publishes_only_c_at_revision_a_plus_one(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity_a = _selected_entity(hass, "switch.a", label="label-a")
    entity_b = _selected_entity(hass, "switch.b", label="label-b")
    entity_c = _selected_entity(hass, "switch.c", label="label-c")
    entry = _entry(hass, "office", label="label-a")
    plane = get_control_plane(hass)
    await plane.async_attach(entry)
    real_publish = mqtt.async_publish

    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_HA_CONTROL_LABEL: "label-b"}
    )

    async def fail_b_state(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if topic == state_topic(stable_id(entity_b.entity_id)):
            raise RuntimeError("candidate B state failed")
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
        patch(
            "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
            side_effect=fail_b_state,
        ),
        pytest.raises(RuntimeError, match="candidate B state failed"),
    ):
        await plane.async_reload_settings()

    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_HA_CONTROL_LABEL: "label-c"}
    )
    await plane.async_reload_settings()

    manifests = [_payload(call) for call in _published(mqtt_mock, manifest_topic())]
    assert [(item["revision"], item["entities"][0]["entity_id"]) for item in manifests] == [
        (1, entity_a.entity_id),
        (2, entity_c.entity_id),
    ]
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_command_queued_during_rebuild_validates_after_manifest_last_commit(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    old_entity = _selected_entity(hass, "switch.old", label="label-a")
    new_entity = _selected_entity(hass, "switch.new", label="label-b")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    entry = _entry(hass, "office", label="label-a")
    plane = get_control_plane(hass)
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    real_publish = mqtt.async_publish
    state_started = asyncio.Event()
    release_state = asyncio.Event()

    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_HA_CONTROL_LABEL: "label-b"}
    )

    async def block_new_state(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if topic == state_topic(stable_id(new_entity.entity_id)):
            state_started.set()
            await release_state.wait()
        await real_publish(
            hass,
            topic,
            payload,
            qos,
            retain,
            encoding,
            message_expiry_interval=message_expiry_interval,
        )

    with patch(
        "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
        side_effect=block_new_state,
    ):
        reload_task = hass.async_create_task(plane.async_reload_settings())
        await state_started.wait()
        command_id, payload = _command_payload(old_entity.entity_id, "turn_on", None)
        async_fire_mqtt_message(hass, command_topic(stable_id(old_entity.entity_id)), payload)
        await asyncio.sleep(0)
        assert calls == []
        assert _published(mqtt_mock, result_topic(command_id)) == []
        release_state.set()
        await reload_task
        await hass.async_block_till_done()

    assert calls == []
    result_calls = _published(mqtt_mock, result_topic(command_id))
    assert len(result_calls) == 1
    assert _payload(result_calls[0])["accepted"] is False
    topics = [call.args[0] for call in mqtt_mock.async_publish.call_args_list]
    assert (
        topics.index(state_topic(stable_id(new_entity.entity_id)))
        < topics.index(manifest_topic())
        < topics.index(result_topic(command_id))
    )
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_detach_fences_a_queued_command_while_first_command_drains(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    entered = asyncio.Event()
    release = asyncio.Event()
    calls: list[ServiceCall] = []

    async def blocking_handler(call: ServiceCall) -> None:
        calls.append(call)
        entered.set()
        await release.wait()

    hass.services.async_register("switch", "turn_on", blocking_handler)
    entry = _entry(hass, "office")
    plane = get_control_plane(hass)
    await plane.async_attach(entry)
    first_id, first_payload = _command_payload(entity.entity_id, "turn_on", None)
    second_id, second_payload = _command_payload(entity.entity_id, "turn_on", None)

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), first_payload)
    await entered.wait()
    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), second_payload)
    await asyncio.sleep(0)
    detach = hass.async_create_task(plane.async_detach(entry.entry_id))
    await asyncio.sleep(0)
    release.set()
    await detach
    await hass.async_block_till_done()

    assert len(calls) == 1
    assert len(_published(mqtt_mock, result_topic(first_id))) == 1
    assert _published(mqtt_mock, result_topic(second_id)) == []


@pytest.mark.allow_lingering_timers
async def test_soft_rebuild_cannot_clear_hard_detach_fence(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    _selected_entity(hass, "switch.a", label="label-a")
    entity_b = _selected_entity(hass, "switch.b", label="label-b")
    entity_c = _selected_entity(hass, "switch.c", label="label-c")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    zulu = _entry(hass, "zulu", label="label-c")
    alpha = _entry(hass, "alpha", label="label-a")
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    await plane.async_attach(alpha)
    real_publish = mqtt.async_publish
    b_started = asyncio.Event()
    release_b = asyncio.Event()
    c_started = asyncio.Event()
    release_c = asyncio.Event()

    async def block_candidate_states(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if topic == state_topic(stable_id(entity_b.entity_id)):
            b_started.set()
            await release_b.wait()
        elif topic == state_topic(stable_id(entity_c.entity_id)):
            c_started.set()
            await release_c.wait()
        await real_publish(
            hass,
            topic,
            payload,
            qos,
            retain,
            encoding,
            message_expiry_interval=message_expiry_interval,
        )

    hass.config_entries.async_update_entry(
        alpha, data={**alpha.data, CONF_HA_CONTROL_LABEL: "label-b"}
    )
    with patch(
        "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
        side_effect=block_candidate_states,
    ):
        soft_reload = hass.async_create_task(plane.async_reload_settings())
        await b_started.wait()
        detach = hass.async_create_task(plane.async_detach(alpha.entry_id))
        await asyncio.sleep(0)
        assert plane._hard_fenced is True
        release_b.set()
        await c_started.wait()
        await soft_reload

        command_id, payload = _command_payload(entity_c.entity_id, "turn_on", None)
        async_fire_mqtt_message(hass, command_topic(stable_id(entity_c.entity_id)), payload)
        await asyncio.sleep(0)
        release_c.set()
        await detach
        await hass.async_block_till_done()

    assert calls == []
    assert not any(
        _payload(call)["accepted"] is True
        for call in _published(mqtt_mock, result_topic(command_id))
    )
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
async def test_newer_concurrent_detach_owns_hard_fence_until_it_completes(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    _selected_entity(hass, "switch.a", label="label-a")
    entity_b = _selected_entity(hass, "switch.b", label="label-b")
    entity_c = _selected_entity(hass, "switch.c", label="label-c")
    zulu = _entry(hass, "zulu", label="label-c")
    beta = _entry(hass, "beta", label="label-b")
    alpha = _entry(hass, "alpha", label="label-a")
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    await plane.async_attach(beta)
    await plane.async_attach(alpha)
    real_publish = mqtt.async_publish
    b_started = asyncio.Event()
    release_b = asyncio.Event()
    c_started = asyncio.Event()
    release_c = asyncio.Event()

    async def block_candidate_states(
        hass: HomeAssistant,
        topic: str,
        payload: str | bytes | int | float | None,
        qos: int = 0,
        retain: bool = False,
        encoding: str | None = "utf-8",
        *,
        message_expiry_interval: int | None = None,
    ) -> None:
        if topic == state_topic(stable_id(entity_b.entity_id)):
            b_started.set()
            await release_b.wait()
        elif topic == state_topic(stable_id(entity_c.entity_id)):
            c_started.set()
            await release_c.wait()
        await real_publish(
            hass,
            topic,
            payload,
            qos,
            retain,
            encoding,
            message_expiry_interval=message_expiry_interval,
        )

    with patch(
        "custom_components.brilliant_mqtt.ha_control.mqtt.async_publish",
        side_effect=block_candidate_states,
    ):
        first_detach = hass.async_create_task(plane.async_detach(alpha.entry_id))
        await b_started.wait()
        second_detach = hass.async_create_task(plane.async_detach(beta.entry_id))
        await asyncio.sleep(0)
        newer_generation = plane._command_fence_generation
        release_b.set()
        await c_started.wait()
        await first_detach
        hard_fenced_while_newer_pending = plane._hard_fenced
        generation_while_newer_pending = plane._command_fence_generation
        release_c.set()
        await second_detach

    assert hard_fenced_while_newer_pending is True
    assert generation_while_newer_pending == newer_generation
    assert plane._hard_fenced is False
    await plane.async_detach(zulu.entry_id)


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(
    ("domain", "kind", "invalid_value"),
    [
        ("light", "set_brightness", -1),
        ("light", "set_brightness", 256),
        ("light", "set_brightness", True),
        ("cover", "set_position", -1),
        ("cover", "set_position", 101),
        ("cover", "set_tilt", 42.0),
    ],
)
async def test_invalid_command_ranges_never_call_a_service(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    domain: str,
    kind: str,
    invalid_value: object,
) -> None:
    entity = _entity_for_domain(hass, domain)
    service = "turn_on" if domain == "light" else "set_cover_position"
    calls: list[ServiceCall] = []
    hass.services.async_register(domain, service, calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(entity.entity_id, kind, invalid_value)

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert calls == []
    assert _payload(_published(mqtt_mock, result_topic(command_id))[0])["accepted"] is False
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_duplicate_command_replays_byte_identical_cached_result(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(entity.entity_id, "turn_on", None)

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()
    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    assert len(calls) == 1
    results = _published(mqtt_mock, result_topic(command_id))
    assert len(results) == 2
    assert results[0].args[1] == results[1].args[1]
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_result_cache_is_bounded_to_1024_entries(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    entity_stable_id = stable_id(entity.entity_id)
    first_payload = ""

    for number in range(1, 1026):
        command_id = str(UUID(int=number))
        _, payload = _command_payload(entity.entity_id, "turn_on", None, command_id=command_id)
        if number == 1:
            first_payload = payload
        async_fire_mqtt_message(hass, command_topic(entity_stable_id), payload)
    await hass.async_block_till_done()

    assert plane.result_cache_size == 1024
    async_fire_mqtt_message(hass, command_topic(entity_stable_id), first_payload)
    await hass.async_block_till_done()
    assert len(calls) == 1026
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_result_cache_expires_after_ten_minutes(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entity = _selected_entity(hass, "switch.controlled")
    calls: list[ServiceCall] = []
    hass.services.async_register("switch", "turn_on", calls.append)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    command_id, payload = _command_payload(entity.entity_id, "turn_on", None)

    with patch("custom_components.brilliant_mqtt.ha_control._monotonic", return_value=10.0):
        async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
        await hass.async_block_till_done()
    with patch("custom_components.brilliant_mqtt.ha_control._monotonic", return_value=610.001):
        async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
        await hass.async_block_till_done()

    assert len(calls) == 2
    await plane.async_detach(entry.entry_id)


@pytest.mark.allow_lingering_timers
async def test_service_failure_result_is_sanitized(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient, caplog: pytest.LogCaptureFixture
) -> None:
    entity = _selected_entity(hass, "switch.controlled")

    async def fail(_call: ServiceCall) -> None:
        raise RuntimeError("secret\nsecond line")

    hass.services.async_register("switch", "turn_on", fail)
    plane = get_control_plane(hass)
    entry = _entry(hass, "office")
    await plane.async_attach(entry)
    mqtt_mock.async_publish.reset_mock()
    command_id, payload = _command_payload(entity.entity_id, "turn_on", None)

    async_fire_mqtt_message(hass, command_topic(stable_id(entity.entity_id)), payload)
    await hass.async_block_till_done()

    result_call = _published(mqtt_mock, result_topic(command_id))[0]
    result = _payload(result_call)
    assert result["accepted"] is False
    assert result["error"] == "service_call_failed"
    assert "secret" not in result_call.args[1]
    assert "RuntimeError" in caplog.text
    assert "secret second line" in caplog.text
    await plane.async_detach(entry.entry_id)
