"""Behavior tests for the singleton Brilliant scene/mode MQTT runtime."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Mapping
from typing import Any
from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import CALLBACK_TYPE, Event, HomeAssistant, ServiceCall
from homeassistant.exceptions import HomeAssistantError
from pytest_homeassistant_custom_component.common import MockConfigEntry, async_fire_mqtt_message
from pytest_homeassistant_custom_component.typing import MqttMockHAClient

from custom_components.brilliant_mqtt import scene_control as scene_control_module
from custom_components.brilliant_mqtt.const import (
    CONF_HA_CONTROL_ENABLED,
    CONF_PANEL,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    DOMAIN,
)
from custom_components.brilliant_mqtt.ha_control import get_control_plane
from custom_components.brilliant_mqtt.ha_control_protocol import (
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    mode_result_topic,
    scene_result_topic,
)
from custom_components.brilliant_mqtt.scene_control import (
    EVENT_MODE,
    EVENT_SCENE,
    MAX_DEDUPLICATION_KEYS,
    MAX_PENDING_COMMANDS,
    ModeOption,
    SceneControl,
    SceneOption,
)

_SUBSCRIPTIONS = {
    "brilliant/ha-control/v1/scene/catalog/+",
    "brilliant/ha-control/v1/mode/catalog/+",
    "brilliant/ha-control/v1/scene/event/+",
    "brilliant/ha-control/v1/mode/event/+",
    "brilliant/ha-control/v1/scene/result/+",
    "brilliant/ha-control/v1/mode/result/+",
    "brilliant/ha-control/v1/status/scene/+",
    "brilliant/ha-control/v1/status/mode/+",
}


def _scene_catalog(
    generated_at_ms: int,
    scenes: list[Mapping[str, object]],
    *,
    panel: str = "office",
) -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": panel,
            "generated_at_ms": generated_at_ms,
            "scenes": scenes,
        }
    )


def _scene_status(timestamp_ms: int, *, available: bool = True, panel: str = "office") -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "transport": "scene",
            "panel": panel,
            "available": available,
            "reason": None if available else "execution_unavailable",
            "timestamp_ms": timestamp_ms,
        }
    )


def _mode_catalog(
    generated_at_ms: int,
    modes: list[Mapping[str, object]],
    *,
    panel: str = "office",
) -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": panel,
            "generated_at_ms": generated_at_ms,
            "modes": modes,
        }
    )


def _mode_status(timestamp_ms: int, *, available: bool = True, panel: str = "office") -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "transport": "mode",
            "panel": panel,
            "available": available,
            "reason": None if available else "malformed_data",
            "timestamp_ms": timestamp_ms,
        }
    )


def _scene_event(executed_at_ms: int, *, scene_id: str = "all_off") -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": "office",
            "scene_id": scene_id,
            "executed_at_ms": executed_at_ms,
            "deduplication_key": f"office:{scene_id}:{executed_at_ms}",
        }
    )


def _mode_event(executed_at_ms: int, *, mode_id: str = "away") -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": "office",
            "mode_id": mode_id,
            "executed_at_ms": executed_at_ms,
            "deduplication_key": f"office:{mode_id}:{executed_at_ms}",
        }
    )


def _published(mqtt_mock: MqttMockHAClient, prefix: str) -> list[Any]:
    return [
        call for call in mqtt_mock.async_publish.call_args_list if call.args[0].startswith(prefix)
    ]


@pytest.mark.allow_lingering_timers
async def test_start_subscribes_once_to_every_scene_and_mode_topic(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)

    await runtime.async_start({"office"}, default_panel="office", actions={})
    await runtime.async_start({"office"}, default_panel="office", actions={})

    scene_calls = [
        call for call in mqtt_mock.async_subscribe.call_args_list if call.args[0] in _SUBSCRIPTIONS
    ]
    assert {call.args[0] for call in scene_calls} == _SUBSCRIPTIONS
    assert len(scene_calls) == len(_SUBSCRIPTIONS)

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_retained_catalog_replaces_atomically_and_ignores_stale_updates(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            200,
            [
                {"scene_id": "all_off", "display_name": "All Lights Off", "icon": None},
                {"scene_id": "all_on", "display_name": "All Lights On", "icon": "light"},
            ],
        ),
        retain=True,
    )
    await hass.async_block_till_done()
    assert runtime.scene_options("office") == (
        SceneOption("all_off", "All Lights Off"),
        SceneOption("all_on", "All Lights On"),
    )

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            199,
            [{"scene_id": "stale", "display_name": "Stale", "icon": None}],
        ),
    )
    await hass.async_block_till_done()
    assert [item.scene_id for item in runtime.scene_options("office")] == ["all_off", "all_on"]

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(201, []),
    )
    await hass.async_block_till_done()
    assert runtime.scene_options("office") == ()

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_scene_event_fires_before_action_and_deduplicates_or_rejects_stale(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    order: list[tuple[str, object]] = []

    async def event_listener(event: Event[dict[str, object]]) -> None:
        order.append(("event", event.data))

    async def action_handler(call: ServiceCall) -> None:
        order.append(("action", call.data))

    hass.bus.async_listen(EVENT_SCENE, event_listener)
    hass.services.async_register("scene", "turn_on", action_handler)
    runtime = SceneControl(hass)
    await runtime.async_start(
        {"office"},
        default_panel="office",
        actions={
            "office:all_off": {
                "domain": "scene",
                "service": "turn_on",
                "target": {"entity_id": ["scene.downstairs_off"]},
                "data": {"transition": 0},
            }
        },
    )

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        _scene_event(300),
    )
    await hass.async_block_till_done()
    assert [kind for kind, _value in order] == ["event", "action"]
    assert order[0][1] == {
        "panel": "office",
        "scene_id": "all_off",
        "executed_at_ms": 300,
        "deduplication_key": "office:all_off:300",
    }
    assert order[1][1] == {
        "entity_id": ["scene.downstairs_off"],
        "transition": 0,
    }

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        _scene_event(300),
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        _scene_event(299, scene_id="all_on"),
    )
    await hass.async_block_till_done()
    assert len(order) == 2

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_scene_service_publishes_non_retained_and_waits_for_exact_confirmation(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office", "kitchen"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            200,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(201),
        retain=True,
    )
    await hass.async_block_till_done()
    mqtt_mock.async_publish.reset_mock()

    service_task = hass.async_create_task(runtime.async_run_scene(None, "all_off"))
    await asyncio.sleep(0)
    calls = _published(mqtt_mock, "brilliant/ha-control/v1/scene/command/")
    assert len(calls) == 1
    assert calls[0].args[0] == "brilliant/ha-control/v1/scene/command/office"
    assert calls[0].args[3] is False
    command = json.loads(calls[0].args[1])
    assert command == {
        "schema_version": SCHEMA_VERSION,
        "mapping_version": MAPPING_VERSION,
        "command_id": command["command_id"],
        "panel": "office",
        "scene_id": "all_off",
        "issued_at_ms": command["issued_at_ms"],
    }
    assert isinstance(command["issued_at_ms"], int) and command["issued_at_ms"] >= 0

    async_fire_mqtt_message(
        hass,
        scene_result_topic(command["command_id"]),
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "command_id": command["command_id"],
                "panel": "office",
                "scene_id": "different_scene",
                "accepted": True,
                "timestamp_ms": command["issued_at_ms"] + 1,
            }
        ),
    )
    await asyncio.sleep(0)
    assert not service_task.done()

    async_fire_mqtt_message(
        hass,
        scene_result_topic(command["command_id"]),
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "command_id": command["command_id"],
                "panel": "office",
                "scene_id": "all_off",
                "accepted": True,
                "timestamp_ms": command["issued_at_ms"] + 2,
            }
        ),
    )
    await service_task

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_stop_fails_pending_service_and_removes_every_subscription(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            200,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(201),
        retain=True,
    )
    await hass.async_block_till_done()

    service_task = hass.async_create_task(runtime.async_run_scene("office", "all_off"))
    await asyncio.sleep(0)
    await runtime.async_stop()

    with pytest.raises(HomeAssistantError, match="stopped"):
        await service_task
    assert all(not mqtt_mock.is_active_subscription(topic) for topic in _SUBSCRIPTIONS)


@pytest.mark.allow_lingering_timers
async def test_mode_catalog_event_and_rejected_confirmation_are_symmetric(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    events: list[Event[dict[str, object]]] = []
    hass.bus.async_listen(EVENT_MODE, events.append)
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/mode/catalog/office",
        _mode_catalog(100, [{"mode_id": "away", "display_name": "Away"}]),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/mode/office",
        _mode_status(101),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/mode/event/office",
        _mode_event(102),
    )
    await hass.async_block_till_done()
    assert runtime.mode_options("office") == (ModeOption("away", "Away"),)
    assert [event.data for event in events] == [
        {
            "panel": "office",
            "mode_id": "away",
            "executed_at_ms": 102,
            "deduplication_key": "office:away:102",
        }
    ]

    mqtt_mock.async_publish.reset_mock()
    service_task = hass.async_create_task(runtime.async_set_mode("office", "away"))
    await asyncio.sleep(0)
    calls = _published(mqtt_mock, "brilliant/ha-control/v1/mode/command/")
    assert len(calls) == 1 and calls[0].args[3] is False
    command = json.loads(calls[0].args[1])
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
                "accepted": False,
                "error": "write_failed",
                "timestamp_ms": command["issued_at_ms"] + 1,
            }
        ),
    )
    with pytest.raises(HomeAssistantError, match="write_failed"):
        await service_task

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(
    ("panel", "scene_id", "message"),
    [
        ("garage", "all_off", "panel"),
        ("office", "unknown", "available"),
    ],
)
async def test_scene_service_rejects_unattached_or_unknown_selection(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    panel: str,
    scene_id: str,
    message: str,
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(11),
        retain=True,
    )
    await hass.async_block_till_done()

    with pytest.raises(HomeAssistantError, match=message):
        await runtime.async_run_scene(panel, scene_id)
    assert _published(mqtt_mock, "brilliant/ha-control/v1/scene/command/") == []

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_scene_service_rejects_offline_transport_and_times_out_cleanly(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(11, available=False),
        retain=True,
    )
    await hass.async_block_till_done()
    with pytest.raises(HomeAssistantError, match="offline"):
        await runtime.async_run_scene("office", "all_off")

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(12),
    )
    await hass.async_block_till_done()
    monkeypatch.setattr(scene_control_module, "_RESULT_TIMEOUT_SECONDS", 0.01)
    with pytest.raises(HomeAssistantError, match="timed out"):
        await runtime.async_run_scene("office", "all_off")
    assert runtime.pending_count == 0

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
@pytest.mark.parametrize(
    "actions",
    [
        {"office-all_off": {}},
        {"office:all_off:extra": {}},
        {
            "office:all_off": {
                "domain": "Scene",
                "service": "turn_on",
                "target": {},
                "data": {},
            }
        },
        {
            "office:all_off": {
                "domain": "scene",
                "service": "turn.on",
                "target": {},
                "data": {},
            }
        },
        {
            "office:all_off": {
                "domain": "scene",
                "service": "turn_on",
                "target": {"floor_id": "downstairs"},
                "data": {},
            }
        },
        {
            "office:all_off": {
                "domain": "scene",
                "service": "turn_on",
                "target": {},
                "data": {},
                "unexpected": True,
            }
        },
    ],
)
async def test_malformed_actions_fail_closed_without_suppressing_events(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    actions: Mapping[str, object],
) -> None:
    events: list[Event[dict[str, object]]] = []
    calls: list[ServiceCall] = []
    hass.bus.async_listen(EVENT_SCENE, events.append)
    hass.services.async_register("scene", "turn_on", calls.append)
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions=actions)

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        _scene_event(20),
    )
    await hass.async_block_till_done()
    assert len(events) == 1
    assert calls == []

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_oversized_action_mapping_fails_closed_without_suppressing_events(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    events: list[Event[dict[str, object]]] = []
    calls: list[ServiceCall] = []
    hass.bus.async_listen(EVENT_SCENE, events.append)
    hass.services.async_register("scene", "turn_on", calls.append)
    action: dict[str, object] = {
        "domain": "scene",
        "service": "turn_on",
        "target": {},
        "data": {},
    }
    actions = {f"office:scene_{index}": action for index in range(1_025)}
    actions["office:all_off"] = action
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions=actions)

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        _scene_event(20),
    )
    await hass.async_block_till_done()
    assert len(events) == 1
    assert calls == []

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_malformed_retained_and_mismatched_messages_leave_state_unchanged_and_hide_payload(
    hass: HomeAssistant,
    mqtt_mock: MqttMockHAClient,
    caplog: pytest.LogCaptureFixture,
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    secret = "do-not-log-this-payload"
    candidates = [
        (
            "brilliant/ha-control/v1/scene/catalog/office",
            _scene_catalog(
                1,
                [{"scene_id": secret, "display_name": "Secret", "icon": None}],
                panel="kitchen",
            ),
            True,
        ),
        (
            "brilliant/ha-control/v1/scene/catalog/office",
            _scene_catalog(-1, []),
            True,
        ),
        ("brilliant/ha-control/v1/scene/catalog/office", "[]", False),
        (
            "brilliant/ha-control/v1/scene/event/office",
            _scene_event(3),
            True,
        ),
        (
            "brilliant/ha-control/v1/scene/event/unknown",
            _scene_event(4),
            False,
        ),
        (
            "brilliant/ha-control/v1/status/scene/office",
            _scene_status(5, panel="kitchen"),
            True,
        ),
        ("brilliant/ha-control/v1/scene/catalog/office", secret, True),
    ]
    for topic, payload, retained in candidates:
        async_fire_mqtt_message(hass, topic, payload, retain=retained)
    await hass.async_block_till_done()

    assert runtime.scene_options("office") == ()
    assert runtime.scene_transport_available("office") is False
    integration_logs = "\n".join(
        record.getMessage()
        for record in caplog.records
        if record.name.startswith("custom_components.brilliant_mqtt")
    )
    assert secret not in integration_logs

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_hostile_input_and_pending_state_are_bounded(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    for timestamp_ms in range(1, MAX_DEDUPLICATION_KEYS + 50):
        async_fire_mqtt_message(
            hass,
            "brilliant/ha-control/v1/scene/event/office",
            _scene_event(timestamp_ms),
        )
    await hass.async_block_till_done()
    assert runtime.deduplication_cache_size <= MAX_DEDUPLICATION_KEYS

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            MAX_DEDUPLICATION_KEYS + 100,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(MAX_DEDUPLICATION_KEYS + 101),
        retain=True,
    )
    await hass.async_block_till_done()
    tasks = [
        hass.async_create_task(runtime.async_run_scene("office", "all_off"))
        for _ in range(MAX_PENDING_COMMANDS + 1)
    ]
    for _ in range(MAX_PENDING_COMMANDS * 3):
        if runtime.pending_count == MAX_PENDING_COMMANDS and tasks[-1].done():
            break
        await asyncio.sleep(0)
    assert runtime.pending_count == MAX_PENDING_COMMANDS
    with pytest.raises(HomeAssistantError, match="busy"):
        await tasks[-1]

    await runtime.async_stop()
    for task in tasks[:-1]:
        with pytest.raises(HomeAssistantError, match="stopped"):
            await task


@pytest.mark.allow_lingering_timers
async def test_stop_then_restart_clears_state_without_duplicate_subscriptions(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    await hass.async_block_till_done()
    await runtime.async_stop()
    assert runtime.scene_options("office") == ()

    await runtime.async_start({"office"}, default_panel="office", actions={})
    active = [topic for topic in _SUBSCRIPTIONS if mqtt_mock.is_active_subscription(topic)]
    assert set(active) == _SUBSCRIPTIONS
    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_control_plane_owns_one_runtime_and_reconfigures_attached_panels(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    zulu = MockConfigEntry(
        domain=DOMAIN,
        unique_id="zulu",
        data={
            CONF_PANEL: "zulu",
            CONF_HA_CONTROL_ENABLED: True,
            CONF_SCENE_PANEL: "zulu",
            CONF_SCENE_ACTIONS: {},
        },
    )
    alpha = MockConfigEntry(
        domain=DOMAIN,
        unique_id="alpha",
        data={
            CONF_PANEL: "alpha",
            CONF_HA_CONTROL_ENABLED: True,
            CONF_SCENE_PANEL: "alpha",
            CONF_SCENE_ACTIONS: {},
        },
    )
    zulu.add_to_hass(hass)
    alpha.add_to_hass(hass)
    plane = get_control_plane(hass)

    await plane.async_attach(zulu)
    await plane.async_attach(alpha)
    assert plane.scene_control.attached_panels == frozenset({"alpha", "zulu"})
    assert plane.scene_control.default_panel == "alpha"
    assert len(
        [
            call
            for call in mqtt_mock.async_subscribe.call_args_list
            if call.args[0] in _SUBSCRIPTIONS
        ]
    ) == len(_SUBSCRIPTIONS)

    await plane.async_detach(alpha.entry_id)
    assert plane.scene_control.attached_panels == frozenset({"zulu"})
    assert plane.scene_control.default_panel == "zulu"
    await plane.async_detach(zulu.entry_id)
    assert plane.scene_control.started is False


@pytest.mark.allow_lingering_timers
async def test_noncanonical_deduplication_key_is_rejected(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    events: list[Event[dict[str, object]]] = []
    hass.bus.async_listen(EVENT_SCENE, events.append)
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    payload = json.loads(_scene_event(10))
    payload["deduplication_key"] = "arbitrary"

    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/event/office",
        encode_json(payload),
    )
    await hass.async_block_till_done()
    assert events == []

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_command_publish_failure_is_sanitized_and_does_not_leak_pending_state(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(11),
        retain=True,
    )
    await hass.async_block_till_done()
    secret = "broker-secret-detail"

    with (
        patch(
            "custom_components.brilliant_mqtt.scene_control.mqtt.async_publish",
            side_effect=RuntimeError(secret),
        ),
        pytest.raises(HomeAssistantError, match="publish") as error,
    ):
        await runtime.async_run_scene("office", "all_off")
    assert secret not in str(error.value)
    assert runtime.pending_count == 0

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_scene_runtime_start_failure_rolls_back_before_manifest_commit(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    entry = MockConfigEntry(
        domain=DOMAIN,
        unique_id="scene-start-failure",
        data={CONF_PANEL: "office", CONF_HA_CONTROL_ENABLED: True},
    )
    entry.add_to_hass(hass)
    plane = get_control_plane(hass)

    with (
        patch.object(
            plane.scene_control,
            "async_start",
            new=AsyncMock(side_effect=RuntimeError("scene subscriptions failed")),
        ),
        pytest.raises(RuntimeError, match="scene subscriptions failed"),
    ):
        await plane.async_attach(entry)

    assert plane.started is False
    assert not mqtt_mock.is_active_subscription("brilliant/ha-control/v1/command/+")
    assert _published(mqtt_mock, "brilliant/ha-control/v1/manifest") == []


@pytest.mark.allow_lingering_timers
async def test_reconfigure_hard_fences_a_service_already_queued_on_runtime_lock(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(11),
        retain=True,
    )
    await hass.async_block_till_done()
    mqtt_mock.async_publish.reset_mock()

    await runtime._lifecycle_lock.acquire()
    service_task = hass.async_create_task(runtime.async_run_scene("office", "all_off"))
    await asyncio.sleep(0)
    reconfigure_task = hass.async_create_task(
        runtime.async_reconfigure(set(), default_panel=None, actions={})
    )
    await asyncio.sleep(0)
    runtime._lifecycle_lock.release()

    await reconfigure_task
    with pytest.raises(HomeAssistantError, match="reconfiguring"):
        await service_task
    assert _published(mqtt_mock, "brilliant/ha-control/v1/scene/command/") == []

    await runtime.async_stop()


@pytest.mark.allow_lingering_timers
async def test_stop_drains_every_unsubscriber_even_when_one_raises(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    calls: list[int] = []

    def make_unsubscriber(index: int, *, fails: bool = False) -> CALLBACK_TYPE:
        def unsubscribe() -> None:
            calls.append(index)
            if fails:
                raise RuntimeError("unsubscribe failed")

        return unsubscribe

    unsubscribers = [
        make_unsubscriber(index, fails=index == 3) for index in range(len(_SUBSCRIPTIONS))
    ]
    runtime = SceneControl(hass)
    with patch(
        "custom_components.brilliant_mqtt.scene_control.mqtt.async_subscribe",
        new=AsyncMock(side_effect=unsubscribers),
    ):
        await runtime.async_start({"office"}, default_panel="office", actions={})

    with pytest.raises(RuntimeError, match="unsubscribe failed"):
        await runtime.async_stop()
    assert calls == list(reversed(range(len(_SUBSCRIPTIONS))))
    assert runtime.started is False
    assert runtime.attached_panels == frozenset()


@pytest.mark.allow_lingering_timers
async def test_nested_runtime_fence_cannot_be_reopened_by_inner_reconfigure(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    runtime = SceneControl(hass)
    await runtime.async_start({"office"}, default_panel="office", actions={})
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/office",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/office",
        _scene_status(11),
        retain=True,
    )
    await hass.async_block_till_done()
    mqtt_mock.async_publish.reset_mock()

    outer_fence_token = runtime.fence_commands()
    await runtime.async_reconfigure({"office"}, default_panel="office", actions={})
    service_task = hass.async_create_task(runtime.async_run_scene("office", "all_off"))
    await asyncio.sleep(0)
    command_calls = _published(mqtt_mock, "brilliant/ha-control/v1/scene/command/")

    runtime.release_command_fence(outer_fence_token)
    await runtime.async_stop()
    await asyncio.gather(service_task, return_exceptions=True)
    assert command_calls == []


@pytest.mark.allow_lingering_timers
async def test_outer_plane_fence_stays_closed_through_manifest_failure_then_reopens(
    hass: HomeAssistant, mqtt_mock: MqttMockHAClient
) -> None:
    zulu = MockConfigEntry(
        domain=DOMAIN,
        unique_id="zulu-fence",
        data={CONF_PANEL: "zulu", CONF_HA_CONTROL_ENABLED: True},
    )
    alpha = MockConfigEntry(
        domain=DOMAIN,
        unique_id="alpha-fence",
        data={CONF_PANEL: "alpha", CONF_HA_CONTROL_ENABLED: True},
    )
    zulu.add_to_hass(hass)
    alpha.add_to_hass(hass)
    plane = get_control_plane(hass)
    await plane.async_attach(zulu)
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/scene/catalog/zulu",
        _scene_catalog(
            10,
            [{"scene_id": "all_off", "display_name": "All Lights Off", "icon": None}],
            panel="zulu",
        ),
        retain=True,
    )
    async_fire_mqtt_message(
        hass,
        "brilliant/ha-control/v1/status/scene/zulu",
        _scene_status(11, panel="zulu"),
        retain=True,
    )
    await hass.async_block_till_done()
    mqtt_mock.async_publish.reset_mock()
    rebuild_started = asyncio.Event()
    release_rebuild = asyncio.Event()

    async def fail_rebuild() -> None:
        rebuild_started.set()
        await release_rebuild.wait()
        raise RuntimeError("manifest failed")

    with patch.object(plane, "_async_rebuild_manifest", side_effect=fail_rebuild):
        attach_task = hass.async_create_task(plane.async_attach(alpha))
        await rebuild_started.wait()
        during_task = hass.async_create_task(plane.scene_control.async_run_scene("zulu", "all_off"))
        await asyncio.sleep(0)
        during_calls = _published(mqtt_mock, "brilliant/ha-control/v1/scene/command/")
        if not during_task.done():
            during_task.cancel()
        await asyncio.gather(during_task, return_exceptions=True)
        release_rebuild.set()
        with pytest.raises(RuntimeError, match="manifest failed"):
            await attach_task
    assert during_calls == []

    mqtt_mock.async_publish.reset_mock()
    after_task = hass.async_create_task(plane.scene_control.async_run_scene("zulu", "all_off"))
    await asyncio.sleep(0)
    command_calls = _published(mqtt_mock, "brilliant/ha-control/v1/scene/command/")
    assert len(command_calls) == 1
    command = json.loads(command_calls[0].args[1])
    async_fire_mqtt_message(
        hass,
        scene_result_topic(command["command_id"]),
        encode_json(
            {
                "schema_version": SCHEMA_VERSION,
                "mapping_version": MAPPING_VERSION,
                "command_id": command["command_id"],
                "panel": "zulu",
                "scene_id": "all_off",
                "accepted": True,
                "timestamp_ms": command["issued_at_ms"] + 1,
            }
        ),
    )
    await after_task

    await plane.async_detach(alpha.entry_id)
    await plane.async_detach(zulu.entry_id)
