"""Off-panel tests for the bounded BVD current-owner light pilot."""

from __future__ import annotations

import asyncio
import inspect
import json
from collections.abc import Awaitable, Callable, Mapping
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from brilliant_mqtt.ha_control_protocol import (
    command_topic,
    result_topic,
    stable_id,
    state_topic,
)
from tools.brilliant_bvd.single_light_pilot import (
    BVD_CONFIGURATION_PERIPHERAL_ID,
    BVD_DEVICE_ID,
    EXPECTED_BVD_PERIPHERAL_TYPES,
    EXPECTED_PROCESS_CONFIGS,
    OFFICE_DEVICE_ID,
    TARGET_ENTITY_ID,
    BvdBus,
    BvdTopology,
    CleanupError,
    CleanupReport,
    PeripheralFact,
    PilotConfig,
    PilotController,
    PilotGuardError,
    PilotLifecycle,
    ScopedPeripheralProbe,
    VariableDefinition,
    VirtualLightHost,
    brightness_to_intensity,
    build_command_payload,
    build_light_variables,
    decode_state_payload,
    intensity_to_brightness,
    peripheral_id_for_entity,
    validate_active_topology,
    validate_manifest_authority,
    validate_postflight,
    validate_preflight,
)

NOW_MS = 1_800_000_000_000
STABLE_ID = stable_id(TARGET_ENTITY_ID)


def _fact(
    peripheral_id: str,
    *,
    status: int = 1,
    configuration_id: str | None = BVD_CONFIGURATION_PERIPHERAL_ID,
    relay_device: str | None = None,
) -> PeripheralFact:
    variables: dict[str, str] = {}
    if configuration_id is not None:
        variables["configuration_peripheral_id"] = configuration_id
    if relay_device is not None:
        variables["relay_device"] = relay_device
    return PeripheralFact(
        peripheral_id=peripheral_id,
        peripheral_type=EXPECTED_BVD_PERIPHERAL_TYPES[peripheral_id],
        status=status,
        variables=variables,
    )


def _topology(
    *,
    owner: str = OFFICE_DEVICE_ID,
    owner_timestamp_ms: int = NOW_MS - 1_000,
    relay_device: str = OFFICE_DEVICE_ID,
    stock_host_running: bool = True,
    facts: tuple[PeripheralFact, ...] | None = None,
) -> BvdTopology:
    if facts is None:
        facts = tuple(
            _fact(
                peripheral_id,
                configuration_id=(
                    None if peripheral_id == "remote_bridge" else BVD_CONFIGURATION_PERIPHERAL_ID
                ),
                relay_device=(relay_device if peripheral_id == "remote_bridge" else None),
            )
            for peripheral_id in EXPECTED_BVD_PERIPHERAL_TYPES
        )
    return BvdTopology(
        owning_device_id=OFFICE_DEVICE_ID,
        configuration_owner=owner,
        owner_timestamp_ms=owner_timestamp_ms,
        bvd_device_type=3,
        stock_host_running=stock_host_running,
        stock_host_identity="123:456789",
        process_config_peripheral_ids=EXPECTED_PROCESS_CONFIGS,
        peripherals=facts,
    )


def _config(**overrides: object) -> PilotConfig:
    values: dict[str, object] = {
        "room_assignment_id": "backyard-room:1700000000000",
        "display_name": "HA Backyard Light Group Pilot",
        "active_runtime_s": 120,
    }
    values.update(overrides)
    return PilotConfig(
        room_assignment_id=cast(str, values["room_assignment_id"]),
        display_name=cast(str, values["display_name"]),
        active_runtime_s=cast(int, values["active_runtime_s"]),
    )


def test_pilot_is_pinned_to_office_bvd_and_one_ha_entity() -> None:
    assert OFFICE_DEVICE_ID == "017ff60733f100038e04fa0fbab29096"
    assert BVD_DEVICE_ID == "brilliant_virtual_device"
    assert BVD_CONFIGURATION_PERIPHERAL_ID == "brilliant_virtual_device_configuration"
    assert TARGET_ENTITY_ID == "light.backyard_light_group"
    assert set(inspect.signature(PilotConfig).parameters) == {
        "room_assignment_id",
        "display_name",
        "active_runtime_s",
    }


def test_stable_peripheral_id_is_derived_from_ha_control_stable_id() -> None:
    expected = f"ha_bvd_{stable_id(TARGET_ENTITY_ID).replace('-', '')}"
    assert peripheral_id_for_entity(TARGET_ENTITY_ID) == expected
    assert len(expected) == len("ha_bvd_") + 32


@pytest.mark.parametrize(
    ("overrides", "message"),
    [
        ({"room_assignment_id": ""}, "room assignment"),
        ({"room_assignment_id": "bad room"}, "room assignment"),
        ({"display_name": ""}, "display name"),
        ({"display_name": "x" * 81}, "display name"),
        ({"active_runtime_s": 59}, "active runtime"),
        ({"active_runtime_s": 121}, "active runtime"),
        ({"active_runtime_s": True}, "active runtime"),
    ],
)
def test_config_rejects_unbounded_or_unsafe_values(
    overrides: dict[str, object], message: str
) -> None:
    with pytest.raises(PilotGuardError, match=message):
        _config(**overrides)


def test_preflight_accepts_fresh_natural_owner_and_exact_stock_host() -> None:
    validate_preflight(_config(), _topology(), now_ms=NOW_MS)


@pytest.mark.parametrize(
    ("topology", "message"),
    [
        (_topology(owner="another-panel"), "naturally elected owner"),
        (_topology(relay_device="another-panel"), "relay"),
        (_topology(owner_timestamp_ms=NOW_MS - 30_001), "fresh"),
        (_topology(owner_timestamp_ms=NOW_MS + 5_001), "future"),
        (_topology(stock_host_running=False), "stock BVD host"),
    ],
)
def test_preflight_rejects_owner_lease_or_stock_host_mismatch(
    topology: BvdTopology, message: str
) -> None:
    with pytest.raises(PilotGuardError, match=message):
        validate_preflight(_config(), topology, now_ms=NOW_MS)


def test_preflight_rejects_missing_extra_offline_or_wrong_type_peripheral() -> None:
    exact = list(_topology().peripherals)

    with pytest.raises(PilotGuardError, match="exact built-in set"):
        validate_preflight(_config(), _topology(facts=tuple(exact[:-1])), now_ms=NOW_MS)

    extra = PeripheralFact("unexpected", 27, 1, {})
    with pytest.raises(PilotGuardError, match="exact built-in set"):
        validate_preflight(_config(), _topology(facts=tuple([*exact, extra])), now_ms=NOW_MS)

    offline = [
        _fact(
            item.peripheral_id,
            status=0 if item.peripheral_id == "weather_peripheral" else 1,
            configuration_id=(
                None if item.peripheral_id == "remote_bridge" else BVD_CONFIGURATION_PERIPHERAL_ID
            ),
            relay_device=(OFFICE_DEVICE_ID if item.peripheral_id == "remote_bridge" else None),
        )
        for item in exact
    ]
    with pytest.raises(PilotGuardError, match="ONLINE"):
        validate_preflight(_config(), _topology(facts=tuple(offline)), now_ms=NOW_MS)

    wrong_type = list(exact)
    wrong_type[0] = PeripheralFact(
        wrong_type[0].peripheral_id,
        27,
        1,
        wrong_type[0].variables,
    )
    with pytest.raises(PilotGuardError, match="type"):
        validate_preflight(_config(), _topology(facts=tuple(wrong_type)), now_ms=NOW_MS)


def test_preflight_rejects_wrong_configuration_link_or_relay() -> None:
    exact = list(_topology().peripherals)
    request_index = next(
        index for index, item in enumerate(exact) if item.peripheral_id == "request_dispatcher"
    )
    request = exact[request_index]
    exact[request_index] = PeripheralFact(
        request.peripheral_id,
        request.peripheral_type,
        request.status,
        {"configuration_peripheral_id": "wrong_configuration"},
    )
    with pytest.raises(PilotGuardError, match="configuration link"):
        validate_preflight(_config(), _topology(facts=tuple(exact)), now_ms=NOW_MS)

    exact = list(_topology().peripherals)
    remote_index = next(
        index for index, item in enumerate(exact) if item.peripheral_id == "remote_bridge"
    )
    remote = exact[remote_index]
    exact[remote_index] = PeripheralFact(
        remote.peripheral_id,
        remote.peripheral_type,
        remote.status,
        {"relay_device": "another-panel"},
    )
    with pytest.raises(PilotGuardError, match="relay"):
        validate_preflight(_config(), _topology(facts=tuple(exact)), now_ms=NOW_MS)


def test_preflight_requires_type3_exact_process_configs_and_host_identity() -> None:
    base = _topology()
    with pytest.raises(PilotGuardError, match="DeviceType 3"):
        validate_preflight(
            _config(),
            BvdTopology(
                base.owning_device_id,
                base.configuration_owner,
                base.owner_timestamp_ms,
                1,
                base.stock_host_running,
                base.stock_host_identity,
                base.process_config_peripheral_ids,
                base.peripherals,
            ),
            now_ms=NOW_MS,
        )
    with pytest.raises(PilotGuardError, match="process configuration"):
        validate_preflight(
            _config(),
            BvdTopology(
                base.owning_device_id,
                base.configuration_owner,
                base.owner_timestamp_ms,
                base.bvd_device_type,
                base.stock_host_running,
                base.stock_host_identity,
                frozenset({"solar_peripheral"}),
                base.peripherals,
            ),
            now_ms=NOW_MS,
        )


def test_active_and_postflight_have_distinct_exact_topologies() -> None:
    baseline = _topology()
    pilot = PeripheralFact(
        peripheral_id_for_entity(TARGET_ENTITY_ID),
        27,
        1,
        {"configuration_peripheral_id": BVD_CONFIGURATION_PERIPHERAL_ID},
    )
    active = _topology(
        owner_timestamp_ms=NOW_MS + 1,
        facts=(*baseline.peripherals, pilot),
    )
    validate_active_topology(baseline, active)
    validate_postflight(baseline, _topology(owner_timestamp_ms=NOW_MS + 2))

    with pytest.raises(PilotGuardError, match="active peripheral set"):
        validate_active_topology(baseline, baseline)
    with pytest.raises(PilotGuardError, match="postflight peripheral set"):
        validate_postflight(baseline, active)


def _manifest() -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "mapping_version": 1,
            "revision": 12,
            "generated_at_ms": NOW_MS,
            "entities": [
                {
                    "stable_id": STABLE_ID,
                    "entity_id": TARGET_ENTITY_ID,
                    "domain": "light",
                    "device_class": None,
                    "friendly_name": "Backyard",
                    "ha_area": "Backyard",
                    "brilliant_room": "Backyard",
                    "commands": ["turn_on", "turn_off", "set_brightness"],
                    "capabilities": {"brightness": True},
                }
            ],
            "unsupported_domains": [],
        }
    )


def test_manifest_authority_requires_retained_exact_light_command_route() -> None:
    authority = validate_manifest_authority(_manifest(), retained=True)
    assert authority.revision == 12
    assert authority.generated_at_ms == NOW_MS

    with pytest.raises(PilotGuardError, match="retained"):
        validate_manifest_authority(_manifest(), retained=False)
    value = cast(dict[str, object], json.loads(_manifest()))
    entities = cast(list[dict[str, object]], value["entities"])
    entities[0]["commands"] = ["turn_on", "turn_off"]
    with pytest.raises(PilotGuardError, match="commands"):
        validate_manifest_authority(json.dumps(value), retained=True)
    entities[0]["commands"] = ["turn_on", "turn_off", "set_brightness"]
    entities[0]["capabilities"] = {"brightness": False}
    with pytest.raises(PilotGuardError, match="brightness capability"):
        validate_manifest_authority(json.dumps(value), retained=True)


def test_bus_boundary_is_read_only_and_source_has_no_owner_forwarding_rpc() -> None:
    methods = set(BvdBus.__dict__)
    assert methods >= {"snapshot", "peripheral_exists", "on_reconnect", "shutdown"}
    assert methods.isdisjoint({"set_variables", "request_owner", "release_owner"})

    source = (
        Path(__file__).resolve().parents[1] / "tools" / "brilliant_bvd" / "single_light_pilot.py"
    ).read_text(encoding="utf-8")
    assert "request_set_variables_in_peripheral" not in source


class _RoomAssignment:
    def __init__(self, *, room_ids: list[str]) -> None:
        self.room_ids = room_ids

    def __eq__(self, other: object) -> bool:
        return isinstance(other, _RoomAssignment) and other.room_ids == self.room_ids


def test_light_schema_is_exact_typed_and_links_to_bvd_configuration() -> None:
    config = _config()
    variables = build_light_variables(config, room_assignment_type=_RoomAssignment)

    assert variables == {
        "on": VariableDefinition(int, True, 0),
        "intensity": VariableDefinition(int, True, 500),
        "dimmable": VariableDefinition(int, False, 1),
        "max_intensity_value": VariableDefinition(int, False, 1_000),
        "minimum_dim_level": VariableDefinition(int, True, 100),
        "maximum_dim_level": VariableDefinition(int, True, 1_000),
        "display_name": VariableDefinition(str, True, config.display_name),
        "room_assignment": VariableDefinition(
            _RoomAssignment,
            True,
            _RoomAssignment(room_ids=[config.room_assignment_id]),
        ),
        "mode_transition_settings": VariableDefinition(str, True, "{}"),
        "configuration_peripheral_id": VariableDefinition(
            str,
            False,
            BVD_CONFIGURATION_PERIPHERAL_ID,
        ),
    }


@pytest.mark.parametrize(
    ("brightness", "intensity"),
    [(0, 0), (1, 4), (127, 498), (128, 502), (254, 996), (255, 1_000)],
)
def test_brightness_to_intensity_rounds_half_up(brightness: int, intensity: int) -> None:
    assert brightness_to_intensity(brightness) == intensity


@pytest.mark.parametrize(
    ("intensity", "brightness"),
    [(0, 0), (1, 0), (499, 127), (500, 128), (999, 255), (1_000, 255)],
)
def test_intensity_to_brightness_rounds_half_up(intensity: int, brightness: int) -> None:
    assert intensity_to_brightness(intensity) == brightness


@pytest.mark.parametrize("value", [-1, 256, True, 1.0, "1"])
def test_brightness_rejects_non_integer_or_out_of_range(value: object) -> None:
    with pytest.raises(PilotGuardError, match="brightness"):
        brightness_to_intensity(value)


@pytest.mark.parametrize("value", [-1, 1_001, True, 1.0, "1"])
def test_intensity_rejects_non_integer_or_out_of_range(value: object) -> None:
    with pytest.raises(PilotGuardError, match="intensity"):
        intensity_to_brightness(value)


def _state(
    *,
    sequence: int = 7,
    generated_at_ms: int = NOW_MS,
    available: bool = True,
    state: str = "on",
    brightness: int | None = 128,
) -> str:
    attributes: dict[str, object] = {}
    if brightness is not None:
        attributes["brightness"] = brightness
    return json.dumps(
        {
            "schema_version": 1,
            "mapping_version": 1,
            "stable_id": STABLE_ID,
            "entity_id": TARGET_ENTITY_ID,
            "sequence": sequence,
            "generated_at_ms": generated_at_ms,
            "available": available,
            "state": state,
            "attributes": attributes,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def _result(
    command_id: str,
    *,
    accepted: bool,
    resulting_sequence: int = 7,
    error: str | None = None,
) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "mapping_version": 1,
            "command_id": command_id,
            "stable_id": STABLE_ID,
            "accepted": accepted,
            "resulting_sequence": resulting_sequence,
            "timestamp_ms": NOW_MS,
            "error": error,
            "elapsed_ms": 12,
        },
        separators=(",", ":"),
        sort_keys=True,
    )


def test_state_decoder_accepts_exact_target_and_scales_brightness() -> None:
    state = decode_state_payload(_state(), stable_id=STABLE_ID)
    assert state.sequence == 7
    assert state.generated_at_ms == NOW_MS
    assert state.available is True
    assert state.on == 1
    assert state.intensity == 502


@pytest.mark.parametrize(
    ("mutate", "message"),
    [
        (lambda value: value.update(entity_id="light.somewhere_else"), "entity"),
        (lambda value: value.update(stable_id=str(UUID(int=1))), "stable_id"),
        (lambda value: value.update(sequence=-1), "sequence"),
        (lambda value: value.update(available="yes"), "availability"),
        (lambda value: value.update(state="unknown"), "state"),
        (lambda value: value.update(extra=True), "fields"),
    ],
)
def test_state_decoder_rejects_wrong_route_shape_or_value(
    mutate: Callable[[dict[str, object]], None], message: str
) -> None:
    value = cast(dict[str, object], json.loads(_state()))
    mutate(value)
    with pytest.raises(PilotGuardError, match=message):
        decode_state_payload(json.dumps(value), stable_id=STABLE_ID)


def test_state_decoder_rejects_boolean_protocol_versions() -> None:
    value = cast(dict[str, object], json.loads(_state()))
    value["schema_version"] = True
    with pytest.raises(PilotGuardError, match="version"):
        decode_state_payload(json.dumps(value), stable_id=STABLE_ID)


def test_available_state_requires_authoritative_brightness() -> None:
    with pytest.raises(PilotGuardError, match="authoritative brightness"):
        decode_state_payload(_state(brightness=None), stable_id=STABLE_ID)


def test_build_command_payload_maps_on_off_and_brightness() -> None:
    on = build_command_payload(
        variable="on",
        value=1,
        observed_sequence=7,
        command_id="11111111-1111-4111-8111-111111111111",
        issued_at_ms=NOW_MS,
    )
    off = build_command_payload(
        variable="on",
        value="0",
        observed_sequence=7,
        command_id="22222222-2222-4222-8222-222222222222",
        issued_at_ms=NOW_MS,
    )
    brightness = build_command_payload(
        variable="intensity",
        value=500,
        observed_sequence=7,
        command_id="33333333-3333-4333-8333-333333333333",
        issued_at_ms=NOW_MS,
    )

    assert on["kind"] == "turn_on" and on["value"] is None
    assert off["kind"] == "turn_off" and off["value"] is None
    assert brightness["kind"] == "set_brightness" and brightness["value"] == 128
    assert on["stable_id"] == STABLE_ID
    assert on["observed_sequence"] == 7


class _Publisher:
    def __init__(self) -> None:
        self.publications: list[tuple[str, str, bool]] = []

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        self.publications.append((topic, payload, retain))


class _FailOncePublisher(_Publisher):
    def __init__(self) -> None:
        super().__init__()
        self._failed = False

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        if not self._failed:
            self._failed = True
            raise RuntimeError("publish failed")
        await super().publish(topic, payload, retain)


class _Sink:
    def __init__(self) -> None:
        self.updates: list[dict[str, object]] = []

    async def update_variables(self, values: Mapping[str, object]) -> None:
        self.updates.append(dict(values))


def _controller() -> tuple[PilotController, _Publisher, _Sink]:
    publisher = _Publisher()
    sink = _Sink()
    ids = iter(
        [
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
            "33333333-3333-4333-8333-333333333333",
        ]
    )
    controller = PilotController(
        publisher=publisher,
        state_sink=sink,
        command_id_factory=lambda: next(ids),
        clock_ms=lambda: NOW_MS,
        monotonic_ms=lambda: NOW_MS,
    )
    return controller, publisher, sink


async def test_authoritative_state_reflects_without_publishing_command() -> None:
    controller, publisher, sink = _controller()

    changed = await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    assert changed is True
    assert sink.updates == [{"on": 1, "intensity": 502}]
    assert publisher.publications == []
    assert controller.observed_sequence == 7


async def test_initial_and_post_fence_authority_must_be_retained() -> None:
    controller, _, sink = _controller()

    with pytest.raises(PilotGuardError, match="initial HA state authority must be retained"):
        await controller.handle_state_message(
            state_topic(STABLE_ID),
            _state(),
            retained=False,
        )
    assert controller.authority_available is False
    assert sink.updates == []

    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    assert controller.authority_available is True
    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=8, generated_at_ms=NOW_MS + 1, brightness=129),
        retained=False,
    )
    assert controller.authority_available is True

    await controller.fence_transport()
    assert controller.authority_available is False
    with pytest.raises(PilotGuardError, match="initial HA state authority must be retained"):
        await controller.handle_state_message(
            state_topic(STABLE_ID),
            _state(sequence=8, generated_at_ms=NOW_MS + 1, brightness=129),
            retained=False,
        )


async def test_command_requires_authority_and_is_non_retained() -> None:
    controller, publisher, _ = _controller()
    with pytest.raises(PilotGuardError, match="authoritative"):
        await controller.handle_panel_push("on", 1)

    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    assert await controller.handle_panel_push("intensity", "900") is True

    topic, payload, retained = publisher.publications[-1]
    assert topic == command_topic(STABLE_ID)
    assert retained is False
    decoded = json.loads(payload)
    assert decoded["kind"] == "set_brightness"
    assert decoded["value"] == 230
    assert decoded["observed_sequence"] == 7


async def test_intensity_is_not_a_noop_while_authoritative_light_is_off() -> None:
    controller, publisher, _ = _controller()
    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(state="off", brightness=128),
        retained=True,
    )

    assert await controller.handle_panel_push("intensity", 500) is True

    topic, payload, retained = publisher.publications[-1]
    assert topic == command_topic(STABLE_ID)
    assert retained is False
    decoded = json.loads(payload)
    assert decoded["kind"] == "set_brightness"
    assert decoded["value"] == 128


async def test_authoritative_noop_push_is_suppressed_and_pending_age_is_visible() -> None:
    controller, publisher, sink = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    assert await controller.handle_panel_push("on", 1) is False
    assert await controller.handle_panel_push("intensity", 503) is False
    assert publisher.publications == []
    assert sink.updates[-1] == {"on": 1, "intensity": 502}
    assert len(sink.updates) == 2
    assert controller.pending_command_age_ms(now_ms=NOW_MS + 10) is None

    assert await controller.handle_panel_push("intensity", 900) is True
    assert controller.pending_command_age_ms(now_ms=NOW_MS + 123) == 123

    command_id = cast(dict[str, object], json.loads(publisher.publications[0][1]))["command_id"]
    assert isinstance(command_id, str)
    assert controller.accepts_result_topic(result_topic(command_id))
    assert not controller.accepts_result_topic(result_topic("22222222-2222-4222-8222-222222222222"))


async def test_pending_age_uses_monotonic_time_not_the_command_wall_clock() -> None:
    publisher = _Publisher()
    sink = _Sink()
    wall_ms = [NOW_MS]
    monotonic_ms = [10_000]
    controller = PilotController(
        publisher=publisher,
        state_sink=sink,
        command_id_factory=lambda: "11111111-1111-4111-8111-111111111111",
        clock_ms=lambda: wall_ms[0],
        monotonic_ms=lambda: monotonic_ms[0],
    )
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    assert await controller.handle_panel_push("on", 0) is True
    wall_ms[0] = 1
    monotonic_ms[0] = 25_000

    assert controller.pending_command_age_ms() == 15_000
    assert json.loads(publisher.publications[0][1])["issued_at_ms"] == NOW_MS


async def test_publish_failure_clears_reservation_for_a_real_retry() -> None:
    publisher = _FailOncePublisher()
    sink = _Sink()
    ids = iter(
        [
            "11111111-1111-4111-8111-111111111111",
            "22222222-2222-4222-8222-222222222222",
        ]
    )
    controller = PilotController(
        publisher=publisher,
        state_sink=sink,
        command_id_factory=lambda: next(ids),
        clock_ms=lambda: NOW_MS,
        monotonic_ms=lambda: NOW_MS,
    )
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    with pytest.raises(RuntimeError, match="publish failed"):
        await controller.handle_panel_push("on", 0)
    assert controller.pending_command_age_ms(now_ms=NOW_MS) is None
    assert await controller.handle_panel_push("on", 0) is True


async def test_duplicate_is_suppressed_until_new_state_or_rejection() -> None:
    controller, publisher, sink = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    assert await controller.handle_panel_push("on", 0) is True
    assert await controller.handle_panel_push("on", 0) is False
    assert len(publisher.publications) == 1

    command_id = cast(dict[str, object], json.loads(publisher.publications[0][1]))["command_id"]
    assert isinstance(command_id, str)
    with pytest.raises(PilotGuardError, match="rejected"):
        await controller.handle_result_message(
            result_topic(command_id),
            _result(command_id, accepted=False, error="observed_sequence is stale"),
            retained=False,
        )
    assert sink.updates[-1] == {"on": 1, "intensity": 502}
    assert await controller.handle_panel_push("on", 0) is True


async def test_slider_burst_has_one_in_flight_and_coalesces_latest_value() -> None:
    controller, publisher, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    assert await controller.handle_panel_push("intensity", 100) is True
    assert await controller.handle_panel_push("intensity", 200) is False
    assert await controller.handle_panel_push("intensity", 900) is False
    assert len(publisher.publications) == 1

    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=8, generated_at_ms=NOW_MS + 1, brightness=26),
        retained=True,
    )

    assert len(publisher.publications) == 2
    queued = cast(dict[str, object], json.loads(publisher.publications[-1][1]))
    assert queued["kind"] == "set_brightness"
    assert queued["value"] == 230
    assert queued["observed_sequence"] == 8


async def test_slider_burst_return_to_in_flight_value_clears_stale_queue() -> None:
    controller, publisher, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    assert await controller.handle_panel_push("intensity", 100) is True
    assert await controller.handle_panel_push("intensity", 900) is False
    assert await controller.handle_panel_push("intensity", 100) is False
    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=8, generated_at_ms=NOW_MS + 1, brightness=26),
        retained=False,
    )

    assert len(publisher.publications) == 1


async def test_coalesced_value_is_dropped_if_new_authority_already_matches() -> None:
    controller, publisher, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    assert await controller.handle_panel_push("intensity", 100) is True
    assert await controller.handle_panel_push("intensity", 900) is False

    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=8, generated_at_ms=NOW_MS + 1, brightness=230),
        retained=True,
    )

    assert len(publisher.publications) == 1


async def test_unrelated_advancing_state_does_not_settle_in_flight_command() -> None:
    controller, publisher, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    assert await controller.handle_panel_push("on", 0) is True
    assert await controller.handle_panel_push("intensity", 900) is False

    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=8, generated_at_ms=NOW_MS + 1, state="on", brightness=129),
        retained=False,
    )

    assert len(publisher.publications) == 1
    assert controller.pending_command_age_ms(now_ms=NOW_MS + 14_999) == 14_999

    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=9, generated_at_ms=NOW_MS + 2, state="off", brightness=129),
        retained=False,
    )
    assert len(publisher.publications) == 2
    queued = cast(dict[str, object], json.loads(publisher.publications[-1][1]))
    assert queued["kind"] == "set_brightness"
    assert queued["observed_sequence"] == 9


async def test_same_coordinate_conflicting_state_is_rejected_even_after_fence() -> None:
    controller, _, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    await controller.fence_transport()

    with pytest.raises(PilotGuardError, match="conflicts"):
        await controller.handle_state_message(
            state_topic(STABLE_ID),
            _state(state="off", brightness=128),
            retained=True,
        )


async def test_accepted_result_is_not_authority_and_waits_for_state_echo() -> None:
    controller, publisher, sink = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    await controller.handle_panel_push("on", 0)
    command_id = cast(dict[str, object], json.loads(publisher.publications[0][1]))["command_id"]
    assert isinstance(command_id, str)

    assert await controller.handle_result_message(
        result_topic(command_id), _result(command_id, accepted=True), retained=False
    )
    assert sink.updates == [{"on": 1, "intensity": 502}]
    assert await controller.handle_panel_push("on", 0) is False

    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=8, generated_at_ms=NOW_MS + 1, state="off", brightness=128),
        retained=True,
    )
    assert sink.updates[-1] == {"on": 0, "intensity": 502}


async def test_canonical_v1_result_without_optional_diagnostics_is_accepted() -> None:
    controller, publisher, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    await controller.handle_panel_push("on", 0)
    command_id = cast(dict[str, object], json.loads(publisher.publications[0][1]))["command_id"]
    assert isinstance(command_id, str)
    vectors_path = Path(__file__).parent / "fixtures" / "ha_control_v1_vectors.json"
    vectors = cast(dict[str, object], json.loads(vectors_path.read_text(encoding="utf-8")))
    payloads = cast(dict[str, object], vectors["payloads"])
    entity_result = cast(dict[str, object], payloads["entity_result"])
    result = cast(dict[str, object], entity_result["value"])
    result["stable_id"] = STABLE_ID

    assert await controller.handle_result_message(
        result_topic(command_id),
        json.dumps(result),
        retained=False,
    )
    assert controller.pending_command_age_ms(now_ms=NOW_MS) == 0


async def test_transport_fence_then_identical_replay_restores_complete_state() -> None:
    controller, _, sink = _controller()
    payload = _state()
    await controller.handle_state_message(state_topic(STABLE_ID), payload, retained=True)
    await controller.fence_transport()
    assert sink.updates[-1] == {"on": 0}

    assert await controller.handle_state_message(state_topic(STABLE_ID), payload, retained=True)
    assert sink.updates[-1] == {"on": 1, "intensity": 502}
    assert await controller.handle_panel_push("on", 0) is True


async def test_unavailable_state_is_off_and_fenced_until_available() -> None:
    controller, _, sink = _controller()
    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(available=False, state="unavailable", brightness=None),
        retained=True,
    )
    assert sink.updates == [{"on": 0}]
    assert controller.authority_available is False
    with pytest.raises(PilotGuardError, match="authoritative"):
        await controller.handle_panel_push("on", 1)


async def test_state_rejects_older_timestamp_and_same_epoch_sequence_regression() -> None:
    controller, _, _ = _controller()
    await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)
    with pytest.raises(PilotGuardError, match="predates"):
        await controller.handle_state_message(
            state_topic(STABLE_ID),
            _state(sequence=8, generated_at_ms=NOW_MS - 1),
            retained=True,
        )
    with pytest.raises(PilotGuardError, match="regressed"):
        await controller.handle_state_message(
            state_topic(STABLE_ID),
            _state(sequence=6, generated_at_ms=NOW_MS),
            retained=True,
        )


class _LifecycleHost:
    def __init__(self, events: list[str]) -> None:
        self.events = events
        self.start_calls = 0
        self.started_variables: Mapping[str, VariableDefinition] | None = None
        self.delete_calls: list[tuple[str, int]] = []
        self.start_error: BaseException | None = None
        self.delete_failures = 0

    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, VariableDefinition],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None:
        del on_command
        self.started_variables = variables
        self.start_calls += 1
        self.events.append(f"start:{virtual_device_id}:{peripheral_id}")
        if self.start_error is not None:
            raise self.start_error

    async def update_variables(self, values: Mapping[str, object]) -> None:
        del values

    async def delete(self, peripheral_id: str, deletion_time_ms: int) -> None:
        self.delete_calls.append((peripheral_id, deletion_time_ms))
        self.events.append(f"delete:{deletion_time_ms}")
        if self.delete_failures:
            self.delete_failures -= 1
            raise RuntimeError("delete failed")

    async def shutdown(self) -> None:
        self.events.append("host-shutdown")


class _Probe:
    def __init__(
        self,
        *,
        name: str,
        present: bool,
        events: list[str],
        read_error: BaseException | None = None,
    ) -> None:
        self.name = name
        self.present = present
        self.events = events
        self.read_error = read_error

    async def contains(self, device_id: str, peripheral_id: str) -> bool:
        self.events.append(f"read:{self.name}:{device_id}:{peripheral_id}")
        if self.read_error is not None:
            raise self.read_error
        return self.present

    async def shutdown(self) -> None:
        self.events.append(f"probe-shutdown:{self.name}")


class _ProbeFactory:
    def __init__(
        self,
        events: list[str],
        outcomes: list[bool | BaseException],
    ) -> None:
        self.events = events
        self.outcomes = outcomes
        self.created: list[_Probe] = []

    async def __call__(self) -> _Probe:
        outcome = self.outcomes.pop(0)
        probe = _Probe(
            name=str(len(self.created) + 1),
            present=outcome if isinstance(outcome, bool) else False,
            events=self.events,
            read_error=outcome if isinstance(outcome, BaseException) else None,
        )
        self.created.append(probe)
        return probe


def _lifecycle(
    *,
    host: _LifecycleHost,
    probes: _ProbeFactory,
    events: list[str],
    operation_timeout_s: float = 1.0,
) -> PilotLifecycle:
    async def sleep(delay: float) -> None:
        events.append(f"sleep:{delay:g}")

    async def on_command(variable: str, value: object) -> bool:
        del variable, value
        return True

    return PilotLifecycle(
        config=_config(),
        host=host,
        room_assignment_type=_RoomAssignment,
        on_command=on_command,
        probe_factory=probes,
        clock_ms=lambda: NOW_MS,
        sleep=sleep,
        absence_interval_s=30.0,
        operation_timeout_s=operation_timeout_s,
    )


def test_lifecycle_protocols_remain_off_panel() -> None:
    assert set(dir(VirtualLightHost)) >= {
        "start",
        "update_variables",
        "delete",
        "shutdown",
    }
    assert set(ScopedPeripheralProbe.__dict__) >= {"contains", "shutdown"}


async def test_lifecycle_starts_once_with_exact_immutable_bvd_identity() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    lifecycle = _lifecycle(
        host=host,
        probes=_ProbeFactory(events, [False, False]),
        events=events,
    )

    await lifecycle.start()
    await lifecycle.start()

    assert host.start_calls == 1
    assert events[0] == (f"start:{BVD_DEVICE_ID}:{peripheral_id_for_entity(TARGET_ENTITY_ID)}")


async def test_lifecycle_registers_with_buffered_authoritative_defaults() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)

    async def on_command(variable: str, value: object) -> bool:
        del variable, value
        return True

    lifecycle = PilotLifecycle(
        config=_config(),
        host=host,
        room_assignment_type=_RoomAssignment,
        on_command=on_command,
        probe_factory=_ProbeFactory(events, [False, False]),
        initial_values={"on": 1, "intensity": 900},
    )
    await lifecycle.start()

    assert host.started_variables is not None
    assert host.started_variables["on"].default_value == 1
    assert host.started_variables["intensity"].default_value == 900


@pytest.mark.parametrize("failure", [RuntimeError("partial"), asyncio.CancelledError()])
async def test_partial_start_failure_still_deletes_exact_id(
    failure: BaseException,
) -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    host.start_error = failure
    lifecycle = _lifecycle(
        host=host,
        probes=_ProbeFactory(events, [False, False]),
        events=events,
    )

    with pytest.raises(type(failure)):
        await lifecycle.start()
    report = await lifecycle.cleanup()

    assert report == CleanupReport(False, True, True)
    assert host.delete_calls == [(peripheral_id_for_entity(TARGET_ENTITY_ID), NOW_MS)]


async def test_start_timeout_is_cleanup_eligible() -> None:
    events: list[str] = []

    class _HangingHost(_LifecycleHost):
        async def start(
            self,
            *,
            peripheral_id: str,
            virtual_device_id: str,
            variables: Mapping[str, VariableDefinition],
            on_command: Callable[[str, object], Awaitable[bool]],
        ) -> None:
            await super().start(
                peripheral_id=peripheral_id,
                virtual_device_id=virtual_device_id,
                variables=variables,
                on_command=on_command,
            )
            await asyncio.Event().wait()

    host = _HangingHost(events)
    lifecycle = _lifecycle(
        host=host,
        probes=_ProbeFactory(events, [False, False]),
        events=events,
        operation_timeout_s=0.01,
    )
    with pytest.raises(PilotGuardError, match="registration.*exceeded"):
        await lifecycle.start()
    await lifecycle.cleanup()
    assert host.delete_calls == [(peripheral_id_for_entity(TARGET_ENTITY_ID), NOW_MS)]


async def test_cleanup_deletes_then_shuts_host_before_two_independent_reads() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    probes = _ProbeFactory(events, [False, False])
    lifecycle = _lifecycle(host=host, probes=probes, events=events)
    await lifecycle.start()

    report = await lifecycle.cleanup()

    assert report == CleanupReport(False, True, True)
    assert len(probes.created) == 2 and probes.created[0] is not probes.created[1]
    cleanup_events = events[1:]
    assert cleanup_events == [
        f"delete:{NOW_MS}",
        "host-shutdown",
        f"read:1:{BVD_DEVICE_ID}:{peripheral_id_for_entity(TARGET_ENTITY_ID)}",
        "probe-shutdown:1",
        "sleep:30",
        f"read:2:{BVD_DEVICE_ID}:{peripheral_id_for_entity(TARGET_ENTITY_ID)}",
        "probe-shutdown:2",
    ]

    assert await lifecycle.cleanup() == CleanupReport(True, True, True)
    assert host.delete_calls == [(peripheral_id_for_entity(TARGET_ENTITY_ID), NOW_MS)]


async def test_cleanup_retries_delete_before_shutting_host() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    host.delete_failures = 1
    lifecycle = _lifecycle(
        host=host,
        probes=_ProbeFactory(events, [False, False]),
        events=events,
    )
    await lifecycle.start()

    await lifecycle.cleanup()
    assert len(host.delete_calls) == 2
    assert events.count("host-shutdown") == 1
    assert events.index("delete:1800000000000", 2) < events.index("host-shutdown")


async def test_repeated_delete_failure_still_shuts_host_and_guard() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    host.delete_failures = 2

    async def before_probes() -> None:
        events.append("guard-shutdown")

    lifecycle = PilotLifecycle(
        config=_config(),
        host=host,
        room_assignment_type=_RoomAssignment,
        on_command=lambda _variable, _value: asyncio.sleep(0, result=True),
        probe_factory=_ProbeFactory(events, []),
        before_probes=before_probes,
        clock_ms=lambda: NOW_MS,
    )
    await lifecycle.start()

    with pytest.raises(CleanupError, match="deletion failed after two attempts"):
        await lifecycle.cleanup()

    assert len(host.delete_calls) == 2
    assert events[-2:] == ["host-shutdown", "guard-shutdown"]


async def test_probe_failure_retries_proof_without_redeleting() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    probes = _ProbeFactory(events, [RuntimeError("read failed"), False, False])
    lifecycle = _lifecycle(host=host, probes=probes, events=events)
    await lifecycle.start()

    with pytest.raises(RuntimeError, match="read failed"):
        await lifecycle.cleanup()
    assert events.count("probe-shutdown:1") == 1

    await lifecycle.cleanup()
    assert len(host.delete_calls) == 1
    assert len(probes.created) == 3


async def test_residual_presence_is_a_hard_cleanup_failure() -> None:
    events: list[str] = []
    host = _LifecycleHost(events)
    lifecycle = _lifecycle(
        host=host,
        probes=_ProbeFactory(events, [True]),
        events=events,
    )
    await lifecycle.start()

    with pytest.raises(CleanupError, match="still present"):
        await lifecycle.cleanup()
