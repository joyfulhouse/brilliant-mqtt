from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from tools.brilliant_vc.gates import GateLedger, GateName, GateStatus
from tools.brilliant_vc.single_light_pilot import (
    LIGHT_PERIPHERAL_TYPE,
    PHYSICAL_BUS_SOCKET,
    CleanupError,
    PeripheralRecord,
    PilotConfig,
    PilotController,
    PilotGuardError,
    PilotLease,
    PilotLifecycle,
    TopologySnapshot,
    VariableDefinition,
    _canonical_vc_socket,
    _finish_live_resources,
    _LivePublisher,
    _normalize_framework_push,
    _parser,
    _record_live_peripheral,
    _require_matching_live_topology,
    _run_reconnecting_transport,
    _StateReplayTimeout,
    _wait_for_session_authority,
    brightness_to_intensity,
    build_command_payload,
    build_variable_definitions,
    command_topic,
    discover_configuration_peripheral,
    intensity_to_brightness,
    peripheral_id_for,
    state_topic,
    validate_gate_ledger,
    validate_topology,
)

STABLE_ID = "d353e38a-793e-5b6f-813b-17a1c38aba96"
VC_ID = "a" * 32
OFFICE_ID = "b" * 32
ROOM_ID = "room-backyard"
CONFIG_ID = "device_config_peripheral"
SOCKET = "/run/brilliant-vc/server_socket"
COMMAND_ID = "11111111-1111-4111-8111-111111111111"


@dataclass(eq=True)
class FakeRoomAssignment:
    room_ids: list[str]


def _config(*, display_name: str = "HA VC Pilot Light") -> PilotConfig:
    return PilotConfig(
        stable_id=STABLE_ID,
        display_name=display_name,
        room_id=ROOM_ID,
        vc_device_id=VC_ID,
        office_device_id=OFFICE_ID,
        vc_socket=SOCKET,
        runtime_s=1_800,
    )


def _topology() -> TopologySnapshot:
    return TopologySnapshot(
        owner_device_id=VC_ID,
        device_type=6,
        peripherals=(
            PeripheralRecord(
                owner_device_id=VC_ID,
                peripheral_id=CONFIG_ID,
                role="configuration",
                peripheral_type=19,
            ),
            PeripheralRecord(VC_ID, "art_config_peripheral", "configuration", 16),
            PeripheralRecord(
                VC_ID,
                "motion_detection_config_peripheral",
                "configuration",
                20,
            ),
            PeripheralRecord(VC_ID, "alarm_config_peripheral", "configuration", 48),
        ),
        room_ids=frozenset({ROOM_ID}),
    )


def _state(
    *,
    sequence: int = 7,
    generated_at_ms: int = 1_700_000_000_000,
    state: str = "on",
    brightness: int = 128,
    available: bool = True,
) -> str:
    return json.dumps(
        {
            "schema_version": 1,
            "mapping_version": 1,
            "stable_id": STABLE_ID,
            "entity_id": "light.ha_vc_pilot",
            "sequence": sequence,
            "generated_at_ms": generated_at_ms,
            "available": available,
            "state": state,
            "attributes": {"brightness": brightness},
        }
    )


def test_peripheral_id_is_stable_across_display_rename() -> None:
    first = peripheral_id_for(_config().stable_id)
    second = peripheral_id_for(_config(display_name="Renamed").stable_id)

    assert first == second == "ha_vc_d353e38a793e5b6f813b17a1c38aba96"


@pytest.mark.parametrize(
    ("field", "value", "message"),
    [
        ("stable_id", "not-a-uuid", "stable_id"),
        ("vc_device_id", "brilliant_virtual_device", "VC device ID"),
        ("vc_device_id", "configuration_virtual_device", "VC device ID"),
        ("vc_device_id", "ble_mesh", "VC device ID"),
        ("vc_device_id", OFFICE_ID, "physical Office"),
        ("vc_device_id", "A" * 32, "lowercase hex"),
        ("vc_socket", PHYSICAL_BUS_SOCKET, "physical Control bus"),
        (
            "vc_socket",
            "/var/run/brilliant-vc/../brilliant/server_socket",
            "physical Control bus",
        ),
        ("runtime_s", 179, "from 180"),
        ("runtime_s", 1_801, "at most 1800"),
    ],
)
def test_config_fails_closed_on_unsafe_identity_or_runtime(
    field: str, value: object, message: str
) -> None:
    data = {
        "stable_id": STABLE_ID,
        "display_name": "HA VC Pilot Light",
        "room_id": ROOM_ID,
        "vc_device_id": VC_ID,
        "office_device_id": OFFICE_ID,
        "vc_socket": SOCKET,
        "runtime_s": 1_800,
    }
    data[field] = value

    with pytest.raises(PilotGuardError, match=message):
        PilotConfig(**data)  # type: ignore[arg-type]


def test_topology_requires_type_6_exact_owner_room_and_own_configuration() -> None:
    topology = _topology()

    assert validate_topology(_config(), topology) == CONFIG_ID

    with pytest.raises(PilotGuardError, match="DeviceType 6"):
        validate_topology(
            _config(),
            TopologySnapshot(
                owner_device_id=VC_ID,
                device_type=1,
                peripherals=topology.peripherals,
                room_ids=topology.room_ids,
            ),
        )
    with pytest.raises(PilotGuardError, match="room"):
        validate_topology(
            _config(),
            TopologySnapshot(
                owner_device_id=VC_ID,
                device_type=6,
                peripherals=topology.peripherals,
                room_ids=frozenset(),
            ),
        )


def test_live_topology_requires_exact_room_and_peripheral_snapshot() -> None:
    expected = _topology()
    _require_matching_live_topology(expected, expected, _config())

    with pytest.raises(PilotGuardError, match="room catalog"):
        _require_matching_live_topology(
            expected,
            TopologySnapshot(
                owner_device_id=VC_ID,
                device_type=6,
                peripherals=expected.peripherals,
                room_ids=frozenset({ROOM_ID, "another-room"}),
            ),
            _config(),
        )
    with pytest.raises(PilotGuardError, match="peripheral set"):
        _require_matching_live_topology(
            expected,
            TopologySnapshot(
                owner_device_id=VC_ID,
                device_type=6,
                peripherals=expected.peripherals + (PeripheralRecord(VC_ID, "extra", "other", 27),),
                room_ids=expected.room_ids,
            ),
            _config(),
        )


def test_socket_canonicalization_rejects_symlink_escape(tmp_path: Path) -> None:
    allowed = tmp_path / "brilliant-vc"
    allowed.mkdir()
    outside = tmp_path / "physical-server-socket"
    link = allowed / "server_socket"
    link.symlink_to(outside)

    with pytest.raises(PilotGuardError, match="resolve inside"):
        _canonical_vc_socket(
            str(link),
            allowed_roots=(allowed,),
            physical_socket=tmp_path / "different-physical-socket",
        )


def test_live_process_lease_allows_only_one_apply_and_can_be_reacquired(tmp_path: Path) -> None:
    runtime = tmp_path / "brilliant-vc"
    runtime.mkdir(mode=0o700)
    first = PilotLease.acquire(
        runtime,
        required_uid=os.geteuid(),
        allowed_roots=(runtime,),
    )
    try:
        with pytest.raises(PilotGuardError, match="already active"):
            PilotLease.acquire(
                runtime,
                required_uid=os.geteuid(),
                allowed_roots=(runtime,),
            )
    finally:
        first.release()

    second = PilotLease.acquire(
        runtime,
        required_uid=os.geteuid(),
        allowed_roots=(runtime,),
    )
    second.release()
    assert (runtime / "single-light-pilot.lock").stat().st_mode & 0o777 == 0o600


def test_live_pilot_lease_uses_a_root_control_dir_not_the_service_socket_dir() -> None:
    args = _parser().parse_args(
        [
            "--vc-identity-dir",
            "/data/brilliant-vc-private/identity",
            "--topology-json",
            "/data/brilliant-vc/evidence/topology.json",
            "--ledger",
            "/data/brilliant-vc/evidence/ledger.json",
            "--run-id",
            "pilot-run",
            "--stable-id",
            STABLE_ID,
            "--display-name",
            "HA VC Pilot Light",
            "--room-id",
            ROOM_ID,
            "--office-device-id",
            OFFICE_ID,
        ]
    )

    assert args.lease_dir == Path("/run/brilliant-vc-control")
    assert args.lease_dir != Path(args.vc_socket).parent


def test_live_process_lease_rejects_a_directory_outside_the_control_roots(
    tmp_path: Path,
) -> None:
    runtime = tmp_path / "brilliant-vc"
    runtime.mkdir(mode=0o700)

    with pytest.raises(PilotGuardError, match="control root"):
        PilotLease.acquire(
            runtime,
            required_uid=os.geteuid(),
            allowed_roots=(tmp_path / "different-control-root",),
        )


def test_configuration_discovery_rejects_shared_physical_and_ambiguous_candidates() -> None:
    with pytest.raises(PilotGuardError, match="own configuration"):
        discover_configuration_peripheral(
            VC_ID,
            (
                PeripheralRecord(
                    owner_device_id="configuration_virtual_device",
                    peripheral_id="brilliant_virtual_device_configuration",
                    role="configuration",
                    peripheral_type=35,
                ),
            ),
        )

    candidates = (
        PeripheralRecord(VC_ID, CONFIG_ID, "configuration", 19),
        PeripheralRecord(VC_ID, CONFIG_ID, "configuration", 19),
    )
    with pytest.raises(PilotGuardError, match="exactly one"):
        discover_configuration_peripheral(VC_ID, candidates)

    with pytest.raises(PilotGuardError, match="wrong peripheral ID"):
        discover_configuration_peripheral(
            VC_ID,
            (PeripheralRecord(VC_ID, "renamed_device_config", "configuration", 19),),
        )

    assert (
        discover_configuration_peripheral(
            VC_ID,
            (
                PeripheralRecord(VC_ID, "art_config_peripheral", "configuration", 16),
                PeripheralRecord(VC_ID, CONFIG_ID, "configuration", 19),
                PeripheralRecord(
                    VC_ID,
                    "motion_detection_config_peripheral",
                    "configuration",
                    20,
                ),
                PeripheralRecord(VC_ID, "alarm_config_peripheral", "configuration", 48),
            ),
        )
        == CONFIG_ID
    )


def test_live_peripheral_metadata_is_typed_and_fails_closed() -> None:
    assert _record_live_peripheral(
        VC_ID,
        CONFIG_ID,
        SimpleNamespace(peripheral_type=19),
    ) == PeripheralRecord(VC_ID, CONFIG_ID, "configuration", 19)

    for value in (None, "19", True, -1, 256):
        with pytest.raises(PilotGuardError, match="live peripheral_type"):
            _record_live_peripheral(
                VC_ID,
                CONFIG_ID,
                SimpleNamespace(peripheral_type=value),
            )


def test_topology_snapshot_requires_exact_peripheral_type_metadata() -> None:
    peripherals: list[dict[str, object]] = [
        {
            "owner_device_id": VC_ID,
            "peripheral_id": CONFIG_ID,
            "role": "configuration",
            "peripheral_type": 19,
        }
    ]
    payload = {
        "schema_version": 2,
        "owner_device_id": VC_ID,
        "device_type": 6,
        "peripherals": peripherals,
        "room_ids": [ROOM_ID],
    }

    assert TopologySnapshot.from_payload(payload).peripherals[0].peripheral_type == 19
    del peripherals[0]["peripheral_type"]
    with pytest.raises(PilotGuardError, match="peripheral record"):
        TopologySnapshot.from_payload(payload)


def test_exact_light_variable_schema_and_virtual_owner() -> None:
    definitions = build_variable_definitions(
        _config(),
        configuration_peripheral_id=CONFIG_ID,
        room_assignment_type=FakeRoomAssignment,
    )

    assert definitions == {
        "on": VariableDefinition(int, True, 0),
        "intensity": VariableDefinition(int, True, 500),
        "dimmable": VariableDefinition(int, False, 1),
        "max_intensity_value": VariableDefinition(int, False, 1000),
        "minimum_dim_level": VariableDefinition(int, True, 100),
        "maximum_dim_level": VariableDefinition(int, True, 1000),
        "display_name": VariableDefinition(str, True, "HA VC Pilot Light"),
        "room_assignment": VariableDefinition(
            FakeRoomAssignment,
            True,
            FakeRoomAssignment(room_ids=[ROOM_ID]),
        ),
        "mode_transition_settings": VariableDefinition(str, True, "{}"),
        "configuration_peripheral_id": VariableDefinition(str, False, CONFIG_ID),
    }
    assert LIGHT_PERIPHERAL_TYPE == 27


@pytest.mark.parametrize(
    ("brightness", "intensity"),
    [(0, 0), (1, 4), (127, 498), (128, 502), (254, 996), (255, 1000)],
)
def test_brightness_to_intensity_rounds_half_up(brightness: int, intensity: int) -> None:
    assert brightness_to_intensity(brightness) == intensity


@pytest.mark.parametrize(
    ("intensity", "brightness"),
    [(0, 0), (1, 0), (2, 1), (498, 127), (500, 128), (1000, 255)],
)
def test_intensity_to_brightness_rounds_half_up(intensity: int, brightness: int) -> None:
    assert intensity_to_brightness(intensity) == brightness


@pytest.mark.parametrize("value", [-1, 256, True, "1"])
def test_brightness_rejects_out_of_range_or_non_integer(value: object) -> None:
    with pytest.raises(PilotGuardError, match="brightness"):
        brightness_to_intensity(value)


@pytest.mark.parametrize("value", [-1, 1001, True, "1"])
def test_intensity_rejects_out_of_range_or_non_integer(value: object) -> None:
    with pytest.raises(PilotGuardError, match="intensity"):
        intensity_to_brightness(value)


def test_command_payload_matches_existing_ha_control_contract() -> None:
    on = build_command_payload(
        stable_id=STABLE_ID,
        variable="on",
        value=1,
        observed_sequence=7,
        command_id=COMMAND_ID,
        issued_at_ms=1_700_000_000_000,
    )
    brightness = build_command_payload(
        stable_id=STABLE_ID,
        variable="intensity",
        value=500,
        observed_sequence=7,
        command_id=COMMAND_ID,
        issued_at_ms=1_700_000_000_000,
    )

    assert on["kind"] == "turn_on"
    assert on["value"] is None
    assert brightness["kind"] == "set_brightness"
    assert brightness["value"] == 128
    assert brightness["observed_sequence"] == 7
    assert command_topic(STABLE_ID).endswith(f"/command/{STABLE_ID}")
    assert state_topic(STABLE_ID).endswith(f"/state/{STABLE_ID}")


class FakePublisher:
    def __init__(self) -> None:
        self.published: list[tuple[str, str, bool]] = []

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))


class FakeStateSink:
    def __init__(self) -> None:
        self.updates: list[Mapping[str, object]] = []

    async def update_variables(self, values: Mapping[str, object]) -> None:
        self.updates.append(dict(values))


async def test_retained_state_drives_native_values_without_command_echo() -> None:
    publisher = FakePublisher()
    sink = FakeStateSink()
    controller = PilotController(
        config=_config(),
        publisher=publisher,
        state_sink=sink,
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_000_000,
    )

    applied = await controller.handle_state_message(state_topic(STABLE_ID), _state(), retained=True)

    assert applied is True
    assert sink.updates == [{"on": 1, "intensity": 502}]
    assert publisher.published == []
    assert controller.observed_sequence == 7


async def test_state_route_rejects_wrong_entity_regression_and_unavailability() -> None:
    controller = PilotController(
        config=_config(),
        publisher=FakePublisher(),
        state_sink=FakeStateSink(),
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_000_000,
    )
    assert await controller.handle_state_message(state_topic(STABLE_ID), _state(), True)
    assert not await controller.handle_state_message(state_topic(STABLE_ID), _state(), False)

    with pytest.raises(PilotGuardError, match="topic"):
        await controller.handle_state_message("other", _state(sequence=8), False)
    with pytest.raises(PilotGuardError, match="boolean"):
        unavailable = json.loads(_state(sequence=8))
        unavailable["available"] = "false"
        await controller.handle_state_message(
            state_topic(STABLE_ID), json.dumps(unavailable), False
        )


async def test_newer_ha_epoch_resets_sequence_but_delayed_old_state_is_rejected() -> None:
    sink = FakeStateSink()
    controller = PilotController(
        config=_config(),
        publisher=FakePublisher(),
        state_sink=sink,
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_000_000,
    )
    assert await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=9, generated_at_ms=1_700_000_000_000),
        True,
    )
    assert await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=3, generated_at_ms=1_700_000_001_000, brightness=64),
        True,
    )
    assert await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=3, generated_at_ms=1_700_000_002_000, brightness=32),
        True,
    )

    with pytest.raises(PilotGuardError, match="predates"):
        await controller.handle_state_message(
            state_topic(STABLE_ID),
            _state(sequence=8, generated_at_ms=1_700_000_000_500),
            False,
        )

    assert controller.observed_sequence == 3
    assert sink.updates[-1] == {"on": 1, "intensity": 125}


async def test_unavailable_restart_state_fences_commands_until_available_state_returns() -> None:
    sink = FakeStateSink()
    publisher = FakePublisher()
    controller = PilotController(
        config=_config(),
        publisher=publisher,
        state_sink=sink,
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_002_000,
    )
    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=7, generated_at_ms=1_700_000_000_000),
        True,
    )
    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(
            sequence=1,
            generated_at_ms=1_700_000_001_000,
            state="unavailable",
            available=False,
        ),
        False,
    )

    with pytest.raises(PilotGuardError, match="available"):
        await controller.handle_panel_push("on", 1)
    assert sink.updates[-1] == {"on": 0}

    await controller.handle_state_message(
        state_topic(STABLE_ID),
        _state(sequence=2, generated_at_ms=1_700_000_002_000, brightness=200),
        False,
    )
    assert await controller.handle_panel_push("on", 1)


async def test_transport_reconnect_requires_and_accepts_identical_retained_replay() -> None:
    sink = FakeStateSink()
    publisher = FakePublisher()
    controller = PilotController(
        config=_config(),
        publisher=publisher,
        state_sink=sink,
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_000_000,
    )
    payload = _state()
    await controller.handle_state_message(state_topic(STABLE_ID), payload, True)
    assert await controller.handle_panel_push("on", 1)

    await controller.fence_transport()
    with pytest.raises(PilotGuardError, match="available"):
        await controller.handle_panel_push("on", 1)

    assert not await controller.handle_state_message(state_topic(STABLE_ID), payload, True)
    assert await controller.handle_panel_push("on", 1)
    assert sink.updates == [{"on": 1, "intensity": 502}, {"on": 0}]


@pytest.mark.parametrize(
    ("variable", "raw", "normalized"),
    [("on", 1, 1), ("on", "0", 0), ("intensity", 500, 500), ("intensity", "500", 500)],
)
def test_framework_push_boundary_accepts_only_canonical_integer_values(
    variable: str, raw: object, normalized: int
) -> None:
    assert _normalize_framework_push(variable, raw) == normalized


@pytest.mark.parametrize("raw", [True, " 1", "1.0", "0500", "-1", "1001", object()])
def test_framework_push_boundary_rejects_noncanonical_or_out_of_range_values(raw: object) -> None:
    with pytest.raises(PilotGuardError):
        _normalize_framework_push("intensity", raw)


async def test_panel_push_requires_state_and_suppresses_exact_duplicate_until_new_state() -> None:
    publisher = FakePublisher()
    controller = PilotController(
        config=_config(),
        publisher=publisher,
        state_sink=FakeStateSink(),
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_000_000,
    )

    with pytest.raises(PilotGuardError, match="authoritative HA state"):
        await controller.handle_panel_push("on", 1)

    await controller.handle_state_message(state_topic(STABLE_ID), _state(), True)
    assert await controller.handle_panel_push("intensity", 500)
    assert await controller.handle_panel_push("on", 1)
    assert not await controller.handle_panel_push("intensity", 500)
    await controller.handle_state_message(state_topic(STABLE_ID), _state(sequence=8), False)
    assert await controller.handle_panel_push("intensity", 500)

    assert len(publisher.published) == 3
    assert all(not retained for _, _, retained in publisher.published)


class FakeHost:
    def __init__(
        self,
        *,
        remain_after_delete: bool = False,
        fail_after_register: bool = False,
        hang_after_register: bool = False,
    ) -> None:
        self.starts: list[tuple[str, str]] = []
        self.deletes: list[str] = []
        self.deleted = asyncio.Event()
        self.shutdowns = 0
        self.present = False
        self.remain_after_delete = remain_after_delete
        self.fail_after_register = fail_after_register
        self.hang_after_register = hang_after_register
        self.callback: Callable[[str, object], Awaitable[bool]] | None = None

    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, VariableDefinition],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None:
        del variables
        self.starts.append((peripheral_id, virtual_device_id))
        self.present = True
        self.callback = on_command
        if self.fail_after_register:
            raise RuntimeError("synthetic partial start")
        if self.hang_after_register:
            await asyncio.Event().wait()

    async def update_variables(self, values: Mapping[str, object]) -> None:
        del values

    async def delete(self, peripheral_id: str) -> None:
        self.deletes.append(peripheral_id)
        self.deleted.set()
        if not self.remain_after_delete:
            self.present = False

    async def contains(self, peripheral_id: str) -> bool:
        del peripheral_id
        return self.present

    async def shutdown(self) -> None:
        self.shutdowns += 1


class FakeDisconnect(ConnectionError):
    pass


@dataclass
class FakeMessage:
    topic: str
    payload: str
    retain: bool


class FakeMessageStream(AsyncIterator[FakeMessage]):
    def __init__(
        self,
        messages: list[FakeMessage],
        *,
        after_messages: Callable[[], Awaitable[None]],
    ) -> None:
        self._messages = messages
        self._after_messages = after_messages
        self._after_ran = False

    def __aiter__(self) -> FakeMessageStream:
        return self

    async def __anext__(self) -> FakeMessage:
        if self._messages:
            return self._messages.pop(0)
        if not self._after_ran:
            self._after_ran = True
            await self._after_messages()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class FakeMqttClient:
    def __init__(self, messages: FakeMessageStream) -> None:
        self.messages = messages
        self.entered = 0
        self.subscriptions: list[str] = []
        self.unsubscriptions: list[str] = []
        self.published: list[tuple[str, str, bool]] = []

    async def __aenter__(self) -> FakeMqttClient:
        self.entered += 1
        return self

    async def __aexit__(self, *_args: object) -> None:
        return None

    async def subscribe(self, topic: str, *, timeout: float) -> None:
        del timeout
        self.subscriptions.append(topic)

    async def unsubscribe(self, topic: str, *, timeout: float) -> None:
        del timeout
        self.unsubscriptions.append(topic)

    async def publish(self, topic: str, *, payload: str, retain: bool, timeout: float) -> None:
        del timeout
        self.published.append((topic, payload, retain))


async def test_lifecycle_has_one_host_one_registration_and_idempotent_cleanup() -> None:
    host = FakeHost()
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=lambda _name, _value: _true(),
        sleep=lambda _seconds: _noop(),
        absence_interval_s=0,
    )

    await lifecycle.start()
    await lifecycle.start()
    first = await lifecycle.cleanup()
    second = await lifecycle.cleanup()

    assert host.starts == [(peripheral_id_for(STABLE_ID), VC_ID)]
    assert first.absent_first is True and first.absent_second is True
    assert second.already_clean is True
    assert host.deletes == [peripheral_id_for(STABLE_ID)]
    assert host.shutdowns == 1


async def test_cleanup_failure_is_not_hidden() -> None:
    host = FakeHost(remain_after_delete=True)
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=lambda _name, _value: _true(),
        sleep=lambda _seconds: _noop(),
        absence_interval_s=0,
    )
    await lifecycle.start()

    with pytest.raises(CleanupError, match="still present"):
        await lifecycle.cleanup()

    assert host.shutdowns == 1


async def test_partial_start_is_deletable_and_cleanup_is_not_skipped() -> None:
    host = FakeHost(fail_after_register=True)
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=lambda _name, _value: _true(),
        sleep=lambda _seconds: _noop(),
        absence_interval_s=0,
    )

    with pytest.raises(RuntimeError, match="partial start"):
        await lifecycle.start()
    result = await lifecycle.cleanup()

    assert result.absent_first is True and result.absent_second is True
    assert host.deletes == [peripheral_id_for(STABLE_ID)]
    assert host.shutdowns == 1


async def test_framework_start_timeout_is_bounded_and_partial_registration_is_cleanable() -> None:
    host = FakeHost(hang_after_register=True)
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=lambda _name, _value: _true(),
        sleep=lambda _seconds: _noop(),
        absence_interval_s=0,
        operation_timeout_s=0.01,
    )

    with pytest.raises(PilotGuardError, match="bounded timeout"):
        await lifecycle.start()
    result = await lifecycle.cleanup()

    assert result.absent_first and result.absent_second
    assert host.deletes == [peripheral_id_for(STABLE_ID)]


async def test_failed_mqtt_reader_cannot_skip_live_cleanup() -> None:
    host = FakeHost()
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=lambda _name, _value: _true(),
        sleep=lambda _seconds: _noop(),
        absence_interval_s=0,
    )
    await lifecycle.start()

    async def fail_reader() -> None:
        raise PilotGuardError("synthetic malformed retained state")

    reader = asyncio.create_task(fail_reader())
    await asyncio.sleep(0)
    unsubscribed = False

    async def unsubscribe() -> None:
        nonlocal unsubscribed
        unsubscribed = True

    await _finish_live_resources(reader, lifecycle, unsubscribe)

    assert host.deletes == [peripheral_id_for(STABLE_ID)]
    assert host.shutdowns == 1
    assert unsubscribed is True


async def test_cancellation_waits_for_bounded_cleanup_before_propagating() -> None:
    release = asyncio.Event()

    async def wait_for_release(_seconds: float) -> None:
        await release.wait()

    host = FakeHost()
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=lambda _name, _value: _true(),
        sleep=wait_for_release,
        absence_interval_s=0,
    )
    await lifecycle.start()
    unsubscribed = False

    async def unsubscribe() -> None:
        nonlocal unsubscribed
        unsubscribed = True

    finishing = asyncio.create_task(
        _finish_live_resources(
            None,
            lifecycle,
            unsubscribe,
            deadline=asyncio.get_running_loop().time() + 1,
        )
    )
    await host.deleted.wait()
    finishing.cancel()
    release.set()

    with pytest.raises(asyncio.CancelledError):
        await finishing
    assert host.shutdowns == 1
    assert unsubscribed is True


async def test_mqtt_disconnect_reconnects_without_restarting_native_host() -> None:
    stop = asyncio.Event()

    async def disconnect() -> None:
        raise FakeDisconnect("synthetic broker restart")

    command_accepted = False

    async def command_after_retained_replay() -> None:
        nonlocal command_accepted
        command_accepted = await controller.handle_panel_push("on", 1)
        stop.set()

    first = FakeMqttClient(
        FakeMessageStream(
            [FakeMessage(state_topic(STABLE_ID), _state(), True)],
            after_messages=disconnect,
        )
    )
    second = FakeMqttClient(
        FakeMessageStream(
            [FakeMessage(state_topic(STABLE_ID), _state(), True)],
            after_messages=command_after_retained_replay,
        )
    )
    clients = [first, second]
    host = FakeHost()
    publisher = _LivePublisher()
    controller = PilotController(
        config=_config(),
        publisher=publisher,
        state_sink=host,
        command_id_factory=lambda: COMMAND_ID,
        clock_ms=lambda: 1_700_000_000_000,
    )
    lifecycle = PilotLifecycle(
        config=_config(),
        topology=_topology(),
        host=host,
        room_assignment_type=FakeRoomAssignment,
        on_command=controller.handle_panel_push,
        sleep=lambda _seconds: _noop(),
        absence_interval_s=0,
    )

    await _run_reconnecting_transport(
        config=_config(),
        lifecycle=lifecycle,
        controller=controller,
        publisher=publisher,
        client_factory=lambda: clients.pop(0),
        retryable_errors=(FakeDisconnect,),
        stop=stop,
        deadline=asyncio.get_running_loop().time() + 1,
        reconnect_backoff_s=0,
    )

    assert first.entered == second.entered == 1
    assert host.starts == [(peripheral_id_for(STABLE_ID), VC_ID)]
    assert command_accepted is True
    assert len(second.published) == 1


async def test_connected_session_without_authoritative_state_times_out() -> None:
    controller = PilotController(
        config=_config(),
        publisher=FakePublisher(),
        state_sink=FakeStateSink(),
    )

    async def wait_forever() -> None:
        await asyncio.Event().wait()

    reader = asyncio.create_task(wait_forever())
    try:
        with pytest.raises(_StateReplayTimeout, match="authoritative state"):
            await _wait_for_session_authority(
                controller=controller,
                reader=reader,
                stop=asyncio.Event(),
                deadline=asyncio.get_running_loop().time() + 0.01,
            )
    finally:
        reader.cancel()
        await asyncio.gather(reader, return_exceptions=True)


def test_live_gate_requires_vc0_through_vc4_pass(tmp_path: Path) -> None:
    path = tmp_path / "ledger.json"
    ledger = GateLedger.new(run_id="vc-pilot")
    for gate in (GateName.VC0, GateName.VC1, GateName.VC2, GateName.VC3):
        ledger.record(gate, GateStatus.PASS, "validated", [])
    ledger.save(path)

    with pytest.raises(PilotGuardError, match="VC4 must pass"):
        validate_gate_ledger(path, expected_run_id="vc-pilot", required_uid=os.geteuid())

    ledger = GateLedger.load(path)
    ledger.record(GateName.VC4, GateStatus.PASS, "resource gate passed", [])
    ledger.save(path)
    validate_gate_ledger(path, expected_run_id="vc-pilot", required_uid=os.geteuid())

    with pytest.raises(PilotGuardError, match="run_id"):
        validate_gate_ledger(path, expected_run_id="different-run", required_uid=os.geteuid())

    symlink = tmp_path / "ledger-link.json"
    symlink.symlink_to(path)
    with pytest.raises(PilotGuardError, match="non-symlink"):
        validate_gate_ledger(
            symlink,
            expected_run_id="vc-pilot",
            required_uid=os.geteuid(),
        )


async def _true() -> bool:
    return True


async def _noop() -> None:
    return None
