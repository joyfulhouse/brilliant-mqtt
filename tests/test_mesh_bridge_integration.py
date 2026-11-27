"""Connected Bridge schedules use the actual adapter ingress and native boundary."""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator
from dataclasses import dataclass
from pathlib import Path
from types import SimpleNamespace

import pytest

from brilliant_mqtt import mqttio
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.discovery import command_topic, config_topic
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.model import CaptureProvenance
from brilliant_mqtt.write_admission import AdmissionTicket, CommandAdmission, command_admission
from tests.fakes import (
    FakeClock,
    FakeMqtt,
    FakeSleeper,
    _RawDevice,
    _RawPeripheral,
    _RawVariable,
    _StartHarness,
)
from tests.mesh_regression_support import turns
from tests.test_mesh_mqtt_ingress import message


@dataclass
class Connected:
    bridge: Bridge
    adapter: RpcBusAdapter
    mqtt: FakeMqtt
    harness: _StartHarness
    clock: FakeClock
    raw: _RawDevice
    sleeper: FakeSleeper

    def owner(self, peripheral_id: str = "load") -> MeshSettlement:
        registry = self.bridge.mesh_registry
        assert registry is not None
        owner = registry.current(peripheral_id)
        assert owner is not None
        return owner

    async def notify(self, **values: str) -> None:
        assert self.raw.peripherals is not None
        peripheral = self.raw.peripherals["load"]
        for name, value in values.items():
            peripheral.variables[name] = _RawVariable(value)
        notification = SimpleNamespace(
            updated_device=self.raw,
            deleted=False,
            modified_peripherals=[
                SimpleNamespace(
                    peripheral_id="load",
                    deleted=False,
                    modified_variables=[
                        SimpleNamespace(
                            variable_name=name,
                            deletion=False,
                            variable=SimpleNamespace(value=value, externally_settable=True),
                        )
                        for name, value in values.items()
                    ],
                    deleted_variables=[],
                )
            ],
        )
        await self.harness.observers[0].handle_notification(notification)
        await turns()


@pytest.fixture
async def connected(
    monkeypatch: pytest.MonkeyPatch,
) -> AsyncIterator[Connected]:
    clock = FakeClock()
    harness = _StartHarness(monkeypatch, gated_writes=True)
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",), clock=clock)
    await adapter.start()
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    peripheral.variables.update(
        {
            "intensity": _RawVariable("0"),
            "max_intensity_value": _RawVariable("255"),
            "enable_motion_score": _RawVariable("0"),
        }
    )
    blocker = _RawPeripheral("1")
    blocker.peripheral_type = 27
    raw = _RawDevice("ble_mesh", {"load": peripheral, "blocker": blocker})

    async def get_device(device_id: str) -> _RawDevice:
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(harness.observers[0], "get_device", get_device, raising=False)
    mqtt = FakeMqtt()
    sleeper = FakeSleeper()
    bridge = Bridge(
        adapter,
        mqtt,
        "mesh",
        include=lambda device_id: device_id == "ble_mesh",
        clock=clock,
        wall_clock=lambda: 1000 + clock(),
        sleep=sleeper,
    )
    try:
        await bridge.reconcile()
        yield Connected(bridge, adapter, mqtt, harness, clock, raw, sleeper)
    finally:
        harness.write_release.set()
        await bridge.withdraw()
        await adapter.shutdown()


async def test_command_without_original_owner_is_rejected(
    connected: Connected,
) -> None:
    bridge, harness = connected.bridge, connected.harness
    harness.write_release.set()
    await bridge._on_command(command_topic("mesh", "load"), '{"state":"OFF"}')
    assert harness.write_starts == []


async def test_direct_mesh_translation_without_original_context_cannot_admit_native_work(
    connected: Connected,
) -> None:
    rig = connected
    rig.harness.write_release.set()
    owner = rig.owner()
    before = owner.view.command_id
    await rig.bridge._handle_primary_command(
        command_topic("mesh", "load"), "load", '{"state":"OFF"}'
    )
    assert rig.harness.write_starts == []
    assert owner.view.command_id == before


@pytest.mark.parametrize("admission", ["missing", "revoked", "missing_route", "stale_descriptor"])
async def test_direct_mesh_aux_rejects_invalid_admission_before_persisting_intent(
    connected: Connected,
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    admission: str,
) -> None:
    rig = connected
    desired_path = tmp_path / "desired.json"
    desired = DesiredState(desired_path)
    desired.record("load", "enable_motion_score", "0")
    before = desired_path.read_text()
    rig.bridge._desired = desired
    topic = "brilliant/mesh/load/set_enable_motion_score"
    original = rig.owner()
    descriptor = rig.bridge._by_cmd_topic[topic][1]
    assert descriptor is not None
    if admission == "revoked":
        rig.harness.procs[0].reconnect_cbs[0]()
        await rig.bridge.reconcile()
        assert rig.owner() is not original
    elif admission == "missing_route":
        rig.bridge._by_cmd_topic.pop(topic)
    elif admission == "stale_descriptor":
        assert rig.raw.peripherals is not None
        rig.raw.peripherals["load"].variables.pop("enable_motion_score")
        await rig.adapter.get_all()

    payloads: list[tuple[str, str, dict[str, str]]] = []
    native = rig.harness.observers[0].request_set_variables_in_peripheral

    async def record(peripheral_id: str, values: dict[str, str], *, device_id: str) -> str:
        payloads.append((device_id, peripheral_id, dict(values)))
        result = await native(peripheral_id, values, device_id=device_id)
        assert isinstance(result, str)
        return result

    monkeypatch.setattr(rig.harness.observers[0], "request_set_variables_in_peripheral", record)
    rig.harness.write_release.set()
    token = command_admission.set(
        None if admission == "missing" else CommandAdmission(mesh_owner=original)
    )
    try:
        await rig.bridge._handle_aux_command(topic, "load", descriptor, "ON")
    finally:
        command_admission.reset(token)

    await rig.bridge._enforce_desired(await rig.adapter.get_all())

    assert {"desired": desired.wanted("load"), "native": payloads} == {
        "desired": {"enable_motion_score": "0"},
        "native": [],
    }
    assert desired_path.read_text() == before
    assert rig.harness.write_starts == []


async def test_owner_created_by_sync_ingress_before_async_repair(
    connected: Connected,
) -> None:
    bridge, adapter = connected.bridge, connected.adapter
    registry = bridge.mesh_registry
    assert registry is not None
    owner = registry.current("load")
    assert owner is not None
    carriers = await adapter.get_all()
    assert carriers[0].variables == {}
    assert owner.materialize(0).variables["on"].value == "1"


async def test_actual_native_return_reduces_before_waiter_resumes(
    connected: Connected,
) -> None:
    bridge, adapter, mqtt, harness, clock = (
        connected.bridge,
        connected.adapter,
        connected.mqtt,
        connected.harness,
        connected.clock,
    )
    registry = bridge.mesh_registry
    assert registry is not None
    owner = registry.current("load")
    assert owner is not None
    task = asyncio.create_task(mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    await turns()
    ticket = adapter.outstanding_mesh_ticket("load")
    assert ticket is not None and ticket.mesh_owner is owner
    clock.advance(7)
    harness.write_release.set()
    await turns()
    assert ticket.settled
    assert owner.view.receipt == "'ok'"
    assert owner.view.return_boundary is not None
    assert ticket.returned_at is not None
    assert owner.view.return_boundary.sequence == ticket.returned_at.sequence
    assert owner.view.deadline == 87
    await task


async def test_production_lane_fold_owns_merged_targets_before_rehandler(
    connected: Connected,
) -> None:
    rig = connected
    owner = rig.owner()
    first_returned, resume = asyncio.Event(), asyncio.Event()
    calls = 0

    async def handle(inbound: mqttio._InboundMessage) -> None:
        nonlocal calls
        calls += 1
        await rig.bridge._on_command(inbound.topic, inbound.payload)
        if calls == 1:
            first_returned.set()
            await resume.wait()

    queue = mqttio._BoundedTransportQueue(
        8,
        max_bytes=4096,
        overload=mqttio._TransportOverloadLatch(),
        owner_for_topic=rig.bridge._resolve_mesh_owner,
    )
    dispatcher = mqttio._TopicDispatcher(handle)
    native_payloads: list[dict[str, str]] = []
    original_native = rig.harness.observers[0].request_set_variables_in_peripheral

    async def record_native(peripheral_id: str, values: dict[str, str], *, device_id: str) -> str:
        native_payloads.append(dict(values))
        result = await original_native(peripheral_id, values, device_id=device_id)
        assert isinstance(result, str)
        return result

    rig.harness.observers[0].request_set_variables_in_peripheral = record_native
    blocker = asyncio.create_task(
        rig.adapter.set_variables(
            "ble_mesh",
            "blocker",
            [VarSet("on", "1")],
            ticket=AdmissionTicket(mesh_owner=rig.owner("blocker")),
        )
    )

    async def deliver(payload: str) -> None:
        queue.put_nowait(message(payload))
        raw = queue.get_nowait()
        inbound = mqttio._InboundMessage(
            str(raw.topic),
            mqttio._decode_payload(raw.payload),
            False,
            (),
            (),
            getattr(raw, "mesh_owner", None),
        )
        await dispatcher.dispatch(inbound, latest_wins=True)

    try:
        await turns()
        await deliver('{"state":"ON","brightness":40}')
        await turns()
        first_command = owner.view.command_id
        await deliver('{"brightness":80}')
        assert owner.view.command_id == first_command + 1
        assert dict(owner.view.targets) == {"on": "1", "intensity": "80"}
        await first_returned.wait()
        assert calls == 1
        rig.clock.advance(7)
        rig.harness.write_release.set()
        await blocker
        await turns()
        assert owner.view.receipt == "'ok'"
        assert owner.view.deadline == 87
        command_id, window = owner.view.command_id, owner.view.window
        assert window is not None
        assert native_payloads == [{"on": "1"}, {"on": "1", "intensity": "80"}]
        await rig.notify(on="1", intensity="0")
        assert owner.view.status == "contradicted"
        assert owner.view.window is None
        assert calls == 1
        resume.set()
        await dispatcher.shutdown()
        assert calls == 2
        assert owner.view.command_id == command_id
        assert owner.view.status == "contradicted"
        assert owner.view.window is None
        assert owner.view.receipt == "'ok'"
        assert len(native_payloads) == 2
        assert rig.harness.max_writes_in_flight == 1
    finally:
        resume.set()
        rig.harness.write_release.set()
        await blocker
        await dispatcher.shutdown()


async def test_prepared_issue_commit_refusal_never_manufactures_native_abandonment(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt.bus import _WriteAdmission
    from brilliant_mqtt.mesh_settlement_types import Phase

    rig = connected
    owner = rig.owner()
    prepared: list[CaptureProvenance] = []

    def refuse(admission: _WriteAdmission, boundary: CaptureProvenance) -> bool:
        assert admission.ticket.issued_at is None
        assert owner.view.issue_boundary is not None
        prepared.append(boundary)
        return False

    monkeypatch.setattr(rig.adapter, "_commit_mesh_issue", refuse)
    await rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}')
    assert len(prepared) == 1
    assert rig.harness.write_starts == []
    assert owner.view.phase is Phase.QUIET
    assert owner.view.window is None
    assert owner.view.completion_hold is None
    assert owner.view.outcome is None
    assert owner.render(rig.clock())["state"] == "ON"
    assert rig.sleeper.requested == []


@pytest.mark.parametrize("returned_before_cancel", [False, True])
async def test_issued_worker_abandonment_remains_unknown_through_real_return(
    connected: Connected,
    returned_before_cancel: bool,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    owner = rig.owner()
    entered, release = asyncio.Event(), asyncio.Event()
    original_publish = rig.mqtt.publish

    async def publish(topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        if returned_before_cancel and topic.endswith("/load/state") and '"pending"' in payload:
            entered.set()
            await release.wait()
        await original_publish(topic, payload, retain, qos)

    monkeypatch.setattr(rig.mqtt, "publish", publish)
    command = asyncio.create_task(rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    try:
        await turns()
        ticket = rig.adapter.outstanding_mesh_ticket("load")
        assert ticket is not None and ticket.issued_at is not None
        if returned_before_cancel:
            rig.harness.write_release.set()
            await entered.wait()
            assert ticket.settled
            assert owner.view.status == "pending"
        rig.clock.advance(1)
        command.cancel()
        await asyncio.gather(command, return_exceptions=True)

        cancellation_floor = owner.view.retirement_sequence
        assert owner.view.status == "unconfirmed"
        assert owner.view.window is None
        assert owner.render(rig.clock())["state"] is None
        rig.clock.advance(5)
        await rig.notify(on="1")
        assert owner.render(rig.clock())["state"] is ("ON" if returned_before_cancel else None)
        rig.clock.advance(1)
        rig.harness.write_release.set()
        await turns()
        assert ticket.settled
        assert owner.view.retirement_sequence >= cancellation_floor
        if not returned_before_cancel:
            assert owner.view.status == "unconfirmed"
            assert owner.render(rig.clock())["state"] is None
        await rig.notify(on="1")
        assert owner.render(rig.clock())["state"] == "ON"
        assert owner.view.window is None
        assert rig.harness.max_writes_in_flight == 1
    finally:
        release.set()
        rig.harness.write_release.set()
        await asyncio.gather(command, return_exceptions=True)


async def test_retired_cleanup_transport_failure_waits_for_existing_reconcile(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    owner = rig.owner()
    original = rig.mqtt.unsubscribe
    attempts = 0

    async def unsubscribe(topic: str) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("ordinary transport failure")
        await original(topic)

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsubscribe)
    assert rig.raw.peripherals is not None
    rig.raw.peripherals.pop("load")
    await rig.adapter.get_all()
    await turns()
    assert rig.bridge.mesh_registry is not None
    assert rig.bridge.mesh_registry.current("load") is None
    assert rig.bridge.mesh_registry.retired.get("load") is owner
    assert attempts == 1
    assert rig.adapter.mesh_admission_open
    await turns()
    assert attempts == 1
    await rig.bridge.reconcile()
    assert attempts > 1
    assert "load" not in rig.bridge.mesh_registry.retired
    assert command_topic("mesh", "load") not in rig.bridge._by_cmd_topic


@pytest.mark.parametrize("hook_kind", ["loop", "foreign"])
async def test_failed_retired_cleanup_survives_registered_reconnect_without_state_admission(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    hook_kind: str,
) -> None:
    rig = connected
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    old = rig.owner()
    configs = {config_topic(descriptor) for descriptor in rig.bridge._mesh_descriptors[old]}
    unsubscribe = rig.mqtt.unsubscribe

    async def unavailable(topic: str) -> None:
        raise RuntimeError("ordinary broker failure")

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unavailable)
    assert rig.raw.peripherals is not None
    rig.raw.peripherals.pop("load")
    await rig.adapter.get_all()
    await turns()
    assert registry.retired.get("load") is old
    rig.mqtt.published.clear()
    hook = rig.harness.procs[0].reconnect_cbs[0]
    if hook_kind == "foreign":
        from tests.test_mesh_bus_reconnect import foreign

        foreign(hook)
    else:
        hook()
    await turns()
    assert not old.active
    assert not registry.live(old, publication=True)
    assert registry.reserve(old, "publish") is None
    await rig.bridge.reconcile()
    assert registry.retired.get("load") is old
    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsubscribe)

    await rig.bridge.reconcile()
    await turns()

    assert not registry.retired
    assert old not in driver._lanes
    assert old not in rig.bridge._mesh_descriptors
    assert old not in rig.bridge._mesh_route_owners.values()
    assert command_topic("mesh", "load") in rig.mqtt.unsubscriptions
    assert command_topic("mesh", "load") not in rig.bridge._by_cmd_topic
    assert command_topic("mesh", "load") not in rig.bridge._subscribed
    assert "load" not in rig.bridge._devices
    assert {
        topic for topic, payload, _ in rig.mqtt.published if topic in configs and payload == ""
    } == configs
    assert not any(topic == "brilliant/mesh/load/state" for topic, _, _ in rig.mqtt.published)
    assert rig.harness.write_starts == []


@pytest.mark.parametrize("distinct_ids", [False, True])
async def test_failed_cleanup_reconnect_churn_releases_all_old_ownership_maps(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
    distinct_ids: bool,
) -> None:
    rig = connected
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    unsubscribe = rig.mqtt.unsubscribe
    old_owners = []

    async def unavailable(topic: str) -> None:
        raise RuntimeError("ordinary broker failure")

    assert rig.raw.peripherals is not None
    for index in range(4):
        pid = "load" if not distinct_ids or index == 0 else f"load-{index}"
        if index:
            peripheral = _RawPeripheral("1")
            peripheral.peripheral_type = 27
            rig.raw.peripherals[pid] = peripheral
            await rig.bridge.reconcile()
        old = rig.owner(pid)
        old_owners.append(old)
        configs = {config_topic(descriptor) for descriptor in rig.bridge._mesh_descriptors[old]}
        monkeypatch.setattr(rig.mqtt, "unsubscribe", unavailable)
        rig.raw.peripherals.pop(pid)
        await rig.adapter.get_all()
        await turns()
        assert registry.retired.get(pid) is old
        rig.harness.procs[0].reconnect_cbs[0]()
        await turns()
        monkeypatch.setattr(rig.mqtt, "unsubscribe", unsubscribe)
        await rig.bridge.reconcile()
        await turns()

        assert not registry.retired
        assert all(owner not in driver._lanes for owner in old_owners)
        assert all(owner not in rig.bridge._mesh_descriptors for owner in old_owners)
        assert all(owner not in rig.bridge._mesh_route_owners.values() for owner in old_owners)
        assert pid not in rig.bridge._devices
        assert command_topic("mesh", pid) not in rig.bridge._by_cmd_topic
        assert command_topic("mesh", pid) not in rig.bridge._subscribed
        assert configs <= {topic for topic, payload, _ in rig.mqtt.published if payload == ""}
    assert len(registry.owners) == 1
    assert len(rig.bridge._mesh_descriptors) == 1
    assert len(rig.bridge._mesh_route_owners) == 1
    assert len(rig.bridge._by_cmd_topic) == 1
    assert len(rig.bridge._subscribed) == 1
    assert len(rig.bridge._devices) == 1


async def test_reappearance_supersedes_revoked_cleanup_without_tombstoning_successor(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    rig = connected
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    old = rig.owner()
    unsubscribe = rig.mqtt.unsubscribe

    async def unavailable(topic: str) -> None:
        raise RuntimeError("ordinary broker failure")

    monkeypatch.setattr(rig.mqtt, "unsubscribe", unavailable)
    assert rig.raw.peripherals is not None
    peripheral = rig.raw.peripherals.pop("load")
    await rig.adapter.get_all()
    await turns()
    rig.harness.procs[0].reconnect_cbs[0]()
    await turns()
    monkeypatch.setattr(rig.mqtt, "unsubscribe", unsubscribe)
    rig.raw.peripherals["load"] = peripheral
    await rig.bridge.reconcile()
    successor = rig.owner()
    assert successor is not old
    rig.mqtt.published.clear()
    unsubscriptions = list(rig.mqtt.unsubscriptions)

    await driver.flush(old)
    await rig.bridge._cleanup_mesh(old)

    assert not registry.retired
    assert old not in rig.bridge._mesh_descriptors
    assert old not in rig.bridge._mesh_route_owners.values()
    assert registry.current("load") is successor
    assert rig.bridge._mesh_route_owners[command_topic("mesh", "load")] is successor
    assert command_topic("mesh", "load") in rig.bridge._subscribed
    assert "load" in rig.bridge._devices
    assert rig.mqtt.unsubscriptions == unsubscriptions
    assert rig.mqtt.published == []


async def test_original_queued_owner_cannot_write_after_same_topic_replacement(
    connected: Connected,
) -> None:
    rig = connected
    old = rig.owner()
    queue = mqttio._BoundedTransportQueue(
        8,
        max_bytes=4096,
        overload=mqttio._TransportOverloadLatch(),
        owner_for_topic=rig.bridge._resolve_mesh_owner,
    )
    queue.put_nowait(message('{"state":"OFF"}'))
    assert rig.raw.peripherals is not None
    peripheral = rig.raw.peripherals.pop("load")
    await rig.adapter.get_all()
    rig.raw.peripherals["load"] = peripheral
    await rig.bridge.reconcile()
    successor = rig.owner()
    assert successor is not old and not old.active
    queue.put_nowait(message('{"state":"ON"}'))
    assert queue.qsize() == 2
    rig.harness.write_release.set()

    async def handle(inbound: mqttio._InboundMessage) -> None:
        await rig.bridge._on_command(inbound.topic, inbound.payload)

    dispatcher = mqttio._TopicDispatcher(handle)
    try:
        for _ in range(2):
            raw = queue.get_nowait()
            await dispatcher.dispatch(
                mqttio._InboundMessage(
                    str(raw.topic),
                    mqttio._decode_payload(raw.payload),
                    False,
                    (),
                    (),
                    getattr(raw, "mesh_owner", None),
                ),
                latest_wins=True,
            )
        await dispatcher.shutdown()
        assert rig.harness.write_starts == ["ble_mesh"]
        assert successor.view.targets == (("on", "1"),)
        assert successor.view.receipt == "'ok'"
    finally:
        await dispatcher.shutdown()


async def test_aux_actual_return_then_native_contradiction_survives_delayed_caller(
    connected: Connected,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt.bus import _WriteAdmission

    rig = connected
    owner = rig.owner()
    original_finish = rig.adapter._finish_admission
    held: list[tuple[_WriteAdmission, asyncio.Task[str]]] = []

    def finish(admission: _WriteAdmission, task: asyncio.Task[str]) -> None:
        held.append((admission, task))

    monkeypatch.setattr(rig.adapter, "_finish_admission", finish)
    rig.harness.write_release.set()
    command = asyncio.create_task(
        rig.mqtt.inject(
            "brilliant/mesh/load/set_enable_motion_score",
            "ON",
        )
    )
    try:
        await turns()
        assert held and not command.done()
        ticket = held[0][0].ticket
        assert ticket.settled and ticket.returned_at is not None
        projected = owner.materialize(rig.clock()).variables["enable_motion_score"]
        assert projected.value == "1" and projected.timestamp_ms is None
        primary_before = (owner.view.window, owner.view.deadline, owner.view.evidence)
        await rig.notify(enable_motion_score="0")
        assert owner.materialize(rig.clock()).variables["enable_motion_score"].value == "0"
        for admission, task in held:
            original_finish(admission, task)
        await command
        assert owner.materialize(rig.clock()).variables["enable_motion_score"].value == "0"
        assert (owner.view.window, owner.view.deadline, owner.view.evidence) == primary_before
        assert dict(owner.view.projections) == {}
    finally:
        if not command.done():
            for admission, task in held:
                original_finish(admission, task)
        await asyncio.gather(command, return_exceptions=True)


async def test_repeated_mesh_create_write_remove_reacquire_bounds_all_live_maps(
    connected: Connected,
) -> None:
    rig = connected
    rig.harness.write_release.set()
    assert rig.raw.peripherals is not None
    peripheral = rig.raw.peripherals.pop("load")
    await rig.bridge.reconcile()
    for index in range(12):
        peripheral_id = f"load-{index}"
        rig.raw.peripherals[peripheral_id] = peripheral
        await rig.bridge.reconcile()
        await rig.mqtt.inject(command_topic("mesh", peripheral_id), '{"state":"OFF"}')
        rig.raw.peripherals.pop(peripheral_id)
        await rig.bridge.reconcile()
        await turns()
        registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
        assert registry is not None and driver is not None
        assert not registry.retired
        assert registry.current(peripheral_id) is None
        assert tuple(owner.metadata.peripheral_id for owner in registry.owners) == ("blocker",)
        assert set(rig.bridge._devices) == {"blocker"}
        assert not rig.bridge._command_generation
        assert not rig.bridge._last_state_fields
        assert not rig.bridge._last_state_payload
        assert not rig.bridge._last_reassert
        assert not rig.bridge._reassert_retry_interval_s
        assert not rig.bridge._missing_var_logged
        assert len(rig.bridge._by_cmd_topic) == 1
        assert len(rig.bridge._subscribed) == 1
        assert len(rig.bridge._mesh_route_owners) == 1
        assert len(rig.bridge._mesh_descriptors) == 1
        assert len(driver._lanes) <= 1
        assert rig.bridge._mesh_commands is not None
        assert not rig.bridge._mesh_commands._associations
        assert not rig.adapter._write_admissions
        assert not rig.adapter._write_tasks
        assert not rig.adapter._write_serving
        assert not any(device_id == "ble_mesh" for device_id, _, _ in rig.adapter._field_eras)
        await rig.bridge.withdraw()
        await rig.bridge.reconcile()
        assert len(rig.bridge._mesh_route_owners) == 1
        assert len(rig.bridge._mesh_descriptors) == 1
        assert len(driver._lanes) <= 1
    assert len(rig.adapter._mesh_sinks) == 1


async def test_dynamic_standby_and_shared_panel_bridge_keep_one_mesh_guard(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = FakeClock()
    harness = _StartHarness(monkeypatch)
    adapter = RpcBusAdapter(extra_device_ids=("ble_mesh",), clock=clock)
    await adapter.start()
    reads: list[str] = []
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = 27
    raw = _RawDevice("ble_mesh", {"load": peripheral})

    async def get_device(device_id: str) -> _RawDevice:
        reads.append(device_id)
        return raw if device_id == "ble_mesh" else _RawDevice(device_id, {})

    monkeypatch.setattr(harness.observers[0], "get_device", get_device, raising=False)
    leader = False
    mqtt = FakeMqtt()
    mesh = Bridge(
        adapter,
        mqtt,
        "mesh",
        clock=clock,
        include=lambda device_id: leader and device_id == "ble_mesh",
    )
    resolver = mqtt._mesh_owner_resolver
    panel = Bridge(
        adapter, mqtt, "panel", clock=clock, include=lambda device_id: device_id == "own-device"
    )
    try:
        registry = mesh.mesh_registry
        assert registry is not None and not registry.enabled
        assert panel.mesh_registry is None
        assert mqtt._mesh_owner_resolver is resolver
        assert registry.want_fields("load") is None
        await mesh.reconcile()
        assert reads == [] and not registry.enabled
        standby = await adapter.get_all(include_extras=False)
        await panel.poll_once(standby)
        assert reads == ["own-device"]
        assert registry.owners == ()
        leader = True
        await mesh.reconcile()
        owner = registry.current("load")
        assert owner is not None and registry.live(owner)
        assert reads == ["own-device", "own-device", "ble_mesh"]
        await mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}')
        assert owner.view.receipt == "'ok'"
        assert owner.view.window is not None
    finally:
        await mesh.withdraw()
        await panel.withdraw()
        await adapter.shutdown()


@pytest.mark.parametrize("native_type", [0, 80])
async def test_unmapped_infrastructure_never_allocates_owner_or_publisher(
    connected: Connected,
    native_type: int,
) -> None:
    rig = connected
    assert rig.raw.peripherals is not None
    peripheral = _RawPeripheral("1")
    peripheral.peripheral_type = native_type
    rig.raw.peripherals["infrastructure"] = peripheral
    await rig.bridge.reconcile()
    registry, driver = rig.bridge.mesh_registry, rig.bridge.mesh_driver
    assert registry is not None and driver is not None
    assert registry.current("infrastructure") is None
    assert all(owner.metadata.peripheral_id != "infrastructure" for owner in driver._lanes)
    assert not any(
        topic == "brilliant/mesh/infrastructure/state" for topic, _, _ in rig.mqtt.published
    )


async def test_mapped_binding_retyped_to_infrastructure_retires_exact_old_routes(
    connected: Connected,
) -> None:
    rig = connected
    owner = rig.owner()
    assert rig.raw.peripherals is not None
    rig.raw.peripherals["load"].peripheral_type = 0
    await rig.bridge.reconcile()
    assert rig.bridge.mesh_registry is not None
    assert rig.bridge.mesh_registry.current("load") is None
    assert not rig.bridge.mesh_registry.retired
    assert not owner.active
    assert command_topic("mesh", "load") not in rig.bridge._by_cmd_topic
    assert "load" not in rig.bridge._devices


@pytest.mark.parametrize("replacement", ["loop_hook", "foreign_hook", "reacquire"])
async def test_old_worker_cancellation_preserves_successor_exact_completion_listener(
    connected: Connected,
    replacement: str,
) -> None:
    rig = connected
    old = rig.owner()
    command = asyncio.create_task(rig.mqtt.inject(command_topic("mesh", "load"), '{"state":"OFF"}'))
    try:
        await turns()
        ticket = rig.adapter.outstanding_mesh_ticket("load")
        assert ticket is not None and ticket.issued_at is not None
        if replacement == "reacquire":
            await rig.bridge.withdraw()
            await rig.bridge.reconcile()
        else:
            hook = rig.harness.procs[0].reconnect_cbs[0]
            if replacement == "foreign_hook":
                from tests.test_mesh_bus_reconnect import foreign

                foreign(hook)
            else:
                hook()
            assert rig.bridge.mesh_registry is not None
            assert rig.bridge.mesh_registry.reserve(old, "flush_generation") is None
        successor = rig.owner()
        assert successor is not old
        assert successor.view.completion_hold is not None
        assert successor.view.completion_hold.ticket is ticket
        inherited_listener = ticket._on_settled
        command.cancel()
        await asyncio.gather(command, return_exceptions=True)
        assert ticket._on_settled is inherited_listener
        assert rig.harness.writes_in_flight == 1
        assert not ticket.settled
        rig.clock.advance(7)
        rig.harness.write_release.set()
        await turns()
        assert ticket.settled and ticket.returned_at is not None
        assert successor.view.completion_hold is None
        assert successor.view.retirement_sequence == ticket.returned_at.sequence
        assert successor.render(rig.clock())["state"] is None
        assert successor.view.window is None and successor.view.receipt is None
        await rig.notify(on="1")
        assert successor.render(rig.clock())["state"] == "ON"
        assert rig.harness.max_writes_in_flight == 1
    finally:
        rig.harness.write_release.set()
        await asyncio.gather(command, return_exceptions=True)
