"""Safe bidirectional scene/mode bridge tests."""

from __future__ import annotations

import asyncio
import base64
import json
import stat
import struct
import threading
from pathlib import Path
from typing import cast
from uuid import UUID

import pytest

from brilliant_mqtt import scene_bridge as scene_bridge_module
from brilliant_mqtt import scene_state
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.ha_control_protocol import (
    COMMAND_TTL_MS,
    MAPPING_VERSION,
    SCHEMA_VERSION,
    encode_json,
    mode_catalog_topic,
    mode_command_topic,
    mode_event_topic,
    mode_result_topic,
    scene_catalog_topic,
    scene_command_topic,
    scene_event_topic,
    scene_result_topic,
    transport_status_topic,
)
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.scene_bridge import SceneBridge
from tests.fakes import FakeBus, FakeClockMs, FakeMqtt

_PANEL = "office"
_DEVICE_ID = "panel-device-id"
_SCENE_PREFIX = "execution_state:scene_execution_handler:scene:"
_NOW_MS = 1_700_000_010_000


def _field_string(field_id: int, value: str) -> bytes:
    encoded = value.encode()
    return b"\x0b" + struct.pack(">hI", field_id, len(encoded)) + encoded


def _field_i64(field_id: int, value: int) -> bytes:
    return b"\x0a" + struct.pack(">hq", field_id, value)


def _blob(*fields: bytes) -> str:
    return base64.b64encode(b"".join((*fields, b"\x00"))).decode()


def _device(
    peripheral_id: str,
    variables: dict[str, Variable],
    *,
    device_id: str = _DEVICE_ID,
) -> BrilliantDevice:
    return BrilliantDevice(
        device_id=device_id,
        peripheral_id=peripheral_id,
        name=peripheral_id,
        kind=DeviceKind.UNKNOWN,
        variables=variables,
    )


def _execution(
    scene_id: str | None = None,
    executed_at_ms: int = 0,
    *,
    mode_id: str | None = None,
    mode_at_ms: int | None = None,
    malformed_scene: bool = False,
) -> BrilliantDevice:
    variables: dict[str, Variable] = {}
    if scene_id is not None:
        name = f"{_SCENE_PREFIX}{scene_id}"
        value = "not-base64" if malformed_scene else _blob(_field_i64(1, executed_at_ms))
        variables[name] = Variable(name, value, timestamp_ms=executed_at_ms + 99)
    if mode_id is not None:
        variables["manual_mode_id"] = Variable("manual_mode_id", mode_id, timestamp_ms=mode_at_ms)
    return _device("execution_peripheral", variables)


def _scene_catalog(*scene_ids: str) -> BrilliantDevice:
    variables = {
        f"scene:{scene_id}": Variable(
            f"scene:{scene_id}",
            _blob(
                _field_string(1, scene_id),
                _field_string(2, scene_id.replace("_", " ").title()),
                _field_string(3, f"icon:{scene_id}"),
            ),
        )
        for scene_id in scene_ids
    }
    return _device("scene_configuration", variables, device_id="configuration_virtual_device")


def _mode_catalog(*mode_ids: str) -> BrilliantDevice:
    variables = {
        f"mode:{mode_id}": Variable(
            f"mode:{mode_id}",
            _blob(_field_string(1, mode_id), _field_string(2, mode_id.title())),
        )
        for mode_id in mode_ids
    }
    return _device("mode_configuration", variables, device_id="configuration_virtual_device")


def _command(command_id: str, kind: str, value: str, *, issued_at_ms: int = _NOW_MS) -> str:
    return encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": command_id,
            "panel": _PANEL,
            f"{kind}_id": value,
            "issued_at_ms": issued_at_ms,
        }
    )


def _published(mqtt: FakeMqtt, topic: str) -> list[tuple[str, str, bool]]:
    return [item for item in mqtt.published if item[0] == topic]


def _published_qos(mqtt: FakeMqtt, topic: str) -> list[int]:
    """QoS levels seen on the wire for ``topic``, in publish order.

    ``FakeMqtt.published`` and ``FakeMqtt.published_qos`` are index-aligned, so
    this reports the QoS the delivery loop actually passed for each frame.
    """
    return [
        mqtt.published_qos[index] for index, item in enumerate(mqtt.published) if item[0] == topic
    ]


def _payload(item: tuple[str, str, bool]) -> dict[str, object]:
    return cast(dict[str, object], json.loads(item[1]))


async def _wait_for_publish(mqtt: FakeMqtt, topic: str, count: int = 1) -> None:
    for _ in range(200):
        if len(_published(mqtt, topic)) >= count:
            return
        await asyncio.sleep(0.001)
    pytest.fail(f"timed out waiting for {count} publication(s) on {topic}")


async def _wait_for_bus_commands(bus: FakeBus, count: int) -> None:
    """Wait until at least ``count`` bus writes have landed.

    The command handler dispatches the bus write on a background task (so a hung
    write cannot block the command or its timeout), so ``bus.commands`` is only
    populated once that task runs. Assertions on ``bus.commands`` must wait for
    it rather than assuming it completed synchronously with ``mqtt.inject``.
    """
    for _ in range(200):
        if len(bus.commands) >= count:
            return
        await asyncio.sleep(0.001)
    pytest.fail(f"timed out waiting for {count} bus command(s); got {len(bus.commands)}")


async def _started(
    tmp_path: Path,
    *,
    execution: BrilliantDevice | None = None,
    clock: FakeClockMs | None = None,
    scene_ids: tuple[str, ...] = ("all_off",),
    mode_ids: tuple[str, ...] = ("away",),
) -> tuple[SceneBridge, FakeBus, FakeMqtt, FakeClockMs, Path]:
    execution = execution or _execution()
    bus = FakeBus(
        [execution], scoped_devices=[_scene_catalog(*scene_ids), _mode_catalog(*mode_ids)]
    )
    mqtt = FakeMqtt()
    clock = clock or FakeClockMs(_NOW_MS)
    path = tmp_path / "private" / "scene-watermarks.json"
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    return bridge, bus, mqtt, clock, path


def _write_delivered_result_state(
    path: Path,
    kind: scene_state.StateKind,
) -> tuple[str, str]:
    command_id = "22222222-2222-4222-8222-222222222222"
    value = "all_off" if kind == "scene" else "away"
    executed_at_ms = _NOW_MS - 1
    event_key = f"{kind}:{_PANEL}:{value}:{executed_at_ms}"
    event_topic = scene_event_topic(_PANEL) if kind == "scene" else mode_event_topic(_PANEL)
    result_topic = (
        scene_result_topic(command_id) if kind == "scene" else mode_result_topic(command_id)
    )
    event_payload = encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "panel": _PANEL,
            f"{kind}_id": value,
            "executed_at_ms": executed_at_ms,
            "deduplication_key": f"{_PANEL}:{value}:{executed_at_ms}",
        }
    )
    result_payload = encode_json(
        {
            "schema_version": SCHEMA_VERSION,
            "mapping_version": MAPPING_VERSION,
            "command_id": command_id,
            "panel": _PANEL,
            f"{kind}_id": value,
            "accepted": True,
            "timestamp_ms": executed_at_ms,
        }
    )
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "watermarks": {},
                "mode_watermarks": {},
                "events": {
                    event_key: {
                        "topic": event_topic,
                        "payload": event_payload,
                        "delivered": True,
                        "created_at_ms": executed_at_ms,
                    }
                },
                "results": {
                    f"{kind}:{command_id}": {
                        "kind": kind,
                        "command_id": command_id,
                        "fingerprint": scene_state.command_fingerprint_fields(
                            kind,
                            command_id,
                            _PANEL,
                            value,
                            executed_at_ms,
                        ),
                        "command_panel": _PANEL,
                        "command_value": value,
                        "issued_at_ms": executed_at_ms,
                        "topic": result_topic,
                        "payload": result_payload,
                        "delivered": True,
                        "expires_at_ms": _NOW_MS + COMMAND_TTL_MS,
                        "event_key": event_key,
                    }
                },
                "pending": {},
            }
        )
    )
    return command_id, value


async def _assert_second_command_after_delivery_and_prune(
    tmp_path: Path,
    kind: scene_state.StateKind,
) -> None:
    bridge, bus, mqtt, _, path = await _started(tmp_path)
    first_id = "22222222-2222-4222-8222-222222222222"
    second_id = "44444444-4444-4444-8444-444444444444"
    command_topic = scene_command_topic(_PANEL) if kind == "scene" else mode_command_topic(_PANEL)
    result_topic = scene_result_topic(first_id) if kind == "scene" else mode_result_topic(first_id)
    execution = (
        _execution("all_off", _NOW_MS + 1)
        if kind == "scene"
        else _execution(mode_id="away", mode_at_ms=_NOW_MS + 1)
    )
    value = "all_off" if kind == "scene" else "away"
    try:
        await mqtt.inject(command_topic, _command(first_id, kind, value))
        await _wait_for_bus_commands(bus, 1)
        await bus.emit(execution)
        await _wait_for_publish(mqtt, result_topic)
        delivery_task = bridge._delivery_task
        assert delivery_task is not None
        await asyncio.wait_for(delivery_task, timeout=2)

        first_result = bridge._results[(kind, first_id)]
        assert first_result.delivered is True
        assert first_result.event_key is None
        assert bridge._events == {}
        stored = json.loads(path.read_text())
        assert stored["results"][f"{kind}:{first_id}"]["event_key"] is None
        assert stored["events"] == {}

        await mqtt.inject(command_topic, _command(second_id, kind, value))
        assert bridge._state_trusted is True
        if kind == "mode":
            await _wait_for_publish(mqtt, mode_result_topic(second_id))
            assert len(bus.commands) == 1
            result = _payload(_published(mqtt, mode_result_topic(second_id))[-1])
            assert result["accepted"] is True
        else:
            await _wait_for_bus_commands(bus, 2)
            assert len(bus.commands) == 2
    finally:
        await bridge.async_shutdown()


async def _assert_restart_repairs_delivered_result_dependency(
    tmp_path: Path,
    kind: scene_state.StateKind,
) -> None:
    path = tmp_path / "state.json"
    first_id, value = _write_delivered_result_state(path, kind)
    assert scene_state.load_state(path).trusted is True
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    try:
        await bridge.async_start()

        assert bridge._state_trusted is True
        assert bridge._results[(kind, first_id)].event_key is None
        assert bridge._events == {}
        status = _payload(_published(mqtt, transport_status_topic(kind, _PANEL))[-1])
        assert status["available"] is True
        stored = json.loads(path.read_text())
        assert stored["results"][f"{kind}:{first_id}"]["event_key"] is None
        assert stored["events"] == {}

        second_id = "44444444-4444-4444-8444-444444444444"
        command_topic = (
            scene_command_topic(_PANEL) if kind == "scene" else mode_command_topic(_PANEL)
        )
        await mqtt.inject(command_topic, _command(second_id, kind, value))
        await _wait_for_bus_commands(bus, 1)
        assert bridge._state_trusted is True
    finally:
        await bridge.async_shutdown()


async def test_start_seeds_history_persists_privately_and_publishes_scoped_catalogs(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, path = await _started(
        tmp_path, execution=_execution("all_off", 1_700_000_000_300)
    )

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert path.exists()
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    persisted = json.loads(path.read_text())
    assert persisted["watermarks"]["office"]["all_off"]["executed_at_ms"] == (1_700_000_000_300)
    assert set(persisted["watermarks"]["office"]["all_off"]) == {
        "executed_at_ms",
        "payload_sha256",
    }
    assert bus.scoped_reads == [
        ("configuration_virtual_device", "scene_configuration"),
        ("configuration_virtual_device", "mode_configuration"),
    ]
    scene_payload = _payload(_published(mqtt, scene_catalog_topic(_PANEL))[-1])
    assert scene_payload["scenes"] == [
        {"display_name": "All Off", "icon": "icon:all_off", "scene_id": "all_off"}
    ]
    assert _published(mqtt, scene_catalog_topic(_PANEL))[-1][2] is True
    assert _published(mqtt, mode_catalog_topic(_PANEL))[-1][2] is True
    assert mqtt.subscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]
    assert bus.change_callback_modes == [False]

    await bridge.async_shutdown()


async def test_later_scene_event_publishes_once_and_reconnect_restart_replay_is_suppressed(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, clock, path = await _started(tmp_path, execution=_execution("all_off", 100))
    later = _execution("all_off", 200)

    await bus.emit(later)
    await bus.emit(later)
    bus.set_devices([later])
    await bus.fire_reconnect()

    events = _published(mqtt, scene_event_topic(_PANEL))
    assert len(events) == 1
    assert events[0][2] is False
    assert _payload(events[0]) == {
        "deduplication_key": f"{_PANEL}:all_off:200",
        "executed_at_ms": 200,
        "mapping_version": MAPPING_VERSION,
        "panel": _PANEL,
        "scene_id": "all_off",
        "schema_version": SCHEMA_VERSION,
    }
    assert bus.scoped_reads.count(("configuration_virtual_device", "scene_configuration")) == 2

    await bridge.async_shutdown()
    restarted_bus = FakeBus(
        [later], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()

    assert _published(restarted_mqtt, scene_event_topic(_PANEL)) == []
    await restarted.async_shutdown()


async def test_poll_only_execution_publishes_event_and_persists_watermark(
    tmp_path: Path,
) -> None:
    bridge, _, mqtt, _, path = await _started(tmp_path)
    mqtt.published.clear()

    await bridge.poll_executions([_execution("all_off", 200)])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    persisted = json.loads(path.read_text())
    assert persisted["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 200
    await bridge.async_shutdown()


async def test_repeated_scene_poll_with_new_execution_time_publishes_again(
    tmp_path: Path,
) -> None:
    bridge, _, mqtt, _, _ = await _started(tmp_path)
    mqtt.published.clear()

    await bridge.poll_executions([_execution("all_off", 200)])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))
    await bridge.poll_executions([_execution("all_off", 300)])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL), count=2)

    assert [
        _payload(item)["executed_at_ms"] for item in _published(mqtt, scene_event_topic(_PANEL))
    ] == [200, 300]
    await bridge.async_shutdown()


async def test_failed_poll_processing_retries_identical_snapshot(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge, _, mqtt, _, _ = await _started(tmp_path)
    execution = _execution("all_off", 200)
    process = bridge._async_process_execution
    attempts = 0

    async def fail_once(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("transient processing failure")
        await process(device, emit_events=emit_events, epoch=epoch)

    monkeypatch.setattr(bridge, "_async_process_execution", fail_once)
    mqtt.published.clear()

    await bridge.poll_executions([execution])
    assert _published(mqtt, scene_event_topic(_PANEL)) == []

    await bridge.poll_executions([execution])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    assert attempts == 2
    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_failed_older_poll_does_not_clobber_newer_execution(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    older = _device(
        "execution_peripheral",
        _execution("all_off", 200).variables,
        device_id="old-panel",
    )
    newer = _device(
        "execution_peripheral",
        _execution("all_off", 200).variables,
        device_id="new-panel",
    )
    older_started = asyncio.Event()
    release_older = asyncio.Event()
    process = bridge._async_process_execution
    processed: list[BrilliantDevice] = []

    async def fail_older(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        processed.append(device)
        if device is older:
            older_started.set()
            await release_older.wait()
            raise RuntimeError("older processing failure")
        await process(device, emit_events=emit_events, epoch=epoch)

    monkeypatch.setattr(bridge, "_async_process_execution", fail_older)
    mqtt.published.clear()

    older_task = asyncio.create_task(bridge.poll_executions([older]))
    await asyncio.wait_for(older_started.wait(), timeout=0.1)
    await bridge.poll_executions([newer])
    release_older.set()
    await older_task
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    event = _payload(_published(mqtt, scene_event_topic(_PANEL))[-1])
    assert event["executed_at_ms"] == 200
    await bridge.poll_executions([newer])
    assert processed == [older, newer]
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    assert bus.commands[-1][0] == "new-panel"
    await bridge.async_shutdown()


async def test_poll_commits_fingerprint_captured_before_processing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge, _, mqtt, _, _ = await _started(tmp_path)
    execution = _execution("all_off", 200)
    process = bridge._async_process_execution
    processed = asyncio.Event()
    release = asyncio.Event()

    async def process_then_wait(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        await process(device, emit_events=emit_events, epoch=epoch)
        processed.set()
        await release.wait()

    monkeypatch.setattr(bridge, "_async_process_execution", process_then_wait)
    mqtt.published.clear()

    first_poll = asyncio.create_task(bridge.poll_executions([execution]))
    await asyncio.wait_for(processed.wait(), timeout=0.1)
    execution.variables.clear()
    execution.variables.update(_execution("all_off", 300).variables)
    release.set()
    await first_poll

    await bridge.poll_executions([execution])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL), count=2)

    assert [
        _payload(item)["executed_at_ms"] for item in _published(mqtt, scene_event_topic(_PANEL))
    ] == [200, 300]
    await bridge.async_shutdown()


async def test_reconcile_absence_fences_inflight_poll_fingerprint_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    execution = _execution("all_off", 200)
    process = bridge._async_process_execution
    processed = asyncio.Event()
    release = asyncio.Event()

    async def process_then_wait(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        await process(device, emit_events=emit_events, epoch=epoch)
        processed.set()
        await release.wait()

    monkeypatch.setattr(bridge, "_async_process_execution", process_then_wait)
    poll = asyncio.create_task(bridge.poll_executions([execution]))
    await asyncio.wait_for(processed.wait(), timeout=0.1)
    bus.set_devices([])
    await bridge.async_reconcile()
    release.set()
    await poll

    reappeared = _device(
        "execution_peripheral",
        _execution("all_off", 200).variables,
        device_id="reappeared-panel",
    )
    await bridge.poll_executions([reappeared])

    assert bridge._execution_available is True
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    assert bus.commands[-1][0] == "reappeared-panel"
    await bridge.async_shutdown()


async def test_reappearing_execution_with_identical_values_restores_route(
    tmp_path: Path,
) -> None:
    initial = _execution("all_off", 100)
    bridge, bus, mqtt, _, _ = await _started(tmp_path, execution=initial)
    bus.set_devices([])
    await bridge.async_reconcile()

    reappeared = _device(
        "execution_peripheral",
        _execution("all_off", 100).variables,
        device_id="restarted-panel",
    )
    await bridge.poll_executions([reappeared])

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is True
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    assert bus.commands[-1][0] == "restarted-panel"
    await bridge.async_shutdown()


async def test_poll_gate_ignores_timestamp_only_variable_refresh(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    initial = _execution("all_off", 100)
    bridge, _, _, _, _ = await _started(tmp_path, execution=initial)
    process = bridge._async_process_execution
    processed: list[BrilliantDevice] = []

    async def observed_process(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        processed.append(device)
        await process(device, emit_events=emit_events, epoch=epoch)

    monkeypatch.setattr(bridge, "_async_process_execution", observed_process)
    refreshed = _device(
        "execution_peripheral",
        {
            name: Variable(
                name,
                variable.value,
                externally_settable=variable.externally_settable,
                timestamp_ms=999,
            )
            for name, variable in initial.variables.items()
        },
    )

    await bridge.poll_executions([refreshed])

    assert processed == []
    await bridge.async_shutdown()


async def test_poll_during_shutdown_skips_processing(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bridge, _, _, _, _ = await _started(tmp_path)
    process = bridge._async_process_execution
    processed: list[BrilliantDevice] = []

    async def observed_process(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        processed.append(device)
        await process(device, emit_events=emit_events, epoch=epoch)

    monkeypatch.setattr(bridge, "_async_process_execution", observed_process)
    bridge._stopping = True

    await bridge.poll_executions([_execution("all_off", 200)])

    assert processed == []
    bridge._stopping = False
    await bridge.async_shutdown()


async def test_unchanged_poll_skips_publishes_and_state_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    writes: list[Path] = []
    real_write = scene_state.atomic_write_state

    def observed_write(path: Path, state: scene_state.SceneState) -> None:
        writes.append(path)
        real_write(path, state)

    monkeypatch.setattr(scene_bridge_module, "atomic_write_state", observed_write)
    execution = _execution("all_off", 100)
    bridge, _, mqtt, _, _ = await _started(tmp_path, execution=execution)
    writes.clear()
    mqtt.published.clear()

    await bridge.poll_executions([_execution("all_off", 100)])

    assert mqtt.published == []
    assert writes == []
    await bridge.async_shutdown()


async def test_same_execution_from_push_then_poll_publishes_once(tmp_path: Path) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    execution = _execution("all_off", 200)
    mqtt.published.clear()

    await bus.emit(execution)
    await bridge.poll_executions([execution])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_poll_execution_never_republishes_retained_catalogs(tmp_path: Path) -> None:
    bridge, _, mqtt, _, _ = await _started(tmp_path)
    mqtt.published.clear()

    await bridge.poll_executions([_execution("all_off", 200)])
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    assert _published(mqtt, scene_catalog_topic(_PANEL)) == []
    assert _published(mqtt, mode_catalog_topic(_PANEL)) == []
    assert all(not retained for _, _, retained in mqtt.published)
    await bridge.async_shutdown()


async def test_watermark_update_preserves_other_panel_records(tmp_path: Path) -> None:
    path = tmp_path / "scene-watermarks.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "events": {},
                "results": {},
                "pending": {},
                "mode_watermarks": {},
                "watermarks": {
                    "kitchen": {
                        "dinner": {
                            "executed_at_ms": 42,
                            "payload_sha256": "a" * 64,
                        }
                    }
                },
            }
        )
    )
    bus = FakeBus(
        [_execution("all_off", 100)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    bridge = SceneBridge(bus, FakeMqtt(), _PANEL, path, FakeClockMs(_NOW_MS))

    await bridge.async_start()
    await bridge.async_shutdown()

    persisted = json.loads(path.read_text())
    assert persisted["watermarks"]["kitchen"]["dinner"] == {
        "executed_at_ms": 42,
        "payload_sha256": "a" * 64,
    }


async def test_valid_commands_write_only_execution_variables_and_wait_for_confirmation(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    scene_id = "22222222-2222-4222-8222-222222222222"
    mode_id = "33333333-3333-4333-8333-333333333333"

    await mqtt.inject(scene_command_topic(_PANEL), _command(scene_id, "scene", "all_off"))
    await mqtt.inject(mode_command_topic(_PANEL), _command(mode_id, "mode", "away"))

    await _wait_for_bus_commands(bus, 2)
    assert bus.commands == [
        (_DEVICE_ID, "execution_peripheral", [VarSet("last_executed_scene_id", "all_off")]),
        (_DEVICE_ID, "execution_peripheral", [VarSet("manual_mode_id", "away")]),
    ]
    assert _published(mqtt, scene_result_topic(scene_id)) == []
    assert _published(mqtt, mode_result_topic(mode_id)) == []

    await bridge.async_shutdown()


async def test_hung_write_does_not_block_timeout_or_shutdown(tmp_path: Path) -> None:
    class HangingBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.write_started = asyncio.Event()
            self.write_cancelled = asyncio.Event()

        async def set_variables(
            self, device_id: str, peripheral_id: str, sets: list[VarSet]
        ) -> str:
            await super().set_variables(device_id, peripheral_id, sets)
            self.write_started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                self.write_cancelled.set()
                raise
            raise AssertionError("unreachable: the write only ends by cancellation")

    bus = HangingBus()
    mqtt = FakeMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", clock)
    await bridge.async_start()
    command_id = "22222222-2222-4222-8222-222222222222"

    await asyncio.wait_for(
        mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off")),
        timeout=0.1,
    )
    await asyncio.wait_for(bus.write_started.wait(), timeout=0.1)
    await clock.advance_ms(15_000)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    result = _payload(_published(mqtt, scene_result_topic(command_id))[-1])
    assert result["error"] == "timeout"
    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    assert bus.write_cancelled.is_set()


async def test_matching_execution_publishes_event_before_accepted_result_and_caches_replay(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, path = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_bus_commands(bus, 1)

    # #94: the confirming execution must be stamped at or after the command's
    # panel-clock baseline (the injected clock at issue time == _NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    event_index = next(
        i for i, item in enumerate(mqtt.published) if item[0] == scene_event_topic(_PANEL)
    )
    result_index = next(
        i for i, item in enumerate(mqtt.published) if item[0] == scene_result_topic(command_id)
    )
    assert event_index < result_index
    result = _published(mqtt, scene_result_topic(command_id))[-1]
    assert _payload(result) == {
        "accepted": True,
        "command_id": command_id,
        "mapping_version": MAPPING_VERSION,
        "panel": _PANEL,
        "scene_id": "all_off",
        "schema_version": SCHEMA_VERSION,
        "timestamp_ms": _NOW_MS,
    }
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    assert bridge._results[("scene", command_id)].event_key is None
    stored_result = json.loads(path.read_text())["results"][f"scene:{command_id}"]
    assert stored_result["event_key"] is None
    assert stored_result["delivered"] is True
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == 2
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == result[1]
    await bridge.async_shutdown()


async def test_completed_command_does_not_replay_after_original_command_ttl(tmp_path: Path) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_bus_commands(bus, 1)
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    first = _published(mqtt, scene_result_topic(command_id))[-1]

    await clock.advance_ms(20_000)
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == 1
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == first[1]
    await bridge.async_shutdown()


async def test_new_mode_execution_at_baseline_confirms_only_mode_pending_request(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(
        tmp_path,
        execution=_execution(mode_id="home", mode_at_ms=_NOW_MS - 1_000),
        mode_ids=("away", "home"),
    )
    scene_command_id = "22222222-2222-4222-8222-222222222222"
    mode_command_id = "33333333-3333-4333-8333-333333333333"
    await mqtt.inject(scene_command_topic(_PANEL), _command(scene_command_id, "scene", "all_off"))
    await mqtt.inject(mode_command_topic(_PANEL), _command(mode_command_id, "mode", "away"))

    await bus.emit(_execution(mode_id="away", mode_at_ms=_NOW_MS - 1))
    await _wait_for_publish(mqtt, mode_event_topic(_PANEL))
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    assert _published(mqtt, mode_result_topic(mode_command_id)) == []

    await bus.emit(_execution(mode_id="away", mode_at_ms=_NOW_MS))
    await _wait_for_publish(mqtt, mode_result_topic(mode_command_id))

    assert len(_published(mqtt, mode_event_topic(_PANEL))) == 2
    assert len(_published(mqtt, mode_result_topic(mode_command_id))) == 1
    assert _published(mqtt, scene_result_topic(scene_command_id)) == []
    await bridge.async_shutdown()


async def test_timeout_is_exactly_fifteen_seconds_and_shutdown_cancels_remaining_tasks(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))

    await clock.advance_ms(14_999)
    assert _published(mqtt, scene_result_topic(command_id)) == []
    await clock.advance_ms(1)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    timeout = _payload(_published(mqtt, scene_result_topic(command_id))[-1])
    assert timeout["accepted"] is False
    assert timeout["error"] == "timeout"

    second_id = "44444444-4444-4444-8444-444444444444"
    await mqtt.inject(scene_command_topic(_PANEL), _command(second_id, "scene", "all_off"))
    await bridge.async_shutdown()
    await clock.advance_ms(20_000)
    assert _published(mqtt, scene_result_topic(second_id)) == []
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


@pytest.mark.parametrize("case", ["expired", "unknown", "retained", "duplicate_pending"])
async def test_unsafe_or_duplicate_scene_commands_never_write(tmp_path: Path, case: str) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    issued_at_ms = _NOW_MS - 15_001 if case == "expired" else _NOW_MS
    scene_id = "unknown" if case == "unknown" else "all_off"
    command = _command(command_id, "scene", scene_id, issued_at_ms=issued_at_ms)

    await mqtt.inject(scene_command_topic(_PANEL), command, retained=case == "retained")
    if case == "duplicate_pending":
        await _wait_for_bus_commands(bus, 1)
        await mqtt.inject(scene_command_topic(_PANEL), command)

    assert len(bus.commands) == (1 if case == "duplicate_pending" else 0)
    await bridge.async_shutdown()


async def test_wrong_topic_panel_and_malformed_payload_never_write(tmp_path: Path) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject("unrelated", _command(command_id, "scene", "all_off"))
    mismatched = json.loads(_command(command_id, "scene", "all_off"))
    mismatched["panel"] = "kitchen"
    await mqtt.inject(scene_command_topic(_PANEL), encode_json(mismatched))
    await mqtt.inject(scene_command_topic(_PANEL), "not-json")

    assert bus.commands == []
    await bridge.async_shutdown()


async def test_write_failure_is_sanitized_and_cached(tmp_path: Path) -> None:
    class FailingBus(FakeBus):
        async def set_variables(
            self, device_id: str, peripheral_id: str, sets: list[VarSet]
        ) -> str:
            await super().set_variables(device_id, peripheral_id, sets)
            raise RuntimeError("token=secret\nunsafe")

    execution = _execution()
    bus = FailingBus([execution], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FakeMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", clock)
    await bridge.async_start()
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")

    await mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    first = _published(mqtt, scene_result_topic(command_id))[-1]
    await mqtt.inject(scene_command_topic(_PANEL), command)

    assert _payload(first)["error"] == "write_failed"
    assert "secret" not in first[1]
    assert len(bus.commands) == 1
    assert _published(mqtt, scene_result_topic(command_id))[-1][1] == first[1]
    await bridge.async_shutdown()


async def test_malformed_execution_degrades_status_without_breaking_fanout(tmp_path: Path) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    reached: list[str] = []

    async def other_consumer(device: BrilliantDevice) -> None:
        reached.append(device.peripheral_id)

    bus.on_change(other_consumer)
    await bus.emit(_execution("all_off", malformed_scene=True))

    assert reached == ["execution_peripheral"]
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "malformed_data"
    await bus.emit(_execution("all_off", 500))
    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_mixed_initial_execution_seeds_valid_history_while_degrading_status(
    tmp_path: Path,
) -> None:
    valid = _execution("all_off", 500)
    malformed = _execution("broken", malformed_scene=True)
    mixed = _device("execution_peripheral", {**valid.variables, **malformed.variables})
    bridge, bus, mqtt, _, path = await _started(tmp_path, execution=mixed)

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert json.loads(path.read_text())["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False

    await bus.emit(valid)
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    await bridge.async_shutdown()


async def test_reconnect_malformed_execution_stays_degraded_after_valid_catalog(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    bus.set_devices([_execution("all_off", malformed_scene=True)])

    await bus.fire_reconnect()

    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "malformed_data"
    await bridge.async_shutdown()


async def test_missing_execution_is_unavailable_and_reconnect_clears_stale_route(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    bus.set_devices([])

    await bus.fire_reconnect()

    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "execution_unavailable"
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    assert bus.commands == []
    assert _payload(_published(mqtt, scene_result_topic(command_id))[-1])["error"] == (
        "execution_unavailable"
    )
    await bridge.async_shutdown()


async def test_failed_terminal_result_publish_retries_until_delivered(tmp_path: Path) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"

    class OneResultFailsMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.failed = False

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_result_topic(command_id) and not self.failed:
                self.failed = True
                raise RuntimeError("broker unavailable")
            await super().publish(topic, payload, retain, qos)

    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = OneResultFailsMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    for _ in range(200):
        if mqtt.failed:
            break
        await asyncio.sleep(0.001)
    assert mqtt.failed is True
    assert _published(mqtt, scene_result_topic(command_id)) == []

    await clock.advance_ms(1_000)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    results = _published(mqtt, scene_result_topic(command_id))
    assert len(results) == 1
    assert _payload(results[0])["accepted"] is True
    await bridge.async_shutdown()


async def test_failed_scene_catalog_read_does_not_prevent_scoped_mode_read(tmp_path: Path) -> None:
    class OneReadFailsBus(FakeBus):
        async def get_peripheral(
            self, device_id: str, peripheral_id: str
        ) -> BrilliantDevice | None:
            if peripheral_id == "scene_configuration":
                self.scoped_reads.append((device_id, peripheral_id))
                raise RuntimeError("scene read failed")
            return await super().get_peripheral(device_id, peripheral_id)

    bus = OneReadFailsBus([_execution()], scoped_devices=[_mode_catalog("away")])
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "watermarks.json", FakeClockMs(_NOW_MS))

    await bridge.async_start()

    assert bus.scoped_reads == [
        ("configuration_virtual_device", "scene_configuration"),
        ("configuration_virtual_device", "mode_configuration"),
    ]
    assert len(_published(mqtt, mode_catalog_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_reconnect_runs_existing_forward_reconcile_and_scene_reconcile(
    tmp_path: Path,
) -> None:
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    forward_reconciles: list[str] = []

    async def existing_panel_and_mesh_reconcile() -> None:
        forward_reconciles.append("panel-and-mesh")

    bus.on_reconnect(existing_panel_and_mesh_reconcile)
    bridge = SceneBridge(
        bus, FakeMqtt(), _PANEL, tmp_path / "watermarks.json", FakeClockMs(_NOW_MS)
    )
    await bridge.async_start()

    await bus.fire_reconnect()

    assert forward_reconciles == ["panel-and-mesh"]
    assert bus.scoped_reads.count(("configuration_virtual_device", "scene_configuration")) == 2
    await bridge.async_shutdown()


async def test_start_and_shutdown_are_idempotent_and_reject_callbacks_after_stop(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    await bridge.async_start()
    assert mqtt.subscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]

    await bridge.async_shutdown()
    await bridge.async_shutdown()
    before = list(mqtt.published)
    await bus.emit(_execution("all_off", 999))
    command_id = str(UUID("22222222-2222-4222-8222-222222222222"))
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))

    assert mqtt.published == before
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


async def test_undelivered_accepted_result_survives_process_restart(tmp_path: Path) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"

    class ResultOfflineMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_result_topic(command_id):
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain, qos)

    path = tmp_path / "state.json"
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = ResultOfflineMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    command = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), command)
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await asyncio.sleep(0)
    await bridge.async_shutdown()

    stored = json.loads(path.read_text())
    outcome = stored["results"][f"scene:{command_id}"]
    assert outcome["delivered"] is False
    assert outcome["fingerprint"]
    assert outcome["payload"]
    assert outcome["expires_at_ms"] > _NOW_MS

    restarted_bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()
    await _wait_for_publish(restarted_mqtt, scene_result_topic(command_id))

    results = _published(restarted_mqtt, scene_result_topic(command_id))
    assert len(results) == 1
    assert results[0][1] == outcome["payload"]
    await restarted.async_shutdown()


async def test_event_outbox_survives_restart_and_gates_accepted_result(tmp_path: Path) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"

    class FirstEventOfflineMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.fail_events = True

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_event_topic(_PANEL) and self.fail_events:
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain, qos)

    path = tmp_path / "state.json"
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FirstEventOfflineMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await asyncio.sleep(0)

    assert _published(mqtt, scene_result_topic(command_id)) == []
    stored = json.loads(path.read_text())
    assert any(not event["delivered"] for event in stored["events"].values())
    await bridge.async_shutdown()

    restarted_bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()
    await _wait_for_publish(restarted_mqtt, scene_result_topic(command_id))
    event_index = next(
        index
        for index, item in enumerate(restarted_mqtt.published)
        if item[0] == scene_event_topic(_PANEL)
    )
    result_index = next(
        index
        for index, item in enumerate(restarted_mqtt.published)
        if item[0] == scene_result_topic(command_id)
    )
    assert event_index < result_index
    await restarted.async_shutdown()


async def test_outbox_event_and_result_publish_at_qos1(tmp_path: Path) -> None:
    # Issue #92: durable outbox frames must go out at QoS 1 so publish() blocks
    # on the broker PUBACK before the record is committed delivered.
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    assert _published_qos(mqtt, scene_event_topic(_PANEL)) == [1]
    assert _published_qos(mqtt, scene_result_topic(command_id)) == [1]
    # The default QoS 0 stays for the out-of-scope retained catalog publishes.
    assert _published(mqtt, scene_catalog_topic(_PANEL))
    assert all(qos == 0 for qos in _published_qos(mqtt, scene_catalog_topic(_PANEL)))
    await bridge.async_shutdown()


async def test_event_publish_without_puback_replays_same_key_at_qos1(tmp_path: Path) -> None:
    # A disconnect between local send and PUBACK surfaces as a publish() raise.
    # The event record must stay undelivered and replay the identical dedup
    # key/topic/payload at QoS 1 on the next attempt (event still before result).
    command_id = "22222222-2222-4222-8222-222222222222"

    class FirstEventNoPubackMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.event_attempts = 0

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_event_topic(_PANEL):
                self.event_attempts += 1
                if self.event_attempts == 1:
                    raise RuntimeError("disconnected before PUBACK")
            await super().publish(topic, payload, retain, qos)

    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FirstEventNoPubackMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    for _ in range(200):
        if mqtt.event_attempts >= 1:
            break
        await asyncio.sleep(0.001)
    assert mqtt.event_attempts >= 1
    # No PUBACK -> nothing committed on the wire, and the result stays gated.
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert _published(mqtt, scene_result_topic(command_id)) == []

    await clock.advance_ms(1_000)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    events = _published(mqtt, scene_event_topic(_PANEL))
    assert len(events) == 1
    assert _published_qos(mqtt, scene_event_topic(_PANEL)) == [1]
    event_index = next(
        i for i, item in enumerate(mqtt.published) if item[0] == scene_event_topic(_PANEL)
    )
    result_index = next(
        i for i, item in enumerate(mqtt.published) if item[0] == scene_result_topic(command_id)
    )
    assert event_index < result_index
    await bridge.async_shutdown()


async def test_result_publish_without_puback_replays_same_command_id_at_qos1(
    tmp_path: Path,
) -> None:
    # Same guarantee for the command result: a raise before PUBACK leaves it
    # undelivered and the identical command_id/payload is replayed at QoS 1.
    command_id = "22222222-2222-4222-8222-222222222222"

    class FirstResultNoPubackMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.result_attempts = 0

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_result_topic(command_id):
                self.result_attempts += 1
                if self.result_attempts == 1:
                    raise RuntimeError("disconnected before PUBACK")
            await super().publish(topic, payload, retain, qos)

    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FirstResultNoPubackMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))
    for _ in range(200):
        if mqtt.result_attempts >= 1:
            break
        await asyncio.sleep(0.001)
    assert mqtt.result_attempts >= 1
    assert _published(mqtt, scene_result_topic(command_id)) == []

    await clock.advance_ms(1_000)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    results = _published(mqtt, scene_result_topic(command_id))
    assert len(results) == 1
    assert _payload(results[0])["command_id"] == command_id
    assert _payload(results[0])["accepted"] is True
    assert _published_qos(mqtt, scene_result_topic(command_id)) == [1]
    await bridge.async_shutdown()


async def test_undelivered_event_and_result_replay_identically_at_qos1_after_restart(
    tmp_path: Path,
) -> None:
    # End-to-end durability: with the broker offline both outbox frames persist
    # undelivered; a fresh process reconstructed from that file replays the exact
    # same payloads at QoS 1, event before result.
    command_id = "22222222-2222-4222-8222-222222222222"

    class OfflineOutboxMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic in (scene_event_topic(_PANEL), scene_result_topic(command_id)):
                raise RuntimeError("offline before PUBACK")
            await super().publish(topic, payload, retain, qos)

    path = tmp_path / "state.json"
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = OfflineOutboxMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await asyncio.sleep(0)
    await bridge.async_shutdown()

    stored = json.loads(path.read_text())
    stored_event = next(iter(stored["events"].values()))
    assert stored_event["delivered"] is False
    stored_result = stored["results"][f"scene:{command_id}"]
    assert stored_result["delivered"] is False

    restarted_bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    restarted_mqtt = FakeMqtt()
    restarted = SceneBridge(restarted_bus, restarted_mqtt, _PANEL, path, clock)
    await restarted.async_start()
    await _wait_for_publish(restarted_mqtt, scene_result_topic(command_id))

    assert [item[1] for item in _published(restarted_mqtt, scene_event_topic(_PANEL))] == [
        stored_event["payload"]
    ]
    assert [item[1] for item in _published(restarted_mqtt, scene_result_topic(command_id))] == [
        stored_result["payload"]
    ]
    assert _published_qos(restarted_mqtt, scene_event_topic(_PANEL)) == [1]
    assert _published_qos(restarted_mqtt, scene_result_topic(command_id)) == [1]
    event_index = next(
        i for i, item in enumerate(restarted_mqtt.published) if item[0] == scene_event_topic(_PANEL)
    )
    result_index = next(
        i
        for i, item in enumerate(restarted_mqtt.published)
        if item[0] == scene_result_topic(command_id)
    )
    assert event_index < result_index
    await restarted.async_shutdown()


async def test_second_command_after_delivery_and_prune(tmp_path: Path) -> None:
    await _assert_second_command_after_delivery_and_prune(tmp_path, "scene")


async def test_second_mode_command_after_delivery_and_prune(tmp_path: Path) -> None:
    await _assert_second_command_after_delivery_and_prune(tmp_path, "mode")


async def test_restart_repairs_delivered_scene_result_dependency(tmp_path: Path) -> None:
    await _assert_restart_repairs_delivered_result_dependency(tmp_path, "scene")


async def test_restart_repairs_delivered_mode_result_dependency(tmp_path: Path) -> None:
    await _assert_restart_repairs_delivered_result_dependency(tmp_path, "mode")


async def test_lost_puback_after_broker_receipt_republishes_identical_duplicate_at_qos1(
    tmp_path: Path,
) -> None:
    # PUBACK lost after the broker already stored the frame: publish() still
    # raises, so the loop replays the SAME command_id/payload at QoS 1 -> a
    # duplicate on the wire, not a fabricated second record. Downstream HA-side
    # dedup on command_id makes the duplicate safe (stated in the PR contract).
    command_id = "22222222-2222-4222-8222-222222222222"

    class LostPubackMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.result_attempts = 0

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_result_topic(command_id):
                self.result_attempts += 1
                if self.result_attempts == 1:
                    # Broker got the frame; record it, then the PUBACK is lost.
                    await super().publish(topic, payload, retain, qos)
                    raise RuntimeError("PUBACK lost after broker receipt")
            await super().publish(topic, payload, retain, qos)

    path = tmp_path / "state.json"
    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = LostPubackMqtt()
    clock = FakeClockMs(_NOW_MS)
    bridge = SceneBridge(bus, mqtt, _PANEL, path, clock)
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    for _ in range(200):
        if mqtt.result_attempts >= 1:
            break
        await asyncio.sleep(0.001)

    await clock.advance_ms(1_000)
    await _wait_for_publish(mqtt, scene_result_topic(command_id), count=2)
    await bridge.async_shutdown()

    results = _published(mqtt, scene_result_topic(command_id))
    assert len(results) == 2
    assert results[0][1] == results[1][1]
    assert _payload(results[0])["command_id"] == command_id
    assert _payload(results[1])["command_id"] == command_id
    assert _published_qos(mqtt, scene_result_topic(command_id)) == [1, 1]
    stored = json.loads(path.read_text())
    assert [key for key in stored["results"] if command_id in key] == [f"scene:{command_id}"]


async def test_corrupt_state_seeds_baseline_without_history_and_normalizes_permissions(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    path.write_text("{corrupt")
    path.chmod(0o644)
    bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))

    await bridge.async_start()

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    stored = json.loads(path.read_text())
    assert stored["version"] == 1
    assert stored["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    await bridge.async_shutdown()


async def test_corrupt_state_without_snapshot_suppresses_first_observed_record(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "watermarks": {
                    _PANEL: {
                        "all_off": {
                            "executed_at_ms": "bad",
                            "payload_sha256": "not-a-hash",
                        }
                    }
                },
                "events": {},
                "results": {},
                "pending": {},
                "mode_watermarks": {},
            }
        )
    )
    bus = FakeBus([], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await bridge.async_start()

    await bus.emit(_execution("all_off", 500))
    await asyncio.sleep(0)
    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    await bus.emit(_execution("all_off", 600))
    await asyncio.sleep(0)
    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    await bridge.async_shutdown()


async def test_persistence_failure_refuses_commands_and_degrades_status(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    def fail_write(_path: Path, _state: scene_state.SceneState) -> None:
        raise OSError("disk full")

    monkeypatch.setattr(scene_bridge_module, "atomic_write_state", fail_write)
    bridge, bus, mqtt, _, _ = await _started(tmp_path, execution=_execution("all_off", 500))
    command_id = "22222222-2222-4222-8222-222222222222"

    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))

    assert bus.commands == []
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["available"] is False
    assert status["reason"] == "state_untrusted"
    await bridge.async_shutdown()


async def test_completed_id_reuse_validates_context_and_fingerprint_before_replay(
    tmp_path: Path,
) -> None:
    bridge, bus, mqtt, clock, _ = await _started(tmp_path, scene_ids=("all_off", "all_on"))
    command_id = "22222222-2222-4222-8222-222222222222"
    original = _command(command_id, "scene", "all_off")
    await mqtt.inject(scene_command_topic(_PANEL), original)
    await _wait_for_bus_commands(bus, 1)
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    original_results = len(_published(mqtt, scene_result_topic(command_id)))

    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command(command_id, "scene", "all_on"),
    )
    await mqtt.inject(scene_command_topic(_PANEL), original, retained=True)
    await clock.advance_ms(15_001)
    await mqtt.inject(scene_command_topic(_PANEL), original)

    assert len(bus.commands) == 1
    assert len(_published(mqtt, scene_result_topic(command_id))) == original_results
    await bridge.async_shutdown()


async def test_hung_reconcile_and_publish_callbacks_do_not_block_shutdown(tmp_path: Path) -> None:
    class HangingBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.hang_reads = False
            self.read_started = asyncio.Event()

        async def get_all(self) -> list[BrilliantDevice]:
            if self.hang_reads:
                self.read_started.set()
                await asyncio.Future()
            return await super().get_all()

    class HangingEventMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.hang_events = False
            self.publish_started = asyncio.Event()

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_event_topic(_PANEL) and self.hang_events:
                self.publish_started.set()
                await asyncio.Future()
            await super().publish(topic, payload, retain, qos)

    bus = HangingBus()
    mqtt = HangingEventMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    bus.hang_reads = True
    mqtt.hang_events = True

    await asyncio.wait_for(bus.fire_reconnect(), timeout=0.1)
    await asyncio.wait_for(bus.read_started.wait(), timeout=0.1)
    await asyncio.wait_for(bus.emit(_execution("all_off", 500)), timeout=0.1)
    await asyncio.sleep(0)

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    before = list(mqtt.published)
    await asyncio.sleep(0)
    assert mqtt.published == before


async def test_hung_event_publish_is_cancelled_before_shutdown_returns(tmp_path: Path) -> None:
    class HangingEventMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.publish_started = asyncio.Event()
            self.publish_cancelled = asyncio.Event()

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_event_topic(_PANEL):
                self.publish_started.set()
                try:
                    await asyncio.Future()
                except asyncio.CancelledError:
                    self.publish_cancelled.set()
                    raise
            await super().publish(topic, payload, retain, qos)

    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = HangingEventMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()

    await asyncio.wait_for(bus.emit(_execution("all_off", 500)), timeout=0.1)
    await asyncio.wait_for(mqtt.publish_started.wait(), timeout=0.1)
    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)

    assert mqtt.publish_cancelled.is_set()
    assert _published(mqtt, scene_event_topic(_PANEL)) == []


async def test_valid_existing_state_permissions_are_normalized_on_load(tmp_path: Path) -> None:
    path = tmp_path / "state.json"
    path.write_text(
        json.dumps(
            {
                "version": 1,
                "watermarks": {},
                "mode_watermarks": {},
                "events": {},
                "results": {},
                "pending": {},
            }
        )
    )
    path.chmod(0o644)
    bridge = SceneBridge(
        FakeBus([], scoped_devices=[_scene_catalog(), _mode_catalog()]),
        FakeMqtt(),
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )

    await bridge.async_start()

    assert stat.S_IMODE(path.stat().st_mode) == 0o600
    assert stat.S_IMODE(path.parent.stat().st_mode) == 0o700
    await bridge.async_shutdown()


async def test_unreadable_state_requires_and_seeds_a_fresh_baseline(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    path = tmp_path / "state.json"
    path.write_text("unreadable")
    original_read_text = Path.read_text

    def fail_target_read(
        target: Path, encoding: str | None = None, errors: str | None = None
    ) -> str:
        if target == path:
            raise PermissionError("denied")
        return original_read_text(target, encoding, errors)

    monkeypatch.setattr(Path, "read_text", fail_target_read)
    bus = FakeBus(
        [_execution("all_off", 500)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))

    await bridge.async_start()

    assert _published(mqtt, scene_event_topic(_PANEL)) == []
    monkeypatch.setattr(Path, "read_text", original_read_text)
    assert json.loads(path.read_text())["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    await bridge.async_shutdown()


async def test_result_capacity_never_discards_undelivered_outcome(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("brilliant_mqtt.scene_bridge._RESULT_CACHE_LIMIT", 1)
    first_id = "22222222-2222-4222-8222-222222222222"
    second_id = "44444444-4444-4444-8444-444444444444"

    class ResultOfflineMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if "/scene/result/" in topic:
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain, qos)

    bus = FakeBus([_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")])
    mqtt = ResultOfflineMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await mqtt.inject(scene_command_topic(_PANEL), _command(first_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await mqtt.inject(scene_command_topic(_PANEL), _command(second_id, "scene", "all_off"))

    assert len(bus.commands) == 1
    stored = json.loads((tmp_path / "state.json").read_text())
    assert list(stored["results"]) == [f"scene:{first_id}"]
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_capacity"
    await bridge.async_shutdown()


async def test_delivered_result_at_capacity_is_evicted_for_new_physical_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scene_bridge_module, "_RESULT_CACHE_LIMIT", 1)
    first_id = "22222222-2222-4222-8222-222222222222"
    second_id = "44444444-4444-4444-8444-444444444444"
    bridge, bus, mqtt, _, _ = await _started(
        tmp_path,
        scene_ids=("all_off", "all_on"),
    )
    await mqtt.inject(scene_command_topic(_PANEL), _command(first_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    # #94: confirming execution stamped at the command baseline (_NOW_MS).
    await bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(mqtt, scene_result_topic(first_id))

    await mqtt.inject(scene_command_topic(_PANEL), _command(second_id, "scene", "all_on"))

    await _wait_for_bus_commands(bus, 2)
    assert len(bus.commands) == 2
    await bridge.async_shutdown()


async def test_event_capacity_never_discards_undelivered_event(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr("brilliant_mqtt.scene_bridge._EVENT_OUTBOX_LIMIT", 1)

    class EventOfflineMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_event_topic(_PANEL):
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain, qos)

    path = tmp_path / "state.json"
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off", "all_on"), _mode_catalog("away")],
    )
    mqtt = EventOfflineMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await bus.emit(_execution("all_off", 500))
    await bus.emit(_execution("all_on", 600))

    stored = json.loads(path.read_text())
    assert len(stored["events"]) == 1
    assert "all_on" not in stored["watermarks"][_PANEL]
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_capacity"
    await bridge.async_shutdown()


async def test_inflight_command_is_durable_before_write_and_never_rewrites_after_restart(
    tmp_path: Path,
) -> None:
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    path = tmp_path / "state.json"
    first_bus = FakeBus(
        [_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    first_mqtt = FakeMqtt()
    first = SceneBridge(first_bus, first_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await first.async_start()

    await first_mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_bus_commands(first_bus, 1)
    assert len(first_bus.commands) == 1
    await first.async_shutdown()
    assert f"scene:{command_id}" in json.loads(path.read_text())["pending"]

    second_bus = FakeBus(
        [_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    second_mqtt = FakeMqtt()
    second = SceneBridge(second_bus, second_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await second.async_start()
    await second_mqtt.inject(scene_command_topic(_PANEL), command)

    assert second_bus.commands == []
    # #94: the persisted pending's confirm_after_ms is the original baseline
    # (_NOW_MS); a confirming execution must be stamped at or after it.
    await second_bus.emit(_execution("all_off", _NOW_MS))
    await _wait_for_publish(second_mqtt, scene_result_topic(command_id))
    assert _payload(_published(second_mqtt, scene_result_topic(command_id))[-1])["accepted"] is True
    await second.async_shutdown()


async def test_partial_persisted_result_payload_marks_entire_state_untrusted(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    command_id = "22222222-2222-4222-8222-222222222222"
    state = {
        "version": 1,
        "watermarks": {},
        "mode_watermarks": {},
        "events": {},
        "pending": {},
        "results": {
            f"scene:{command_id}": {
                "kind": "scene",
                "command_id": command_id,
                "fingerprint": "a" * 64,
                "topic": scene_result_topic(command_id),
                "payload": json.dumps({"command_id": command_id}),
                "delivered": False,
                "expires_at_ms": _NOW_MS + 1_000,
                "event_key": None,
            }
        },
    }
    path.write_text(json.dumps(state))
    mqtt = FakeMqtt()
    bridge = SceneBridge(
        FakeBus([], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]),
        mqtt,
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )

    await bridge.async_start()

    assert _published(mqtt, scene_result_topic(command_id)) == []
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_untrusted"
    await bridge.async_shutdown()


async def test_mode_watermark_survives_restart_without_initial_execution_snapshot(
    tmp_path: Path,
) -> None:
    path = tmp_path / "state.json"
    first = SceneBridge(
        FakeBus(
            [_execution(mode_id="away", mode_at_ms=500)],
            scoped_devices=[_scene_catalog(), _mode_catalog("away")],
        ),
        FakeMqtt(),
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )
    await first.async_start()
    await first.async_shutdown()

    second_bus = FakeBus([], scoped_devices=[_scene_catalog(), _mode_catalog("away")])
    second_mqtt = FakeMqtt()
    second = SceneBridge(second_bus, second_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await second.async_start()
    await second_bus.emit(_execution(mode_id="away", mode_at_ms=500))

    assert _published(second_mqtt, mode_event_topic(_PANEL)) == []
    await second.async_shutdown()


async def test_shutdown_cancels_inflight_start_and_unsubscribes_started_topics(
    tmp_path: Path,
) -> None:
    class PausedStartBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.read_started = asyncio.Event()

        async def get_all(self) -> list[BrilliantDevice]:
            self.read_started.set()
            await asyncio.Future()
            return []

    bus = PausedStartBus()
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    start_task = asyncio.create_task(bridge.async_start())
    await asyncio.wait_for(bus.read_started.wait(), timeout=0.1)

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    await asyncio.gather(start_task, return_exceptions=True)

    assert mqtt.subscriptions == []
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


async def test_shutdown_abandons_write_that_delays_cancellation(tmp_path: Path) -> None:
    class DelayedCancellationBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.started = asyncio.Event()
            self.release = asyncio.Event()

        async def set_variables(
            self, device_id: str, peripheral_id: str, sets: list[VarSet]
        ) -> str:
            await super().set_variables(device_id, peripheral_id, sets)
            self.started.set()
            try:
                await asyncio.Future()
            except asyncio.CancelledError:
                await self.release.wait()
            return self.set_variables_receipt

    bus = DelayedCancellationBus()
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command("22222222-2222-4222-8222-222222222222", "scene", "all_off"),
    )
    await asyncio.wait_for(bus.started.wait(), timeout=0.1)

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.1)
    bus.release.set()
    await asyncio.sleep(0)

    assert _published(mqtt, scene_result_topic("22222222-2222-4222-8222-222222222222")) == []


async def test_state_load_and_writes_never_run_on_event_loop_thread(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    event_loop_thread = threading.get_ident()
    observed: list[tuple[str, int]] = []
    real_load = scene_state.load_state
    real_write = scene_state.atomic_write_state

    def observed_load(path: Path) -> scene_state.LoadedSceneState:
        observed.append(("load", threading.get_ident()))
        return real_load(path)

    def observed_write(path: Path, state: scene_state.SceneState) -> None:
        observed.append(("write", threading.get_ident()))
        real_write(path, state)

    monkeypatch.setattr(scene_bridge_module, "load_state", observed_load, raising=False)
    monkeypatch.setattr(scene_bridge_module, "atomic_write_state", observed_write, raising=False)
    bus = FakeBus(
        [_execution("all_off", 100)],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    bridge = SceneBridge(bus, FakeMqtt(), _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))

    await bridge.async_start()
    await bus.emit(_execution("all_off", 200))
    await bridge.async_shutdown()

    assert {kind for kind, _ in observed} == {"load", "write"}
    assert all(thread != event_loop_thread for _, thread in observed)


async def test_cancelled_persistence_keeps_snapshot_writes_serial_and_newest_wins(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    first_write_started = threading.Event()
    release_first_write = threading.Event()
    guard = threading.Lock()
    active_writers = 0
    maximum_active = 0
    write_order: list[int | None] = []
    real_write = scene_state.atomic_write_state

    def ordered_write(path: Path, state: scene_state.SceneState) -> None:
        nonlocal active_writers, maximum_active
        watermarks = dict(state.watermarks)
        watermark = watermarks.get((_PANEL, "all_off"))
        executed_at_ms = None if watermark is None else watermark.executed_at_ms
        with guard:
            active_writers += 1
            maximum_active = max(maximum_active, active_writers)
            write_order.append(executed_at_ms)
        try:
            if executed_at_ms == 100:
                first_write_started.set()
                assert release_first_write.wait(timeout=1)
            real_write(path, state)
        finally:
            with guard:
                active_writers -= 1

    monkeypatch.setattr(scene_bridge_module, "atomic_write_state", ordered_write)
    path = tmp_path / "state.json"
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await bridge.async_start()

    try:
        await bus.emit(_execution("all_off", 100))
        assert first_write_started.wait(timeout=0.1)
        await bus.emit(_execution("all_off", 200))
        before_shutdown_release = list(mqtt.published)
        await asyncio.wait_for(bridge.async_shutdown(), timeout=0.2)
    finally:
        release_first_write.set()

    for _ in range(200):
        try:
            persisted = json.loads(path.read_text())
            if persisted["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 200:
                break
        except (FileNotFoundError, KeyError, json.JSONDecodeError):
            pass
        await asyncio.sleep(0.001)
    else:
        pytest.fail("newest queued snapshot did not win after cancellation")

    assert maximum_active == 1
    assert write_order.index(100) < write_order.index(200)
    assert mqtt.published == before_shutdown_release


async def test_replacement_bridge_state_io_queues_behind_abandoned_old_writer(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    blocked = threading.Event()
    release = threading.Event()
    old_write_finished = threading.Event()
    observations: list[tuple[str, int | None]] = []
    guard = threading.Lock()
    real_load = scene_state.load_state
    real_write = scene_state.atomic_write_state

    def observed_load(path: Path) -> scene_state.LoadedSceneState:
        loaded = real_load(path)
        watermark = dict(loaded.state.watermarks).get((_PANEL, "all_off"))
        with guard:
            observations.append(("load", None if watermark is None else watermark.executed_at_ms))
        return loaded

    def observed_write(path: Path, state: scene_state.SceneState) -> None:
        watermark = dict(state.watermarks).get((_PANEL, "all_off"))
        executed_at_ms = None if watermark is None else watermark.executed_at_ms
        with guard:
            observations.append(("write", executed_at_ms))
        if executed_at_ms == 100:
            blocked.set()
            assert release.wait(timeout=1)
        real_write(path, state)
        if executed_at_ms == 100:
            old_write_finished.set()

    monkeypatch.setattr(scene_bridge_module, "load_state", observed_load)
    monkeypatch.setattr(scene_bridge_module, "atomic_write_state", observed_write)
    path = tmp_path / "state.json"
    old_bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    old = SceneBridge(old_bus, FakeMqtt(), _PANEL, path, FakeClockMs(_NOW_MS))
    await old.async_start()
    replacement = SceneBridge(
        FakeBus(
            [_execution("all_off", 200)],
            scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
        ),
        FakeMqtt(),
        _PANEL,
        path,
        FakeClockMs(_NOW_MS),
    )
    try:
        await old_bus.emit(_execution("all_off", 100))
        assert blocked.wait(timeout=0.1)
        await asyncio.wait_for(old.async_shutdown(), timeout=0.2)
        replacement_start = asyncio.create_task(replacement.async_start())
        await asyncio.sleep(0.02)
        replacement_was_queued = not replacement_start.done()
    finally:
        release.set()
    await asyncio.wait_for(replacement_start, timeout=0.2)
    for _ in range(200):
        if old_write_finished.is_set():
            break
        await asyncio.sleep(0.001)

    assert replacement_was_queued is True
    assert ("load", 100) in observations
    assert json.loads(path.read_text())["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 200
    await replacement.async_shutdown()


async def test_event_capacity_blocks_new_physical_command_even_with_result_room(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scene_bridge_module, "_EVENT_OUTBOX_LIMIT", 1)

    class EventOfflineMqtt(FakeMqtt):
        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            if topic == scene_event_topic(_PANEL):
                raise RuntimeError("offline")
            await super().publish(topic, payload, retain, qos)

    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    mqtt = EventOfflineMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    await bus.emit(_execution("all_off", 500))

    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command("22222222-2222-4222-8222-222222222222", "scene", "all_off"),
    )

    assert bus.commands == []
    await bridge.async_shutdown()


async def test_pending_capacity_blocks_every_additional_physical_command(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scene_bridge_module, "_PENDING_LIMIT", 1, raising=False)
    bridge, bus, mqtt, _, _ = await _started(tmp_path, scene_ids=("all_off", "all_on"))
    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command("22222222-2222-4222-8222-222222222222", "scene", "all_off"),
    )

    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command("44444444-4444-4444-8444-444444444444", "scene", "all_on"),
    )

    await _wait_for_bus_commands(bus, 1)
    assert len(bus.commands) == 1
    await bridge.async_shutdown()


async def test_scene_watermark_growth_is_bounded_during_live_execution(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    monkeypatch.setattr(scene_bridge_module, "_SCENE_WATERMARK_LIMIT", 1, raising=False)
    bridge, bus, mqtt, _, path = await _started(
        tmp_path,
        execution=_execution("all_off", 100),
        scene_ids=("all_off", "all_on"),
    )

    await bus.emit(_execution("all_on", 200))

    stored = json.loads(path.read_text())
    assert set(stored["watermarks"][_PANEL]) == {"all_off"}
    status = _payload(_published(mqtt, transport_status_topic("scene", _PANEL))[-1])
    assert status["reason"] == "state_capacity"
    await bridge.async_shutdown()


async def test_startup_merges_execution_change_after_snapshot_before_activation(
    tmp_path: Path,
) -> None:
    class PausedCatalogBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [_execution()],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.catalog_started = asyncio.Event()
            self.release_catalog = asyncio.Event()

        async def get_peripheral(
            self, device_id: str, peripheral_id: str
        ) -> BrilliantDevice | None:
            if peripheral_id == "scene_configuration":
                self.catalog_started.set()
                await self.release_catalog.wait()
            return await super().get_peripheral(device_id, peripheral_id)

    bus = PausedCatalogBus()
    mqtt = FakeMqtt()
    path = tmp_path / "state.json"
    bridge = SceneBridge(bus, mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    start_task = asyncio.create_task(bridge.async_start())
    await asyncio.wait_for(bus.catalog_started.wait(), timeout=0.1)

    await bus.emit(_execution("all_off", 500))
    bus.release_catalog.set()
    await asyncio.wait_for(start_task, timeout=0.1)

    events = _published(mqtt, scene_event_topic(_PANEL))
    assert len(events) == 1
    assert _payload(events[0])["executed_at_ms"] == 500
    assert json.loads(path.read_text())["watermarks"][_PANEL]["all_off"]["executed_at_ms"] == 500
    await bridge.async_shutdown()


async def test_reconcile_discards_snapshot_older_than_same_epoch_live_change(
    tmp_path: Path,
) -> None:
    old_execution = _device(
        "execution_peripheral", _execution("all_off", 100).variables, device_id="old-panel"
    )
    new_execution = _device(
        "execution_peripheral", _execution("all_off", 200).variables, device_id="new-panel"
    )

    class DelayedReconcileBus(FakeBus):
        def __init__(self) -> None:
            super().__init__(
                [old_execution],
                scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
            )
            self.delay_reads = False
            self.read_started = asyncio.Event()
            self.release_read = asyncio.Event()

        async def get_all(self) -> list[BrilliantDevice]:
            snapshot = await super().get_all()
            if self.delay_reads:
                self.read_started.set()
                await self.release_read.wait()
            return snapshot

    bus = DelayedReconcileBus()
    mqtt = FakeMqtt()
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    bus.delay_reads = True
    reconcile_task = asyncio.create_task(bridge.async_reconcile())
    await asyncio.wait_for(bus.read_started.wait(), timeout=0.1)

    await bus.emit(new_execution)
    bus.release_read.set()
    await asyncio.wait_for(reconcile_task, timeout=0.1)
    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command("22222222-2222-4222-8222-222222222222", "scene", "all_off"),
    )

    await _wait_for_bus_commands(bus, 1)
    assert bus.commands[-1][0] == "new-panel"
    await bridge.async_shutdown()


async def test_cancel_after_subscribe_return_always_releases_acquired_topic(
    tmp_path: Path,
) -> None:
    class PausedSubscribeMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.return_gate = asyncio.Event()
            self.subscribed = asyncio.Event()

        async def subscribe(self, topic: str) -> None:
            await super().subscribe(topic)
            if len(self.subscriptions) == 1:
                self.subscribed.set()
                await self.return_gate.wait()

    mqtt = PausedSubscribeMqtt()
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    start_task = asyncio.create_task(bridge.async_start())
    await asyncio.wait_for(mqtt.subscribed.wait(), timeout=0.1)
    await bridge._lock.acquire()
    mqtt.return_gate.set()
    await asyncio.sleep(0)
    shutdown_task = asyncio.create_task(bridge.async_shutdown())
    await asyncio.sleep(0)
    await asyncio.sleep(0)
    bridge._lock.release()

    await asyncio.wait_for(shutdown_task, timeout=0.2)
    await asyncio.gather(start_task, return_exceptions=True)

    assert mqtt.subscriptions == []
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


async def test_shutdown_bounds_each_unsubscribe(tmp_path: Path) -> None:
    class HangingUnsubscribeMqtt(FakeMqtt):
        async def unsubscribe(self, topic: str) -> None:
            await super().unsubscribe(topic)
            await asyncio.Future()

    mqtt = HangingUnsubscribeMqtt()
    bridge = SceneBridge(
        FakeBus(
            [_execution()],
            scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
        ),
        mqtt,
        _PANEL,
        tmp_path / "state.json",
        FakeClockMs(_NOW_MS),
    )
    await bridge.async_start()

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.2)

    assert mqtt.subscriptions == []
    assert mqtt.unsubscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]


class _CancellationResistantUnsubscribeMqtt(FakeMqtt):
    def __init__(self) -> None:
        super().__init__()
        self.release = asyncio.Event()
        self.started: set[str] = set()
        self.cancelled: set[str] = set()

    async def unsubscribe(self, topic: str) -> None:
        await super().unsubscribe(topic)
        self.started.add(topic)
        try:
            await asyncio.Future()
        except asyncio.CancelledError:
            self.cancelled.add(topic)
            await self.release.wait()


async def test_shutdown_abandons_cancellation_resistant_unsubscribe_tasks(
    tmp_path: Path,
) -> None:
    mqtt = _CancellationResistantUnsubscribeMqtt()
    bridge = SceneBridge(
        FakeBus(
            [_execution()],
            scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
        ),
        mqtt,
        _PANEL,
        tmp_path / "state.json",
        FakeClockMs(_NOW_MS),
    )
    await bridge.async_start()
    started_at = asyncio.get_running_loop().time()

    await asyncio.wait_for(bridge.async_shutdown(), timeout=0.2)

    try:
        assert asyncio.get_running_loop().time() - started_at < 0.15
        assert mqtt.started == {scene_command_topic(_PANEL), mode_command_topic(_PANEL)}
        assert mqtt.cancelled == mqtt.started
        assert len(bridge._abandoned_cleanup_tasks) == 2
    finally:
        mqtt.release.set()
    for _ in range(200):
        if not bridge._abandoned_cleanup_tasks:
            break
        await asyncio.sleep(0.001)
    assert bridge._abandoned_cleanup_tasks == set()


async def test_restart_is_fail_closed_until_abandoned_unsubscribe_finishes(
    tmp_path: Path,
) -> None:
    mqtt = _CancellationResistantUnsubscribeMqtt()
    bus = FakeBus(
        [_execution()],
        scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")],
    )
    bridge = SceneBridge(bus, mqtt, _PANEL, tmp_path / "state.json", FakeClockMs(_NOW_MS))
    await bridge.async_start()
    initial_reads = list(bus.scoped_reads)
    await bridge.async_shutdown()

    try:
        with pytest.raises(RuntimeError, match="cleanup incomplete"):
            await bridge.async_start()
        assert bus.scoped_reads == initial_reads
        assert mqtt.subscriptions == []
    finally:
        mqtt.release.set()
    for _ in range(200):
        if not bridge._abandoned_cleanup_tasks:
            break
        await asyncio.sleep(0.001)
    await bridge.async_start()
    assert mqtt.subscriptions == [scene_command_topic(_PANEL), mode_command_topic(_PANEL)]
    await bridge.async_shutdown()


# --- #94: request-relative execution evidence before confirming scene commands ---


async def test_delayed_historical_execution_never_confirms_scene_command(tmp_path: Path) -> None:
    """The #94 repro: an execution newer than the per-scene watermark but older
    than a just-issued command's panel-clock baseline must NOT confirm it. The
    delayed record still emits its native event and advances the watermark."""
    # Seed an ancient watermark (T-2000) silently at start.
    bridge, bus, mqtt, clock, _ = await _started(
        tmp_path, execution=_execution("all_off", _NOW_MS - 2_000)
    )
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)  # command issued at T; bus write completed

    # Delayed/historical execution: newer than the watermark (T-2000) but stamped
    # before the command baseline (T). It must publish, never confirm.
    await bus.emit(_execution("all_off", _NOW_MS - 1_000))
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    assert len(_published(mqtt, scene_event_topic(_PANEL))) == 1
    assert _payload(_published(mqtt, scene_event_topic(_PANEL))[-1])["executed_at_ms"] == (
        _NOW_MS - 1_000
    )
    assert _published(mqtt, scene_result_topic(command_id)) == []

    # The command stays pending and can only terminate by timeout: the sole
    # result is the timeout, and no accepted:true is ever emitted.
    await clock.advance_ms(COMMAND_TTL_MS)
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    results = _published(mqtt, scene_result_topic(command_id))
    assert [_payload(item)["accepted"] for item in results] == [False]
    assert _payload(results[-1])["error"] == "timeout"
    await bridge.async_shutdown()


async def test_execution_at_or_after_baseline_confirms_scene_command(tmp_path: Path) -> None:
    """Fresh success: an execution stamped after the command baseline confirms."""
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)

    await bus.emit(_execution("all_off", _NOW_MS + 500))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    assert _payload(_published(mqtt, scene_result_topic(command_id))[-1])["accepted"] is True
    await bridge.async_shutdown()


async def test_pre_baseline_record_never_confirms_and_pending_times_out(
    tmp_path: Path,
) -> None:
    """A stale record older than the baseline (but newer than the prior watermark)
    never confirms: the command stays pending and settles to a timeout (proving no
    silent confirmation). The mechanism still confirms a fresh command whose
    execution is at/after its own baseline."""
    bridge, bus, mqtt, clock, _ = await _started(
        tmp_path, execution=_execution("all_off", _NOW_MS - 3_000)
    )
    first_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(first_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)

    # Newer than the watermark (T-3000) yet older than the baseline (T): must not
    # confirm. Settle the negative by letting the command time out -- had the stale
    # record wrongly confirmed, an accepted:true would appear here instead of the
    # timeout (checking right after the event lands would be too early to notice).
    await bus.emit(_execution("all_off", _NOW_MS - 1_500))
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))
    await clock.advance_ms(COMMAND_TTL_MS)
    await _wait_for_publish(mqtt, scene_result_topic(first_id))
    first_results = _published(mqtt, scene_result_topic(first_id))
    assert [_payload(item)["accepted"] for item in first_results] == [False]
    assert _payload(first_results[-1])["error"] == "timeout"

    # A fresh command still confirms when its execution is at/after its baseline
    # (the clock is now T + COMMAND_TTL_MS).
    second_id = "44444444-4444-4444-8444-444444444444"
    await mqtt.inject(
        scene_command_topic(_PANEL),
        _command(second_id, "scene", "all_off", issued_at_ms=clock.now_ms),
    )
    await _wait_for_bus_commands(bus, 2)
    await bus.emit(_execution("all_off", clock.now_ms))
    await _wait_for_publish(mqtt, scene_result_topic(second_id))
    assert _payload(_published(mqtt, scene_result_topic(second_id))[-1])["accepted"] is True
    await bridge.async_shutdown()


async def test_stale_record_does_not_consume_pending_then_genuine_confirms(
    tmp_path: Path,
) -> None:
    # Regression guard: a pre-baseline (stale) record must NOT consume the pending
    # -- a later at/after-baseline execution must still confirm the SAME command.
    bridge, bus, mqtt, clock, _ = await _started(
        tmp_path, execution=_execution("all_off", _NOW_MS - 3_000)
    )
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)

    # Stale: newer than the watermark (T-3000), older than the baseline (T). It
    # publishes its event but must neither confirm nor consume the pending.
    await bus.emit(_execution("all_off", _NOW_MS - 1_500))
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))
    assert _published(mqtt, scene_result_topic(command_id)) == []

    # Genuine: at/after the baseline -> confirms the still-open pending.
    await bus.emit(_execution("all_off", _NOW_MS + 100))
    await _wait_for_publish(mqtt, scene_result_topic(command_id))
    assert _payload(_published(mqtt, scene_result_topic(command_id))[-1])["accepted"] is True

    # The genuine execution consumed the pending, so advancing past the TTL yields
    # no timeout: the sole result stays the accepted confirmation.
    await clock.advance_ms(COMMAND_TTL_MS + 1_000)
    await asyncio.sleep(0)
    results = _published(mqtt, scene_result_topic(command_id))
    assert [_payload(item)["accepted"] for item in results] == [True]
    await bridge.async_shutdown()


async def test_execution_equal_to_baseline_confirms_scene_command(tmp_path: Path) -> None:
    """Tie semantics: executed_at_ms == confirm_after_ms confirms (>=)."""
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)

    await bus.emit(_execution("all_off", _NOW_MS))  # exactly at the baseline
    await _wait_for_publish(mqtt, scene_result_topic(command_id))

    assert _payload(_published(mqtt, scene_result_topic(command_id))[-1])["accepted"] is True
    await bridge.async_shutdown()


@pytest.mark.parametrize(
    ("executed_at_ms", "expected"),
    [
        (_NOW_MS - 500, ()),  # E < B1: neither confirms
        (_NOW_MS + 500, ("first",)),  # B1 <= E < B2: first only
        (_NOW_MS + 2_000, ("first", "second")),  # E >= B2: both
    ],
)
async def test_multiple_pending_same_scene_confirm_by_baseline_band(
    tmp_path: Path,
    executed_at_ms: int,
    expected: tuple[str, ...],
) -> None:
    """Two commands for one scene with baselines B1 < B2: an execution at E
    confirms exactly those whose confirm_after_ms <= E."""
    bridge, bus, mqtt, clock, path = await _started(tmp_path)
    ids = {
        "first": "22222222-2222-4222-8222-222222222222",  # baseline B1 = T
        "second": "44444444-4444-4444-8444-444444444444",  # baseline B2 = T + 1000
    }
    await mqtt.inject(scene_command_topic(_PANEL), _command(ids["first"], "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)
    await clock.advance_ms(1_000)
    await mqtt.inject(scene_command_topic(_PANEL), _command(ids["second"], "scene", "all_off"))
    await _wait_for_bus_commands(bus, 2)

    await bus.emit(_execution("all_off", executed_at_ms))
    await _wait_for_publish(mqtt, scene_event_topic(_PANEL))

    for name in expected:
        command_id = ids[name]
        await _wait_for_publish(mqtt, scene_result_topic(command_id))
        assert _payload(_published(mqtt, scene_result_topic(command_id))[-1])["accepted"] is True

    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    if expected:
        stored_result = json.loads(path.read_text())["results"][f"scene:{ids[expected[0]]}"]
        assert stored_result["event_key"] is None
        assert stored_result["delivered"] is True

    # Settle every non-confirmed command to a timeout so an over-confirmation (a
    # wrong accepted:true) is caught, rather than checked before a result lands.
    timeout_tasks: list[asyncio.Task[None]] = []
    for name, command_id in ids.items():
        if name in expected:
            continue
        timeout_task = bridge._scene_pending[command_id].timeout_task
        assert timeout_task is not None
        timeout_tasks.append(timeout_task)
    await clock.advance_ms(COMMAND_TTL_MS)
    await asyncio.gather(*timeout_tasks)
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    for name, command_id in ids.items():
        if name in expected:
            continue
        await _wait_for_publish(mqtt, scene_result_topic(command_id))
        results = _published(mqtt, scene_result_topic(command_id))
        assert [_payload(item)["accepted"] for item in results] == [False]
        assert _payload(results[-1])["error"] == "timeout"

    stored = json.loads(path.read_text())
    assert set(stored["results"]) == {f"scene:{command_id}" for command_id in ids.values()}
    assert all(result["delivered"] is True for result in stored["results"].values())
    assert all(result["event_key"] is None for result in stored["results"].values())
    assert stored["events"] == {}
    status_topics = {
        transport_status_topic("scene", _PANEL),
        transport_status_topic("mode", _PANEL),
    }
    statuses = [_payload(item) for item in mqtt.published if item[0] in status_topics]
    assert all(status["reason"] != "state_untrusted" for status in statuses)
    await bridge.async_shutdown()


async def test_persisted_baseline_rejects_pre_baseline_replay_after_restart(
    tmp_path: Path,
) -> None:
    """confirm_after_ms is persisted on the durable pending, so after a restart a
    pre-baseline (delayed/historical) execution still refuses to confirm."""
    command_id = "22222222-2222-4222-8222-222222222222"
    command = _command(command_id, "scene", "all_off")
    path = tmp_path / "state.json"
    first_bus = FakeBus(
        [_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    first_mqtt = FakeMqtt()
    first = SceneBridge(first_bus, first_mqtt, _PANEL, path, FakeClockMs(_NOW_MS))
    await first.async_start()
    await first_mqtt.inject(scene_command_topic(_PANEL), command)
    await _wait_for_bus_commands(first_bus, 1)
    await first.async_shutdown()

    entry = json.loads(path.read_text())["pending"][f"scene:{command_id}"]
    assert entry["confirm_after_ms"] == _NOW_MS
    assert entry["expires_at_ms"] == _NOW_MS + COMMAND_TTL_MS

    second_bus = FakeBus(
        [_execution()], scoped_devices=[_scene_catalog("all_off"), _mode_catalog("away")]
    )
    second_mqtt = FakeMqtt()
    second_clock = FakeClockMs(_NOW_MS)
    second = SceneBridge(second_bus, second_mqtt, _PANEL, path, second_clock)
    await second.async_start()
    assert second_bus.commands == []

    # Pre-baseline replay must not confirm the restored pending. Settle to a
    # timeout: had the persisted confirm_after_ms failed to reject the replay, an
    # accepted:true would appear here instead of the timeout.
    await second_bus.emit(_execution("all_off", _NOW_MS - 1_000))
    await _wait_for_publish(second_mqtt, scene_event_topic(_PANEL))
    await second_clock.advance_ms(COMMAND_TTL_MS)
    await _wait_for_publish(second_mqtt, scene_result_topic(command_id))
    results = _published(second_mqtt, scene_result_topic(command_id))
    assert [_payload(item)["accepted"] for item in results] == [False]
    assert _payload(results[-1])["error"] == "timeout"
    await second.async_shutdown()


async def test_malformed_execution_never_confirms_pending_scene_command(tmp_path: Path) -> None:
    """A missing/invalid execution timestamp is dropped upstream by the codec, so
    it can never confirm a pending command (behavior unchanged by #94)."""
    bridge, bus, mqtt, _, _ = await _started(tmp_path)
    command_id = "22222222-2222-4222-8222-222222222222"
    await mqtt.inject(scene_command_topic(_PANEL), _command(command_id, "scene", "all_off"))
    await _wait_for_bus_commands(bus, 1)

    await bus.emit(_execution("all_off", malformed_scene=True))
    await asyncio.sleep(0)

    assert _published(mqtt, scene_result_topic(command_id)) == []
    await bridge.async_shutdown()


async def test_requesting_current_mode_confirms_immediately_without_execution_stamp(
    tmp_path: Path,
) -> None:
    # The bus does not re-stamp a same-value write, so state equality must settle
    # the request without creating a pending command that can only time out.
    seeded = _execution(mode_id="away", mode_at_ms=_NOW_MS - 1_000)
    bridge, bus, mqtt, _, _ = await _started(tmp_path, execution=seeded)
    mqtt.published.clear()

    command_id = "33333333-3333-4333-8333-333333333333"
    await mqtt.inject(mode_command_topic(_PANEL), _command(command_id, "mode", "away"))
    await _wait_for_publish(mqtt, mode_result_topic(command_id))

    assert bus.commands == []
    assert command_id not in bridge._mode_pending
    assert ("mode", command_id) not in bridge._pending_records
    assert _published(mqtt, mode_event_topic(_PANEL)) == []
    result = _payload(_published(mqtt, mode_result_topic(command_id))[-1])
    assert result["accepted"] is True
    assert "error" not in result
    await bridge.async_shutdown()


async def test_mode_watermark_and_request_baseline_reject_old_execution_stamps(
    tmp_path: Path,
) -> None:
    seeded_at_ms = _NOW_MS - 3_000
    bridge, bus, mqtt, clock, _ = await _started(
        tmp_path,
        execution=_execution(mode_id="away", mode_at_ms=seeded_at_ms),
        mode_ids=("away", "home"),
    )
    # Move current state away from the requested value while retaining chronology
    # that began at away@T-3000.
    await bridge.poll_executions([_execution(mode_id="home", mode_at_ms=_NOW_MS - 2_000)])
    await _wait_for_publish(mqtt, mode_event_topic(_PANEL))
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    mqtt.published.clear()
    delayed_id = "44444444-4444-4444-8444-444444444444"
    await mqtt.inject(mode_command_topic(_PANEL), _command(delayed_id, "mode", "away"))
    await _wait_for_bus_commands(bus, 1)

    await bridge.poll_executions([_execution(mode_id="away", mode_at_ms=seeded_at_ms - 1)])

    assert bridge._mode_watermarks[_PANEL] == (_NOW_MS - 2_000, "home")
    assert _published(mqtt, mode_event_topic(_PANEL)) == []
    assert _published(mqtt, mode_result_topic(delayed_id)) == []

    # This stamp is newer than the global watermark but still predates the
    # request, so it may emit an event but must not confirm the pending command.
    await bridge.poll_executions([_execution(mode_id="away", mode_at_ms=_NOW_MS - 1_000)])
    await _wait_for_publish(mqtt, mode_event_topic(_PANEL))
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)

    timeout_task = bridge._mode_pending[delayed_id].timeout_task
    assert timeout_task is not None
    await clock.advance_ms(COMMAND_TTL_MS)
    await timeout_task
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    await _wait_for_publish(mqtt, mode_result_topic(delayed_id))
    timed_out = _payload(_published(mqtt, mode_result_topic(delayed_id))[-1])
    assert timed_out["accepted"] is False
    assert timed_out["error"] == "timeout"
    assert bridge._state_trusted is True
    await bridge.async_shutdown()


async def test_mode_execution_newer_than_watermark_but_before_request_does_not_confirm(
    tmp_path: Path,
) -> None:
    clock = FakeClockMs(200)
    bridge, bus, mqtt, _, _ = await _started(tmp_path, clock=clock)
    bridge._mode_watermarks[_PANEL] = (100, "away")
    command_id = "55555555-5555-4555-8555-555555555555"

    await mqtt.inject(
        mode_command_topic(_PANEL),
        _command(command_id, "mode", "away", issued_at_ms=clock.now_ms),
    )
    await _wait_for_bus_commands(bus, 1)

    # away@101 is previously unseen and newer than away@100, but both executions
    # occurred before the request baseline at 200.
    await bus.emit(_execution(mode_id="away", mode_at_ms=101))
    await _wait_for_publish(mqtt, mode_event_topic(_PANEL))
    delivery_task = bridge._delivery_task
    assert delivery_task is not None
    await asyncio.wait_for(delivery_task, timeout=2)
    assert _published(mqtt, mode_result_topic(command_id)) == []

    timeout_task = bridge._mode_pending[command_id].timeout_task
    assert timeout_task is not None
    await clock.advance_ms(COMMAND_TTL_MS)
    await timeout_task
    await _wait_for_publish(mqtt, mode_result_topic(command_id))
    result = _payload(_published(mqtt, mode_result_topic(command_id))[-1])
    assert result["accepted"] is False
    assert result["error"] == "timeout"
    await bridge.async_shutdown()


async def test_poll_gate_suppresses_identical_mode_value_and_timestamp(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    # A poll delivering the SAME mode value AND the SAME bus timestamp is a pure
    # re-read and must remain gated (no reprocessing, no duplicate event).
    seeded = _execution(mode_id="away", mode_at_ms=500)
    bridge, _, _, _, _ = await _started(tmp_path, execution=seeded)
    process = bridge._async_process_execution
    processed: list[BrilliantDevice] = []

    async def observed_process(
        device: BrilliantDevice,
        *,
        emit_events: bool,
        epoch: int,
    ) -> None:
        processed.append(device)
        await process(device, emit_events=emit_events, epoch=epoch)

    monkeypatch.setattr(bridge, "_async_process_execution", observed_process)

    await bridge.poll_executions([_execution(mode_id="away", mode_at_ms=500)])

    assert processed == []
    # Pin the fix: the seeded away@500 fingerprint must carry the synthetic mode
    # timestamp key. A value-only fingerprint would omit it, leaving this test
    # unable to distinguish the fixed gate from the buggy one.
    fingerprint = scene_bridge_module._execution_fingerprint(seeded)
    assert fingerprint["@manual_mode_id.timestamp_ms"] == str(500)
    await bridge.async_shutdown()


async def test_poll_recovers_mode_health_when_invalid_timestamp_becomes_valid(
    tmp_path: Path,
) -> None:
    # Malformed-to-valid recovery: the seed carries an invalid (None) mode
    # timestamp, so decode_mode_execution raises and mode health is False. A
    # later poll delivers the SAME mode value with a now-valid timestamp; the
    # folded-in timestamp makes the fingerprint change, so the poll reprocesses,
    # health recovers, and the activation event emits.
    seeded = _execution(mode_id="away", mode_at_ms=None)
    bridge, _, mqtt, _, _ = await _started(tmp_path, execution=seeded)
    assert bridge._mode_execution_healthy is False
    assert _published(mqtt, mode_event_topic(_PANEL)) == []
    mqtt.published.clear()

    await bridge.poll_executions([_execution(mode_id="away", mode_at_ms=700)])
    await _wait_for_publish(mqtt, mode_event_topic(_PANEL))

    assert len(_published(mqtt, mode_event_topic(_PANEL))) == 1
    assert bridge._mode_execution_healthy is True
    await bridge.async_shutdown()
