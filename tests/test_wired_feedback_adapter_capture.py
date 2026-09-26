"""Real adapter capture fencing for delayed whole-device push delivery."""

from __future__ import annotations

import asyncio
import json

from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.mqttio import _InboundMessage, _TopicDispatcher
from brilliant_mqtt.write_admission import WriteResult
from tests.fakes import FakeClock, FakeMqtt, FakeSleeper, _settle

PANEL = "office"
DEVICE_ID = "device_001"
PID = "gangbox_peripheral_0"
OTHER_PID = "gangbox_peripheral_1"
SET_TOPIC = f"brilliant/{PANEL}/{PID}/set"
STATE_TOPIC = f"brilliant/{PANEL}/{PID}/state"
OTHER_STATE_TOPIC = f"brilliant/{PANEL}/{OTHER_PID}/state"


class _RawVariable:
    def __init__(self, value: str, timestamp: int) -> None:
        self.value = value
        self.externally_settable = True
        self.timestamp = timestamp


class _RawPeripheral:
    def __init__(self, name: str, on: str, timestamp: int) -> None:
        self.name = name
        self.peripheral_type = 27
        self.variables = {
            "on": _RawVariable(on, timestamp),
            "intensity": _RawVariable("333", timestamp),
        }


class _RawDevice:
    def __init__(self, peripherals: dict[str, _RawPeripheral]) -> None:
        self.id = DEVICE_ID
        self.peripherals = peripherals


class _Observer:
    def __init__(self, mirror: _RawDevice) -> None:
        self.mirror = mirror
        self.writes: list[tuple[str, dict[str, str]]] = []

    async def get_device(self, device_id: str) -> _RawDevice:
        assert device_id == DEVICE_ID
        return self.mirror

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> str:
        assert device_id == DEVICE_ID
        self.writes.append((peripheral_id, dict(values)))
        return "ok"


class _BlockedOtherObserver(_Observer):
    def __init__(self, mirror: _RawDevice) -> None:
        super().__init__(mirror)
        self.release = asyncio.Event()

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> str:
        if peripheral_id == OTHER_PID:
            await self.release.wait()
        return await super().request_set_variables_in_peripheral(
            peripheral_id, values, device_id=device_id
        )


class _BlockingMqtt(FakeMqtt):
    def __init__(self) -> None:
        super().__init__()
        self.armed = False
        self.blocked = asyncio.Event()
        self.release = asyncio.Event()

    async def publish(
        self,
        topic: str,
        payload: str,
        retain: bool = False,
        qos: int = 0,
    ) -> None:
        if self.armed and topic == OTHER_STATE_TOPIC and not self.blocked.is_set():
            self.blocked.set()
            await self.release.wait()
        await super().publish(topic, payload, retain, qos)


async def test_real_adapter_fences_push_captured_before_issued_write() -> None:
    mirror = _RawDevice(
        {
            OTHER_PID: _RawPeripheral("Other", "0", 1000),
            PID: _RawPeripheral("Lights", "0", 1000),
        }
    )
    observer = _Observer(mirror)
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = DEVICE_ID
    mqtt = _BlockingMqtt()
    bridge = Bridge(
        adapter,
        mqtt,
        PANEL,
        clock=FakeClock(),
        wall_clock=FakeClock(),
        sleep=FakeSleeper(),
    )
    await bridge.reconcile()
    mqtt.published.clear()
    mqtt.armed = True

    captured_before_command = _RawDevice(
        {
            OTHER_PID: _RawPeripheral("Other", "1", 1500),
            PID: _RawPeripheral("Lights", "0", 1000),
        }
    )
    adapter._dispatch_raw_device(captured_before_command)
    await mqtt.blocked.wait()

    await mqtt.inject(SET_TOPIC, '{"state":"ON","brightness":85}')
    assert observer.writes == [(PID, {"on": "1", "intensity": "333"})]

    mqtt.release.set()
    await asyncio.gather(*list(adapter._pending_tasks))

    states = [
        json.loads(payload) for topic, payload, _retain in mqtt.published if topic == STATE_TOPIC
    ]
    assert [state["state"] for state in states] == ["ON"]
    assert states[-1]["wired_write_status"] == "provisional"

    await bridge.shutdown_wired_feedback()
    await adapter.shutdown()


async def test_folded_wired_slider_waits_for_inherited_deadline() -> None:
    mirror = _RawDevice(
        {
            OTHER_PID: _RawPeripheral("Other", "0", 1000),
            PID: _RawPeripheral("Lights", "0", 1000),
        }
    )
    observer = _BlockedOtherObserver(mirror)
    adapter = RpcBusAdapter()
    adapter._obs = observer
    adapter._own_device_id = DEVICE_ID
    mqtt = FakeMqtt()
    bridge = Bridge(
        adapter, mqtt, PANEL, clock=FakeClock(), wall_clock=FakeClock(), sleep=FakeSleeper()
    )
    await bridge.reconcile()
    dispatcher = _TopicDispatcher(
        lambda message: bridge._on_command(message.topic, message.payload)
    )

    blocker: asyncio.Task[WriteResult] | None = None
    try:
        await dispatcher.dispatch(
            _InboundMessage(SET_TOPIC, '{"state":"ON","brightness":85}', False, (), ()),
            latest_wins=True,
        )
        await _settle(20)
        mqtt.published.clear()
        blocker = asyncio.create_task(
            adapter.set_variables(DEVICE_ID, OTHER_PID, [VarSet("on", "1")])
        )
        await _settle(10)
        await dispatcher.dispatch(
            _InboundMessage(SET_TOPIC, '{"state":"ON","brightness":150}', False, (), ()),
            latest_wins=True,
        )
        await _settle(10)
        await dispatcher.dispatch(
            _InboundMessage(SET_TOPIC, '{"state":"ON","brightness":200}', False, (), ()),
            latest_wins=True,
        )
        await _settle(20)
        states_during_fold = [
            json.loads(payload)
            for topic, payload, _retain in mqtt.published
            if topic == STATE_TOPIC
        ]
        assert states_during_fold == []

        observer.release.set()
        await blocker
        await _settle(20)
        states = [
            json.loads(payload)
            for topic, payload, _retain in mqtt.published
            if topic == STATE_TOPIC
        ]
        assert [state["state"] for state in states] == ["ON"]
        assert states[-1]["wired_write_status"] == "provisional"
        assert states[-1]["brightness"] == 200
    finally:
        observer.release.set()
        await dispatcher.shutdown()
        await bridge.shutdown_wired_feedback()
        await adapter.shutdown()
        if blocker is not None:
            await asyncio.gather(blocker, return_exceptions=True)
