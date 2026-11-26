"""Transport gates and read-only diagnostics for the historical #232 port."""

from __future__ import annotations

import asyncio
import json
from collections.abc import Callable, Mapping
from dataclasses import dataclass, field
from types import SimpleNamespace
from typing import Protocol, cast

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.model import CaptureProvenance
from brilliant_mqtt.write_admission import AdmissionTicket
from tests.fakes import FakeClock, FakeSleeper, _GatedRpcObserver, _RawDevice
from tests.test_bridge import MESH_PID, MESH_STATE_TOPIC, _BlockingStatePublishMqtt


class SettlementTicket(Protocol):
    @property
    def settled(self) -> bool: ...
    @property
    def accepted_at(self) -> CaptureProvenance | None: ...
    def mark_settled(self, outcome: str | BaseException) -> None: ...
    def set_settle_callback(self, callback: Callable[[str | BaseException], None]) -> None: ...
    def clear_settle_callback(self) -> None: ...


def settlement(ticket: AdmissionTicket) -> SettlementTicket:
    # This cast declares a future boundary; it supplies no missing behavior.
    return cast(SettlementTicket, ticket)


async def turns(count: int = 20) -> None:
    for _ in range(count):
        await asyncio.sleep(0)


class _ObserverBase:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        self.loop = loop


@dataclass
class Rig:
    observer: _GatedRpcObserver
    adapter: RpcBusAdapter
    raw: _RawDevice
    mqtt: _BlockingStatePublishMqtt
    bridge: Bridge
    clock: FakeClock
    sleeper: FakeSleeper
    tickets: list[AdmissionTicket] = field(default_factory=list)
    native_payloads: list[dict[str, str]] = field(default_factory=list)

    def state(self) -> dict[str, object]:
        states = [payload for topic, payload, _ in self.mqtt.published if topic == MESH_STATE_TOPIC]
        assert states, "the current settlement must produce a state publication"
        result: dict[str, object] = json.loads(states[-1])
        return result

    def set_on(self, value: str | None) -> None:
        assert self.raw.peripherals is not None
        variables = self.raw.peripherals[MESH_PID].variables
        if value is None:
            variables.pop("on", None)
        else:
            variables["on"].value = value

    async def report(self, value: str) -> None:
        self.set_on(value)
        await self.notify()
        await turns()

    async def notify(self) -> None:
        # Use the native notification envelope. Main discards this reliable
        # delta; the new ingress must preserve it without trusting mirror keys.
        observer_type = bus_mod._make_observer_class(_ObserverBase)
        observer = observer_type(
            asyncio.get_running_loop(), self.adapter._dispatch_raw_device, lambda: None
        )
        assert self.raw.peripherals is not None
        value = self.raw.peripherals[MESH_PID].variables["on"].value
        notification = SimpleNamespace(
            updated_device=self.raw,
            deleted=False,
            modified_peripherals=[
                SimpleNamespace(
                    peripheral_id=MESH_PID,
                    deleted=False,
                    modified_variables=[
                        SimpleNamespace(
                            variable_name="on",
                            variable=SimpleNamespace(value=value, externally_settable=True),
                            deletion=False,
                        )
                    ],
                    deleted_variables=[],
                )
            ],
        )
        await observer.handle_notification(notification)

    async def accept(self) -> None:
        self.observer.release.set()
        await turns()

    async def expire(self, seconds: float = 80) -> None:
        self.clock.advance(seconds)
        await self.sleeper.release_all()
        await turns()

    def timers(self) -> Mapping[str, asyncio.Task[None]]:
        return self.bridge._mesh_confirm_tasks

    def assert_pending(self, target: str = "0", deadline: float | None = None) -> None:
        state = self.state()
        assert state["mesh_write_status"] == "pending"
        assert state["state"] is None
        assert state["mesh_requested"] == {"on": target}
        assert state["brightness"] == 153
        if deadline is not None:
            assert state["mesh_write_deadline"] == deadline

    def assert_terminal(self, status: str, primary: str | None) -> None:
        state = self.state()
        assert state["mesh_write_status"] == status
        assert state["state"] == primary
        assert state["mesh_requested"] == {}
        assert state["mesh_write_deadline"] is None
        assert state["brightness"] == 153
