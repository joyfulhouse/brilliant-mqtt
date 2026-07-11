"""Fake implementations of BusClient and MqttClient for unit tests.

These satisfy the Protocols defined in brilliant_mqtt.protocols and allow
the bridge to be tested fully off-panel with no real MQTT or Thrift connection.
FakeClock backs clock-injected components (the mesh leader) so timing logic
runs deterministically without real sleeps.
"""

from __future__ import annotations

from collections.abc import Awaitable, Callable, Mapping

from brilliant_ha_mirror.mapping import HaEntity, PeripheralSpec, ServiceCall
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice


class FakeBus:
    """Fake BusClient that starts with a fixed device list and records writes."""

    def __init__(self, devices: list[BrilliantDevice]) -> None:
        self._devices = list(devices)
        # Multiple consumers (panel bridge + mesh publisher) each register their
        # own change callback on the one shared bus — mirror the adapter's fan-out.
        self._change_cbs: list[Callable[[BrilliantDevice], Awaitable[None]]] = []
        self._reconnect_cb: Callable[[], Awaitable[None]] | None = None
        # Each entry is (device_id, peripheral_id, [VarSet, ...]): writes are
        # ROUTED to the bus device owning the peripheral (the panel's own
        # CONTROL id, or "ble_mesh" for mesh loads), so tests assert the route.
        self.commands: list[tuple[str, str, list[VarSet]]] = []
        # Returned verbatim by seconds_since_last_push (None = no pushes yet).
        self.last_push_age: float | None = None
        # Returned verbatim by recent_reconnects; the window it was queried with
        # is recorded so tests can assert the run loop forwards the config value.
        self.reconnect_count: int = 0
        self.reconnect_window_queried: float | None = None

    async def start(self) -> None:
        pass

    async def get_all(self) -> list[BrilliantDevice]:
        return list(self._devices)

    def on_change(self, cb: Callable[[BrilliantDevice], Awaitable[None]]) -> None:
        self._change_cbs.append(cb)

    def on_reconnect(self, cb: Callable[[], Awaitable[None]]) -> None:
        self._reconnect_cb = cb

    def seconds_since_last_push(self) -> float | None:
        return self.last_push_age

    def recent_reconnects(self, window_s: float) -> int:
        self.reconnect_window_queried = window_s
        return self.reconnect_count

    async def set_variables(self, device_id: str, peripheral_id: str, sets: list[VarSet]) -> None:
        self.commands.append((device_id, peripheral_id, list(sets)))

    async def shutdown(self) -> None:
        pass

    async def emit(self, device: BrilliantDevice) -> None:
        """Test helper: invoke every registered on_change callback with *device*."""
        assert self._change_cbs, "on_change was never registered"
        for cb in list(self._change_cbs):
            await cb(device)

    def set_devices(self, devices: list[BrilliantDevice]) -> None:
        """Test helper: replace what subsequent get_all() calls return."""
        self._devices = list(devices)

    async def fire_reconnect(self) -> None:
        """Test helper: invoke the registered on_reconnect callback."""
        assert self._reconnect_cb is not None, "on_reconnect was never registered"
        await self._reconnect_cb()


class FakeClock:
    """Deterministic monotonic clock: tests advance time explicitly.

    Injected wherever production code defaults to time.monotonic, so
    timing-dependent state machines are exercised without real sleeps.
    """

    def __init__(self) -> None:
        self.now = 0.0

    def __call__(self) -> float:
        return self.now

    def advance(self, seconds: float) -> None:
        self.now += seconds


class FakeMqtt:
    """Fake MqttClient that records publishes/subscriptions and accepts injected commands."""

    def __init__(self) -> None:
        # Each entry is (topic, payload, retain).
        self.published: list[tuple[str, str, bool]] = []
        self.subscriptions: list[str] = []
        self.unsubscriptions: list[str] = []
        # Multiple consumers may register (see FakeBus._change_cbs) — fan out.
        self._command_cbs: list[Callable[[str, str], Awaitable[None]]] = []
        self.connect_count = 0
        self.disconnect_count = 0

    async def connect(self) -> None:
        self.connect_count += 1

    async def disconnect(self) -> None:
        self.disconnect_count += 1

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        self.published.append((topic, payload, retain))

    def on_command(self, cb: Callable[[str, str], Awaitable[None]]) -> None:
        self._command_cbs.append(cb)

    async def subscribe(self, topic: str) -> None:
        self.subscriptions.append(topic)

    async def unsubscribe(self, topic: str) -> None:
        """Record the unsubscribe AND drop the topic from ``subscriptions``.

        Removing it lets tests assert the NET subscription state (what the
        broker would still deliver), not just the raw call log.
        """
        self.unsubscriptions.append(topic)
        if topic in self.subscriptions:
            self.subscriptions.remove(topic)

    async def inject(self, topic: str, payload: str) -> None:
        """Test helper: invoke every registered on_command callback."""
        assert self._command_cbs, "on_command was never registered"
        for cb in list(self._command_cbs):
            await cb(topic, payload)


class FakeHaClient:
    """Fake Home Assistant client that exposes entities and records calls."""

    def __init__(self, entities: list[HaEntity]) -> None:
        self.entities = entities
        self.calls: list[ServiceCall] = []
        self._state_change_cb: Callable[[HaEntity], Awaitable[None]] | None = None
        # Flip to False to simulate a dropped Home Assistant connection.
        self.running = True

    async def start(self) -> None:
        pass

    def is_running(self) -> bool:
        return self.running

    async def get_entities(self, label: str) -> list[HaEntity]:
        return list(self.entities)

    def on_state_change(self, cb: Callable[[HaEntity], Awaitable[None]]) -> None:
        self._state_change_cb = cb

    async def call_service(self, call: ServiceCall) -> None:
        self.calls.append(call)

    async def shutdown(self) -> None:
        pass

    async def emit_state(self, entity: HaEntity) -> None:
        """Test helper: invoke the registered state-change callback."""
        assert self._state_change_cb is not None, "on_state_change was never registered"
        await self._state_change_cb(entity)


class FakePeripheralHost:
    """Fake peripheral host that records registrations, updates, and deletes."""

    def __init__(self) -> None:
        self.registered: list[str] = []
        self.registered_types: list[int] = []
        self.specs: dict[str, PeripheralSpec] = {}
        self.variables: dict[str, dict[str, str]] = {}
        self.commands: dict[str, Callable[[str, str], Awaitable[None]]] = {}
        self.deleted: list[str] = []

    async def start(self) -> None:
        pass

    async def register(
        self,
        name: str,
        spec: PeripheralSpec,
        on_command: Callable[[str, str], Awaitable[None]],
    ) -> None:
        self.registered.append(name)
        self.registered_types.append(spec.peripheral_type)
        self.specs[name] = spec
        self.variables[name] = dict(spec.variables)
        self.commands[name] = on_command

    async def update_variables(self, name: str, values: Mapping[str, str]) -> None:
        self.variables[name].update(values)

    async def delete(self, name: str) -> None:
        self.deleted.append(name)

    async def shutdown(self) -> None:
        pass

    async def fire_command(self, name: str, var: str, value: str) -> None:
        """Test helper: invoke the command callback registered for *name*."""
        await self.commands[name](var, value)
