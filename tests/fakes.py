"""Fake implementations of BusClient and MqttClient for unit tests.

These satisfy the Protocols defined in brilliant_mqtt.protocols and allow
the bridge to be tested fully off-panel with no real MQTT or Thrift connection.
FakeClock backs clock-injected components (the mesh leader) so timing logic
runs deterministically without real sleeps.
"""

from __future__ import annotations

import asyncio
import sys
import types
from collections.abc import Awaitable, Callable, Mapping
from dataclasses import replace
from typing import Any, Protocol

import aiomqtt
import paho.mqtt.client as paho
import pytest

from brilliant_ha_mirror.mapping import HaEntity, PeripheralSpec, ServiceCall
from brilliant_mqtt import mqttio
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.model import BrilliantDevice, CaptureProvenance, DeviceKind, Variable
from brilliant_mqtt.write_admission import AdmissionTicket, WriteClass


class FakeBus:
    """Fake BusClient that starts with a fixed device list and records writes."""

    def __init__(
        self,
        devices: list[BrilliantDevice],
        scoped_devices: list[BrilliantDevice] | None = None,
    ) -> None:
        self._devices = list(devices)
        self._scoped_devices = list(scoped_devices or ())
        self.scoped_reads: list[tuple[str, str]] = []
        # Multiple consumers (panel bridge + mesh publisher) each register their
        # own change callback on the one shared bus — mirror the adapter's fan-out.
        self._change_cbs: list[Callable[[BrilliantDevice], Awaitable[None]]] = []
        self.change_callback_modes: list[bool] = []
        # want_device scope predicate recorded per registration (issue #98):
        # None means the callback wants every device. emit() applies it so the
        # fake mirrors the real adapter's pre-filter skip.
        self.change_callback_wants: list[Callable[[str], bool] | None] = []
        self._reconnect_cbs: list[Callable[[], Awaitable[None]]] = []
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
        self.write_timeout_latched = False
        # Raised (when set) by every set_variables call — simulates a failed
        # bus write; the attempt is NOT recorded in ``commands``.
        self.set_variables_error: Exception | None = None
        # Returned by set_variables as the normalized transport-ack receipt.
        self.set_variables_receipt: str = "FakeSetVariablesResponse()"
        self._capture_generation = 1
        self._capture_sequence = 0

    async def start(self) -> None:
        pass

    async def get_all(self) -> list[BrilliantDevice]:
        return [self._captured(device) for device in self._devices]

    async def get_peripheral(self, device_id: str, peripheral_id: str) -> BrilliantDevice | None:
        self.scoped_reads.append((device_id, peripheral_id))
        for device in self._devices + self._scoped_devices:
            if device.device_id == device_id and device.peripheral_id == peripheral_id:
                return self._captured(device)
        return None

    def _next_provenance(self) -> CaptureProvenance:
        self._capture_sequence += 1
        return CaptureProvenance(self._capture_generation, self._capture_sequence)

    def _captured(self, device: BrilliantDevice) -> BrilliantDevice:
        if device.device_id == "ble_mesh" or device.kind not in (
            DeviceKind.LIGHT,
            DeviceKind.SWITCH,
        ):
            return device
        return replace(device, capture_provenance=self._next_provenance())

    def on_change(
        self,
        cb: Callable[[BrilliantDevice], Awaitable[None]],
        *,
        coalesce_pushes: bool = True,
        want_device: Callable[[str], bool] | None = None,
    ) -> None:
        self._change_cbs.append(cb)
        self.change_callback_modes.append(coalesce_pushes)
        self.change_callback_wants.append(want_device)

    def on_reconnect(self, cb: Callable[[], Awaitable[None]]) -> None:
        self._reconnect_cbs.append(cb)

    def seconds_since_last_push(self) -> float | None:
        return self.last_push_age

    def recent_reconnects(self, window_s: float) -> int:
        self.reconnect_window_queried = window_s
        return self.reconnect_count

    def consume_write_timeout(self) -> bool:
        timed_out = self.write_timeout_latched
        self.write_timeout_latched = False
        return timed_out

    def try_supersede(self, ticket: AdmissionTicket, new_payload: list[VarSet]) -> bool:
        return False

    async def set_variables(
        self,
        device_id: str,
        peripheral_id: str,
        sets: list[VarSet],
        *,
        write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
        ticket: AdmissionTicket | None = None,
    ) -> str:
        if ticket is not None:
            ticket.mark_issued(self._next_provenance())
        if self.set_variables_error is not None:
            raise self.set_variables_error
        self.commands.append((device_id, peripheral_id, list(sets)))
        return self.set_variables_receipt

    async def shutdown(self) -> None:
        pass

    async def emit(self, device: BrilliantDevice) -> None:
        """Test helper: invoke every in-scope on_change callback with *device*.

        Mirrors the real adapter's pre-filter (issue #98): a callback whose
        want_device predicate rejects this device's id is skipped, exactly as
        the adapter would drop the push before normalization.
        """
        assert self._change_cbs, "on_change was never registered"
        if device.capture_provenance is None:
            device = self._captured(device)
        for cb, want in zip(list(self._change_cbs), list(self.change_callback_wants), strict=True):
            if want is None or want(device.device_id):
                await cb(device)

    def set_devices(self, devices: list[BrilliantDevice]) -> None:
        """Test helper: replace what subsequent get_all() calls return."""
        self._devices = list(devices)

    async def fire_reconnect(self) -> None:
        """Test helper: invoke the registered on_reconnect callback."""
        assert self._reconnect_cbs, "on_reconnect was never registered"
        self._capture_generation += 1
        self._capture_sequence = 0
        for reconnect_cb in list(self._reconnect_cbs):
            try:
                await reconnect_cb()
            except Exception:
                continue


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


class FakeSleeper:
    """Deterministic replacement for asyncio.sleep: callers block until released.

    Injected as the Bridge's ``sleep`` seam so the mesh confirmation deadline
    fires exactly when a test releases it, never on real time. Requested
    durations are recorded so tests can assert the confirmation window.
    """

    def __init__(self) -> None:
        self.requested: list[float] = []
        self._waiters: list[asyncio.Future[None]] = []

    async def __call__(self, seconds: float) -> None:
        self.requested.append(seconds)
        future: asyncio.Future[None] = asyncio.get_running_loop().create_future()
        self._waiters.append(future)
        try:
            await future
        finally:
            if future in self._waiters:
                self._waiters.remove(future)

    async def release_all(self) -> None:
        """Wake every blocked sleeper, then let the released tasks run."""
        # Let freshly-created tasks reach their sleep before releasing.
        await asyncio.sleep(0)
        for future in list(self._waiters):
            if not future.done():
                future.set_result(None)
        for _ in range(3):
            await asyncio.sleep(0)


class FakeClockMs:
    """Deterministic wall clock and async sleeper for command deadlines."""

    def __init__(self, now_ms: int) -> None:
        self.now_ms = now_ms
        self._sleepers: list[tuple[int, asyncio.Future[None]]] = []

    def __call__(self) -> int:
        return self.now_ms

    async def sleep(self, seconds: float) -> None:
        deadline = self.now_ms + round(seconds * 1_000)
        if deadline <= self.now_ms:
            return
        future = asyncio.get_running_loop().create_future()
        self._sleepers.append((deadline, future))
        try:
            await future
        finally:
            self._sleepers = [(at, item) for at, item in self._sleepers if item is not future]

    async def advance_ms(self, milliseconds: int) -> None:
        await asyncio.sleep(0)
        self.now_ms += milliseconds
        for deadline, future in list(self._sleepers):
            if deadline <= self.now_ms and not future.done():
                future.set_result(None)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        await asyncio.sleep(0)


class FakeMqtt:
    """Fake MqttClient that records publishes/subscriptions and accepts injected commands."""

    def __init__(self) -> None:
        # Each entry is (topic, payload, retain).
        self.published: list[tuple[str, str, bool]] = []
        self.published_qos: list[int] = []
        self.subscriptions: list[str] = []
        self.unsubscriptions: list[str] = []
        # Multiple consumers may register (see FakeBus._change_cbs) — fan out.
        self._command_cbs: list[Callable[[str, str], Awaitable[None]]] = []
        self._message_cbs: list[Callable[[str, str, bool], Awaitable[None]]] = []
        self.connect_count = 0
        self.disconnect_count = 0
        self.reader_failure_latched = False

    async def connect(self) -> None:
        self.connect_count += 1

    async def disconnect(self) -> None:
        self.disconnect_count += 1

    def consume_reader_failure(self) -> bool:
        # Test double: read-and-clear a latch the test sets to inject one
        # reader failure per check. The real adapter latches permanently after
        # its first True; here each check simply reflects the flag's current
        # value, so tests stay in full control of when a failure is signalled.
        failed = self.reader_failure_latched
        self.reader_failure_latched = False
        return failed

    async def publish(self, topic: str, payload: str, retain: bool = False, qos: int = 0) -> None:
        self.published.append((topic, payload, retain))
        self.published_qos.append(qos)

    def on_command(self, cb: Callable[[str, str], Awaitable[None]]) -> None:
        self._command_cbs.append(cb)

    def on_message(self, cb: Callable[[str, str, bool], Awaitable[None]]) -> None:
        self._message_cbs.append(cb)

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

    async def inject(self, topic: str, payload: str, *, retained: bool = False) -> None:
        """Test helper: invoke every registered on_command callback."""
        assert self._command_cbs or self._message_cbs, "no MQTT callback was ever registered"
        for command_cb in list(self._command_cbs):
            await command_cb(topic, payload)
        for message_cb in list(self._message_cbs):
            await message_cb(topic, payload, retained)


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


# -- Shared cross-module harness helpers -------------------------------------------
# Single definition point for helpers used by more than one test module
# (repo convention: shared test harness lives here, not in sibling test
# modules — see tests/test_diagnostics.py).


class _RawVariable:
    """Duck-typed stand-in for a bus Variable (normalize_peripheral contract)."""

    def __init__(self, value: str) -> None:
        self.value = value
        self.externally_settable = True


class _RawPeripheral:
    """Duck-typed stand-in for a bus Peripheral."""

    def __init__(self, value: str = "1", variable_name: str = "on") -> None:
        self.name = "Mesh Switch"
        self.peripheral_type = 1
        self.variables = {variable_name: _RawVariable(value)}


class _RawDevice:
    """Duck-typed stand-in for a bus Device push; ``id`` is optional so the
    fallback path (no id on the raw struct) is exercisable, and ``peripherals``
    may be None to exercise the housekeeping-notification guard."""

    def __init__(
        self, device_id: str | None, peripherals: dict[str, _RawPeripheral] | None
    ) -> None:
        if device_id is not None:
            self.id = device_id
        self.peripherals = peripherals


class _GatedRpcObserver:
    """Observer whose writes block until ``release`` is set.

    Tracks write concurrency (the per-device lock contract), cancellation
    (the detach contract) and whether reads got through while a write was
    blocked (reads must never queue behind a stalled write).
    """

    def __init__(self, *, fail_with: Exception | None = None) -> None:
        self.release = asyncio.Event()
        self.in_flight = 0
        self.max_in_flight = 0
        self.write_cancelled = False
        self.writes: list[tuple[str, str]] = []
        self.reads: list[str] = []
        self.shutdowns = 0
        self._fail_with = fail_with

    async def get_device(self, device_id: str) -> _RawDevice:
        self.reads.append(device_id)
        return _RawDevice(device_id, {f"{device_id}-peripheral": _RawPeripheral()})

    async def request_set_variables_in_peripheral(
        self,
        peripheral_id: str,
        values: dict[str, str],
        *,
        device_id: str,
    ) -> str:
        del values
        self.writes.append((device_id, peripheral_id))
        self.in_flight += 1
        self.max_in_flight = max(self.max_in_flight, self.in_flight)
        try:
            await self._block()
        finally:
            self.in_flight -= 1
        if self._fail_with is not None:
            raise self._fail_with
        return "ok"

    async def _block(self) -> None:
        """The blocking core of a write; subclasses vary the cancellation reaction."""
        try:
            await self.release.wait()
        except asyncio.CancelledError:
            self.write_cancelled = True
            raise

    async def shutdown(self) -> None:
        self.shutdowns += 1


class _StartHarness:
    """Fakes the panel libraries into ``sys.modules`` so ``RpcBusAdapter.start()``
    runs off-panel, and records every processor/observer it constructs.

    #88: start() must own the observer/processor before starting them and unwind
    a partial startup on any failure. ``fail_at`` injects a failure at a chosen
    stage; the recorded ``procs``/``observers`` then prove the constructed
    resources were shut down (``live_*`` empty) rather than stranded.
    """

    def __init__(
        self,
        monkeypatch: pytest.MonkeyPatch,
        *,
        fail_at: str | tuple[str, str] | None = None,
        block_proc_start: bool = False,
        gated_writes: bool = False,
        obs_shutdown_raises: BaseException | None = None,
        hang_obs_shutdown: bool = False,
        hang_proc_shutdown: bool = False,
    ) -> None:
        self.fail_at = fail_at
        self.block_proc_start = block_proc_start
        self.gated_writes = gated_writes
        # When set, the observer's shutdown() raises this (e.g. CancelledError)
        # to prove _close_bus_resources still closes the processor afterwards.
        self.obs_shutdown_raises = obs_shutdown_raises
        self.hang_obs_shutdown = hang_obs_shutdown
        self.hang_proc_shutdown = hang_proc_shutdown
        self.procs: list[Any] = []
        self.observers: list[Any] = []
        self.subscribed: list[str] = []
        self.obs_shutdown_started = asyncio.Event()
        self.proc_shutdown_started = asyncio.Event()
        self.shutdown_release = asyncio.Event()
        # Gated-write bookkeeping (gated_writes=True): every set-variables RPC
        # blocks on write_release, recording the device order it actually
        # started on and the peak concurrency, so a test can prove same-device
        # writes serialize (max_writes_in_flight stays 1) across a restart.
        self.write_release = asyncio.Event()
        self.writes_in_flight = 0
        self.max_writes_in_flight = 0
        self.write_starts: list[str] = []
        self._proc_start_gate = asyncio.Event()
        self._install(monkeypatch)

    @property
    def live_procs(self) -> list[Any]:
        return [p for p in self.procs if not p.shut_down]

    @property
    def live_observers(self) -> list[Any]:
        return [o for o in self.observers if not o.shut_down]

    def _install(self, monkeypatch: pytest.MonkeyPatch) -> None:
        harness = self

        class _FakeObserverBase:
            def __init__(self, loop: Any) -> None:
                self.loop = loop
                self.started = False
                self.shut_down = False
                harness.observers.append(self)

            async def start(self, proc: Any, _extra: Any) -> None:
                if harness.fail_at == "obs_start":
                    raise RuntimeError("obs start boom")
                self.started = True

            def get_owning_device_id(self) -> str:
                return "own-device"

            async def subscribe(self, request: Any) -> None:
                device_id = str(request.device_id)
                harness.subscribed.append(device_id)
                if harness.fail_at == "own_sub" and device_id == "own-device":
                    raise RuntimeError("own subscribe boom")
                if harness.fail_at == ("extra_sub", device_id):
                    raise RuntimeError(f"extra subscribe boom: {device_id}")

            async def request_set_variables_in_peripheral(
                self, peripheral_id: str, values: dict[str, str], *, device_id: str
            ) -> str:
                del peripheral_id, values
                if not harness.gated_writes:
                    return "ok"
                harness.write_starts.append(device_id)
                harness.writes_in_flight += 1
                harness.max_writes_in_flight = max(
                    harness.max_writes_in_flight, harness.writes_in_flight
                )
                try:
                    try:
                        await harness.write_release.wait()
                    except asyncio.CancelledError:
                        # Wedged closed-source write: swallow the teardown cancel
                        # and keep holding the device lock until finally released
                        # (models the #73 detached-straggler case).
                        await harness.write_release.wait()
                finally:
                    harness.writes_in_flight -= 1
                return "ok"

            async def shutdown(self) -> None:
                harness.obs_shutdown_started.set()
                if harness.obs_shutdown_raises is not None:
                    raise harness.obs_shutdown_raises
                if harness.hang_obs_shutdown:
                    try:
                        await harness.shutdown_release.wait()
                    except asyncio.CancelledError:
                        await harness.shutdown_release.wait()
                self.shut_down = True

        class _FakeProc:
            def __init__(
                self,
                *,
                socket_path: str,
                my_name: str,
                handler: Any,
                client_class: Any,
                loop: Any,
            ) -> None:
                del socket_path, handler, client_class, loop
                if harness.fail_at == "proc_construct":
                    raise RuntimeError("proc construct boom")
                self.my_name = my_name
                self.started = False
                self.shut_down = False
                self.reconnect_cbs: list[Any] = []
                harness.procs.append(self)

            async def start(self) -> None:
                if harness.fail_at == "proc_start":
                    raise RuntimeError("proc start boom")
                if harness.block_proc_start:
                    await harness._proc_start_gate.wait()
                self.started = True

            def is_connected(self) -> bool:
                return harness.fail_at != "handshake"

            def add_reconnect_callback(self, cb: Any) -> None:
                self.reconnect_cbs.append(cb)

            async def shutdown(self) -> None:
                harness.proc_shutdown_started.set()
                if harness.hang_proc_shutdown:
                    try:
                        await harness.shutdown_release.wait()
                    except asyncio.CancelledError:
                        await harness.shutdown_release.wait()
                self.shut_down = True

        class _FakeSubscriptionRequest:
            def __init__(self, device_id: str) -> None:
                self.device_id = device_id

        class _FakePeripheralServer:
            def __init__(self, observer: Any) -> None:
                self.observer = observer

        class _FakeMessageBusClient:
            pass

        def mod(name: str, **attrs: Any) -> types.ModuleType:
            module = types.ModuleType(name)
            for key, value in attrs.items():
                setattr(module, key, value)
            monkeypatch.setitem(sys.modules, name, module)
            if "." in name:
                parent_name, child = name.rsplit(".", 1)
                setattr(sys.modules[parent_name], child, module)
            return module

        mod("lib")
        mod("lib.protocol")
        mod(
            "lib.protocol.message_bus_peer_service",
            PeripheralServer=_FakePeripheralServer,
            MessageBusClient=_FakeMessageBusClient,
        )
        mod("lib.protocol.processor", SinglePeerProcessor=_FakeProc)
        mod("lib.message_bus_api")
        mod("lib.message_bus_api.observer_interface", RPCObserver=_FakeObserverBase)
        mod("thrift_types")
        mod("thrift_types.message_bus")
        mod("thrift_types.message_bus.ttypes", SubscriptionRequest=_FakeSubscriptionRequest)


def _panel_dimmer() -> BrilliantDevice:
    return BrilliantDevice(
        device_id="device_001",
        peripheral_id="gangbox_peripheral_0",
        name="Lights",
        kind=DeviceKind.LIGHT,
        variables={"on": Variable("on", "0")},
    )


class _AiomqttClientInternals(Protocol):
    """The private aiomqtt.Client surface this seam test drives (client.py): the
    incoming queue built via our ``queue_type`` hook, and the paho message
    callback. Declared so the test type-checks with no suppression."""

    _queue: mqttio._BoundedTransportQueue

    def _on_message(self, client: object, userdata: object, message: paho.MQTTMessage) -> None: ...


async def _settle(n: int = 3) -> None:
    """Yield a few loop iterations so freshly created tasks reach their awaits."""
    for _ in range(n):
        await asyncio.sleep(0)


def _msg(topic: str, payload: bytes = b"x") -> aiomqtt.Message:
    """A production-shaped inbound message (paho hands aiomqtt bytes payloads)."""
    return aiomqtt.Message(
        topic=topic,
        payload=payload,
        qos=1,
        retain=False,
        mid=0,
        properties=None,
    )
