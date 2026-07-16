"""Panel-only adapters and CLI for the bounded BVD single-light pilot.

The default mode is read-only.  ``--apply`` is intentionally gated on an
operator assertion that the exact cleanup-only command has already been staged
on every panel that could become BVD owner.  This module never imports or calls
LeaseManager and never writes the BVD owner variable.
"""

from __future__ import annotations

import argparse
import asyncio
import fcntl
import inspect
import json
import os
import secrets
import signal
import stat
import time
from collections.abc import AsyncIterator, Awaitable, Callable, Mapping, Sequence
from contextlib import asynccontextmanager
from dataclasses import dataclass
from pathlib import Path
from typing import Any, cast

from brilliant_mqtt.bus import load_rpc_observer_class
from brilliant_mqtt.ha_control_protocol import manifest_topic, stable_id, state_topic
from tools.brilliant_bvd.single_light_pilot import (
    BVD_CONFIGURATION_PERIPHERAL_ID,
    BVD_DEVICE_ID,
    OFFICE_DEVICE_ID,
    TARGET_ENTITY_ID,
    BvdBus,
    BvdTopology,
    CleanupReport,
    PeripheralFact,
    PilotConfig,
    PilotController,
    PilotGuardError,
    PilotLifecycle,
    Publisher,
    ScopedPeripheralProbe,
    StateSink,
    VariableDefinition,
    VirtualLightHost,
    peripheral_id_for_entity,
    validate_active_topology,
    validate_manifest_authority,
    validate_postflight,
    validate_preflight,
)

_SOCKET_PATH = "/var/run/brilliant/server_socket"
_CONNECT_TIMEOUT_S = 10.0
_OPERATION_TIMEOUT_S = 10.0
_AUTHORITY_TIMEOUT_S = 15.0
_MONITOR_INTERVAL_S = 5.0
_SUPERVISOR_TICK_S = 0.25
_NOTIFICATION_STALE_S = 30.0
_HOST_UPDATE_TIMEOUT_S = 3.0
_ACTIVE_SNAPSHOT_TIMEOUT_S = 3.0
_COMMAND_CONFIRMATION_TIMEOUT_MS = 15_000
_RESULT_TOPIC_FILTER = "brilliant/ha-control/v1/result/+"
_PILOT_LOCK_PATH = Path("/run/brilliant-bvd-pilot/single-light.lock")
_MAX_PRIVATE_FILE_BYTES = 64 * 1024
_STOP_SIGNALS = (signal.SIGINT, signal.SIGTERM, signal.SIGHUP)


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await cast(Awaitable[object], value)
    return value


async def _bounded(
    operation: Awaitable[Any], *, description: str, timeout_s: float = _OPERATION_TIMEOUT_S
) -> Any:
    try:
        return await asyncio.wait_for(operation, timeout=timeout_s)
    except asyncio.TimeoutError:
        raise PilotGuardError(f"{description} exceeded its bounded timeout") from None


@dataclass(frozen=True, slots=True)
class BusBindings:
    """Injectable closed-firmware bus constructors for off-panel tests."""

    observer_class: Any
    processor_class: Any
    peripheral_server: Callable[[Any], Any]
    client_class: Any
    subscription_request: Callable[..., Any]


def _load_bus_bindings() -> BusBindings:
    import lib.protocol.message_bus_peer_service as mbps
    from lib.protocol.processor import SinglePeerProcessor
    from thrift_types.message_bus.ttypes import SubscriptionRequest

    return BusBindings(
        observer_class=load_rpc_observer_class(),
        processor_class=SinglePeerProcessor,
        peripheral_server=mbps.PeripheralServer,
        client_class=mbps.MessageBusClient,
        subscription_request=SubscriptionRequest,
    )


def _observer_type(base: Any) -> Any:
    async def handle_notification(self: Any, notification: Any) -> None:
        del notification
        callback = getattr(self, "_pilot_notification_callback", None)
        if callable(callback):
            callback()

    return type("_BvdPilotObserver", (base,), {"handle_notification": handle_notification})


async def _open_bus_peer(
    bindings: BusBindings,
    *,
    name_prefix: str,
    on_notification: Callable[[], None] | None = None,
) -> tuple[Any, Any, str]:
    loop = asyncio.get_running_loop()
    observer = _observer_type(bindings.observer_class)(loop)
    observer._pilot_notification_callback = on_notification
    processor = bindings.processor_class(
        socket_path=_SOCKET_PATH,
        my_name=f"{name_prefix}-{secrets.token_hex(4)}",
        handler=bindings.peripheral_server(observer),
        client_class=bindings.client_class,
        loop=loop,
    )
    try:
        await _bounded(processor.start(), description="message-bus processor start")
        deadline = loop.time() + _CONNECT_TIMEOUT_S
        while not processor.is_connected():
            if loop.time() >= deadline:
                raise PilotGuardError("message-bus connection timed out")
            await asyncio.sleep(0.25)
        await _bounded(observer.start(processor, None), description="message-bus observer start")
        return observer, processor, str(observer.get_owning_device_id())
    except BaseException:
        await _shutdown_components(observer, processor, suppress_errors=True)
        raise


async def _shutdown_components(*components: Any, suppress_errors: bool = False) -> None:
    first_error: BaseException | None = None
    for component in components:
        if component is None:
            continue
        try:
            await asyncio.wait_for(component.shutdown(), timeout=_OPERATION_TIMEOUT_S)
        except BaseException as error:
            if first_error is None:
                first_error = error
    if first_error is not None and not suppress_errors:
        if isinstance(first_error, asyncio.CancelledError):
            raise first_error
        raise PilotGuardError("native bus peer shutdown failed") from first_error


def _variable_value(raw: object) -> str:
    value = getattr(raw, "value", None)
    if isinstance(value, bytes):
        return value.decode("utf-8", errors="replace")
    if not isinstance(value, str):
        raise PilotGuardError("BVD variable value must be text")
    return value


def _stock_host_identity(proc_root: Path = Path("/proc")) -> str:
    """Return PID:start-ticks for the one stock BVD vassal, else empty."""

    matches: list[str] = []
    try:
        entries = tuple(proc_root.iterdir())
    except OSError:
        return ""
    for entry in entries:
        if not entry.name.isdigit():
            continue
        try:
            argv = tuple(
                argument for argument in (entry / "cmdline").read_bytes().split(b"\0") if argument
            )
            if not argv or Path(os.fsdecode(argv[0])).name != "uwsgi":
                continue
            expected_config = (
                b"/var/run/brilliant/startable_configs/brilliant_virtual_device_peripherals"
            )
            if expected_config not in argv:
                continue
            process_stat = (entry / "stat").read_text(encoding="ascii")
            close_parenthesis = process_stat.rfind(")")
            fields_after_name = process_stat[close_parenthesis + 2 :].split()
            start_ticks = fields_after_name[19]
        except (IndexError, OSError, UnicodeError):
            continue
        matches.append(f"{entry.name}:{start_ticks}")
    return matches[0] if len(matches) == 1 else ""


class NativeBvdBus:
    """Persistent read-only observer for BVD preflight and active guards."""

    def __init__(
        self,
        *,
        bindings_factory: Callable[[], BusBindings] = _load_bus_bindings,
        stock_identity: Callable[[], str] = _stock_host_identity,
        clock: Callable[[], float] = time.monotonic,
    ) -> None:
        self._bindings_factory = bindings_factory
        self._stock_identity = stock_identity
        self._clock = clock
        self._observer: Any = None
        self._processor: Any = None
        self._owning_device_id: str | None = None
        self._reconnect_callbacks: list[Callable[[], Awaitable[None]]] = []
        self._callback_tasks: set[asyncio.Task[None]] = set()
        self._notification_serial = 0
        self._last_notification_s: float | None = None
        self._notification_event = asyncio.Event()
        self._reconnect_seen = False

    def _note_notification(self) -> None:
        self._notification_serial += 1
        self._last_notification_s = self._clock()
        self._notification_event.set()

    async def start(self) -> None:
        if self._observer is not None:
            return
        self._reconnect_seen = False
        bindings = self._bindings_factory()
        observer, processor, owning_device_id = await _open_bus_peer(
            bindings,
            name_prefix="brilliant_bvd_guard",
            on_notification=self._note_notification,
        )
        try:
            processor.add_reconnect_callback(self._on_reconnect)
            for device_id in (BVD_DEVICE_ID, "configuration_virtual_device"):
                await _bounded(
                    observer.subscribe(bindings.subscription_request(device_id=device_id)),
                    description=f"{device_id} subscription",
                )
            if self._reconnect_seen:
                raise PilotGuardError("message bus reconnected during initial BVD subscriptions")
        except BaseException:
            for task in tuple(self._callback_tasks):
                task.cancel()
            if self._callback_tasks:
                await asyncio.gather(*self._callback_tasks, return_exceptions=True)
            self._callback_tasks.clear()
            await _shutdown_components(observer, processor, suppress_errors=True)
            raise
        self._observer = observer
        self._processor = processor
        self._owning_device_id = owning_device_id
        self._last_notification_s = self._clock()

    def notification_marker(self) -> int:
        return self._notification_serial

    async def wait_for_notification_after(self, marker: int, timeout_s: float) -> None:
        if type(marker) is not int or marker < 0 or timeout_s <= 0:
            raise ValueError("notification wait arguments are invalid")
        deadline = asyncio.get_running_loop().time() + timeout_s
        while self._notification_serial <= marker:
            self._notification_event.clear()
            if self._notification_serial > marker:
                break
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                raise PilotGuardError("BVD notification stream did not confirm registration")
            try:
                await asyncio.wait_for(self._notification_event.wait(), timeout=remaining)
            except asyncio.TimeoutError:
                raise PilotGuardError(
                    "BVD notification stream did not confirm registration"
                ) from None

    def seconds_since_last_notification(self) -> float | None:
        if self._reconnect_seen or self._last_notification_s is None:
            return None
        return max(0.0, self._clock() - self._last_notification_s)

    def _on_reconnect(self, *args: object, **kwargs: object) -> None:
        del args, kwargs
        # Invalidate synchronous gates before scheduling the public async
        # callback; otherwise one final gate could race the scheduled task.
        self._reconnect_seen = True

        async def notify() -> None:
            for callback in tuple(self._reconnect_callbacks):
                await callback()

        task = asyncio.create_task(notify())
        self._callback_tasks.add(task)
        task.add_done_callback(self._callback_tasks.discard)

    def on_reconnect(self, callback: Callable[[], Awaitable[None]]) -> None:
        self._reconnect_callbacks.append(callback)

    def _require_started(self) -> tuple[Any, str]:
        if self._observer is None or self._owning_device_id is None:
            raise RuntimeError("NativeBvdBus must be started before use")
        return self._observer, self._owning_device_id

    async def _read_configuration(self, observer: Any) -> tuple[str, int, frozenset[str]]:
        raw_configuration = await _bounded(
            observer.get_peripheral(
                "configuration_virtual_device", BVD_CONFIGURATION_PERIPHERAL_ID
            ),
            description="BVD configuration read",
        )
        variables = getattr(raw_configuration, "variables", None)
        if not isinstance(variables, Mapping):
            raise PilotGuardError("BVD configuration variables are unavailable")
        owner_variable = variables.get("owner")
        owner = _variable_value(owner_variable)
        owner_timestamp_ms = getattr(owner_variable, "timestamp", None)
        if type(owner_timestamp_ms) is not int:
            raise PilotGuardError("BVD owner timestamp must be an integer")
        process_ids = frozenset(
            str(name).removeprefix("process_config:")
            for name, raw in variables.items()
            if str(name).startswith("process_config:") and bool(_variable_value(raw))
        )
        return owner, owner_timestamp_ms, process_ids

    async def snapshot(self) -> BvdTopology:
        observer, owning_device_id = self._require_started()
        first_owner, first_owner_timestamp_ms, first_process_ids = await self._read_configuration(
            observer
        )

        raw_device = await _bounded(
            observer.get_device(BVD_DEVICE_ID), description="BVD device read"
        )
        if raw_device is None or getattr(raw_device, "id", None) != BVD_DEVICE_ID:
            raise PilotGuardError("BVD device record is unavailable")
        raw_peripherals = getattr(raw_device, "peripherals", None)
        if not isinstance(raw_peripherals, Mapping):
            raise PilotGuardError("BVD peripheral map is unavailable")
        facts: list[PeripheralFact] = []
        for peripheral_id, raw_peripheral in raw_peripherals.items():
            raw_variables = getattr(raw_peripheral, "variables", None)
            if not isinstance(raw_variables, Mapping):
                raise PilotGuardError("BVD peripheral variables are unavailable")
            selected: dict[str, str] = {}
            for variable_name in ("configuration_peripheral_id", "relay_device"):
                if variable_name in raw_variables:
                    selected[variable_name] = _variable_value(raw_variables[variable_name])
            peripheral_type = getattr(raw_peripheral, "peripheral_type", None)
            status = getattr(raw_peripheral, "status", None)
            if type(peripheral_type) is not int or type(status) is not int:
                raise PilotGuardError("BVD peripheral type/status must be integers")
            facts.append(PeripheralFact(str(peripheral_id), peripheral_type, status, selected))
        identity = self._stock_identity()
        device_type = getattr(raw_device, "device_type", None)
        if type(device_type) is not int:
            raise PilotGuardError("BVD device_type must be an integer")
        owner, owner_timestamp_ms, process_ids = await self._read_configuration(observer)
        if owner != first_owner:
            raise PilotGuardError("BVD owner changed during the scoped snapshot")
        if owner_timestamp_ms < first_owner_timestamp_ms:
            raise PilotGuardError("BVD owner timestamp regressed during the scoped snapshot")
        if process_ids != first_process_ids:
            raise PilotGuardError("BVD process configuration changed during the scoped snapshot")
        return BvdTopology(
            owning_device_id=owning_device_id,
            configuration_owner=owner,
            owner_timestamp_ms=owner_timestamp_ms,
            bvd_device_type=device_type,
            stock_host_running=bool(identity),
            stock_host_identity=identity,
            process_config_peripheral_ids=process_ids,
            peripherals=tuple(facts),
        )

    async def room_ids(self) -> frozenset[str]:
        """Decode the scoped room catalog once for room-assignment admission."""

        from lib.serialization import deserialize
        from thrift_types.configuration.ttypes import Rooms

        observer, _ = self._require_started()
        raw_home = await _bounded(
            observer.get_peripheral("configuration_virtual_device", "home_configuration"),
            description="room catalog read",
        )
        variables = getattr(raw_home, "variables", None)
        if not isinstance(variables, Mapping) or "rooms" not in variables:
            raise PilotGuardError("home_configuration.rooms is unavailable")
        rooms_value = _variable_value(variables["rooms"])
        decoded = deserialize(Rooms, rooms_value)
        room_map = getattr(decoded, "rooms", None)
        if not isinstance(room_map, Mapping):
            raise PilotGuardError("decoded room catalog is invalid")
        return frozenset(str(room_id) for room_id in room_map)

    async def peripheral_exists(self, device_id: str, peripheral_id: str) -> bool:
        observer, _ = self._require_started()
        try:
            raw = await _bounded(
                observer.get_peripheral(device_id, peripheral_id),
                description="scoped peripheral read",
            )
        except KeyError:
            return False
        return raw is not None

    async def shutdown(self) -> None:
        observer, processor = self._observer, self._processor
        for task in tuple(self._callback_tasks):
            task.cancel()
        if self._callback_tasks:
            await asyncio.gather(*self._callback_tasks, return_exceptions=True)
        self._callback_tasks.clear()
        await _shutdown_components(observer, processor)
        if self._observer is observer and self._processor is processor:
            self._observer = None
            self._processor = None
            self._owning_device_id = None


class NativeScopedProbe(ScopedPeripheralProbe):
    """One fresh physical-bus peer used for one exact absence read."""

    def __init__(self, bindings: BusBindings) -> None:
        self._bindings = bindings
        self._observer: Any = None
        self._processor: Any = None

    async def start(self) -> None:
        observer, processor, _ = await _open_bus_peer(
            self._bindings, name_prefix="brilliant_bvd_probe"
        )
        self._observer = observer
        self._processor = processor

    async def contains(self, device_id: str, peripheral_id: str) -> bool:
        if self._observer is None:
            raise RuntimeError("NativeScopedProbe must be started before use")
        try:
            raw = await _bounded(
                self._observer.get_peripheral(device_id, peripheral_id),
                description="cleanup proof read",
            )
        except KeyError:
            return False
        return raw is not None

    async def shutdown(self) -> None:
        observer, processor = self._observer, self._processor
        await _shutdown_components(observer, processor)
        if self._observer is observer and self._processor is processor:
            self._observer = None
            self._processor = None


class NativeProbeFactory:
    def __init__(self, bindings_factory: Callable[[], BusBindings] = _load_bus_bindings) -> None:
        self._bindings_factory = bindings_factory

    async def __call__(self) -> NativeScopedProbe:
        probe = NativeScopedProbe(self._bindings_factory())
        await probe.start()
        return probe


@dataclass(frozen=True, slots=True)
class FrameworkBindings:
    hosted_startable_spec: Any
    peripheral_base: Any
    variable_spec: Any
    peripheral_config: Any
    peripheral_host: Any
    delete_impl: Callable[..., object]


@dataclass(frozen=True, slots=True)
class LiveDependencies:
    """Injectable live boundaries for deterministic off-panel orchestration tests."""

    bus_factory: Callable[[], BvdBus]
    host_factory: Callable[[asyncio.AbstractEventLoop], VirtualLightHost]
    probe_factory: Callable[[], Awaitable[ScopedPeripheralProbe]]
    mqtt_client_factory: Callable[..., Any]
    room_assignment_type: type
    lifecycle_sleep: Callable[[float], Awaitable[None]] = asyncio.sleep
    absence_interval_s: float = 30.0
    lifecycle_operation_timeout_s: float = 3.0


def _load_live_dependencies() -> LiveDependencies:
    import aiomqtt
    from thrift_types.configuration.ttypes import RoomAssignment

    probes = NativeProbeFactory()
    return LiveDependencies(
        bus_factory=NativeBvdBus,
        host_factory=lambda loop: LiveVirtualLightHost(loop=loop),
        probe_factory=probes,
        mqtt_client_factory=aiomqtt.Client,
        room_assignment_type=RoomAssignment,
    )


def _load_framework_bindings() -> FrameworkBindings:
    from lib.startables.startable import HostedStartableSpec
    from peripherals.lib.peripheral_service.conditional_peripheral_host import (
        ConditionalPeripheralHost,
    )
    from peripherals.lib.peripheral_service.peripheral import Peripheral, VariableSpec
    from peripherals.lib.peripheral_service.peripheral_host import (
        PeripheralConfig,
        PeripheralHost,
    )

    return FrameworkBindings(
        HostedStartableSpec,
        Peripheral,
        VariableSpec,
        PeripheralConfig,
        PeripheralHost,
        ConditionalPeripheralHost.__dict__["delete_peripheral"],
    )


class LiveVirtualLightHost(VirtualLightHost):
    """Exactly one type-27 host explicitly targeted at BVD."""

    def __init__(
        self,
        *,
        loop: asyncio.AbstractEventLoop,
        bindings_factory: Callable[[], FrameworkBindings] = _load_framework_bindings,
    ) -> None:
        self._loop = loop
        self._bindings_factory = bindings_factory
        self._bindings: FrameworkBindings | None = None
        self._host: Any = None
        self._instance: Any = None
        self._peripheral_id: str | None = None

    @staticmethod
    def _point_socket_at_panel() -> None:
        import gflags

        flags = gflags.FLAGS
        try:
            _ = flags.message_bus_server_socket_path
        except gflags.UnparsedFlagAccessError:
            flags([""])
        flags.message_bus_server_socket_path = _SOCKET_PATH

    async def start(
        self,
        *,
        peripheral_id: str,
        virtual_device_id: str,
        variables: Mapping[str, VariableDefinition],
        on_command: Callable[[str, object], Awaitable[bool]],
    ) -> None:
        expected_id = peripheral_id_for_entity(TARGET_ENTITY_ID)
        if peripheral_id != expected_id or virtual_device_id != BVD_DEVICE_ID:
            raise PilotGuardError("native host target is outside the fixed pilot")
        if self._host is not None:
            return
        bindings = self._bindings_factory()
        self._point_socket_at_panel()
        adapter = self

        def push_factory(name: str) -> Callable[[object], Awaitable[None]]:
            async def push(value: object) -> None:
                await on_command(name, value)

            return push

        def build_variables() -> dict[str, Any]:
            return {
                name: bindings.variable_spec(
                    definition.value_type,
                    definition.externally_settable,
                    default_value=definition.default_value,
                    push_func=(push_factory(name) if name in {"on", "intensity"} else None),
                )
                for name, definition in variables.items()
            }

        def pilot_init(instance: Any, *args: Any, **kwargs: Any) -> None:
            bindings.peripheral_base.__init__(instance, *args, **kwargs)
            adapter._instance = instance

        def pilot_name(instance: Any) -> str:
            del instance
            return peripheral_id

        def pilot_type(instance: Any) -> int:
            del instance
            return 27

        def pilot_variables(instance: Any) -> dict[str, Any]:
            del instance
            return build_variables()

        pilot_light = type(
            "BvdPilotLight",
            (bindings.peripheral_base,),
            {
                "__init__": pilot_init,
                "name": property(pilot_name),
                "peripheral_type": property(pilot_type),
                "_my_variables": pilot_variables,
            },
        )

        peripheral_config = bindings.peripheral_config(
            peripheral_id,
            pilot_light,
            virtual_device_id=BVD_DEVICE_ID,
        )
        host = bindings.peripheral_host(
            loop=self._loop,
            startable_id=f"brilliant_bvd_light-{secrets.token_hex(8)}",
            startables_to_host=[
                bindings.hosted_startable_spec(peripheral_id, peripheral_config, {})
            ],
            parallel_registration_limit=1,
            raise_errors_for_lost_user_configured_data=False,
            message_bus_address_override=None,
        )
        self._bindings = bindings
        self._host = host
        self._peripheral_id = peripheral_id
        await _bounded(host.start(), description="BVD LIGHT host start")
        if self._instance is None:
            raise PilotGuardError("BVD LIGHT host started without a peripheral instance")

    async def update_variables(self, values: Mapping[str, object]) -> None:
        if self._instance is None or self._bindings is None:
            raise PilotGuardError("native LIGHT instance is not available for HA state")
        update = self._bindings.peripheral_base.__dict__["_set_value_internal"]
        for name, value in values.items():
            await _bounded(
                _maybe_await(update(self._instance, name, value, notify=True)),
                description="native HA state reflection",
                timeout_s=_HOST_UPDATE_TIMEOUT_S,
            )

    async def delete(self, peripheral_id: str, deletion_time_ms: int) -> None:
        if peripheral_id != self._peripheral_id or self._host is None:
            if self._peripheral_id is None:
                return
            raise PilotGuardError("native deletion target does not match the pilot")
        if type(deletion_time_ms) is not int or deletion_time_ms < 0:
            raise PilotGuardError("deletion_time_ms must be a non-negative integer")
        assert self._bindings is not None
        await _maybe_await(self._bindings.delete_impl(self._host, peripheral_id, deletion_time_ms))
        self._instance = None

    async def shutdown(self) -> None:
        host = self._host
        if host is not None:
            await _bounded(host.shutdown(), description="BVD LIGHT host shutdown")
        if self._host is host:
            self._host = None
            self._instance = None
            self._bindings = None


class BufferedStateSink(StateSink):
    """Hold HA state until native registration exists, then replay it once."""

    def __init__(self) -> None:
        self._values: dict[str, object] = {}
        self._target: StateSink | None = None
        self._lock = asyncio.Lock()

    def snapshot(self) -> dict[str, object]:
        return dict(self._values)

    async def attach(self, target: StateSink) -> None:
        async with self._lock:
            self._target = target
            snapshot = dict(self._values)
            if snapshot:
                await target.update_variables(snapshot)

    async def update_variables(self, values: Mapping[str, object]) -> None:
        async with self._lock:
            snapshot = dict(values)
            self._values.update(snapshot)
            if self._target is not None:
                await self._target.update_variables(snapshot)


class LivePublisher(Publisher):
    def __init__(self) -> None:
        self._client: Any = None

    def attach(self, client: Any) -> None:
        self._client = client

    def detach(self) -> None:
        self._client = None

    async def publish(self, topic: str, payload: str, retain: bool = False) -> None:
        if self._client is None:
            raise PilotGuardError("MQTT transport is disconnected")
        await _bounded(
            self._client.publish(topic, payload=payload, retain=retain),
            description="MQTT command publication",
            timeout_s=_HOST_UPDATE_TIMEOUT_S,
        )


@asynccontextmanager
async def _mqtt_authority_session(
    client: Any,
    publisher: LivePublisher,
    controller: PilotController,
) -> AsyncIterator[None]:
    """Fence commands before awaiting MQTT client teardown."""

    await _bounded(client.__aenter__(), description="MQTT session enter")
    primary_error: BaseException | None = None
    try:
        publisher.attach(client)
        try:
            yield
        except BaseException as error:
            primary_error = error
            raise
        finally:
            publisher.detach()
            try:
                await _bounded(
                    controller.fence_transport(),
                    description="MQTT session authority fence",
                    timeout_s=_HOST_UPDATE_TIMEOUT_S,
                )
            except BaseException as error:
                if primary_error is None:
                    primary_error = error
                    raise
    finally:
        if primary_error is None:
            exit_info: tuple[type[BaseException] | None, BaseException | None, object | None] = (
                None,
                None,
                None,
            )
        else:
            exit_info = (
                type(primary_error),
                primary_error,
                primary_error.__traceback__,
            )
        try:
            await _bounded(
                client.__aexit__(*exit_info),
                description="MQTT session exit",
            )
        except BaseException:
            if primary_error is None:
                raise


class PilotLock:
    def __init__(self, descriptor: int) -> None:
        self._descriptor: int | None = descriptor

    @classmethod
    def acquire(cls, path: Path = _PILOT_LOCK_PATH) -> PilotLock:
        if path != _PILOT_LOCK_PATH:
            raise PilotGuardError("pilot lock path is fixed")
        parent = path.parent
        try:
            metadata = parent.lstat()
        except FileNotFoundError:
            raise PilotGuardError("pilot lock directory does not exist") from None
        if (
            not stat.S_ISDIR(metadata.st_mode)
            or stat.S_IMODE(metadata.st_mode) != 0o700
            or metadata.st_uid != os.geteuid()
        ):
            raise PilotGuardError("pilot lock directory must be owned by this UID and mode 0700")
        descriptor = os.open(
            path,
            os.O_RDWR | os.O_CREAT | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
            0o600,
        )
        try:
            opened = os.fstat(descriptor)
            if (
                not stat.S_ISREG(opened.st_mode)
                or stat.S_IMODE(opened.st_mode) != 0o600
                or opened.st_uid != os.geteuid()
            ):
                raise PilotGuardError("pilot lock file must be a mode-0600 regular file")
            try:
                fcntl.flock(descriptor, fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                raise PilotGuardError("another BVD pilot is already active") from None
            return cls(descriptor)
        except BaseException:
            os.close(descriptor)
            raise

    def release(self) -> None:
        descriptor = self._descriptor
        if descriptor is None:
            return
        self._descriptor = None
        try:
            fcntl.flock(descriptor, fcntl.LOCK_UN)
        finally:
            os.close(descriptor)


async def read_owner_status(
    *, bindings_factory: Callable[[], BusBindings] = _load_bus_bindings
) -> tuple[str, str]:
    """Read only local identity and a stable BVD owner for recovery discovery."""

    observer, processor, owning_device_id = await _open_bus_peer(
        bindings_factory(), name_prefix="brilliant_bvd_owner_status"
    )
    try:
        owners: list[str] = []
        for _ in range(2):
            configuration = await _bounded(
                observer.get_peripheral(
                    "configuration_virtual_device",
                    BVD_CONFIGURATION_PERIPHERAL_ID,
                ),
                description="owner-status configuration read",
            )
            variables = getattr(configuration, "variables", None)
            if not isinstance(variables, Mapping) or "owner" not in variables:
                raise PilotGuardError("owner-status cannot read the BVD owner")
            owners.append(_variable_value(variables["owner"]))
        if owners[0] != owners[1]:
            raise PilotGuardError("BVD owner changed during owner-status discovery")
        return owning_device_id, owners[1]
    finally:
        await _shutdown_components(observer, processor)


async def delete_exact_pilot(
    *, bindings_factory: Callable[[], BusBindings] = _load_bus_bindings
) -> None:
    """Idempotently delete only the fixed BVD pilot ID through a fresh peer."""

    observer, processor, owning_device_id = await _open_bus_peer(
        bindings_factory(), name_prefix="brilliant_bvd_cleanup"
    )
    try:
        configuration = await _bounded(
            observer.get_peripheral(
                "configuration_virtual_device", BVD_CONFIGURATION_PERIPHERAL_ID
            ),
            description="cleanup owner read",
        )
        variables = getattr(configuration, "variables", None)
        if not isinstance(variables, Mapping) or "owner" not in variables:
            raise PilotGuardError("cleanup cannot read the BVD owner")
        if _variable_value(variables["owner"]) != owning_device_id:
            raise PilotGuardError("cleanup-only must run on the current BVD owner")
        try:
            existing = await _bounded(
                observer.get_peripheral(
                    BVD_DEVICE_ID,
                    peripheral_id_for_entity(TARGET_ENTITY_ID),
                ),
                description="cleanup target read",
            )
        except KeyError:
            existing = None
        if existing is None:
            return
        client = getattr(processor, "client", None)
        if client is None:
            raise PilotGuardError("cleanup bus client is unavailable")
        await _bounded(
            _maybe_await(
                client.delete_peripheral(
                    BVD_DEVICE_ID,
                    peripheral_id_for_entity(TARGET_ENTITY_ID),
                    time.time_ns() // 1_000_000,
                )
            ),
            description="exact BVD pilot deletion",
        )
    finally:
        await _shutdown_components(observer, processor)


def _decode_mqtt_payload(payload: object) -> str:
    if isinstance(payload, (bytes, bytearray)):
        return bytes(payload).decode("utf-8")
    return str(payload)


def _assert_live_gates(
    *,
    controller: PilotController,
    reader: asyncio.Task[None],
    stop_event: asyncio.Event,
    reconnect_abort: asyncio.Event,
    command_failure: BaseException | None,
    bus: BvdBus,
    phase: str,
    stop_is_error: bool = True,
) -> None:
    """Synchronously recheck every gate after each awaited live operation."""

    if reconnect_abort.is_set():
        raise PilotGuardError(f"message bus reconnected {phase}")
    if reader.done():
        reader.result()
        raise PilotGuardError(f"MQTT authority stream ended {phase}")
    if command_failure is not None:
        raise PilotGuardError("native panel command callback failed") from command_failure
    if not controller.authority_available:
        raise PilotGuardError(f"HA state authority became unavailable {phase}")
    notification_age = bus.seconds_since_last_notification()
    if notification_age is None or notification_age >= _NOTIFICATION_STALE_S:
        raise PilotGuardError("BVD notification stream became stale")
    if stop_is_error and stop_event.is_set():
        raise PilotGuardError(f"pilot stopped {phase}")


async def run_live_pilot(
    *,
    config: PilotConfig,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str | None,
    mqtt_password: str | None,
    stop: asyncio.Event | None = None,
    ready: asyncio.Event | None = None,
    dependencies: LiveDependencies | None = None,
) -> CleanupReport:
    """Run one bounded registration; every exit attempts persistent deletion."""

    if not 1 <= mqtt_port <= 65_535:
        raise PilotGuardError("MQTT port must be from 1 to 65535")
    live = _load_live_dependencies() if dependencies is None else dependencies
    stop_event = asyncio.Event() if stop is None else stop
    loop = asyncio.get_running_loop()
    reconnect_abort = asyncio.Event()
    bus = live.bus_factory()
    host = live.host_factory(loop)
    publisher = LivePublisher()
    sink = BufferedStateSink()
    controller = PilotController(publisher=publisher, state_sink=sink)
    lifecycle: PilotLifecycle | None = None
    command_failure: BaseException | None = None

    async def mark_reconnect() -> None:
        reconnect_abort.set()

    async def handle_panel_command(variable: str, value: object) -> bool:
        nonlocal command_failure
        try:
            return await controller.handle_panel_push(variable, value)
        except BaseException as error:
            command_failure = error
            raise

    bus.on_reconnect(mark_reconnect)
    installed_signals: list[signal.Signals] = []
    for sig in _STOP_SIGNALS:
        try:
            loop.add_signal_handler(sig, stop_event.set)
            installed_signals.append(sig)
        except (NotImplementedError, RuntimeError):
            pass

    baseline: BvdTopology | None = None
    reader: asyncio.Task[None] | None = None
    authority_tasks: list[asyncio.Task[Any]] = []
    failure: BaseException | None = None
    client = live.mqtt_client_factory(
        hostname=mqtt_host,
        port=mqtt_port,
        username=mqtt_username,
        password=mqtt_password,
        identifier=f"brilliant-bvd-pilot-{stable_id(TARGET_ENTITY_ID)[-8:]}",
        timeout=_OPERATION_TIMEOUT_S,
        keepalive=15,
    )
    manifest_ready = asyncio.Event()
    manifest_seen = False
    try:
        await bus.start()
        baseline = await _bounded(bus.snapshot(), description="initial PRE topology snapshot")
        validate_preflight(config, baseline, now_ms=time.time_ns() // 1_000_000)
        if config.room_assignment_id not in await _bounded(
            bus.room_ids(), description="initial scoped room catalog"
        ):
            raise PilotGuardError("requested room is absent from the scoped room catalog")
        async with _mqtt_authority_session(client, publisher, controller):
            await client.subscribe(manifest_topic(), timeout=_OPERATION_TIMEOUT_S)
            await client.subscribe(
                state_topic(stable_id(TARGET_ENTITY_ID)), timeout=_OPERATION_TIMEOUT_S
            )
            await client.subscribe(_RESULT_TOPIC_FILTER, timeout=_OPERATION_TIMEOUT_S)

            async def read_messages() -> None:
                nonlocal manifest_seen
                async for message in client.messages:
                    topic = str(message.topic)
                    payload = _decode_mqtt_payload(message.payload)
                    if topic == manifest_topic():
                        if not manifest_seen and not bool(message.retain):
                            raise PilotGuardError("initial HA manifest authority must be retained")
                        validate_manifest_authority(
                            payload,
                            retained=True,
                        )
                        manifest_seen = True
                        manifest_ready.set()
                    elif topic == state_topic(stable_id(TARGET_ENTITY_ID)):
                        await controller.handle_state_message(
                            topic, payload, retained=bool(message.retain)
                        )
                        if not controller.authority_available:
                            raise PilotGuardError("HA state authority became unavailable")
                    elif topic.startswith("brilliant/ha-control/v1/result/"):
                        if controller.accepts_result_topic(topic):
                            await controller.handle_result_message(
                                topic, payload, retained=bool(message.retain)
                            )
                    else:
                        raise PilotGuardError("MQTT message arrived outside subscriptions")

            reader = asyncio.create_task(read_messages())
            state_waiter = asyncio.create_task(controller.wait_for_state(_AUTHORITY_TIMEOUT_S))
            manifest_waiter = asyncio.create_task(manifest_ready.wait())
            stop_waiter = asyncio.create_task(stop_event.wait())
            reconnect_waiter = asyncio.create_task(reconnect_abort.wait())
            authority_tasks.extend((state_waiter, manifest_waiter, stop_waiter, reconnect_waiter))
            authority_deadline = loop.time() + _AUTHORITY_TIMEOUT_S
            while not state_waiter.done() or not manifest_waiter.done():
                remaining = authority_deadline - loop.time()
                if remaining <= 0:
                    raise PilotGuardError("retained HA manifest/state authority timed out")
                waiters: set[asyncio.Task[Any]] = {
                    reader,
                    stop_waiter,
                    reconnect_waiter,
                }
                if not state_waiter.done():
                    waiters.add(state_waiter)
                if not manifest_waiter.done():
                    waiters.add(manifest_waiter)
                done, _ = await asyncio.wait(
                    waiters,
                    timeout=remaining,
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if reader in done:
                    reader.result()
                    raise PilotGuardError("MQTT authority stream ended before registration")
                if stop_waiter in done:
                    raise PilotGuardError("pilot stopped before retained HA authority")
                if reconnect_waiter in done:
                    raise PilotGuardError("message bus reconnected before retained HA authority")
                if state_waiter in done:
                    state_waiter.result()
                if manifest_waiter in done:
                    manifest_waiter.result()
            state_waiter.result()
            manifest_waiter.result()
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="before native registration",
            )

            admission_marker = bus.notification_marker()
            if config.room_assignment_id not in await _bounded(
                bus.room_ids(), description="final scoped room catalog before topology"
            ):
                raise PilotGuardError("requested room disappeared before native registration")
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="before native registration",
            )
            # Make the owner/topology read the last awaited admission input.
            # Freshness is a heuristic, never a lease-duration guarantee.
            baseline = await _bounded(bus.snapshot(), description="final PRE topology snapshot")
            validate_preflight(config, baseline, now_ms=time.time_ns() // 1_000_000)
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="before native registration",
            )
            if config.room_assignment_id not in await _bounded(
                bus.room_ids(), description="final scoped room catalog after topology"
            ):
                raise PilotGuardError("requested room disappeared before native registration")
            if bus.notification_marker() != admission_marker:
                raise PilotGuardError(
                    "BVD changed during final native-registration admission reads"
                )
            validate_preflight(config, baseline, now_ms=time.time_ns() // 1_000_000)
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="before native registration",
            )
            lifecycle = PilotLifecycle(
                config=config,
                host=host,
                room_assignment_type=live.room_assignment_type,
                on_command=handle_panel_command,
                probe_factory=live.probe_factory,
                before_probes=bus.shutdown,
                initial_values=sink.snapshot(),
                sleep=live.lifecycle_sleep,
                absence_interval_s=live.absence_interval_s,
                operation_timeout_s=live.lifecycle_operation_timeout_s,
            )
            deadline = loop.time() + config.active_runtime_s
            notification_marker = bus.notification_marker()
            await lifecycle.start()
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="during native registration",
            )
            await _bounded(
                sink.attach(host),
                description="initial native HA state replay",
                timeout_s=_HOST_UPDATE_TIMEOUT_S,
            )
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="during native registration",
            )
            await bus.wait_for_notification_after(notification_marker, _ACTIVE_SNAPSHOT_TIMEOUT_S)
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="during native registration",
            )
            remaining = deadline - loop.time()
            if remaining <= 0:
                raise PilotGuardError("active deadline expired before READY")
            active = await _bounded(
                bus.snapshot(),
                description="initial ACTIVE topology snapshot",
                timeout_s=min(_ACTIVE_SNAPSHOT_TIMEOUT_S, remaining),
            )
            _assert_live_gates(
                controller=controller,
                reader=reader,
                stop_event=stop_event,
                reconnect_abort=reconnect_abort,
                command_failure=command_failure,
                bus=bus,
                phase="during native registration",
            )
            validate_active_topology(baseline, active)
            print(
                json.dumps(
                    {
                        "event": "READY",
                        "peripheral_id": peripheral_id_for_entity(TARGET_ENTITY_ID),
                        "runtime_s": config.active_runtime_s,
                    },
                    sort_keys=True,
                ),
                flush=True,
            )
            if ready is not None:
                ready.set()
            next_topology_check = loop.time() + _MONITOR_INTERVAL_S
            while True:
                _assert_live_gates(
                    controller=controller,
                    reader=reader,
                    stop_event=stop_event,
                    reconnect_abort=reconnect_abort,
                    command_failure=command_failure,
                    bus=bus,
                    phase="during the pilot",
                    stop_is_error=False,
                )
                if stop_event.is_set():
                    break
                pending_age_ms = controller.pending_command_age_ms()
                if (
                    pending_age_ms is not None
                    and pending_age_ms >= _COMMAND_CONFIRMATION_TIMEOUT_MS
                ):
                    await _bounded(
                        controller.reapply_authoritative_state(),
                        description="unconfirmed command native restoration",
                        timeout_s=_HOST_UPDATE_TIMEOUT_S,
                    )
                    raise PilotGuardError("HA state confirmation timed out")
                remaining = deadline - loop.time()
                if remaining <= 0:
                    break
                if loop.time() >= next_topology_check and remaining > _ACTIVE_SNAPSHOT_TIMEOUT_S:
                    observed = await _bounded(
                        bus.snapshot(),
                        description="ACTIVE topology snapshot",
                        timeout_s=min(_ACTIVE_SNAPSHOT_TIMEOUT_S, remaining),
                    )
                    _assert_live_gates(
                        controller=controller,
                        reader=reader,
                        stop_event=stop_event,
                        reconnect_abort=reconnect_abort,
                        command_failure=command_failure,
                        bus=bus,
                        phase="during the pilot",
                        stop_is_error=False,
                    )
                    if stop_event.is_set():
                        break
                    validate_active_topology(baseline, observed)
                    next_topology_check = loop.time() + _MONITOR_INTERVAL_S
                    continue
                delay = min(_SUPERVISOR_TICK_S, remaining)
                if loop.time() < next_topology_check:
                    delay = min(delay, next_topology_check - loop.time())
                if pending_age_ms is not None:
                    delay = min(
                        delay,
                        max(
                            0.0,
                            (_COMMAND_CONFIRMATION_TIMEOUT_MS - pending_age_ms) / 1_000,
                        ),
                    )
                try:
                    await asyncio.wait_for(stop_event.wait(), timeout=delay)
                except asyncio.TimeoutError:
                    pass
    except BaseException as run_error:
        failure = run_error
    finally:

        async def finish_cleanup() -> CleanupReport:
            nonlocal failure
            publisher.detach()
            if reader is not None:
                if failure is None and reader.done():
                    try:
                        reader.result()
                    except BaseException as reader_error:
                        failure = reader_error
                    else:
                        failure = PilotGuardError("MQTT authority stream ended during cleanup")
                reader.cancel()
                await asyncio.gather(reader, return_exceptions=True)
            for task in authority_tasks:
                task.cancel()
            if authority_tasks:
                await asyncio.gather(*authority_tasks, return_exceptions=True)
            try:
                await _bounded(
                    controller.fence_transport(),
                    description="HA authority fence",
                    timeout_s=_HOST_UPDATE_TIMEOUT_S,
                )
            except BaseException as fence_error:
                if failure is None:
                    failure = fence_error
            try:
                if lifecycle is None:
                    await host.shutdown()
                    await bus.shutdown()
                    return CleanupReport(True, True, True)
                report = await lifecycle.cleanup()
                assert baseline is not None
                postflight_bus = live.bus_factory()
                await _bounded(
                    postflight_bus.start(),
                    description="POST topology bus start",
                )
                try:
                    postflight = await _bounded(
                        postflight_bus.snapshot(),
                        description="POST topology snapshot",
                    )
                    validate_postflight(baseline, postflight)
                finally:
                    await _bounded(
                        postflight_bus.shutdown(),
                        description="POST topology bus shutdown",
                    )
                return report
            finally:
                await bus.shutdown()

        cleanup_task = asyncio.create_task(finish_cleanup())
        try:
            while True:
                try:
                    cleanup = await asyncio.shield(cleanup_task)
                    break
                except asyncio.CancelledError as cancellation:
                    if cleanup_task.done():
                        cleanup = cleanup_task.result()
                        break
                    if failure is None:
                        failure = cancellation
        finally:
            for sig in installed_signals:
                loop.remove_signal_handler(sig)
    if failure is not None:
        if isinstance(failure, (asyncio.CancelledError, KeyboardInterrupt, SystemExit)):
            raise failure
        raise PilotGuardError("live pilot aborted after bounded cleanup") from failure
    return cleanup


def _read_private_password(path: Path) -> str:
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise PilotGuardError("MQTT password file does not exist") from None
    if (
        not stat.S_ISREG(before.st_mode)
        or stat.S_IMODE(before.st_mode) not in {0o400, 0o600}
        or before.st_uid != os.geteuid()
        or before.st_size > _MAX_PRIVATE_FILE_BYTES
    ):
        raise PilotGuardError(
            "MQTT password file must be owned by this UID, regular, private, and at most 64 KiB"
        )
    descriptor = os.open(
        path,
        os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0) | getattr(os, "O_CLOEXEC", 0),
    )
    raw = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (
            (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino)
            or not stat.S_ISREG(opened.st_mode)
            or stat.S_IMODE(opened.st_mode) not in {0o400, 0o600}
            or opened.st_uid != os.geteuid()
        ):
            raise PilotGuardError("MQTT password file changed during open")
        while True:
            chunk = os.read(
                descriptor,
                min(8192, _MAX_PRIVATE_FILE_BYTES + 1 - len(raw)),
            )
            if not chunk:
                break
            raw.extend(chunk)
            if len(raw) > _MAX_PRIVATE_FILE_BYTES:
                raise PilotGuardError("MQTT password file exceeds 64 KiB")
        try:
            decoded = raw.decode("utf-8").rstrip("\r\n")
        except UnicodeDecodeError as decode_error:
            raise PilotGuardError("MQTT password file is not valid UTF-8") from decode_error
        if not decoded:
            raise PilotGuardError("MQTT password file is empty")
        return decoded
    finally:
        for index in range(len(raw)):
            raw[index] = 0
        os.close(descriptor)


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--room-assignment-id")
    parser.add_argument("--display-name", default="HA Backyard Light Group Pilot")
    parser.add_argument("--active-runtime-s", type=int, default=120)
    parser.add_argument("--mqtt-host")
    parser.add_argument("--mqtt-port", type=int, default=1883)
    parser.add_argument("--mqtt-username")
    parser.add_argument("--mqtt-password-file", type=Path)
    mode = parser.add_mutually_exclusive_group()
    mode.add_argument("--apply", action="store_true")
    mode.add_argument("--cleanup-only", action="store_true")
    mode.add_argument("--owner-status", action="store_true")
    parser.add_argument(
        "--cross-owner-cleanup-staged",
        action="store_true",
        help="assert the exact cleanup command is staged on every possible BVD owner",
    )
    parser.add_argument(
        "--stock-canary-approved",
        action="store_true",
        help="assert a reversible stock BVD functional canary is approved",
    )
    parser.add_argument(
        "--external-observer-approved",
        action="store_true",
        help="assert the required non-writing health observer is armed and recording",
    )
    return parser


async def _async_cli(args: argparse.Namespace) -> int:
    if args.owner_status:
        panel_device_id, configuration_owner = await read_owner_status()
        print(
            json.dumps(
                {
                    "event": "OWNER_STATUS",
                    "panel_device_id": panel_device_id,
                    "configuration_owner": configuration_owner,
                    "current_owner": (panel_device_id == configuration_owner),
                },
                sort_keys=True,
            )
        )
        return 0

    if args.cleanup_only:
        if os.geteuid() != 0:
            raise PilotGuardError("cleanup-only must run as root on the current BVD owner")
        await delete_exact_pilot()
        factory = NativeProbeFactory()
        for index in range(2):
            probe = await factory()
            try:
                if await probe.contains(BVD_DEVICE_ID, peripheral_id_for_entity(TARGET_ENTITY_ID)):
                    raise PilotGuardError("cleanup-only could not prove pilot absence")
            finally:
                await probe.shutdown()
            if index == 0:
                await asyncio.sleep(30)
        print(json.dumps({"event": "CLEANUP_PROVEN", "lease_release": "not_applicable"}))
        return 0

    if not args.room_assignment_id:
        raise PilotGuardError("--room-assignment-id is required outside cleanup-only")
    config = PilotConfig(
        room_assignment_id=args.room_assignment_id,
        display_name=args.display_name,
        active_runtime_s=args.active_runtime_s,
    )
    if not args.apply:
        bus = NativeBvdBus()
        await bus.start()
        try:
            topology = await bus.snapshot()
            validate_preflight(config, topology, now_ms=time.time_ns() // 1_000_000)
            if config.room_assignment_id not in await bus.room_ids():
                raise PilotGuardError("requested room is absent from the scoped room catalog")
        finally:
            await bus.shutdown()
        print(
            json.dumps(
                {
                    "event": "DRY_RUN_OK",
                    "office_device_id": OFFICE_DEVICE_ID,
                    "bvd_owner": topology.configuration_owner,
                    "peripheral_id": peripheral_id_for_entity(TARGET_ENTITY_ID),
                    "lease_release": "not_applicable",
                },
                sort_keys=True,
            )
        )
        return 0
    if os.geteuid() != 0:
        raise PilotGuardError("live apply must run as root on Office")
    if not args.cross_owner_cleanup_staged:
        raise PilotGuardError(
            "live apply is NO-GO until exact cleanup-only is staged on every possible BVD owner"
        )
    if not args.stock_canary_approved:
        raise PilotGuardError("live apply is NO-GO until a reversible stock BVD canary is approved")
    if not args.external_observer_approved:
        raise PilotGuardError(
            "live apply is NO-GO until the required external health observer is approved"
        )
    if not args.mqtt_host:
        raise PilotGuardError("--mqtt-host is required with --apply")
    password = (
        None if args.mqtt_password_file is None else _read_private_password(args.mqtt_password_file)
    )
    await run_live_pilot(
        config=config,
        mqtt_host=args.mqtt_host,
        mqtt_port=args.mqtt_port,
        mqtt_username=args.mqtt_username,
        mqtt_password=password,
    )
    print(json.dumps({"event": "STOPPED_CLEAN", "lease_release": "not_applicable"}))
    return 0


def main(argv: Sequence[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    lock: PilotLock | None = None
    if args.apply or args.cleanup_only:
        lock = PilotLock.acquire()
    try:
        return asyncio.run(_async_cli(args))
    finally:
        if lock is not None:
            lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
