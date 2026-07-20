"""Strictly passive BlueZ D-Bus observation with a low-level, testable seam."""

from __future__ import annotations

import asyncio
import logging
import time
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Mapping, Sequence
from dataclasses import dataclass
from typing import Literal, Protocol, cast

from dbus_next.aio.message_bus import MessageBus
from dbus_next.constants import BusType, MessageType
from dbus_next.message import Message
from dbus_next.signature import Variant

from .model import (
    AllowlistEntry,
    matches_allowlist,
    normalize_address,
    normalize_advertisement_fields,
)

BLUEZ_SERVICE = "org.bluez"
ADAPTER_INTERFACE = "org.bluez.Adapter1"
DEVICE_INTERFACE = "org.bluez.Device1"
OBJECT_MANAGER_INTERFACE = "org.freedesktop.DBus.ObjectManager"
PROPERTIES_INTERFACE = "org.freedesktop.DBus.Properties"
DBUS_INTERFACE = "org.freedesktop.DBus"
DBUS_PATH = "/org/freedesktop/DBus"
SIGNAL_QUEUE_SIZE = 256
MAX_DEVICE_CACHE_ENTRIES = 512

_LOG = logging.getLogger(__name__)


@dataclass(frozen=True)
class DbusSignal:
    """Small transport-neutral view of one D-Bus signal."""

    path: str
    interface: str
    member: str
    body: tuple[object, ...]


@dataclass(frozen=True)
class Observation:
    """One allowlisted, normalized BlueZ advertisement observation."""

    adapter_address: str
    address: str
    address_type: str
    rssi: int
    local_name: str | None
    tx_power: int | None
    service_uuids: Sequence[str]
    service_data: Mapping[str, bytes]
    manufacturer_data: Mapping[int, bytes]
    capture_monotonic_ms: int

    def __post_init__(self) -> None:
        normalized = normalize_advertisement_fields(
            adapter_address=self.adapter_address,
            address=self.address,
            address_type=self.address_type,
            rssi=self.rssi,
            local_name=self.local_name,
            tx_power=self.tx_power,
            service_uuids=self.service_uuids,
            service_data=self.service_data,
            manufacturer_data=self.manufacturer_data,
            capture_monotonic_ms=self.capture_monotonic_ms,
        )
        object.__setattr__(self, "adapter_address", normalized.adapter_address)
        object.__setattr__(self, "address", normalized.address)
        object.__setattr__(self, "address_type", normalized.address_type)
        object.__setattr__(self, "rssi", normalized.rssi)
        object.__setattr__(self, "local_name", normalized.local_name)
        object.__setattr__(self, "tx_power", normalized.tx_power)
        object.__setattr__(self, "service_uuids", normalized.service_uuids)
        object.__setattr__(self, "service_data", normalized.service_data)
        object.__setattr__(self, "manufacturer_data", normalized.manufacturer_data)
        object.__setattr__(self, "capture_monotonic_ms", normalized.capture_monotonic_ms)


SignalHandler = Callable[[DbusSignal], None]
ObservationHandler = Callable[[Observation], Awaitable[None]]


class BluezClient(Protocol):
    """The complete steady-state D-Bus surface: no mutating methods exist."""

    async def connect(self) -> None: ...

    async def get_managed_objects(
        self,
    ) -> Mapping[str, Mapping[str, Mapping[str, object]]]: ...

    async def get_property(self, path: str, interface: str, name: str) -> object: ...

    async def add_signal_match(self, rule: str, handler: SignalHandler) -> None: ...

    async def remove_signal_match(self, rule: str, handler: SignalHandler) -> None: ...

    async def wait_for_disconnect(self) -> None: ...

    async def close(self) -> None: ...


BluezClientFactory = Callable[[], BluezClient]


class BluezObserver:
    """Observe one BlueZ adapter without starting discovery or changing state."""

    def __init__(
        self,
        *,
        adapter: str,
        allowlist: Sequence[AllowlistEntry],
        client_factory: BluezClientFactory = lambda: SystemBluezClient(),
        monotonic: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
        reconnect_backoff: float = 2.0,
        device_cache_size: int = MAX_DEVICE_CACHE_ENTRIES,
    ) -> None:
        self._adapter_path = f"/org/bluez/{adapter}"
        self._allowlist = tuple(allowlist)
        self._client_factory = client_factory
        self._monotonic = monotonic
        self._sleep = sleep
        if reconnect_backoff < 0:
            raise ValueError("reconnect_backoff must be non-negative")
        self._reconnect_backoff = reconnect_backoff
        if (
            not isinstance(device_cache_size, int)
            or isinstance(device_cache_size, bool)
            or device_cache_size < 1
        ):
            raise ValueError("device_cache_size must be a positive integer")
        self._device_cache_size = device_cache_size

    async def run(self, on_observation: ObservationHandler) -> None:
        """Rebuild passive D-Bus sessions after disconnects with fixed backoff."""
        while True:
            try:
                await self.observe_once(on_observation)
                _LOG.warning("BlueZ session disconnected; reconnecting after backoff")
            except asyncio.CancelledError:
                raise
            except Exception as error:
                _LOG.warning(
                    "BlueZ observation session failed (%s); reconnecting after backoff",
                    type(error).__name__,
                )
            await self._sleep(self._reconnect_backoff)

    async def observe_once(self, on_observation: ObservationHandler) -> None:
        """Open one passive session, drain signals, and always unregister/close."""
        client = self._client_factory()
        rules = self._signal_rules()
        registered: list[str] = []
        signal_queue: asyncio.Queue[DbusSignal] = asyncio.Queue(maxsize=SIGNAL_QUEUE_SIZE)
        properties: OrderedDict[str, dict[str, object]] = OrderedDict()

        def receive_signal(signal: DbusSignal) -> None:
            if signal_queue.full():
                try:
                    signal_queue.get_nowait()
                    signal_queue.task_done()
                except asyncio.QueueEmpty:
                    pass
            signal_queue.put_nowait(signal)

        try:
            await client.connect()
            adapter_address = normalize_address(
                await client.get_property(self._adapter_path, ADAPTER_INTERFACE, "Address"),
                field_name="adapter_address",
            )
            for rule in rules:
                await client.add_signal_match(rule, receive_signal)
                registered.append(rule)
            managed = await client.get_managed_objects()
            for path, interfaces in managed.items():
                if not self._is_adapter_device(path):
                    continue
                raw = interfaces.get(DEVICE_INTERFACE)
                if not isinstance(raw, Mapping):
                    continue
                current = _string_mapping(cast(Mapping[object, object], raw))
                self._remember_device(properties, path, current)
                if "RSSI" in current:
                    await self._emit_if_allowed(
                        current,
                        adapter_address=adapter_address,
                        on_observation=on_observation,
                    )
            await self._drain_signals(
                client,
                signal_queue,
                properties,
                adapter_address=adapter_address,
                on_observation=on_observation,
            )
        finally:
            for rule in reversed(registered):
                try:
                    await client.remove_signal_match(rule, receive_signal)
                except Exception as error:
                    _LOG.warning("failed removing BlueZ signal match (%s)", type(error).__name__)
            try:
                await client.close()
            except Exception as error:
                _LOG.warning("failed closing BlueZ client (%s)", type(error).__name__)

    async def _drain_signals(
        self,
        client: BluezClient,
        queue: asyncio.Queue[DbusSignal],
        properties: OrderedDict[str, dict[str, object]],
        *,
        adapter_address: str,
        on_observation: ObservationHandler,
    ) -> None:
        disconnect_task = asyncio.create_task(client.wait_for_disconnect())
        try:
            while True:
                signal_task = asyncio.create_task(queue.get())
                done, _pending = await asyncio.wait(
                    (disconnect_task, signal_task), return_when=asyncio.FIRST_COMPLETED
                )
                if disconnect_task in done:
                    signal_task.cancel()
                    await asyncio.gather(signal_task, return_exceptions=True)
                    await disconnect_task
                    return
                signal = signal_task.result()
                queue.task_done()
                await self._handle_signal(
                    signal,
                    properties,
                    adapter_address=adapter_address,
                    on_observation=on_observation,
                )
        finally:
            disconnect_task.cancel()
            await asyncio.gather(disconnect_task, return_exceptions=True)

    async def _handle_signal(
        self,
        signal: DbusSignal,
        properties: OrderedDict[str, dict[str, object]],
        *,
        adapter_address: str,
        on_observation: ObservationHandler,
    ) -> None:
        if (
            signal.interface == OBJECT_MANAGER_INTERFACE
            and signal.member == "InterfacesAdded"
            and len(signal.body) == 2
        ):
            path, interfaces = signal.body
            if not isinstance(path, str) or not self._is_adapter_device(path):
                return
            if not isinstance(interfaces, Mapping):
                return
            device = interfaces.get(DEVICE_INTERFACE)
            if not isinstance(device, Mapping):
                return
            current = _string_mapping(device)
            self._remember_device(properties, path, current)
            if "RSSI" not in current:
                return
            await self._emit_if_allowed(
                current,
                adapter_address=adapter_address,
                on_observation=on_observation,
            )
            return

        if (
            signal.interface == OBJECT_MANAGER_INTERFACE
            and signal.member == "InterfacesRemoved"
            and len(signal.body) == 2
        ):
            path, interfaces = signal.body
            if not isinstance(path, str) or not self._is_adapter_device(path):
                return
            if isinstance(interfaces, Sequence) and not isinstance(interfaces, (str, bytes)):
                if DEVICE_INTERFACE in interfaces:
                    properties.pop(path, None)
            return

        if (
            signal.interface != PROPERTIES_INTERFACE
            or signal.member != "PropertiesChanged"
            or len(signal.body) != 3
            or not self._is_adapter_device(signal.path)
        ):
            return
        interface_name, changed, invalidated = signal.body
        if interface_name != DEVICE_INTERFACE or not isinstance(changed, Mapping):
            return
        changed_values = _string_mapping(changed)
        current = properties.pop(signal.path, {})
        current.update(changed_values)
        if isinstance(invalidated, Sequence) and not isinstance(invalidated, (str, bytes)):
            for field in invalidated:
                if isinstance(field, str):
                    current.pop(field, None)
        self._remember_device(properties, signal.path, current)
        if "RSSI" not in changed_values:
            return
        await self._emit_if_allowed(
            current,
            adapter_address=adapter_address,
            on_observation=on_observation,
        )

    async def _emit_if_allowed(
        self,
        properties: Mapping[str, object],
        *,
        adapter_address: str,
        on_observation: ObservationHandler,
    ) -> None:
        observation = _observation_from_properties(
            properties,
            adapter_address=adapter_address,
            capture_monotonic_ms=int(self._monotonic() * 1_000),
        )
        if observation is None or not matches_allowlist(
            address=observation.address,
            manufacturer_data=observation.manufacturer_data,
            allowlist=self._allowlist,
        ):
            return
        await on_observation(observation)

    def _is_adapter_device(self, path: object) -> bool:
        return isinstance(path, str) and path.startswith(f"{self._adapter_path}/dev_")

    def _remember_device(
        self,
        properties: OrderedDict[str, dict[str, object]],
        path: str,
        current: dict[str, object],
    ) -> None:
        properties[path] = current
        properties.move_to_end(path)
        while len(properties) > self._device_cache_size:
            properties.popitem(last=False)

    def _signal_rules(self) -> tuple[str, ...]:
        return (
            "type='signal',sender='org.bluez',"
            "interface='org.freedesktop.DBus.ObjectManager',member='InterfacesAdded'",
            "type='signal',sender='org.bluez',"
            "interface='org.freedesktop.DBus.ObjectManager',member='InterfacesRemoved'",
            "type='signal',sender='org.bluez',"
            "interface='org.freedesktop.DBus.Properties',member='PropertiesChanged',"
            f"path_namespace='{self._adapter_path}'",
        )


class SystemBluezClient:
    """Low-level dbus-next system-bus client with no introspection or mutations."""

    def __init__(self) -> None:
        self._bus: MessageBus | None = None
        self._matches: dict[str, SignalHandler] = {}
        self._handler_registered = False

    async def connect(self) -> None:
        bus = MessageBus(bus_type=BusType.SYSTEM)
        self._bus = bus
        await bus.connect()
        bus.add_message_handler(self._on_message)
        self._handler_registered = True

    async def get_managed_objects(
        self,
    ) -> Mapping[str, Mapping[str, Mapping[str, object]]]:
        reply = await self._call(
            Message(
                destination=BLUEZ_SERVICE,
                path="/",
                interface=OBJECT_MANAGER_INTERFACE,
                member="GetManagedObjects",
            )
        )
        if not reply.body or not isinstance(reply.body[0], Mapping):
            raise RuntimeError("BlueZ GetManagedObjects returned an invalid body")
        value = _unwrap(reply.body[0])
        if not isinstance(value, Mapping):
            raise RuntimeError("BlueZ GetManagedObjects returned an invalid body")
        return _managed_objects(value)

    async def get_property(self, path: str, interface: str, name: str) -> object:
        reply = await self._call(
            Message(
                destination=BLUEZ_SERVICE,
                path=path,
                interface=PROPERTIES_INTERFACE,
                member="Get",
                signature="ss",
                body=[interface, name],
            )
        )
        if not reply.body:
            raise RuntimeError("BlueZ Properties.Get returned an empty body")
        return _unwrap(reply.body[0])

    async def add_signal_match(self, rule: str, handler: SignalHandler) -> None:
        if rule in self._matches:
            raise RuntimeError("D-Bus signal match is already registered")
        self._matches[rule] = handler
        try:
            await self._dbus_match("AddMatch", rule)
        except BaseException:
            self._matches.pop(rule, None)
            raise

    async def remove_signal_match(self, rule: str, handler: SignalHandler) -> None:
        if self._matches.get(rule) is not handler:
            return
        try:
            await self._dbus_match("RemoveMatch", rule)
        finally:
            self._matches.pop(rule, None)

    async def wait_for_disconnect(self) -> None:
        wait = cast(Callable[[], Awaitable[object]], self._require_bus().wait_for_disconnect)
        await wait()

    async def close(self) -> None:
        bus = self._bus
        self._bus = None
        self._matches.clear()
        if bus is None:
            return
        if self._handler_registered:
            bus.remove_message_handler(self._on_message)
            self._handler_registered = False
        disconnect = cast(Callable[[], None], bus.disconnect)
        disconnect()

    async def _dbus_match(self, member: Literal["AddMatch", "RemoveMatch"], rule: str) -> None:
        await self._call(
            Message(
                destination=DBUS_INTERFACE,
                path=DBUS_PATH,
                interface=DBUS_INTERFACE,
                member=member,
                signature="s",
                body=[rule],
            )
        )

    async def _call(self, message: Message) -> Message:
        reply = await self._require_bus().call(message)
        if reply is None:
            raise RuntimeError(f"D-Bus {message.member} returned no reply")
        if reply.message_type is MessageType.ERROR:
            raise RuntimeError(f"D-Bus {message.member} failed: {reply.error_name or 'unknown'}")
        return reply

    def _on_message(self, message: Message) -> Message | bool | None:
        if message.message_type is not MessageType.SIGNAL:
            return None
        signal = DbusSignal(
            path=message.path or "",
            interface=message.interface or "",
            member=message.member or "",
            body=tuple(_unwrap(value) for value in message.body),
        )
        for handler in set(self._matches.values()):
            handler(signal)
        return None

    def _require_bus(self) -> MessageBus:
        if self._bus is None:
            raise RuntimeError("D-Bus client is not connected")
        return self._bus


class SystemBluezProbeClient:
    """Narrow opt-in discovery-session client used only by the bounded probe."""

    def __init__(self) -> None:
        self._bus: MessageBus | None = None

    async def connect(self) -> None:
        bus = MessageBus(bus_type=BusType.SYSTEM)
        self._bus = bus
        await bus.connect()

    async def start_discovery(self, adapter_path: str) -> None:
        await self._call_discovery(adapter_path, "StartDiscovery")

    async def stop_discovery(self, adapter_path: str) -> None:
        await self._call_discovery(adapter_path, "StopDiscovery")

    async def close(self) -> None:
        bus = self._bus
        self._bus = None
        if bus is None:
            return
        disconnect = cast(Callable[[], None], bus.disconnect)
        disconnect()

    async def _call_discovery(
        self,
        adapter_path: str,
        member: Literal["StartDiscovery", "StopDiscovery"],
    ) -> None:
        reply = await self._require_bus().call(
            Message(
                destination=BLUEZ_SERVICE,
                path=adapter_path,
                interface=ADAPTER_INTERFACE,
                member=member,
            )
        )
        if reply is None:
            raise RuntimeError(f"D-Bus {member} returned no reply")
        if reply.message_type is MessageType.ERROR:
            raise RuntimeError(f"D-Bus {member} failed: {reply.error_name or 'unknown'}")

    def _require_bus(self) -> MessageBus:
        if self._bus is None:
            raise RuntimeError("D-Bus probe client is not connected")
        return self._bus


def _observation_from_properties(
    properties: Mapping[str, object],
    *,
    adapter_address: str,
    capture_monotonic_ms: int,
) -> Observation | None:
    address = properties.get("Address")
    address_type = properties.get("AddressType")
    rssi = properties.get("RSSI")
    if not isinstance(address, str) or not isinstance(address_type, str) or type(rssi) is not int:
        return None
    name = properties.get("Name")
    alias = properties.get("Alias")
    local_name = name if isinstance(name, str) and name.strip() else alias
    if not isinstance(local_name, str):
        local_name = None
    tx_power = properties.get("TxPower")
    if type(tx_power) is not int:
        tx_power = None
    uuids = properties.get("UUIDs", ())
    service_data = properties.get("ServiceData", {})
    manufacturer_data = properties.get("ManufacturerData", {})
    try:
        return Observation(
            adapter_address=adapter_address,
            address=address,
            address_type=address_type,
            rssi=rssi,
            local_name=local_name,
            tx_power=tx_power,
            service_uuids=cast(Sequence[str], uuids),
            service_data=cast(Mapping[str, bytes], service_data),
            manufacturer_data=cast(Mapping[int, bytes], manufacturer_data),
            capture_monotonic_ms=capture_monotonic_ms,
        )
    except (TypeError, ValueError):
        return None


def _string_mapping(value: Mapping[object, object]) -> dict[str, object]:
    return {str(key): _unwrap(item) for key, item in value.items() if isinstance(key, str)}


def _managed_objects(
    value: Mapping[object, object],
) -> Mapping[str, Mapping[str, Mapping[str, object]]]:
    result: dict[str, dict[str, dict[str, object]]] = {}
    for path, raw_interfaces in value.items():
        if not isinstance(path, str) or not isinstance(raw_interfaces, Mapping):
            continue
        interfaces: dict[str, dict[str, object]] = {}
        for interface, raw_properties in raw_interfaces.items():
            if not isinstance(interface, str) or not isinstance(raw_properties, Mapping):
                continue
            interfaces[interface] = _string_mapping(raw_properties)
        result[path] = interfaces
    return result


def _unwrap(value: object) -> object:
    if isinstance(value, Variant):
        return _unwrap(value.value)
    if isinstance(value, Mapping):
        return {_unwrap(key): _unwrap(item) for key, item in value.items()}
    if isinstance(value, (list, tuple)):
        return [_unwrap(item) for item in value]
    if isinstance(value, (bytes, bytearray, memoryview)):
        return bytes(value)
    return value
