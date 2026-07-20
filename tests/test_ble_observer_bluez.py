"""Read-only BlueZ observation tests with a method-recording fake D-Bus."""

from __future__ import annotations

import asyncio
from collections.abc import Callable, Mapping

import pytest

from brilliant_ble_observer.bluez import (
    DEVICE_INTERFACE,
    MAX_DEVICE_CACHE_ENTRIES,
    SIGNAL_QUEUE_SIZE,
    BluezObserver,
    DbusSignal,
    Observation,
)
from brilliant_ble_observer.model import AllowlistEntry

ADAPTER_PATH = "/org/bluez/hci0"
DEVICE_PATH = f"{ADAPTER_PATH}/dev_AA_BB_CC_DD_EE_FF"
OTHER_ADAPTER_DEVICE = "/org/bluez/hci1/dev_AA_BB_CC_DD_EE_FF"
IBEACON_BYTES = bytes.fromhex("021500112233445566778899aabbccddeeff00420007c5")
BATTERY_UUID = "0000180f-0000-1000-8000-00805f9b34fb"
SignalHandler = Callable[[DbusSignal], None]


class FakeBluezClient:
    """Records only D-Bus method calls; lifecycle calls are separate counters."""

    def __init__(
        self,
        managed_objects: Mapping[str, Mapping[str, Mapping[str, object]]] | None = None,
        *,
        close_error: BaseException | None = None,
        connect_error: BaseException | None = None,
        disconnect_error: BaseException | None = None,
        managed_gate: asyncio.Event | None = None,
    ) -> None:
        self.managed_objects = managed_objects or {}
        self.close_error = close_error
        self.connect_error = connect_error
        self.disconnect_error = disconnect_error
        self.managed_gate = managed_gate
        self.method_calls: list[str] = []
        self.handlers: dict[str, SignalHandler] = {}
        self.connected = asyncio.Event()
        self.matches_ready = asyncio.Event()
        self.managed_entered = asyncio.Event()
        self.disconnect = asyncio.Event()
        self.connect_count = 0
        self.close_count = 0

    async def connect(self) -> None:
        self.connect_count += 1
        self.connected.set()
        if self.connect_error is not None:
            raise self.connect_error

    async def get_managed_objects(
        self,
    ) -> Mapping[str, Mapping[str, Mapping[str, object]]]:
        self.method_calls.append("GetManagedObjects")
        self.managed_entered.set()
        if self.managed_gate is not None:
            await self.managed_gate.wait()
        return self.managed_objects

    async def get_property(self, path: str, interface: str, name: str) -> object:
        self.method_calls.append(f"Properties.Get:{interface}.{name}")
        assert path == ADAPTER_PATH
        assert interface == "org.bluez.Adapter1"
        assert name == "Address"
        return "11-22-33-44-55-66"

    async def add_signal_match(self, rule: str, handler: SignalHandler) -> None:
        self.method_calls.append("AddMatch")
        self.handlers[rule] = handler
        if len(self.handlers) == 3:
            self.matches_ready.set()

    async def remove_signal_match(self, rule: str, handler: SignalHandler) -> None:
        self.method_calls.append("RemoveMatch")
        assert self.handlers.get(rule) is handler
        del self.handlers[rule]

    async def wait_for_disconnect(self) -> None:
        if self.disconnect_error is not None:
            raise self.disconnect_error
        await self.disconnect.wait()

    async def close(self) -> None:
        self.close_count += 1
        if self.close_error is not None:
            raise self.close_error

    def emit(self, signal: DbusSignal) -> None:
        for handler in set(self.handlers.values()):
            handler(signal)


def _device_properties(**changes: object) -> dict[str, object]:
    values: dict[str, object] = {
        "Address": "aa-bb-cc-dd-ee-ff",
        "AddressType": "public",
        "RSSI": -61,
        "Name": "Wallet",
        "Alias": "Wallet alias",
        "TxPower": -59,
        "UUIDs": [BATTERY_UUID.upper()],
        "ServiceData": {BATTERY_UUID.upper(): bytearray.fromhex("aabbcc")},
        "ManufacturerData": {76: memoryview(IBEACON_BYTES)},
    }
    values.update(changes)
    return values


async def _start_observer(
    client: FakeBluezClient,
    *,
    allowlist: tuple[AllowlistEntry, ...] | None = None,
    device_cache_size: int = MAX_DEVICE_CACHE_ENTRIES,
) -> tuple[asyncio.Task[None], list[Observation]]:
    observations: list[Observation] = []

    async def on_observation(observation: Observation) -> None:
        observations.append(observation)

    observer = BluezObserver(
        adapter="hci0",
        allowlist=allowlist
        or (
            AllowlistEntry(
                ibeacon_uuid="00112233-4455-6677-8899-aabbccddeeff",
                ibeacon_major=66,
                ibeacon_minor=7,
            ),
        ),
        client_factory=lambda: client,
        monotonic=lambda: 123.456,
        device_cache_size=device_cache_size,
    )
    task = asyncio.create_task(observer.observe_once(on_observation))
    await asyncio.wait_for(client.matches_ready.wait(), timeout=1)
    await asyncio.sleep(0)
    return task, observations


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _wait_for_count(observations: list[Observation], count: int) -> None:
    for _attempt in range(20):
        if len(observations) >= count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {count} observations, got {len(observations)}")


async def test_managed_objects_seed_cache_without_emitting_cached_rssi() -> None:
    client = FakeBluezClient(
        {
            DEVICE_PATH: {DEVICE_INTERFACE: _device_properties()},
            OTHER_ADAPTER_DEVICE: {DEVICE_INTERFACE: _device_properties(RSSI=-20)},
            f"{ADAPTER_PATH}/not-a-device": {"org.bluez.Media1": {"RSSI": -10}},
        }
    )

    task, observations = await _start_observer(client)
    await asyncio.sleep(0)
    assert observations == []

    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -57}, ()),
        )
    )
    await _wait_for_count(observations, 1)
    await _cancel(task)

    assert observations == [
        Observation(
            adapter_address="11:22:33:44:55:66",
            address="AA:BB:CC:DD:EE:FF",
            address_type="public",
            rssi=-57,
            local_name="Wallet",
            tx_power=-59,
            service_uuids=(BATTERY_UUID,),
            service_data={BATTERY_UUID: bytes.fromhex("aabbcc")},
            manufacturer_data={76: IBEACON_BYTES},
            capture_monotonic_ms=123456,
        )
    ]


async def test_interfaces_added_and_properties_changed_require_fresh_rssi() -> None:
    client = FakeBluezClient()
    task, observations = await _start_observer(client)

    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(DEVICE_PATH, {DEVICE_INTERFACE: _device_properties()}),
        )
    )
    client.emit(
        DbusSignal(
            path=OTHER_ADAPTER_DEVICE,
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(OTHER_ADAPTER_DEVICE, {DEVICE_INTERFACE: _device_properties(RSSI=-10)}),
        )
    )
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"TxPower": -55}, ()),
        )
    )
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -57, "Name": "Wallet 2"}, ()),
        )
    )
    await _wait_for_count(observations, 2)
    await _cancel(task)

    assert [item.rssi for item in observations] == [-61, -57]
    assert observations[-1].local_name == "Wallet 2"
    assert observations[-1].tx_power == -55


async def test_alias_is_name_fallback_and_non_device_signals_are_ignored() -> None:
    client = FakeBluezClient()
    task, observations = await _start_observer(
        client, allowlist=(AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),)
    )

    properties = _device_properties(Name=None, Alias="Fallback alias")
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(DEVICE_PATH, {DEVICE_INTERFACE: properties}),
        )
    )
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=("org.bluez.Battery1", {"RSSI": -1}, ()),
        )
    )
    await _wait_for_count(observations, 1)
    await _cancel(task)

    assert len(observations) == 1
    assert observations[0].local_name == "Fallback alias"


async def test_fresh_payload_that_no_longer_matches_allowlist_is_not_emitted() -> None:
    client = FakeBluezClient({DEVICE_PATH: {DEVICE_INTERFACE: _device_properties()}})
    task, observations = await _start_observer(client)
    await asyncio.sleep(0)
    assert observations == []

    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(
                DEVICE_INTERFACE,
                {"RSSI": -58, "ManufacturerData": {76: b"not-an-ibeacon"}},
                (),
            ),
        )
    )
    for _attempt in range(5):
        await asyncio.sleep(0)
    await _cancel(task)

    assert observations == []


async def test_interfaces_removed_discards_cached_device_properties() -> None:
    barrier_address = "AA:BB:CC:DD:EE:01"
    barrier_path = f"{ADAPTER_PATH}/dev_AA_BB_CC_DD_EE_01"
    client = FakeBluezClient()
    task, observations = await _start_observer(
        client,
        allowlist=(
            AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),
            AllowlistEntry(address=barrier_address),
        ),
    )
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(DEVICE_PATH, {DEVICE_INTERFACE: _device_properties()}),
        )
    )
    await _wait_for_count(observations, 1)

    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesRemoved",
            body=(DEVICE_PATH, (DEVICE_INTERFACE,)),
        )
    )
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -55}, ()),
        )
    )
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(
                barrier_path,
                {DEVICE_INTERFACE: _device_properties(Address=barrier_address, RSSI=-40)},
            ),
        )
    )
    await _wait_for_count(observations, 2)
    await _cancel(task)

    assert [item.rssi for item in observations] == [-61, -40]


async def test_signal_queue_overflow_clears_cached_identity_before_fresh_rssi() -> None:
    """Lost removal/invalidation signals must fail closed, not reuse stale identity."""
    barrier_address = "AA:BB:CC:DD:EE:01"
    barrier_path = f"{ADAPTER_PATH}/dev_AA_BB_CC_DD_EE_01"
    client = FakeBluezClient({DEVICE_PATH: {DEVICE_INTERFACE: _device_properties()}})
    observations: list[Observation] = []
    first_observation = asyncio.Event()
    release_first = asyncio.Event()
    barrier_seen = asyncio.Event()

    async def on_observation(observation: Observation) -> None:
        observations.append(observation)
        if len(observations) == 1:
            first_observation.set()
            await release_first.wait()
        if observation.address == barrier_address:
            barrier_seen.set()

    observer = BluezObserver(
        adapter="hci0",
        allowlist=(
            AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),
            AllowlistEntry(address=barrier_address),
        ),
        client_factory=lambda: client,
        monotonic=lambda: 123.456,
    )
    task = asyncio.create_task(observer.observe_once(on_observation))
    await asyncio.wait_for(client.matches_ready.wait(), timeout=1)
    await asyncio.sleep(0)

    # Block the consumer while a removal plus a full backlog accumulates.
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -60}, ()),
        )
    )
    await asyncio.wait_for(first_observation.wait(), timeout=1)
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesRemoved",
            body=(DEVICE_PATH, (DEVICE_INTERFACE,)),
        )
    )
    irrelevant = DbusSignal(path="/", interface="ignored", member="ignored", body=())
    for _index in range(SIGNAL_QUEUE_SIZE - 1):
        client.emit(irrelevant)

    # This RSSI-only signal triggers overflow. If only the oldest signal is lost,
    # it reuses the stale cached address/identity that InterfacesRemoved should clear.
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -42}, ()),
        )
    )
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(
                barrier_path,
                {DEVICE_INTERFACE: _device_properties(Address=barrier_address, RSSI=-40)},
            ),
        )
    )
    release_first.set()
    await asyncio.wait_for(barrier_seen.wait(), timeout=1)
    await _cancel(task)

    assert [(item.address, item.rssi) for item in observations] == [
        ("AA:BB:CC:DD:EE:FF", -60),
        (barrier_address, -40),
    ]


async def test_overflow_during_snapshot_cannot_reseed_stale_identity() -> None:
    """An in-flight unknown-age snapshot cannot undo overflow cache invalidation."""
    barrier_address = "AA:BB:CC:DD:EE:01"
    barrier_path = f"{ADAPTER_PATH}/dev_AA_BB_CC_DD_EE_01"
    release_snapshot = asyncio.Event()
    client = FakeBluezClient(
        {DEVICE_PATH: {DEVICE_INTERFACE: _device_properties()}},
        managed_gate=release_snapshot,
    )
    observations: list[Observation] = []
    barrier_seen = asyncio.Event()

    async def on_observation(observation: Observation) -> None:
        observations.append(observation)
        if observation.address == barrier_address:
            barrier_seen.set()

    observer = BluezObserver(
        adapter="hci0",
        allowlist=(
            AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),
            AllowlistEntry(address=barrier_address),
        ),
        client_factory=lambda: client,
        monotonic=lambda: 123.456,
    )
    task = asyncio.create_task(observer.observe_once(on_observation))
    await asyncio.wait_for(client.managed_entered.wait(), timeout=1)

    irrelevant = DbusSignal(path="/", interface="ignored", member="ignored", body=())
    for _index in range(SIGNAL_QUEUE_SIZE):
        client.emit(irrelevant)
    client.emit(
        DbusSignal(
            path=DEVICE_PATH,
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -42}, ()),
        )
    )
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(
                barrier_path,
                {DEVICE_INTERFACE: _device_properties(Address=barrier_address, RSSI=-40)},
            ),
        )
    )
    release_snapshot.set()
    await asyncio.wait_for(barrier_seen.wait(), timeout=1)
    await _cancel(task)

    assert [(item.address, item.rssi) for item in observations] == [
        (barrier_address, -40),
    ]


async def test_device_property_cache_evicts_least_recently_seen_path() -> None:
    addresses = (
        "AA:BB:CC:DD:EE:01",
        "AA:BB:CC:DD:EE:02",
        "AA:BB:CC:DD:EE:03",
        "AA:BB:CC:DD:EE:04",
    )
    paths = tuple(f"{ADAPTER_PATH}/dev_{address.replace(':', '_')}" for address in addresses)
    client = FakeBluezClient()
    task, observations = await _start_observer(
        client,
        allowlist=tuple(AllowlistEntry(address=address) for address in addresses),
        device_cache_size=2,
    )
    for path, address in zip(paths[:3], addresses[:3], strict=True):
        client.emit(
            DbusSignal(
                path="/",
                interface="org.freedesktop.DBus.ObjectManager",
                member="InterfacesAdded",
                body=(path, {DEVICE_INTERFACE: _device_properties(Address=address)}),
            )
        )
    await _wait_for_count(observations, 3)

    client.emit(
        DbusSignal(
            path=paths[0],
            interface="org.freedesktop.DBus.Properties",
            member="PropertiesChanged",
            body=(DEVICE_INTERFACE, {"RSSI": -50}, ()),
        )
    )
    client.emit(
        DbusSignal(
            path="/",
            interface="org.freedesktop.DBus.ObjectManager",
            member="InterfacesAdded",
            body=(
                paths[3],
                {DEVICE_INTERFACE: _device_properties(Address=addresses[3], RSSI=-45)},
            ),
        )
    )
    await _wait_for_count(observations, 4)
    await _cancel(task)

    assert [item.address for item in observations] == list(addresses)


async def test_cancellation_unregisters_signals_and_closes_client() -> None:
    client = FakeBluezClient()
    task, _observations = await _start_observer(client)

    await _cancel(task)

    assert client.handlers == {}
    assert client.close_count == 1
    assert client.method_calls == [
        "Properties.Get:org.bluez.Adapter1.Address",
        "AddMatch",
        "AddMatch",
        "AddMatch",
        "GetManagedObjects",
        "RemoveMatch",
        "RemoveMatch",
        "RemoveMatch",
    ]


async def test_connect_failure_still_closes_client_without_calling_methods() -> None:
    client = FakeBluezClient(connect_error=ConnectionError("system bus unavailable"))
    observer = BluezObserver(
        adapter="hci0",
        allowlist=(AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),),
        client_factory=lambda: client,
    )

    async def on_observation(_observation: Observation) -> None:
        pass

    with pytest.raises(ConnectionError, match="system bus unavailable"):
        await observer.observe_once(on_observation)

    assert client.close_count == 1
    assert client.method_calls == []


async def test_close_failure_does_not_mask_observer_cancellation() -> None:
    client = FakeBluezClient(close_error=RuntimeError("close failed"))
    task, _observations = await _start_observer(client)

    await _cancel(task)

    assert client.close_count == 1


async def test_reconnect_backoff_rebuilds_passive_session_only() -> None:
    first = FakeBluezClient(disconnect_error=ConnectionError("bluez restarted"))
    second = FakeBluezClient()
    clients = iter((first, second))
    sleeps: list[float] = []

    async def sleep(seconds: float) -> None:
        sleeps.append(seconds)
        await asyncio.sleep(0)

    async def on_observation(_observation: Observation) -> None:
        pass

    observer = BluezObserver(
        adapter="hci0",
        allowlist=(AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),),
        client_factory=lambda: next(clients),
        sleep=sleep,
        reconnect_backoff=2.0,
    )
    task = asyncio.create_task(observer.run(on_observation))
    await asyncio.wait_for(second.matches_ready.wait(), timeout=1)
    await _cancel(task)

    assert sleeps == [2.0]
    assert first.close_count == second.close_count == 1
    allowed = {
        "Properties.Get:org.bluez.Adapter1.Address",
        "GetManagedObjects",
        "AddMatch",
        "RemoveMatch",
    }
    assert set(first.method_calls + second.method_calls) <= allowed
    forbidden = {
        "StartDiscovery",
        "StopDiscovery",
        "Connect",
        "Pair",
        "RemoveDevice",
        "SetDiscoveryFilter",
        "Properties.Set:org.bluez.Adapter1.Powered",
        "system",
        "sysfs",
    }
    assert forbidden.isdisjoint(first.method_calls + second.method_calls)
