"""BLE observer supervisor and explicit bounded-probe tests."""

from __future__ import annotations

import asyncio
import signal
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import NoReturn
from uuid import UUID

import pytest

import brilliant_ble_observer.__main__ as observer_main
from brilliant_ble_observer.bluez import Observation, SystemBluezClient
from brilliant_ble_observer.config import Settings
from brilliant_ble_observer.model import AdvertisementEnvelope, AllowlistEntry
from brilliant_ble_observer.run import read_boot_id, run_probe, run_service

BOOT_ID = "123e4567-e89b-12d3-a456-426614174000"
SESSION_ID = UUID("223e4567-e89b-12d3-a456-426614174000")


def _settings(*, enabled: bool) -> Settings:
    return Settings(
        panel="shed",
        mqtt_host="mqtt.iot.joyful.house",
        mqtt_username="brilliant-shed",
        mqtt_password="not-a-real-password",
        enabled=enabled,
        allowlist=(AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),),
    )


def _observation(*, address: str, rssi: int) -> Observation:
    return Observation(
        adapter_address="11:22:33:44:55:66",
        address=address,
        address_type="random",
        rssi=rssi,
        local_name="Wallet",
        tx_power=-59,
        service_uuids=(),
        service_data={},
        manufacturer_data={},
        capture_monotonic_ms=123_456,
    )


class FakeObserver:
    def __init__(self, observations: tuple[Observation, ...] = ()) -> None:
        self.observations = observations
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.release = asyncio.Event()

    async def run(self, callback: Callable[[Observation], Awaitable[None]]) -> None:
        self.started.set()
        try:
            for observation in self.observations:
                await callback(observation)
            await self.release.wait()
        finally:
            self.stopped.set()


class FailingObserver:
    def __init__(self, publisher_started: asyncio.Event) -> None:
        self.publisher_started = publisher_started

    async def run(self, _callback: Callable[[Observation], Awaitable[None]]) -> None:
        await self.publisher_started.wait()
        raise ConnectionError("BlueZ failed")


class ImmediateFailingObserver:
    async def run(self, _callback: Callable[[Observation], Awaitable[None]]) -> None:
        raise ConnectionError("BlueZ failed immediately")


class FakePublisher:
    def __init__(self) -> None:
        self.envelopes: list[AdvertisementEnvelope] = []
        self.started = asyncio.Event()
        self.stopped = asyncio.Event()
        self.release = asyncio.Event()

    def enqueue(self, advertisement: AdvertisementEnvelope) -> bool:
        self.envelopes.append(advertisement)
        return True

    async def run(self) -> None:
        self.started.set()
        try:
            await self.release.wait()
        finally:
            self.stopped.set()


class FakeProbeClient:
    def __init__(
        self,
        *,
        connect_error: Exception | None = None,
        block_start: bool = False,
        stop_error: Exception | None = None,
    ) -> None:
        self.connect_error = connect_error
        self.block_start = block_start
        self.stop_error = stop_error
        self.calls: list[str] = []
        self.started = asyncio.Event()
        self.release_start = asyncio.Event()

    async def connect(self) -> None:
        self.calls.append("connect")
        if self.connect_error is not None:
            raise self.connect_error

    async def start_discovery(self, adapter_path: str) -> None:
        self.calls.append(f"StartDiscovery:{adapter_path}")
        self.started.set()
        if self.block_start:
            await self.release_start.wait()

    async def stop_discovery(self, adapter_path: str) -> None:
        self.calls.append(f"StopDiscovery:{adapter_path}")
        if self.stop_error is not None:
            raise self.stop_error

    async def close(self) -> None:
        self.calls.append("close")


async def _cancel(task: asyncio.Task[None]) -> None:
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


async def _wait_for_envelopes(publisher: FakePublisher, count: int) -> None:
    for _attempt in range(20):
        if len(publisher.envelopes) == count:
            return
        await asyncio.sleep(0)
    raise AssertionError(f"expected {count} envelopes, got {len(publisher.envelopes)}")


async def test_disabled_service_opens_neither_dbus_nor_mqtt() -> None:
    calls: list[str] = []

    def forbidden(name: str) -> NoReturn:
        calls.append(name)
        raise AssertionError(f"disabled service called {name}")

    await run_service(
        _settings(enabled=False),
        observer_factory=lambda _settings: forbidden("D-Bus factory"),
        publisher_factory=lambda _settings: forbidden("MQTT factory"),
        boot_id_reader=lambda: forbidden("boot ID"),
        session_id_factory=lambda: forbidden("session ID"),
    )

    assert calls == []


def test_boot_id_is_read_from_supplied_proc_path(tmp_path: Path) -> None:
    boot_id_path = tmp_path / "boot_id"
    boot_id_path.write_text(f"{BOOT_ID}\n", encoding="ascii")

    assert read_boot_id(boot_id_path) == BOOT_ID

    boot_id_path.write_text("not-a-uuid\n", encoding="ascii")
    with pytest.raises(ValueError, match="boot ID"):
        read_boot_id(boot_id_path)


async def test_process_session_is_generated_once_and_sequence_follows_filtering() -> None:
    observer = FakeObserver(
        (
            _observation(address="AA:BB:CC:DD:EE:01", rssi=-90),
            _observation(address="AA:BB:CC:DD:EE:FF", rssi=-61),
            _observation(address="AA:BB:CC:DD:EE:FF", rssi=-60),
        )
    )
    publisher = FakePublisher()
    generated: list[UUID] = []

    def session_id_factory() -> UUID:
        generated.append(SESSION_ID)
        return SESSION_ID

    task = asyncio.create_task(
        run_service(
            _settings(enabled=True),
            observer_factory=lambda _settings: observer,
            publisher_factory=lambda _settings: publisher,
            boot_id_reader=lambda: BOOT_ID,
            session_id_factory=session_id_factory,
        )
    )
    await _wait_for_envelopes(publisher, 2)
    await _cancel(task)

    assert generated == [SESSION_ID]
    assert [envelope.sequence for envelope in publisher.envelopes] == [1, 2]
    assert {envelope.session_id for envelope in publisher.envelopes} == {str(SESSION_ID)}
    assert {envelope.boot_id for envelope in publisher.envelopes} == {BOOT_ID}
    assert observer.stopped.is_set()
    assert publisher.stopped.is_set()


async def test_child_failure_cancels_peer_and_propagates() -> None:
    publisher = FakePublisher()
    observer = FailingObserver(publisher.started)

    with pytest.raises(ConnectionError, match="BlueZ failed"):
        await run_service(
            _settings(enabled=True),
            observer_factory=lambda _settings: observer,
            publisher_factory=lambda _settings: publisher,
            boot_id_reader=lambda: BOOT_ID,
            session_id_factory=lambda: SESSION_ID,
        )

    assert publisher.stopped.is_set()


async def test_child_failure_wins_race_with_requested_stop() -> None:
    publisher = FakePublisher()
    stop_event = asyncio.Event()
    stop_event.set()

    with pytest.raises(ConnectionError, match="failed immediately"):
        await run_service(
            _settings(enabled=True),
            observer_factory=lambda _settings: ImmediateFailingObserver(),
            publisher_factory=lambda _settings: publisher,
            boot_id_reader=lambda: BOOT_ID,
            session_id_factory=lambda: SESSION_ID,
            stop_event=stop_event,
        )

    assert publisher.stopped.is_set()


async def test_stop_event_cancels_both_children_cleanly() -> None:
    observer = FakeObserver()
    publisher = FakePublisher()
    stop_event = asyncio.Event()
    task = asyncio.create_task(
        run_service(
            _settings(enabled=True),
            observer_factory=lambda _settings: observer,
            publisher_factory=lambda _settings: publisher,
            boot_id_reader=lambda: BOOT_ID,
            session_id_factory=lambda: SESSION_ID,
            stop_event=stop_event,
        )
    )
    await asyncio.wait_for(observer.started.wait(), timeout=1)
    await asyncio.wait_for(publisher.started.wait(), timeout=1)

    stop_event.set()
    await asyncio.wait_for(task, timeout=1)

    assert observer.stopped.is_set()
    assert publisher.stopped.is_set()


@pytest.mark.parametrize(("seconds", "expected"), ((1, 5.0), (30, 30.0), (99, 60.0)))
async def test_probe_clamps_duration_and_has_one_balanced_discovery_session(
    seconds: int, expected: float
) -> None:
    client = FakeProbeClient()
    sleeps: list[float] = []

    async def sleep(duration: float) -> None:
        sleeps.append(duration)

    await run_probe(
        adapter="hci2",
        seconds=seconds,
        client_factory=lambda: client,
        sleep=sleep,
    )

    assert sleeps == [expected]
    assert client.calls == [
        "connect",
        "StartDiscovery:/org/bluez/hci2",
        "StopDiscovery:/org/bluez/hci2",
        "close",
    ]


async def test_probe_cancellation_stops_discovery_in_finally() -> None:
    client = FakeProbeClient()

    async def sleep_forever(_duration: float) -> None:
        await asyncio.Event().wait()

    task = asyncio.create_task(
        run_probe(
            adapter="hci0",
            seconds=10,
            client_factory=lambda: client,
            sleep=sleep_forever,
        )
    )
    await asyncio.wait_for(client.started.wait(), timeout=1)
    await _cancel(task)

    assert client.calls == [
        "connect",
        "StartDiscovery:/org/bluez/hci0",
        "StopDiscovery:/org/bluez/hci0",
        "close",
    ]


async def test_probe_cancellation_during_start_still_attempts_balanced_stop() -> None:
    client = FakeProbeClient(block_start=True)
    task = asyncio.create_task(run_probe(adapter="hci0", seconds=10, client_factory=lambda: client))
    await asyncio.wait_for(client.started.wait(), timeout=1)

    await _cancel(task)

    assert client.calls == [
        "connect",
        "StartDiscovery:/org/bluez/hci0",
        "StopDiscovery:/org/bluez/hci0",
        "close",
    ]


async def test_probe_exception_between_start_and_stop_still_cleans_up() -> None:
    client = FakeProbeClient()

    async def fail(_duration: float) -> None:
        raise RuntimeError("probe interrupted")

    with pytest.raises(RuntimeError, match="probe interrupted"):
        await run_probe(
            adapter="hci0",
            seconds=10,
            client_factory=lambda: client,
            sleep=fail,
        )

    assert client.calls[-2:] == ["StopDiscovery:/org/bluez/hci0", "close"]


async def test_probe_cleanup_failure_does_not_mask_primary_failure() -> None:
    client = FakeProbeClient(stop_error=RuntimeError("stop failed"))

    async def fail(_duration: float) -> None:
        raise RuntimeError("probe interrupted")

    with pytest.raises(RuntimeError, match="probe interrupted"):
        await run_probe(
            adapter="hci0",
            seconds=10,
            client_factory=lambda: client,
            sleep=fail,
        )

    assert client.calls[-2:] == ["StopDiscovery:/org/bluez/hci0", "close"]


async def test_probe_connect_failure_still_closes_without_starting() -> None:
    client = FakeProbeClient(connect_error=ConnectionError("system bus unavailable"))

    with pytest.raises(ConnectionError, match="system bus unavailable"):
        await run_probe(adapter="hci0", seconds=10, client_factory=lambda: client)

    assert client.calls == ["connect", "close"]


def test_steady_state_system_client_has_no_generic_mutation_method() -> None:
    assert not hasattr(SystemBluezClient, "call_adapter_method")


async def test_sigterm_wrapper_sets_stop_and_removes_handler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class FakeLoop:
        def __init__(self) -> None:
            self.handlers: dict[signal.Signals, Callable[[], None]] = {}
            self.removed: list[signal.Signals] = []

        def add_signal_handler(
            self, selected_signal: signal.Signals, callback: Callable[[], None]
        ) -> None:
            self.handlers[selected_signal] = callback

        def remove_signal_handler(self, selected_signal: signal.Signals) -> bool:
            self.removed.append(selected_signal)
            return True

    loop = FakeLoop()

    async def fake_run_service(
        _settings: Settings, *, stop_event: asyncio.Event | None = None
    ) -> None:
        assert stop_event is not None
        loop.handlers[signal.SIGTERM]()
        await stop_event.wait()

    monkeypatch.setattr(observer_main, "_get_running_loop", lambda: loop)
    monkeypatch.setattr(observer_main, "run_service", fake_run_service)

    await observer_main.run_service_with_signals(_settings(enabled=True))

    assert signal.SIGTERM in loop.removed
