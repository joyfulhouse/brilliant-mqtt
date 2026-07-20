"""Process-lifetime supervision for the Brilliant BLE observer."""

from __future__ import annotations

import asyncio
import logging
import math
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Protocol
from uuid import UUID, uuid4

from .bluez import BluezObserver, Observation, SystemBluezProbeClient
from .config import Settings, normalize_adapter
from .model import MAX_COUNTER, AdvertisementEnvelope, matches_allowlist, normalize_uuid
from .mqtt import AdvertisementPublisher

BOOT_ID_PATH = Path("/proc/sys/kernel/random/boot_id")
MIN_PROBE_SECONDS = 5.0
MAX_PROBE_SECONDS = 60.0

_LOG = logging.getLogger(__name__)


class ObservationSource(Protocol):
    """Long-running normalized observation source."""

    async def run(self, callback: Callable[[Observation], Awaitable[None]]) -> None: ...


class ObservationSink(Protocol):
    """Long-running bounded advertisement sink."""

    def enqueue(self, advertisement: AdvertisementEnvelope) -> bool: ...

    async def run(self) -> None: ...


class ProbeClient(Protocol):
    """The probe's complete mutation surface: one balanced discovery session."""

    async def connect(self) -> None: ...

    async def start_discovery(self, adapter_path: str) -> None: ...

    async def stop_discovery(self, adapter_path: str) -> None: ...

    async def close(self) -> None: ...


ObserverFactory = Callable[[Settings], ObservationSource]
PublisherFactory = Callable[[Settings], ObservationSink]
ProbeClientFactory = Callable[[], ProbeClient]
Sleep = Callable[[float], Awaitable[None]]


def read_boot_id(path: Path = BOOT_ID_PATH) -> str:
    """Read and validate the current Linux boot identifier."""
    raw = path.read_text(encoding="ascii").strip()
    return normalize_uuid(raw, field_name="kernel boot ID")


def _default_observer(settings: Settings) -> ObservationSource:
    return BluezObserver(adapter=settings.adapter, allowlist=settings.allowlist)


def _default_publisher(settings: Settings) -> ObservationSink:
    return AdvertisementPublisher(settings)


async def run_service(
    settings: Settings,
    *,
    observer_factory: ObserverFactory = _default_observer,
    publisher_factory: PublisherFactory = _default_publisher,
    boot_id_reader: Callable[[], str] = read_boot_id,
    session_id_factory: Callable[[], UUID] = uuid4,
    stop_event: asyncio.Event | None = None,
) -> None:
    """Run the passive observer and publisher until stopped or either fails."""
    if not settings.enabled:
        return

    boot_id = boot_id_reader()
    session_id = str(session_id_factory())
    observer = observer_factory(settings)
    publisher = publisher_factory(settings)
    sequence = 0

    async def on_observation(observation: Observation) -> None:
        nonlocal sequence
        if not matches_allowlist(
            address=observation.address,
            manufacturer_data=observation.manufacturer_data,
            allowlist=settings.allowlist,
        ):
            return
        if sequence == MAX_COUNTER:
            raise RuntimeError("BLE observation sequence exhausted")
        sequence += 1
        publisher.enqueue(
            AdvertisementEnvelope(
                panel=settings.panel,
                adapter_address=observation.adapter_address,
                boot_id=boot_id,
                session_id=session_id,
                sequence=sequence,
                address=observation.address,
                address_type=observation.address_type,
                rssi=observation.rssi,
                local_name=observation.local_name,
                tx_power=observation.tx_power,
                service_uuids=observation.service_uuids,
                service_data=observation.service_data,
                manufacturer_data=observation.manufacturer_data,
                capture_monotonic_ms=observation.capture_monotonic_ms,
            )
        )

    observer_task = asyncio.create_task(observer.run(on_observation), name="ble-bluez-observer")
    publisher_task = asyncio.create_task(publisher.run(), name="ble-mqtt-publisher")
    child_tasks = (observer_task, publisher_task)
    stop_task = (
        asyncio.create_task(stop_event.wait(), name="ble-stop-waiter")
        if stop_event is not None
        else None
    )
    wait_tasks: tuple[asyncio.Task[object], ...] = child_tasks
    if stop_task is not None:
        wait_tasks = (*wait_tasks, stop_task)

    try:
        done, _pending = await asyncio.wait(wait_tasks, return_when=asyncio.FIRST_COMPLETED)
        completed_children = tuple(task for task in child_tasks if task in done)
        for task in completed_children:
            if task.cancelled() or task.exception() is not None:
                await task
        if completed_children:
            raise RuntimeError(f"{completed_children[0].get_name()} stopped unexpectedly")
        if stop_task is not None and stop_task in done:
            return
    finally:
        for cleanup_task in wait_tasks:
            cleanup_task.cancel()
        await asyncio.gather(*wait_tasks, return_exceptions=True)


async def _complete_cleanup(operation: Awaitable[None]) -> BaseException | None:
    """Finish one cleanup operation even if its caller is cancelled."""
    cleanup_task = asyncio.ensure_future(operation)
    try:
        await asyncio.shield(cleanup_task)
    except asyncio.CancelledError as cancellation:
        try:
            await cleanup_task
        except BaseException as error:
            return error
        return cancellation
    except BaseException as error:
        return error
    return None


async def _cleanup_probe(
    client: ProbeClient, adapter_path: str, *, stop_discovery: bool
) -> tuple[BaseException, ...]:
    """Balance a probe session and retain every cleanup failure."""
    errors: list[BaseException] = []
    if stop_discovery:
        if error := await _complete_cleanup(client.stop_discovery(adapter_path)):
            errors.append(error)
    if error := await _complete_cleanup(client.close()):
        errors.append(error)
    return tuple(errors)


async def run_probe(
    *,
    adapter: str,
    seconds: float,
    client_factory: ProbeClientFactory = SystemBluezProbeClient,
    sleep: Sleep = asyncio.sleep,
) -> None:
    """Run one explicit 5–60 second BlueZ discovery session for diagnosis."""
    adapter = normalize_adapter(adapter)
    duration = float(seconds)
    if not math.isfinite(duration):
        raise ValueError("probe seconds must be finite")
    duration = min(MAX_PROBE_SECONDS, max(MIN_PROBE_SECONDS, duration))
    adapter_path = f"/org/bluez/{adapter}"
    client = client_factory()
    start_attempted = False
    failure: BaseException | None = None
    try:
        await client.connect()
        start_attempted = True
        await client.start_discovery(adapter_path)
        await sleep(duration)
    except BaseException as primary_error:
        failure = primary_error

    cleanup_errors = await _cleanup_probe(client, adapter_path, stop_discovery=start_attempted)
    if failure is not None:
        for cleanup_error in cleanup_errors:
            _LOG.warning(
                "BLE probe cleanup failed while propagating %s (%s)",
                type(failure).__name__,
                type(cleanup_error).__name__,
            )
        raise failure
    if cleanup_errors:
        raise cleanup_errors[0]
