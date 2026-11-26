"""One effect worker joins each obsolete publication before attempting another."""

from __future__ import annotations

import asyncio
from typing import cast

import pytest

from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.mesh_driver import MeshDriver
from brilliant_mqtt.mesh_registry import MeshRegistry
from brilliant_mqtt.mesh_settlement_types import Begin, RequestPublication
from tests.fakes import FakeMqtt, FakeSleeper, _RawDevice
from tests.mesh_regression_support import turns
from tests.test_mesh_registry import registry_rig as registry_rig


async def test_latest_owner_publication_and_identical_capture_suppression(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)
    mqtt = FakeMqtt()
    driver = MeshDriver(registry, mqtt, panel="mesh", sleep=FakeSleeper())
    try:
        registry.acquire()
        await adapter.get_all()
        owner = registry.current("load")
        assert owner is not None
        await driver.flush(owner)
        assert len(mqtt.published) == 1
        await adapter.get_all()
        await driver.flush(owner)
        assert len(mqtt.published) == 1
        envelope = registry.reserve(owner, "reconcile")
        assert envelope is not None
        registry.dispatch(owner, RequestPublication(envelope))
        await driver.flush(owner, force=True)
        assert len(mqtt.published) == 2
        assert owner.view.in_flight is None
    finally:
        registry.close()
        await driver.shutdown()


async def test_obsolete_child_cancelled_before_native_gate_release(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)

    class GatedMqtt(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.started = asyncio.Event()
            self.release = asyncio.Event()
            self.cancelled = False

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            self.started.set()
            try:
                await self.release.wait()
            except asyncio.CancelledError:
                self.cancelled = True
                raise
            await super().publish(topic, payload, retain, qos)

    mqtt = GatedMqtt()
    driver = MeshDriver(registry, mqtt, panel="mesh", sleep=FakeSleeper())
    try:
        registry.acquire()
        await adapter.get_all()
        await mqtt.started.wait()
        owner = registry.current("load")
        assert owner is not None
        envelope = registry.reserve(owner, "command")
        assert envelope is not None
        registry.dispatch(owner, Begin(envelope, object(), (("on", "0"),)))
        await turns()
        assert mqtt.cancelled
        registry.close()
        await driver.shutdown()
        mqtt.release.set()
        await turns()
        assert mqtt.published == []
        assert driver.tasks == ()
    finally:
        mqtt.release.set()
        registry.close()
        await driver.shutdown()


async def test_noncooperative_child_latches_existing_adapter_fatal_and_never_competes(
    registry_rig: tuple[MeshRegistry, _RawDevice],
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from brilliant_mqtt import mesh_driver

    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)
    monkeypatch.setattr(mesh_driver, "_SHUTDOWN_WORKER_SETTLE_S", 0)

    class Noncooperative(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.calls = 0
            self.release = asyncio.Event()

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            self.calls += 1
            while not self.release.is_set():
                try:
                    await self.release.wait()
                except asyncio.CancelledError:
                    continue

    mqtt = Noncooperative()
    driver = MeshDriver(registry, mqtt, panel="mesh", sleep=FakeSleeper())
    try:
        registry.acquire()
        await adapter.get_all()
        await turns()
        registry.close()
        await driver.shutdown()
        assert not adapter.mesh_admission_open
        assert adapter.consume_mesh_fatal()
        assert mqtt.calls == 1
        registry.acquire()
        assert not registry.enabled
    finally:
        mqtt.release.set()
        await turns()
        await driver.shutdown()


async def test_publish_failure_waits_for_existing_external_trigger(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)

    class Failing(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            self.attempts += 1
            raise RuntimeError("broker unavailable")

    mqtt = Failing()
    driver = MeshDriver(registry, mqtt, panel="mesh")
    try:
        registry.acquire()
        await adapter.get_all()
        await turns(40)
        assert mqtt.attempts == 1
        await adapter.get_all()
        await turns(40)
        assert mqtt.attempts == 2
    finally:
        registry.close()
        await driver.shutdown()


async def test_prestart_worker_cancellation_releases_old_lanes(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)
    driver = MeshDriver(registry, FakeMqtt(), panel="mesh")
    try:
        registry.acquire()
        await adapter.get_all()
        for _ in range(6):
            adapter._on_proc_reconnect()
        await turns()
        assert len(driver._lanes) == 1
        assert all(owner.active for owner in driver._lanes)
    finally:
        registry.close()
        await driver.shutdown()


async def test_failed_forced_same_payload_is_repaired_by_next_capture(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, _ = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)

    class Intermittent(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.attempts = 0

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            self.attempts += 1
            if self.attempts == 2:
                raise RuntimeError("forced repair failed")
            await super().publish(topic, payload, retain, qos)

    mqtt = Intermittent()
    driver = MeshDriver(registry, mqtt, panel="mesh")
    try:
        registry.acquire()
        await adapter.get_all()
        owner = registry.current("load")
        assert owner is not None
        await driver.flush(owner)
        envelope = registry.reserve(owner, "reconcile")
        assert envelope is not None
        registry.dispatch(owner, RequestPublication(envelope))
        await driver.flush(owner, force=True)
        await turns()
        assert mqtt.attempts == 2
        await adapter.get_all()
        await driver.flush(owner)
        assert mqtt.attempts == 3
        assert len(mqtt.published) == 2
    finally:
        registry.close()
        await driver.shutdown()


async def test_reacquisition_joins_obsolete_child_before_new_owner_attempt(
    registry_rig: tuple[MeshRegistry, _RawDevice],
) -> None:
    registry, raw = registry_rig
    adapter = cast(RpcBusAdapter, registry.bus)

    class SlowCancellation(FakeMqtt):
        def __init__(self) -> None:
            super().__init__()
            self.release = asyncio.Event()
            self.calls = 0
            self.active = 0
            self.peak = 0

        async def publish(
            self, topic: str, payload: str, retain: bool = False, qos: int = 0
        ) -> None:
            self.calls += 1
            self.active += 1
            self.peak = max(self.peak, self.active)
            try:
                while not self.release.is_set():
                    try:
                        await self.release.wait()
                    except asyncio.CancelledError:
                        continue
            finally:
                self.active -= 1

    mqtt = SlowCancellation()
    driver = MeshDriver(registry, mqtt, panel="mesh")
    try:
        registry.acquire()
        await adapter.get_all()
        await turns()
        assert mqtt.calls == 1
        old = registry.current("load")
        registry.close()
        registry.acquire()
        adapter._dispatch_raw_device(raw)
        new = registry.current("load")
        assert new is not None and new is not old
        await turns(40)
        assert mqtt.calls == 1
        mqtt.release.set()
        await turns(60)
        assert mqtt.calls == 2
        assert mqtt.peak == 1
        assert adapter.mesh_admission_open
    finally:
        mqtt.release.set()
        registry.close()
        await driver.shutdown()
