"""Tests: Bridge records reconciled-var commands into DesiredState (Task 3)."""

from __future__ import annotations

import json
import logging
from dataclasses import replace
from pathlib import Path
from typing import cast

import pytest

from brilliant_mqtt.bridge import Bridge, WriteThrottle
from brilliant_mqtt.commands import VarSet
from brilliant_mqtt.desired_state import DesiredState
from brilliant_mqtt.discovery import state_topic
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.write_admission import (
    AdmissionTicket,
    CommandAdmission,
    WriteClass,
    command_admission,
)
from tests.fakes import FakeBus, FakeClock, FakeMqtt


def _vs(name: str, value: str) -> VarSet:
    return VarSet(name, value)


def _mesh_light(pid: str, **vars_: str) -> BrilliantDevice:
    return BrilliantDevice(
        device_id="ble_mesh",
        peripheral_id=pid,
        name=pid,
        kind=DeviceKind.LIGHT,
        peripheral_type=27,
        variables={k: Variable(k, v) for k, v in vars_.items()},
    )


async def _enforce(bridge: Bridge, devices: list[BrilliantDevice]) -> None:
    """Deliver real native inventory before exercising the private enforcement step."""
    bus = cast(FakeBus, bridge._bus)
    if bridge._include is None or bridge._include("ble_mesh"):
        registry = bridge.mesh_registry
        assert registry is not None
        bus._clock = bridge._clock
        registry.acquire()
        bus._devices = list(devices)
        await bus.get_all()
    await bridge._enforce_desired(devices)


async def test_enforcement_cannot_create_mesh_owner_from_unscoped_snapshot(tmp_path: Path) -> None:
    device = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([device])
    desired = DesiredState(tmp_path / "mesh.json")
    desired.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=desired)
    assert bridge.mesh_registry is not None
    bridge.mesh_registry.acquire()
    await bridge._enforce_desired([device])
    assert bus.commands == []
    assert bridge.mesh_registry.current("pidA") is None


@pytest.mark.parametrize(("native", "stale", "writes"), [("0", "1", 1), ("1", "0", 0)])
async def test_enforcement_uses_current_owner_instead_of_supplied_stale_values(
    tmp_path: Path, native: str, stale: str, writes: int
) -> None:
    device = _mesh_light("pidA", enable_motion_score=native, on="0")
    bus = FakeBus([device])
    desired = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=desired)
    await bridge.reconcile()
    desired.record("pidA", "enable_motion_score", "1")

    await bridge._enforce_desired([_mesh_light("pidA", enable_motion_score=stale, on="0")])

    assert len(bus.commands) == writes
    if writes:
        assert bus.commands == [("ble_mesh", "pidA", [_vs("enable_motion_score", "1")])]


async def test_mesh_command_uses_owner_after_metadata_cache_loss(tmp_path: Path) -> None:
    device = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([device])
    mqtt = FakeMqtt()
    desired = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, mqtt, "mesh", desired=desired)
    await bridge.reconcile()
    assert bridge.mesh_registry is not None
    owner = bridge.mesh_registry.current("pidA")
    assert owner is not None
    bridge._devices.clear()

    await mqtt.inject("brilliant/mesh/pidA/set_enable_motion_score", "ON")

    assert bus.commands == [("ble_mesh", "pidA", [_vs("enable_motion_score", "1")])]
    assert desired.wanted("pidA") == {"enable_motion_score": "1"}
    assert owner.materialize(bridge._clock()).variables["enable_motion_score"].value == "1"


async def test_stale_mesh_aux_command_cannot_record_intent_after_owner_loss(tmp_path: Path) -> None:
    device = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([device])
    desired = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=desired)
    await bridge.reconcile()
    assert bridge.mesh_registry is not None
    owner = bridge.mesh_registry.current("pidA")
    assert owner is not None
    bus._devices.clear()
    await bus.get_all()
    token = command_admission.set(CommandAdmission(mesh_owner=owner))
    try:
        await bridge._on_command("brilliant/mesh/pidA/set_enable_motion_score", "ON")
    finally:
        command_admission.reset(token)
    assert bus.commands == []
    assert desired.wanted("pidA") == {}


@pytest.mark.asyncio
async def test_command_records_reconciled_var(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", motion_low_threshold="20", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds)
    await bridge.reconcile()  # registers command topics + snapshots device

    await mqtt.inject("brilliant/mesh/pidA/set_enable_motion_score", "ON")

    assert ds.wanted("pidA") == {"enable_motion_score": "1"}


@pytest.mark.asyncio
async def test_command_ignores_non_reconciled_var(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds)
    await bridge.reconcile()

    await mqtt.inject("brilliant/mesh/pidA/set_on", '{"state": "ON"}')

    assert ds.wanted("pidA") == {}


@pytest.mark.asyncio
async def test_command_without_desired_does_not_crash(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    bridge = Bridge(bus, mqtt, "mesh", desired=None)
    await bridge.reconcile()

    await mqtt.inject("brilliant/mesh/pidA/set_enable_motion_score", "ON")

    # write still routed to the bus; just nothing recorded
    assert any(vs.name == "enable_motion_score" for _, _, sets in bus.commands for vs in sets)


@pytest.mark.asyncio
async def test_enforce_writes_when_drifted(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, clock=FakeClock())

    await _enforce(bridge, [dev])

    assert bus.commands == [("ble_mesh", "pidA", [_vs("enable_motion_score", "1")])]


@pytest.mark.asyncio
async def test_enforce_no_write_when_matching(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="1", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, clock=FakeClock())

    await _enforce(bridge, [dev])

    assert bus.commands == []


@pytest.mark.asyncio
async def test_enforce_batches_multiple_vars_per_peripheral(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", motion_low_threshold="20", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    ds.record("pidA", "motion_low_threshold", "30")
    # spacing=0.0: this test validates batching (one write per peripheral), not
    # cross-tick rate limiting — disable spacing so it does not interfere.
    bridge = Bridge(
        bus, FakeMqtt(), "mesh", desired=ds, reconcile_min_write_spacing_s=0.0, clock=FakeClock()
    )

    await _enforce(bridge, [dev])

    # exactly ONE set_variables call carrying BOTH drifted vars (no same-peripheral race)
    assert len(bus.commands) == 1
    did, pid, sets = bus.commands[0]
    assert (did, pid) == ("ble_mesh", "pidA")
    assert {vs.name: vs.value for vs in sets} == {
        "enable_motion_score": "1",
        "motion_low_threshold": "30",
    }


@pytest.mark.asyncio
async def test_enforce_rate_limited_per_var(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    clock = FakeClock()
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, reconcile_min_interval_s=60.0, clock=clock)

    await _enforce(bridge, [dev])  # writes
    await _enforce(bridge, [dev])  # within interval -> skipped
    assert len(bus.commands) == 1
    clock.advance(61)
    await _enforce(bridge, [dev])  # interval elapsed -> writes again
    assert len(bus.commands) == 2


@pytest.mark.asyncio
async def test_enforce_per_tick_cap(tmp_path: Path) -> None:
    devs = [_mesh_light(f"pid{i}", enable_motion_score="0", on="0") for i in range(5)]
    bus = FakeBus(devs)
    ds = DesiredState(tmp_path / "mesh.json")
    for d in devs:
        ds.record(d.peripheral_id, "enable_motion_score", "1")
    # spacing=0.0: this test validates the per-tick cap, not cross-tick spacing.
    bridge = Bridge(
        bus,
        FakeMqtt(),
        "mesh",
        desired=ds,
        reconcile_max_writes_per_tick=2,
        reconcile_min_write_spacing_s=0.0,
        clock=FakeClock(),
    )

    await _enforce(bridge, devs)

    assert len(bus.commands) == 2  # capped; remaining catch up on later ticks


@pytest.mark.asyncio
async def test_enforce_noop_when_desired_none() -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=None, clock=FakeClock())

    await _enforce(bridge, [dev])

    assert bus.commands == []


@pytest.mark.asyncio
async def test_enforce_continues_after_write_error(tmp_path: Path) -> None:
    class FlakyBus(FakeBus):
        async def set_variables(
            self,
            device_id: str,
            peripheral_id: str,
            sets: list[VarSet],
            *,
            write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
            ticket: AdmissionTicket | None = None,
        ) -> str:
            if peripheral_id == "pidA":
                raise RuntimeError("bus boom")
            return await super().set_variables(
                device_id, peripheral_id, sets, write_class=write_class, ticket=ticket
            )

    devs = [
        _mesh_light("pidA", enable_motion_score="0", on="0"),
        _mesh_light("pidB", enable_motion_score="0", on="0"),
    ]
    bus = FlakyBus(devs)
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    ds.record("pidB", "enable_motion_score", "1")
    # spacing=0.0: this test validates error recovery across multiple writes,
    # not cross-tick rate limiting — disable spacing so it does not interfere.
    bridge = Bridge(
        bus, FakeMqtt(), "mesh", desired=ds, reconcile_min_write_spacing_s=0.0, clock=FakeClock()
    )

    await _enforce(bridge, devs)  # must not raise

    assert ("ble_mesh", "pidB", [_vs("enable_motion_score", "1")]) in bus.commands
    # The failing write for pidA must not appear in the recorded commands.
    assert all(p != "pidA" for _, p, _ in bus.commands)


@pytest.mark.asyncio
async def test_poll_once_enforces(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, clock=FakeClock())

    assert bridge.mesh_registry is not None
    bridge.mesh_registry.acquire()
    await bridge.poll_once()

    assert ("ble_mesh", "pidA", [_vs("enable_motion_score", "1")]) in bus.commands


@pytest.mark.asyncio
async def test_reconcile_enforces(tmp_path: Path) -> None:
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds, clock=FakeClock())

    await bridge.reconcile()

    assert ("ble_mesh", "pidA", [_vs("enable_motion_score", "1")]) in bus.commands
    assert all(topic != "brilliant/mesh/ownership" for topic, _, _ in mqtt.published)


@pytest.mark.asyncio
async def test_enforce_restores_off_when_drifted_to_on(tmp_path: Path) -> None:
    """Enforce re-asserts OFF, not just ON — drift in either direction is corrected."""
    dev = _mesh_light("pidA", enable_motion_score="1", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "0")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, clock=FakeClock())

    await _enforce(bridge, [dev])

    assert bus.commands == [("ble_mesh", "pidA", [_vs("enable_motion_score", "0")])]


@pytest.mark.asyncio
async def test_enforce_min_write_spacing(tmp_path: Path) -> None:
    """Global min-spacing limits writes to one peripheral per tick across calls."""
    devs = [
        _mesh_light("pidA", enable_motion_score="0", on="0"),
        _mesh_light("pidB", enable_motion_score="0", on="0"),
    ]
    bus = FakeBus(devs)
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    ds.record("pidB", "enable_motion_score", "1")
    clock = FakeClock()
    bridge = Bridge(
        bus,
        FakeMqtt(),
        "mesh",
        desired=ds,
        reconcile_min_write_spacing_s=1.0,
        clock=clock,
    )

    # First call: pidA writes (no previous write), spacing blocks pidB.
    await _enforce(bridge, devs)
    assert len(bus.commands) == 1
    assert bus.commands[0][1] == "pidA"

    # Second call without advancing clock: spacing guard still fires, nothing written.
    await _enforce(bridge, devs)
    assert len(bus.commands) == 1

    # After advancing past the spacing window, pidB (not yet rate-limited) writes.
    clock.advance(1.0)
    await _enforce(bridge, devs)
    assert len(bus.commands) == 2
    assert bus.commands[1][1] == "pidB"


@pytest.mark.asyncio
async def test_enforce_spacing_backs_off_on_write_failure(tmp_path: Path) -> None:
    """A failed write must still advance _throttle.last_ts so a second peripheral
    is not attempted in the same tick (write-spacing backs off even on errors)."""

    class AlwaysFailBus(FakeBus):
        def __init__(self, devices: list[BrilliantDevice]) -> None:
            super().__init__(devices)
            self.attempts = 0

        async def set_variables(
            self,
            device_id: str,
            peripheral_id: str,
            sets: list[VarSet],
            *,
            write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
            ticket: AdmissionTicket | None = None,
        ) -> str:
            self.attempts += 1
            raise RuntimeError("bus down")

    devs = [
        _mesh_light("pidA", enable_motion_score="0", on="0"),
        _mesh_light("pidB", enable_motion_score="0", on="0"),
    ]
    bus = AlwaysFailBus(devs)
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    ds.record("pidB", "enable_motion_score", "1")
    bridge = Bridge(
        bus,
        FakeMqtt(),
        "mesh",
        desired=ds,
        reconcile_min_write_spacing_s=1.0,
        clock=FakeClock(),
    )

    await _enforce(bridge, devs)  # must not raise

    # The first write attempt (pidA) raises; _throttle.last_ts is still advanced
    # (set before the try), so the spacing guard blocks pidB in the same tick.
    assert bus.attempts == 1


@pytest.mark.asyncio
async def test_enforce_uses_persisted_desired_after_restart(tmp_path: Path) -> None:
    """Persisted desired state feeds enforcement after a process restart."""
    path = tmp_path / "mesh.json"

    # First "process": record desired and let it persist to disk.
    ds_first = DesiredState(path)
    ds_first.record("pidA", "enable_motion_score", "1")

    # Simulate restart: new DesiredState instance loads from disk.
    ds_loaded = DesiredState(path)
    ds_loaded.load()

    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds_loaded, clock=FakeClock())

    await _enforce(bridge, [dev])

    assert bus.commands == [("ble_mesh", "pidA", [_vs("enable_motion_score", "1")])]


@pytest.mark.asyncio
async def test_enforce_spacing_is_shared_across_bridges(tmp_path: Path) -> None:
    """A WriteThrottle shared between two Bridge instances enforces bus-global
    write-spacing: the second bridge must not write within the spacing window
    after the first bridge wrote."""
    dev_a = _mesh_light("pidA", enable_motion_score="0", on="0")
    dev_b = _mesh_light("pidB", enable_motion_score="0", on="0")

    bus_a = FakeBus([dev_a])
    bus_b = FakeBus([dev_b])

    ds_a = DesiredState(tmp_path / "ds_a.json")
    ds_a.record("pidA", "enable_motion_score", "1")
    ds_b = DesiredState(tmp_path / "ds_b.json")
    ds_b.record("pidB", "enable_motion_score", "1")

    clock = FakeClock()
    throttle = WriteThrottle()

    # Both bridges share one WriteThrottle and one FakeClock (time=0).
    bridge1 = Bridge(
        bus_a,
        FakeMqtt(),
        "panel",
        include=None,
        desired=ds_a,
        reconcile_min_write_spacing_s=1.0,
        clock=clock,
        write_throttle=throttle,
    )
    bridge2 = Bridge(
        bus_b,
        FakeMqtt(),
        "mesh",
        include=None,
        desired=ds_b,
        reconcile_min_write_spacing_s=1.0,
        clock=clock,
        write_throttle=throttle,
    )

    # bridge1 writes pidA; throttle.last_ts is now set.
    await _enforce(bridge1, [dev_a])
    assert len(bus_a.commands) == 1

    # bridge2 must NOT write within the spacing window (clock still at 0).
    await _enforce(bridge2, [dev_b])
    assert len(bus_b.commands) == 0

    # Past the window it DOES write — proving spacing (not scope) was the blocker.
    clock.advance(1.0)
    await _enforce(bridge2, [dev_b])
    assert len(bus_b.commands) == 1


@pytest.mark.asyncio
async def test_enforce_skips_out_of_scope_devices(tmp_path: Path) -> None:
    """The include gate holds inside _enforce_desired: a non-leader mesh bridge
    must never WRITE mesh vars (not just not publish) — two panels writing the
    same mesh device is the exact storm this feature must not create."""
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    in_scope = False
    bridge = Bridge(
        bus, FakeMqtt(), "mesh", include=lambda _did: in_scope, desired=ds, clock=FakeClock()
    )

    await _enforce(bridge, [dev])
    assert bus.commands == []  # not leader -> no bus writes

    in_scope = True
    await _enforce(bridge, [dev])
    assert len(bus.commands) == 1  # leadership acquired -> enforcement active


@pytest.mark.asyncio
async def test_enforce_all_drifted_converge_across_ticks(tmp_path: Path) -> None:
    """The PR's deploy scenario: more drifted peripherals than one tick may
    write (leader restart) — every device converges across ticks, none twice."""
    devs = [_mesh_light(f"pid{i}", enable_motion_score="0", on="0") for i in range(5)]
    bus = FakeBus(devs)
    ds = DesiredState(tmp_path / "mesh.json")
    for d in devs:
        ds.record(d.peripheral_id, "enable_motion_score", "1")
    clock = FakeClock()
    bridge = Bridge(
        bus,
        FakeMqtt(),
        "mesh",
        desired=ds,
        reconcile_max_writes_per_tick=2,
        reconcile_min_write_spacing_s=0.0,
        reconcile_min_interval_s=60.0,
        clock=clock,
    )

    for _ in range(3):
        await _enforce(bridge, devs)
        clock.advance(1.0)

    written = sorted(pid for _, pid, _ in bus.commands)
    assert written == [f"pid{i}" for i in range(5)]  # all five converge, none twice


@pytest.mark.asyncio
async def test_enforce_retries_failed_write_with_capped_backoff(tmp_path: Path) -> None:
    """Failed retries double their delay up to five minutes and retain intent."""

    class AlwaysFailBus(FakeBus):
        def __init__(self, devices: list[BrilliantDevice]) -> None:
            super().__init__(devices)
            self.attempts = 0

        async def set_variables(
            self,
            device_id: str,
            peripheral_id: str,
            sets: list[VarSet],
            *,
            write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
            ticket: AdmissionTicket | None = None,
        ) -> str:
            self.attempts += 1
            raise RuntimeError("bus down")

    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = AlwaysFailBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    clock = FakeClock()
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, reconcile_min_interval_s=60.0, clock=clock)

    await _enforce(bridge, [dev])
    assert bus.attempts == 1
    await _enforce(bridge, [dev])  # same instant: window already consumed
    assert bus.attempts == 1
    for attempts, interval in enumerate((120.0, 240.0, 300.0, 300.0), start=1):
        clock.advance(interval - 1.0)
        await _enforce(bridge, [dev])
        assert bus.attempts == attempts
        clock.advance(1.0)
        await _enforce(bridge, [dev])
        assert bus.attempts == attempts + 1
        assert ds.wanted("pidA") == {"enable_motion_score": "1"}

    restored = DesiredState(tmp_path / "mesh.json")
    restored.load()
    assert restored.wanted("pidA") == {"enable_motion_score": "1"}


@pytest.mark.asyncio
async def test_enforce_backoff_is_per_variable_and_success_resets_it(tmp_path: Path) -> None:
    class RecordingBus(FakeBus):
        def __init__(self, devices: list[BrilliantDevice]) -> None:
            super().__init__(devices)
            self.attempts: list[list[VarSet]] = []

        async def set_variables(
            self,
            device_id: str,
            peripheral_id: str,
            sets: list[VarSet],
            *,
            write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
            ticket: AdmissionTicket | None = None,
        ) -> str:
            self.attempts.append(list(sets))
            return await super().set_variables(
                device_id, peripheral_id, sets, write_class=write_class, ticket=ticket
            )

    dev = _mesh_light("pidA", enable_motion_score="0", motion_low_threshold="0", on="0")
    bus = RecordingBus([dev])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    clock = FakeClock()
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, reconcile_min_interval_s=60.0, clock=clock)
    motion = _vs("enable_motion_score", "1")
    pir = _vs("motion_low_threshold", "1")

    bus.set_variables_error = RuntimeError("bus down")
    await _enforce(bridge, [dev])
    assert bus.attempts == [[motion]]

    clock.advance(60.0)
    ds.record("pidA", "motion_low_threshold", "1")
    bus.set_variables_error = None
    await _enforce(bridge, [dev])
    assert bus.attempts == [[motion], [pir]]

    clock.advance(59.0)
    await _enforce(bridge, [dev])
    assert len(bus.attempts) == 2
    clock.advance(1.0)
    await _enforce(bridge, [dev])
    assert bus.attempts == [[motion], [pir], [motion, pir]]

    # The successful write restores the base interval for subsequent drift.
    clock.advance(59.0)
    await _enforce(bridge, [dev])
    assert len(bus.attempts) == 3
    clock.advance(1.0)
    bus.set_variables_error = RuntimeError("bus down again")
    await _enforce(bridge, [dev])
    assert bus.attempts == [[motion], [pir], [motion, pir], [motion, pir]]

    # A new failure starts at twice the base, rather than retaining old failures.
    clock.advance(119.0)
    await _enforce(bridge, [dev])
    assert len(bus.attempts) == 4
    clock.advance(1.0)
    bus.set_variables_error = None
    await _enforce(bridge, [dev])
    assert bus.attempts == [[motion], [pir], [motion, pir], [motion, pir], [motion, pir]]
    assert ds.wanted("pidA") == {"enable_motion_score": "1", "motion_low_threshold": "1"}


@pytest.mark.asyncio
async def test_enforce_skips_var_absent_from_snapshot_without_consuming_window(
    tmp_path: Path, caplog: pytest.LogCaptureFixture
) -> None:
    """A desired var the peripheral does not expose is skipped (never written),
    logged once (not every tick), and the skip consumes neither the per-var
    window nor the global write-spacing slot."""
    absent = _mesh_light("pidA", on="0")  # no enable_motion_score at all
    bus = FakeBus([absent])
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, FakeMqtt(), "mesh", desired=ds, clock=FakeClock())

    with caplog.at_level(logging.DEBUG, logger="brilliant_mqtt.bridge"):
        await _enforce(bridge, [absent])
        await _enforce(bridge, [absent])
    assert bus.commands == []
    skip_logs = [r for r in caplog.records if "does not expose" in r.getMessage()]
    assert len(skip_logs) == 1  # once per (pid, var), not per tick

    # Same clock instant: the skip consumed no window — the write happens
    # immediately once the var appears at a drifted value.
    present = _mesh_light("pidA", enable_motion_score="0", on="0")
    await _enforce(bridge, [present])
    assert len(bus.commands) == 1


@pytest.mark.asyncio
async def test_command_recorded_without_snapshot_then_enforced_later(tmp_path: Path) -> None:
    """Intent recorded for an undeliverable command (no snapshot) is delivered
    by the reconciler once the peripheral reappears — record-before-guard in
    _handle_aux_command is deliberate and load-bearing."""
    dev = replace(_mesh_light("pidA", enable_motion_score="0", on="0"), device_id="panel")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds, clock=FakeClock())
    await bridge.reconcile()  # registers topics + snapshots device

    # Simulate the snapshot vanishing while the command topic stays registered
    # (private poke: there is no public path to force this interleaving).
    bridge._devices.clear()
    await mqtt.inject("brilliant/mesh/pidA/set_enable_motion_score", "ON")
    assert bus.commands == []  # the immediate write was undeliverable...
    assert ds.wanted("pidA") == {"enable_motion_score": "1"}  # ...but intent kept

    await bridge.poll_once()  # snapshot returns; the reconciler delivers
    assert ("panel", "pidA", [_vs("enable_motion_score", "1")]) in bus.commands


@pytest.mark.asyncio
async def test_enforce_echoes_reasserted_state_to_mqtt(tmp_path: Path) -> None:
    """A successful re-assert folds into the snapshot and republishes state —
    HA must not show the firmware's reverted value for a whole poll interval
    (phantom OFF events in history/automations on every revert cycle)."""
    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = FakeBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds, clock=FakeClock())

    await bridge.reconcile()  # snapshot + state publish, then enforce + echo

    assert len(bus.commands) == 1
    states = [p for t, p, _ in mqtt.published if t == state_topic("mesh", "pidA")]
    assert states, "expected state publishes for pidA"
    assert json.loads(states[-1])["enable_motion_score"] is True


@pytest.mark.asyncio
async def test_enforce_failed_write_does_not_echo(tmp_path: Path) -> None:
    """A failed re-assert must NOT pretend it worked — no optimistic state."""

    class AlwaysFailBus(FakeBus):
        async def set_variables(
            self,
            device_id: str,
            peripheral_id: str,
            sets: list[VarSet],
            *,
            write_class: WriteClass = WriteClass.INTERACTIVE_FIFO,
            ticket: AdmissionTicket | None = None,
        ) -> str:
            raise RuntimeError("bus down")

    dev = _mesh_light("pidA", enable_motion_score="0", on="0")
    bus = AlwaysFailBus([dev])
    mqtt = FakeMqtt()
    ds = DesiredState(tmp_path / "mesh.json")
    ds.record("pidA", "enable_motion_score", "1")
    bridge = Bridge(bus, mqtt, "mesh", desired=ds, clock=FakeClock())

    await bridge.reconcile()

    states = [p for t, p, _ in mqtt.published if t == state_topic("mesh", "pidA")]
    assert states
    assert all(json.loads(p)["enable_motion_score"] is False for p in states)
