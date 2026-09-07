"""Issue #98: avoid eager full-device normalization for work that is discarded.

These are operation-count acceptance tests (no panel hardware in the sandbox,
so no production CPU/allocation numbers are claimed — the issue's own audit is
operation-count based too). They drive the REAL ``RpcBusAdapter`` dispatch path
and the REAL ``Bridge`` callbacks off-panel with the synthetic fixture the issue
describes — 40 peripherals × 30 variables — and count how many times the
expensive normalization / ``Variable`` construction actually runs.

Two independent optimizations are proven here:

* Scope pre-filter — a push no registered consumer wants (a mesh push while a
  panel is a mesh standby) is dropped before ANY normalization/allocation.
* Deferred normalization — for a coalescing consumer that supersedes pending
  snapshots, normalization/``Variable`` construction scale with the snapshots
  actually DELIVERED, not with every push received.
"""

from __future__ import annotations

import asyncio
from typing import Any

import pytest

from brilliant_mqtt import bus as bus_mod
from brilliant_mqtt.bridge import Bridge
from brilliant_mqtt.bus import RpcBusAdapter
from brilliant_mqtt.model import BrilliantDevice, Variable
from tests.fakes import FakeMqtt

_MESH_DEVICE_ID = "ble_mesh"
_OWN_DEVICE_ID = "own-device"
_N_PERIPHERALS = 40
_N_VARIABLES = 30


class _RawVar:
    """Duck-typed bus Variable (the ``normalize_peripheral`` contract)."""

    def __init__(self, value: object, *, settable: bool = True, timestamp: object = None) -> None:
        self.value = value
        self.externally_settable = settable
        self.timestamp = timestamp


class _RawPeripheral:
    """Duck-typed bus Peripheral."""

    def __init__(self, peripheral_type: int, name: str, variables: dict[str, _RawVar]) -> None:
        self.peripheral_type = peripheral_type
        self.name = name
        self.variables = variables


class _RawDevice:
    """Duck-typed bus Device push carrying ``id`` and ``peripherals``."""

    def __init__(self, device_id: str, peripherals: dict[str, _RawPeripheral]) -> None:
        self.id = device_id
        self.peripherals = peripherals


def _raw_snapshot(device_id: str, marker: str) -> _RawDevice:
    """One 40×30 raw device push; every peripheral is a controllable LIGHT.

    LIGHT (peripheral_type 27) with ``on``/``intensity`` means the bridge would
    genuinely publish state for it — so a "zero publications" assertion proves
    the skip, not an empty entity mapping.
    """
    peripherals: dict[str, _RawPeripheral] = {}
    for p in range(_N_PERIPHERALS):
        variables: dict[str, _RawVar] = {
            "on": _RawVar("1"),
            "intensity": _RawVar("500"),
            "dimmable": _RawVar("1"),
            "display_name": _RawVar(f"{marker} light {p}"),
        }
        for v in range(_N_VARIABLES - len(variables)):
            variables[f"var_{v}"] = _RawVar(f"{marker}-{p}-{v}")
        peripherals[f"{device_id}_p{p}"] = _RawPeripheral(27, f"{marker} light {p}", variables)
    return _RawDevice(device_id, peripherals)


class _Counters:
    """Live tallies of the two operations the issue audits."""

    def __init__(self) -> None:
        self.normalizations = 0
        self.variables = 0


def _install_counters(monkeypatch: pytest.MonkeyPatch) -> _Counters:
    """Count real ``normalize_peripheral`` calls and ``Variable`` constructions.

    Both are wrapped where ``bus.py`` looks them up (module globals), so the
    counts reflect exactly the work the dispatch/drain path drives — whether it
    runs eagerly at dispatch or lazily at delivery.
    """
    counters = _Counters()
    real_normalize = bus_mod.normalize_peripheral

    def counting_normalize(device_id: str, peripheral_id: str, raw: Any) -> BrilliantDevice:
        counters.normalizations += 1
        return real_normalize(device_id, peripheral_id, raw)

    def counting_variable(*args: Any, **kwargs: Any) -> Variable:
        counters.variables += 1
        return Variable(*args, **kwargs)

    monkeypatch.setattr(bus_mod, "normalize_peripheral", counting_normalize)
    monkeypatch.setattr(bus_mod, "Variable", counting_variable)
    return counters


class _Leader:
    """Minimal stand-in for the live mesh leader (only ``is_leader`` is read)."""

    def __init__(self, *, is_leader: bool = False) -> None:
        self.is_leader = is_leader


def _mesh_state_publishes(mqtt: FakeMqtt) -> list[str]:
    """Topics published under the mesh namespace (the gated data output)."""
    return [topic for topic, _payload, _retain in mqtt.published if topic.startswith("brilliant/")]


class TestMeshStandbySkipsNormalization:
    """(b) A mesh standby normalizes nothing and publishes nothing.

    The issue's audit: 100 mesh snapshots through both real bridge callbacks
    with the standby predicate produced 4,000 normalizations, 120,000 Variable
    constructions, and zero publications. With the pre-filter the first two
    become zero as well.
    """

    async def test_standby_mesh_does_zero_normalization_and_zero_publishes(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        counters = _install_counters(monkeypatch)
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        mqtt = FakeMqtt()
        leader = _Leader(is_leader=False)

        # The production-equivalent scope predicates, keyed by device id.
        Bridge(adapter, mqtt, "office", include=lambda did: did != _MESH_DEVICE_ID)
        Bridge(
            adapter,
            mqtt,
            "mesh",
            include=lambda did: did == _MESH_DEVICE_ID and leader.is_leader,
        )

        for i in range(100):
            adapter._dispatch_raw_device(_raw_snapshot(_MESH_DEVICE_ID, f"s{i}"))
        await asyncio.gather(*adapter._pending_tasks)

        assert counters.normalizations == 0
        assert counters.variables == 0
        assert mqtt.published == []


class TestLeadershipTransitionAdmitsFirstPush:
    """(c) The first push after leadership is acquired is admitted immediately.

    The want_device predicate closes over the SAME live leader object, so no
    stale leadership snapshot can drop the push that leadership was won to
    deliver.
    """

    async def test_first_push_after_acquire_is_normalized_and_published(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        counters = _install_counters(monkeypatch)
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        mqtt = FakeMqtt()
        leader = _Leader(is_leader=False)

        Bridge(
            adapter,
            mqtt,
            "mesh",
            include=lambda did: did == _MESH_DEVICE_ID and leader.is_leader,
        )

        # Standby: the mesh push is dropped before normalization.
        adapter._dispatch_raw_device(_raw_snapshot(_MESH_DEVICE_ID, "standby"))
        await asyncio.gather(*adapter._pending_tasks)
        assert counters.normalizations == 0
        assert _mesh_state_publishes(mqtt) == []

        # Leadership acquired: the VERY NEXT push must be admitted and delivered.
        leader.is_leader = True
        adapter._dispatch_raw_device(_raw_snapshot(_MESH_DEVICE_ID, "leader"))
        await asyncio.gather(*adapter._pending_tasks)

        assert counters.normalizations == _N_PERIPHERALS
        assert counters.variables == _N_PERIPHERALS * _N_VARIABLES
        assert len(_mesh_state_publishes(mqtt)) == _N_PERIPHERALS


async def _block_on_first(started: asyncio.Event, release: asyncio.Event, seen: list[str]) -> Any:
    """A coalescing consumer that blocks after its FIRST peripheral callback."""

    async def consumer(device: BrilliantDevice) -> None:
        seen.append(device.variables["on"].value)
        if len(seen) == 1:
            started.set()
            await release.wait()

    return consumer


class TestSupersededSnapshotsSkipNormalization:
    """(a) A coalescing consumer's superseded snapshots never get normalized.

    The issue's audit: with the consumer blocked after its first snapshot and
    100 further snapshots injected, only the first and final snapshots reach the
    consumer (80 peripheral callbacks) yet 4,040 peripheral normalizations and
    121,200 Variable constructions happened — the 99 discarded snapshots paid
    3,960 normalizations and 118,800 Variable constructions for nothing.

    With normalization deferred to delivery, both counts scale with DELIVERED
    snapshots (the first + the final = 80 peripherals).
    """

    async def test_normalization_scales_with_delivered_not_received(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        counters = _install_counters(monkeypatch)
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        started = asyncio.Event()
        release = asyncio.Event()
        seen: list[str] = []
        adapter.on_change(await _block_on_first(started, release, seen))

        adapter._dispatch_raw_device(_raw_snapshot(_OWN_DEVICE_ID, "first"))
        await asyncio.wait_for(started.wait(), timeout=1.0)
        # 100 further snapshots arrive while the consumer is blocked: all but the
        # newest are superseded in the coalescing queue.
        for i in range(100):
            adapter._dispatch_raw_device(_raw_snapshot(_OWN_DEVICE_ID, f"super{i}"))
        release.set()
        await asyncio.gather(*adapter._pending_tasks)

        # Only the first and final snapshots are delivered (80 peripherals).
        delivered = 2 * _N_PERIPHERALS
        assert len(seen) == delivered
        assert counters.normalizations == delivered
        assert counters.variables == delivered * _N_VARIABLES


class TestLosslessStreamUnaffected:
    """(d) The lossless (scene) stream still gets every snapshot, in order.

    Deferral changes WHEN normalization happens, never the order or content a
    lossless consumer sees; every snapshot is delivered exactly once, in arrival
    order, so its normalization count equals the number pushed (none coalesced).
    """

    async def test_lossless_delivers_every_snapshot_in_arrival_order(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        counters = _install_counters(monkeypatch)
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        started = asyncio.Event()
        release = asyncio.Event()
        seen: list[str] = []

        async def consumer(device: BrilliantDevice) -> None:
            seen.append(device.variables["on"].value)
            if len(seen) == 1:
                started.set()
                await release.wait()

        adapter.on_change(consumer, coalesce_pushes=False)

        # Single-peripheral pushes with distinct values, queued behind the block.
        def push(value: str) -> None:
            adapter._dispatch_raw_device(
                _RawDevice(
                    _OWN_DEVICE_ID,
                    {"load": _RawPeripheral(27, "Load", {"on": _RawVar(value)})},
                )
            )

        push("0")
        await asyncio.wait_for(started.wait(), timeout=1.0)
        for value in ("1", "2", "3"):
            push(value)
        release.set()
        await asyncio.gather(*adapter._pending_tasks)

        assert seen == ["0", "1", "2", "3"]  # every snapshot, in arrival order
        assert counters.normalizations == 4  # one per delivered snapshot


class TestArrivalSnapshotIsImmutable:
    """Acceptance criterion 3 / poc-findings §8b: the raw push comes from a
    mutable, notification-fed observer mirror, so a deferred consumer must
    normalize from a SAFE synchronous snapshot — never a retained reference to
    the raw struct, which the panel library may mutate in place before delivery.
    """

    async def test_value_mutated_after_dispatch_does_not_reach_the_consumer(
        self,
    ) -> None:
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        started = asyncio.Event()
        release = asyncio.Event()
        seen: list[str] = []

        async def consumer(device: BrilliantDevice) -> None:
            seen.append(device.variables["on"].value)
            if len(seen) == 1:
                started.set()
                await release.wait()

        adapter.on_change(consumer, coalesce_pushes=False)

        # First push occupies the drain and blocks it.
        adapter._dispatch_raw_device(
            _RawDevice(_OWN_DEVICE_ID, {"load": _RawPeripheral(27, "Load", {"on": _RawVar("1")})})
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # Second push: capture its raw variable, then MUTATE it in place after
        # dispatch returns — exactly what the notification-fed mirror can do to a
        # queued reference. Include a mutable bytearray to prove eager decoding.
        mutable = _RawVar("2")
        adapter._dispatch_raw_device(
            _RawDevice(_OWN_DEVICE_ID, {"load": _RawPeripheral(27, "Load", {"on": mutable})})
        )
        mutable.value = bytearray(b"999")
        release.set()
        await asyncio.gather(*adapter._pending_tasks)

        # The consumer must see the value AS IT WAS AT DISPATCH, not the mutation.
        assert seen == ["1", "2"]
