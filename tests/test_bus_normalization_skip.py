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
    """Duck-typed bus Peripheral (``name`` is deliberately loosely typed: the
    real bus delivers a str, but a mutable bytearray name is exercised too)."""

    def __init__(self, peripheral_type: int, name: object, variables: dict[str, _RawVar]) -> None:
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

    async def test_value_name_and_timestamp_survive_raw_mutation_after_dispatch(
        self,
    ) -> None:
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        started = asyncio.Event()
        release = asyncio.Event()
        # (value, name, timestamp_ms) as each delivered device presents them.
        seen: list[tuple[str, str, int | None]] = []

        async def consumer(device: BrilliantDevice) -> None:
            var = device.variables["on"]
            seen.append((var.value, device.name, var.timestamp_ms))
            if len(seen) == 1:
                started.set()
                await release.wait()

        adapter.on_change(consumer, coalesce_pushes=False)

        # First push occupies the drain and blocks it.
        adapter._dispatch_raw_device(
            _RawDevice(
                _OWN_DEVICE_ID,
                {"load": _RawPeripheral(27, "First", {"on": _RawVar("1", timestamp=10)})},
            )
        )
        await asyncio.wait_for(started.wait(), timeout=1.0)

        # Second push: a peripheral with a MUTABLE bytearray name (no display_name
        # var, so the name resolves from it) and a mutable variable. Capture what
        # the snapshot must preserve, then mutate every raw field in place / by
        # reassignment after dispatch — exactly what the notification-fed mirror
        # can do to a queued reference.
        name = bytearray(b"alpha")
        expected_name = str(name)
        var = _RawVar("2", timestamp=111)
        adapter._dispatch_raw_device(
            _RawDevice(_OWN_DEVICE_ID, {"load": _RawPeripheral(27, name, {"on": var})})
        )
        var.value = bytearray(b"999")
        var.timestamp = 222
        name[:] = b"betaXX"
        release.set()
        await asyncio.gather(*adapter._pending_tasks)

        # Every field must be AS IT WAS AT DISPATCH, not the post-dispatch mutation.
        assert seen == [("1", "First", 10), ("2", expected_name, 111)]


class TestSharedNormalizationAcrossConsumers:
    """Fix A: a push delivered to several consumers is normalized once TOTAL.

    Production registers BOTH the panel Bridge (coalescing) and the SceneBridge
    (lossless) on the SAME own device. Deferring normalization to delivery must
    not normalize each delivered peripheral once PER consumer (strictly more
    than main, which shared one BrilliantDevice); the result is memoized on the
    shared per-peripheral holder so it is built at most once and reused.
    """

    async def test_two_consumers_normalize_each_peripheral_once_total(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        counters = _install_counters(monkeypatch)
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        coalescing_seen: list[str] = []
        lossless_seen: list[str] = []

        async def coalescing(device: BrilliantDevice) -> None:
            coalescing_seen.append(device.peripheral_id)

        async def lossless(device: BrilliantDevice) -> None:
            lossless_seen.append(device.peripheral_id)

        adapter.on_change(coalescing)  # panel-like (coalescing)
        adapter.on_change(lossless, coalesce_pushes=False)  # scene-like (lossless)

        adapter._dispatch_raw_device(_raw_snapshot(_OWN_DEVICE_ID, "push"))
        await asyncio.gather(*adapter._pending_tasks)

        # Both consumers received every peripheral...
        assert len(coalescing_seen) == _N_PERIPHERALS
        assert len(lossless_seen) == _N_PERIPHERALS
        # ...but each distinct peripheral was normalized ONCE total, not per
        # consumer (which would be 2 * _N_PERIPHERALS).
        assert counters.normalizations == _N_PERIPHERALS
        assert counters.variables == _N_PERIPHERALS * _N_VARIABLES


class TestNormalizeFailureDoesNotKillDrain:
    """Fix C: a normalize failure logs and skips that peripheral, exactly as
    main's broad handler catch did — it must not kill the drain worker, drop the
    peripheral's siblings, or strand queued snapshots.
    """

    async def test_normalize_error_skips_peripheral_and_delivers_siblings(
        self, monkeypatch: pytest.MonkeyPatch
    ) -> None:
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        real_normalize = bus_mod.normalize_peripheral

        def maybe_raise(device_id: str, peripheral_id: str, raw: Any) -> BrilliantDevice:
            if peripheral_id == "p1":
                raise RuntimeError("boom normalizing p1")
            return real_normalize(device_id, peripheral_id, raw)

        monkeypatch.setattr(bus_mod, "normalize_peripheral", maybe_raise)
        seen: list[str] = []

        async def consumer(device: BrilliantDevice) -> None:
            seen.append(device.peripheral_id)

        adapter.on_change(consumer, coalesce_pushes=False)
        adapter._dispatch_raw_device(
            _RawDevice(
                _OWN_DEVICE_ID,
                {
                    "p0": _RawPeripheral(27, "P0", {"on": _RawVar("1")}),
                    "p1": _RawPeripheral(27, "P1", {"on": _RawVar("1")}),
                    "p2": _RawPeripheral(27, "P2", {"on": _RawVar("1")}),
                },
            )
        )
        results = await asyncio.gather(*adapter._pending_tasks, return_exceptions=True)

        assert all(not isinstance(r, BaseException) for r in results)  # worker survived
        assert seen == ["p0", "p2"]  # p1 skipped, siblings delivered


class TestTimestampCoercion:
    """Fix D: the raw timestamp is captured as an immutable scalar at snapshot,
    preserving normalize's downstream branches exactly.
    """

    async def test_bool_timestamp_delivers_none_through_the_pipeline(self) -> None:
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        seen: list[int | None] = []

        async def consumer(device: BrilliantDevice) -> None:
            seen.append(device.variables["on"].timestamp_ms)

        adapter.on_change(consumer, coalesce_pushes=False)
        adapter._dispatch_raw_device(
            _RawDevice(
                _OWN_DEVICE_ID,
                {"load": _RawPeripheral(27, "L", {"on": _RawVar("1", timestamp=True)})},
            )
        )
        await asyncio.gather(*adapter._pending_tasks)

        assert seen == [None]  # the bool guard still fires end to end

    def test_nonscalar_timestamp_is_coerced_to_none_at_snapshot(self) -> None:
        raw = _RawPeripheral(27, "L", {"on": _RawVar("1", timestamp=[1, 2, 3])})
        snapshot = bus_mod._snapshot_peripheral(raw)
        # An arbitrary (mutable) timestamp object must not be retained by ref.
        assert snapshot.variables["on"].timestamp is None


class TestDeliveryTimeScopeGuard:
    """Fix E3: the want_device pre-filter admits by device id at DISPATCH, but
    Bridge._included must still guard DELIVERY — a push admitted while leader is
    dropped if leadership is lost before it is delivered (withdraw window).
    """

    async def test_included_guards_delivery_after_leadership_lost(self) -> None:
        adapter = RpcBusAdapter()
        adapter._own_device_id = _OWN_DEVICE_ID
        mqtt = FakeMqtt()
        leader = _Leader(is_leader=True)
        Bridge(
            adapter,
            mqtt,
            "mesh",
            include=lambda did: did == _MESH_DEVICE_ID and leader.is_leader,
        )

        # Admitted at dispatch (leader); _dispatch only SCHEDULES the drain.
        adapter._dispatch_raw_device(_raw_snapshot(_MESH_DEVICE_ID, "leader"))
        # Leadership lost before the scheduled drain delivers to _on_change.
        leader.is_leader = False
        await asyncio.gather(*adapter._pending_tasks)

        # Bridge._included rejects at DELIVERY, so nothing is published — proving
        # the pre-filter did not replace the delivery-time scope guard.
        assert _mesh_state_publishes(mqtt) == []
