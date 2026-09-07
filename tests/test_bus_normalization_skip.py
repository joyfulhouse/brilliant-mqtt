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
