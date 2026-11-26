"""Frozen adapter ingress, before async compatibility callbacks and routing.

These are source snapshots, not owner events. The registry admits lifecycle
and binding provenance before constructing reducer envelopes for current owners.
"""

from __future__ import annotations

from collections.abc import Callable
from dataclasses import dataclass

from brilliant_mqtt.mesh_settlement_types import MeshMetadata
from brilliant_mqtt.model import CaptureProvenance, PrimitiveCapture


@dataclass(frozen=True)
class MeshCapture:
    provenance: CaptureProvenance
    metadata: MeshMetadata
    capture: PrimitiveCapture


@dataclass(frozen=True)
class MeshInventory:
    provenance: CaptureProvenance
    peripheral_ids: frozenset[str]


@dataclass(frozen=True)
class MeshPeripheralAbsent:
    provenance: CaptureProvenance
    peripheral_id: str


@dataclass(frozen=True)
class MeshScopeUnknown:
    provenance: CaptureProvenance


@dataclass(frozen=True)
class MeshGeneration:
    provenance: CaptureProvenance


MeshIngress = MeshCapture | MeshInventory | MeshPeripheralAbsent | MeshScopeUnknown | MeshGeneration


@dataclass(frozen=True, eq=False)
class MeshSink:
    callback: Callable[[MeshIngress], None]
    want_fields: Callable[[str], frozenset[str] | None]
    want_inventory: Callable[[], bool]
