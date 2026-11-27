"""Demand-only materialization of the mesh owner's immutable native view."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import replace
from types import MappingProxyType

from brilliant_mqtt.mapping import has_calibrated_motion_thresholds, payload_fields
from brilliant_mqtt.mesh_settlement_types import MeshMetadata, OwnerView, PublicationToken
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable


def materialize(
    metadata: MeshMetadata,
    view: OwnerView,
    now: float,
    hold_s: float,
    native_motion_passthrough: bool,
    derive_motion: bool = True,
) -> BrilliantDevice:
    fields = dict(view.observed)
    fields.update(view.projections)
    device = BrilliantDevice(
        metadata.device_id,
        metadata.peripheral_id,
        metadata.name,
        metadata.kind,
        metadata.peripheral_type,
        {
            name: Variable(name, value.value, value.externally_settable, value.timestamp_ms)
            for name, value in fields.items()
        },
        binding_dimmable=metadata.binding_dimmable,
        binding_max_intensity=metadata.binding_max_intensity,
    )
    variables = device.variables
    motion = variables.get("movement_detected")
    score = variables.get("motion_score")
    enable = variables.get("enable_motion_score")
    if not derive_motion or motion is None or score is None or enable is None:
        return device
    if not enable.as_bool():
        if native_motion_passthrough and has_calibrated_motion_thresholds(device):
            return device
        value = "0"
    else:
        threshold = variables.get("motion_high_threshold")
        valid = (
            score.as_int() is not None and threshold is not None and threshold.as_int() is not None
        )
        value = (
            "1"
            if (valid and view.last_hot is not None and 0 <= now - view.last_hot <= hold_s)
            else "0"
        )
    variables["movement_detected"] = replace(motion, value=value, timestamp_ms=None)
    return device


def render(
    metadata: MeshMetadata,
    view: OwnerView,
    now: float,
    primary_unknown: bool,
    hold_s: float,
    native_motion_passthrough: bool,
    token: PublicationToken | None,
    derive_motion: bool = True,
) -> Mapping[str, object]:
    device = materialize(metadata, view, now, hold_s, native_motion_passthrough, derive_motion)
    fields = payload_fields(device, native_motion_passthrough=native_motion_passthrough)
    if metadata.kind not in (DeviceKind.LIGHT, DeviceKind.SWITCH):
        return MappingProxyType(fields)
    status = view.status
    if token is not None and token.terminal is not None:
        status = token.terminal.reason
    pending = status == "pending"
    if primary_unknown or pending or status == "unconfirmed":
        fields["state"] = None
    fields["mesh_write_status"] = status
    fields["mesh_requested"] = MappingProxyType(dict(view.targets) if pending else {})
    fields["mesh_write_deadline"] = view.wire_deadline if pending else None
    return MappingProxyType(fields)
