"""Pure lifecycle additions required by the synchronous registry/driver."""

from __future__ import annotations

from dataclasses import replace

import pytest

from brilliant_mqtt import mesh_settlement_types as events
from brilliant_mqtt.mesh_settlement import MeshSettlement
from brilliant_mqtt.mesh_settlement_types import (
    Begin,
    NativeOutcome,
    OutcomeKind,
    PublishAccepted,
    PublishStarted,
    Revoke,
    RevokeEffects,
)
from brilliant_mqtt.model import BrilliantDevice, DeviceKind, PrimitiveCapture, PrimitiveField
from tests.test_mesh_settlement import accepted, env, expire, observe, owner


def test_absence_cleanup_is_irreversible_and_rejects_late_outcomes() -> None:
    slot, ticket = accepted()
    old = slot.publication_token(object())
    assert old is not None
    effects = slot.dispatch(events.RetireAbsent(env(slot, 5, 5)))
    assert any(isinstance(effect, RevokeEffects) for effect in effects)
    assert slot.retired
    assert slot.active  # Live only for its bounded absence obligation.
    assert slot.render(5)["state"] is None
    assert slot.render(5)["mesh_write_status"] == "unconfirmed"
    assert slot.view.observed == ()
    assert slot.view.projections == ()
    assert slot.view.targets == ()
    assert slot.view.window is None
    before = slot.view
    observe(slot, 6, 6, on="1", motion_score="255")
    slot.dispatch(Begin(env(slot, 7, 7), object(), (("on", "1"),)))
    slot.dispatch(NativeOutcome(env(slot, 8, 8), before.command_id, ticket, OutcomeKind.ACCEPTED))
    slot.dispatch(events.RequestPublication(env(slot, 9, 9)))
    slot.dispatch(PublishAccepted(env(slot, 10, 10), old))
    assert slot.view == before
    token = slot.publication_token(object())
    assert token is not None
    slot.dispatch(PublishStarted(env(slot, 11, 11), token))
    slot.dispatch(PublishAccepted(env(slot, 12, 12), token))
    assert slot.publication_token(object()) is None
    slot.dispatch(Revoke(env(slot, 13, 13)))
    assert not slot.active


def test_absence_after_successful_idle_does_not_rearm_unconfirmed_feedback() -> None:
    slot, _ = accepted()
    observe(slot, 5, 80, on="1")
    expire(slot, 6, 83)
    assert slot.view.status == "idle"
    slot.dispatch(events.RetireAbsent(env(slot, 7, 84)))
    assert slot.render(84)["state"] is None
    assert slot.render(84)["mesh_write_status"] == "idle"


def test_disabled_derivation_preserves_native_motion_and_custom_hold_is_respected() -> None:
    base = owner()
    slot = MeshSettlement(
        base.metadata,
        source=base.source,
        generation=base.generation,
        birth_sequence=1,
        born_at=0,
        derive_motion=False,
    )
    observe(slot, 2, 1, movement_detected="1", motion_score="0", enable_motion_score="1")
    assert slot.materialize(100).motion_detected is True
    enabled = MeshSettlement(
        base.metadata,
        source=base.source,
        generation=1,
        birth_sequence=1,
        born_at=0,
        motion_hold_s=5,
    )
    observe(
        enabled,
        2,
        1,
        movement_detected="0",
        motion_score="255",
        enable_motion_score="1",
        motion_high_threshold="45",
    )
    assert enabled.materialize(6).motion_detected is True
    assert enabled.materialize(6.1).motion_detected is False


def test_forced_publication_and_metadata_repair_are_typed_owner_events() -> None:
    slot = owner()
    token = slot.publication_token(object())
    assert token is not None
    slot.dispatch(PublishStarted(env(slot, 2, 1), token))
    slot.dispatch(PublishAccepted(env(slot, 3, 2), token))
    assert slot.publication_token(object()) is None
    slot.dispatch(events.RequestPublication(env(slot, 4, 3)))
    assert slot.publication_token(object()) is not None
    metadata = replace(slot.metadata, name="Current name")
    slot.dispatch(events.UpdateMetadata(env(slot, 5, 4), metadata))
    assert slot.metadata.name == "Current name"
    assert slot.view.observed == ()
    capture = PrimitiveCapture((PrimitiveField("on", "1"),))
    slot.dispatch(events.SeedBinding(env(slot, 6, 5), capture))
    assert slot.view.observed == ()


def test_binding_capabilities_do_not_fabricate_primary_values_or_evidence() -> None:
    base = owner()
    metadata = replace(base.metadata, binding_dimmable=True, binding_max_intensity=255)
    slot = MeshSettlement(metadata, source=base.source, generation=1, birth_sequence=1, born_at=0)
    device = slot.materialize(0)
    assert device.is_dimmable
    assert device.max_intensity == 255
    assert "intensity" not in device.variables
    assert "on" not in device.variables
    assert slot.render(0)["state"] is None
    assert slot.view.observed == ()
    assert slot.view.watermarks == ()
    assert slot.view.evidence == ()
    slot.dispatch(
        events.UpdateMetadata(env(slot, 2, 1), replace(metadata, binding_max_intensity=100))
    )
    assert slot.metadata is metadata


@pytest.mark.parametrize("scale", [0, -1, True])
def test_invalid_normalized_binding_scale_preserves_positive_model_fallback(scale: int) -> None:
    device = BrilliantDevice(
        "ble_mesh",
        "load",
        "Load",
        DeviceKind.LIGHT,
        binding_dimmable=True,
        binding_max_intensity=scale,
    )
    assert device.max_intensity == 1000
