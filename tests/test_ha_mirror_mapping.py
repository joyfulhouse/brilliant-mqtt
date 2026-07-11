"""Tests for Home Assistant entity mappings to Brilliant peripherals."""

from __future__ import annotations

from brilliant_ha_mirror.mapping import (
    HaEntity,
    ServiceCall,
    command_to_service,
    spec_for,
    state_to_variables,
)


def _e(eid: str, state: str, **attrs: object) -> HaEntity:
    return HaEntity(entity_id=eid, state=state, attributes=attrs, area="Kitchen")


def test_light_spec_and_state() -> None:
    spec = spec_for(_e("light.k", "on", brightness=128))
    assert spec is not None
    assert spec.peripheral_type == 27
    assert "on" in spec.command_vars and "intensity" in spec.command_vars
    v = state_to_variables(_e("light.k", "on", brightness=128))
    assert v["on"] == "1" and int(v["intensity"]) > 0


def test_light_command_on_off() -> None:
    assert command_to_service("light.k", "on", "0") == ServiceCall(
        "light", "turn_off", {"entity_id": "light.k"}
    )
    assert command_to_service("light.k", "on", "1").service == "turn_on"


def test_switch_generic_on_off() -> None:
    spec = spec_for(_e("switch.s", "off"))
    assert spec is not None
    assert spec.peripheral_type == 45
    assert command_to_service("switch.s", "on", "1") == ServiceCall(
        "switch", "turn_on", {"entity_id": "switch.s"}
    )


def test_lock() -> None:
    spec = spec_for(_e("lock.l", "locked"))
    assert spec is not None
    assert spec.peripheral_type == 1
    assert state_to_variables(_e("lock.l", "locked"))["locked"] == "1"
    assert command_to_service("lock.l", "locked", "0") == ServiceCall(
        "lock", "unlock", {"entity_id": "lock.l"}
    )


def test_cover_position_shade() -> None:
    e = _e("cover.b", "open", current_position=40)
    spec = spec_for(e)
    assert spec is not None
    assert spec.peripheral_type == 53
    assert state_to_variables(e)["position"] == "40"
    assert command_to_service("cover.b", "position", "70") == ServiceCall(
        "cover", "set_cover_position", {"entity_id": "cover.b", "position": 70}
    )


def test_garage_cover() -> None:
    e = _e("cover.g", "closed", device_class="garage")
    spec = spec_for(e)
    assert spec is not None
    assert spec.peripheral_type == 74
    assert command_to_service("cover.g", "event", "open") == ServiceCall(
        "cover", "open_cover", {"entity_id": "cover.g"}
    )


def test_unsupported_returns_none() -> None:
    assert spec_for(_e("climate.t", "heat")) is None


def test_garage_state_reflects_command_vocabulary() -> None:
    # state_to_variables must write the `event` command vocabulary, not HA's
    # state words (review finding M5).
    def _g(state: str) -> HaEntity:
        return HaEntity("cover.g", state, {"device_class": "garage"}, "Garage")

    assert state_to_variables(_g("closed")) == {"event": "close"}
    assert state_to_variables(_g("closing")) == {"event": "close"}
    assert state_to_variables(_g("open")) == {"event": "open"}
    assert state_to_variables(_g("opening")) == {"event": "open"}


def test_int_variables_is_the_shared_type_source() -> None:
    # hosting.py reads mapping.INT_VARIABLES for bus var typing — assert the
    # single source covers exactly the integer-typed Tier-1 variables (DRY #3).
    from brilliant_ha_mirror.mapping import INT_VARIABLES

    assert INT_VARIABLES == frozenset({"on", "dimmable", "intensity", "locked", "position"})
    # Every command var across Tier-1 specs that carries a numeric/bool value is
    # int-typed here; the text-only display/event vars are not.
    assert "display_name" not in INT_VARIABLES
    assert "event" not in INT_VARIABLES
