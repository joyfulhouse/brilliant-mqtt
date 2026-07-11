"""Map Home Assistant entities to Brilliant peripheral variables and commands.

This module is pure Python so entity mapping can be tested without panel,
network, WebSocket, or MQTT dependencies.
"""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass


@dataclass(frozen=True)
class HaEntity:
    """A Home Assistant entity with the fields needed by the mirror."""

    entity_id: str
    state: str
    attributes: Mapping[str, object]
    area: str | None


@dataclass(frozen=True)
class PeripheralSpec:
    """Brilliant peripheral type, variables, and externally settable names."""

    peripheral_type: int
    variables: dict[str, str]
    command_vars: frozenset[str]


@dataclass(frozen=True)
class ServiceCall:
    """A Home Assistant service call produced from a Brilliant command."""

    domain: str
    service: str
    data: dict[str, object]


SUPPORTED_DOMAINS: frozenset[str] = frozenset({"light", "switch", "lock", "cover"})

# The mirrored peripheral variables that are integer-typed on the Brilliant bus
# (thrift BOOL and I32 alike are represented as int); every other variable is
# text. This is the single source of truth for variable bus-typing — the values
# emitted here are always strings, and the host adapter (hosting.py) reads this
# set to build each VariableSpec with the right Python type. Colocated with the
# variable vocabulary so a new variable can only be added in one place.
INT_VARIABLES: frozenset[str] = frozenset({"on", "dimmable", "intensity", "locked", "position"})


def _domain(entity_id: str) -> str:
    return entity_id.partition(".")[0]


def _int_attribute(entity: HaEntity, name: str) -> int:
    value = entity.attributes.get(name, 0)
    return value if isinstance(value, int) else 0


def state_to_variables(entity: HaEntity) -> dict[str, str]:
    """Convert an entity's state and attributes to Brilliant string variables."""
    domain = _domain(entity.entity_id)
    if domain == "light":
        brightness = min(255, max(0, _int_attribute(entity, "brightness")))
        return {
            "on": "1" if entity.state == "on" else "0",
            "dimmable": "1",
            "intensity": str(brightness),
        }
    if domain == "switch":
        return {"on": "1" if entity.state == "on" else "0"}
    if domain == "lock":
        return {"locked": "1" if entity.state == "locked" else "0"}
    if domain == "cover":
        if entity.attributes.get("device_class") == "garage":
            # Reflect into the command vocabulary the `event` variable accepts
            # ("open"/"close"), not HA's state words ("closed"/"opening"/…).
            # Garage reflection is not yet live-verified on panel — confirm the
            # accepted values before relying on the displayed state.
            opening = entity.state in {"open", "opening"}
            return {"event": "open" if opening else "close"}
        position = min(100, max(0, _int_attribute(entity, "current_position")))
        return {"position": str(position)}
    return {}


def spec_for(entity: HaEntity) -> PeripheralSpec | None:
    """Return the Brilliant peripheral specification for a supported entity."""
    domain = _domain(entity.entity_id)
    if domain not in SUPPORTED_DOMAINS:
        return None
    if domain == "light":
        return PeripheralSpec(27, state_to_variables(entity), frozenset({"on", "intensity"}))
    if domain == "switch":
        return PeripheralSpec(45, state_to_variables(entity), frozenset({"on"}))
    if domain == "lock":
        return PeripheralSpec(1, state_to_variables(entity), frozenset({"locked"}))
    if entity.attributes.get("device_class") == "garage":
        return PeripheralSpec(74, state_to_variables(entity), frozenset({"event"}))
    return PeripheralSpec(53, state_to_variables(entity), frozenset({"position"}))


def command_to_service(entity_id: str, var: str, value: str) -> ServiceCall:
    """Convert an externally set Brilliant variable to a Home Assistant call."""
    domain = _domain(entity_id)
    entity_data: dict[str, object] = {"entity_id": entity_id}

    if domain in {"light", "switch"} and var == "on" and value in {"0", "1"}:
        service = "turn_on" if value == "1" else "turn_off"
        return ServiceCall(domain, service, entity_data)
    if domain == "light" and var == "intensity":
        return ServiceCall(domain, "turn_on", {**entity_data, "brightness": int(value)})
    if domain == "lock" and var == "locked" and value in {"0", "1"}:
        service = "lock" if value == "1" else "unlock"
        return ServiceCall(domain, service, entity_data)
    if domain == "cover" and var == "position":
        return ServiceCall(
            domain,
            "set_cover_position",
            {**entity_data, "position": int(value)},
        )
    if domain == "cover" and var == "event" and value in {"open", "close"}:
        service = "open_cover" if value == "open" else "close_cover"
        return ServiceCall(domain, service, entity_data)
    raise ValueError(f"Unsupported command: {domain}.{var}={value}")
