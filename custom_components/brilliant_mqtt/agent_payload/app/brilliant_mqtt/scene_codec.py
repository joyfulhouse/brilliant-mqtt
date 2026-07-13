"""Typed scene and mode records reduced from Brilliant bus variables."""

from __future__ import annotations

import hashlib
from dataclasses import dataclass

from brilliant_mqtt.model import BrilliantDevice
from brilliant_mqtt.thrift_binary import decode_struct_base64

_SCENE_DEFINITION_PREFIX = "scene:"
_MODE_DEFINITION_PREFIX = "mode:"
_SCENE_EXECUTION_PREFIX = "execution_state:scene_execution_handler:scene:"
_MANUAL_MODE_ID = "manual_mode_id"


class SceneCodecError(ValueError):
    """A structurally valid Thrift record with invalid scene semantics."""


@dataclass(frozen=True)
class SceneDefinition:
    scene_id: str
    display_name: str
    icon: str


@dataclass(frozen=True)
class SceneExecution:
    scene_id: str
    executed_at_ms: int
    payload_sha256: str


@dataclass(frozen=True)
class ModeDefinition:
    mode_id: str
    display_name: str


@dataclass(frozen=True)
class ModeExecution:
    mode_id: str
    executed_at_ms: int


def _required_string(decoded: dict[int, object], field_id: int, *, label: str) -> str:
    value = decoded.get(field_id)
    if not isinstance(value, str) or not value:
        raise SceneCodecError(f"{label} has invalid field {field_id}")
    return value


def decode_scene_catalog(device: BrilliantDevice) -> tuple[SceneDefinition, ...]:
    """Decode scene definitions, omitting their action and device details."""
    if device.peripheral_id != "scene_configuration":
        return ()

    definitions: list[SceneDefinition] = []
    for name, variable in device.variables.items():
        if not name.startswith(_SCENE_DEFINITION_PREFIX):
            continue
        name_scene_id = name.removeprefix(_SCENE_DEFINITION_PREFIX)
        decoded = decode_struct_base64(variable.value)
        scene_id = _required_string(decoded, 1, label="scene definition")
        if scene_id != name_scene_id:
            raise SceneCodecError("scene definition id does not match variable name")

        display_name = decoded.get(2)
        if not isinstance(display_name, str) or not display_name:
            raise SceneCodecError("scene definition has invalid display name")
        icon = decoded.get(3)
        if not isinstance(icon, str) or not icon:
            raise SceneCodecError("scene definition has invalid icon")
        definitions.append(SceneDefinition(scene_id=scene_id, display_name=display_name, icon=icon))

    return tuple(sorted(definitions, key=lambda item: item.scene_id))


def decode_mode_catalog(device: BrilliantDevice) -> tuple[ModeDefinition, ...]:
    """Decode configured modes without inventing defaults for an empty catalog."""
    if device.peripheral_id != "mode_configuration":
        return ()

    definitions: list[ModeDefinition] = []
    for name, variable in device.variables.items():
        if not name.startswith(_MODE_DEFINITION_PREFIX):
            continue
        name_mode_id = name.removeprefix(_MODE_DEFINITION_PREFIX)
        decoded = decode_struct_base64(variable.value)
        mode_id = _required_string(decoded, 1, label="mode definition")
        if mode_id != name_mode_id:
            raise SceneCodecError("mode definition id does not match variable name")
        display_name = decoded.get(2)
        if not isinstance(display_name, str) or not display_name:
            raise SceneCodecError("mode definition has invalid display name")
        definitions.append(ModeDefinition(mode_id=mode_id, display_name=display_name))

    return tuple(sorted(definitions, key=lambda item: item.mode_id))


def decode_scene_execution(device: BrilliantDevice) -> tuple[SceneExecution, ...]:
    """Decode retained scene executions using their embedded timestamp."""
    if device.peripheral_id != "execution_peripheral":
        return ()

    records: list[SceneExecution] = []
    for name, variable in device.variables.items():
        if not name.startswith(_SCENE_EXECUTION_PREFIX):
            continue
        scene_id = name.removeprefix(_SCENE_EXECUTION_PREFIX)
        if not scene_id:
            raise SceneCodecError("scene execution has an empty scene id")
        decoded = decode_struct_base64(variable.value)
        executed_at_ms = decoded.get(1)
        if type(executed_at_ms) is not int or executed_at_ms < 0:
            raise SceneCodecError("scene execution is missing its timestamp")
        records.append(
            SceneExecution(
                scene_id=scene_id,
                executed_at_ms=executed_at_ms,
                payload_sha256=hashlib.sha256(variable.value.encode()).hexdigest(),
            )
        )
    return tuple(sorted(records, key=lambda item: (item.executed_at_ms, item.scene_id)))


def decode_mode_execution(device: BrilliantDevice) -> tuple[ModeExecution, ...]:
    """Reduce a non-empty manual mode update using its bus timestamp."""
    if device.peripheral_id != "execution_peripheral":
        return ()
    variable = device.variables.get(_MANUAL_MODE_ID)
    if variable is None or not variable.value:
        return ()
    timestamp_ms = variable.timestamp_ms
    if type(timestamp_ms) is not int or timestamp_ms < 0:
        raise SceneCodecError("mode execution has no valid bus timestamp")
    return (ModeExecution(mode_id=variable.value, executed_at_ms=timestamp_ms),)
