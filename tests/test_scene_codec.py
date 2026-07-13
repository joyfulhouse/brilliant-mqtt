"""Typed reduction tests for Brilliant scene and mode bus records."""

from __future__ import annotations

import base64
import dataclasses
import hashlib
import json
import struct
from pathlib import Path
from typing import cast

import pytest

from brilliant_mqtt.model import BrilliantDevice, DeviceKind, Variable
from brilliant_mqtt.scene_codec import (
    ModeDefinition,
    ModeExecution,
    SceneCodecError,
    SceneDefinition,
    SceneExecution,
    decode_mode_catalog,
    decode_mode_execution,
    decode_scene_catalog,
    decode_scene_execution,
)

_FIXTURES = Path(__file__).parent / "fixtures"


def _fixture(name: str) -> dict[str, object]:
    return cast(dict[str, object], json.loads((_FIXTURES / name).read_text()))


def _device(peripheral_id: str, variables: dict[str, Variable]) -> BrilliantDevice:
    return BrilliantDevice(
        device_id="panel-1",
        peripheral_id=peripheral_id,
        name=peripheral_id,
        kind=DeviceKind.UNKNOWN,
        variables=variables,
    )


def _string_field(field_id: int, value: str) -> bytes:
    encoded = value.encode()
    return b"\x0b" + struct.pack(">hI", field_id, len(encoded)) + encoded


def _i64_field(field_id: int, value: int) -> bytes:
    return b"\x0a" + struct.pack(">hq", field_id, value)


def _bool_field(field_id: int, value: bool) -> bytes:
    return b"\x02" + struct.pack(">hB", field_id, value)


def _struct_value(*fields: bytes) -> str:
    return base64.b64encode(b"".join((*fields, b"\x00"))).decode("ascii")


def test_decodes_redacted_scene_catalog_without_actions() -> None:
    fixture = _fixture("scene_all_off.json")
    name = cast(str, fixture["variable_name"])
    value = cast(str, fixture["value"])
    expected = cast(dict[str, object], fixture["expected"])
    device = _device("scene_configuration", {name: Variable(name, value)})

    result = decode_scene_catalog(device)

    assert expected == {
        "scene_id": "all_off",
        "display_name": "All Lights Off",
        "icon": "qrc:/icons/scenes/all_lights_off.png",
    }
    assert result == (
        SceneDefinition(
            scene_id=cast(str, expected["scene_id"]),
            display_name=cast(str, expected["display_name"]),
            icon=cast(str, expected["icon"]),
        ),
    )
    assert [field.name for field in dataclasses.fields(SceneDefinition)] == [
        "scene_id",
        "display_name",
        "icon",
    ]


def test_scene_catalog_ignores_non_scene_variables() -> None:
    device = _device(
        "scene_configuration",
        {"display_name": Variable("display_name", "not thrift")},
    )

    assert decode_scene_catalog(device) == ()


def test_scene_catalog_ignores_other_peripherals() -> None:
    fixture = _fixture("scene_all_off.json")
    name = cast(str, fixture["variable_name"])
    value = cast(str, fixture["value"])

    assert decode_scene_catalog(_device("other", {name: Variable(name, value)})) == ()


def test_scene_catalog_rejects_definition_name_mismatch() -> None:
    value = _struct_value(
        _string_field(1, "different"),
        _string_field(2, "All Lights Off"),
        _string_field(3, "icon"),
    )
    device = _device("scene_configuration", {"scene:all_off": Variable("scene:all_off", value)})

    with pytest.raises(SceneCodecError, match="scene definition id does not match variable name"):
        decode_scene_catalog(device)


def test_scene_catalog_requires_display_name_and_icon_strings() -> None:
    value = _struct_value(_string_field(1, "all_off"))
    device = _device("scene_configuration", {"scene:all_off": Variable("scene:all_off", value)})

    with pytest.raises(SceneCodecError, match="scene definition has invalid display name"):
        decode_scene_catalog(device)


def test_scene_catalog_requires_icon_string() -> None:
    value = _struct_value(
        _string_field(1, "all_off"),
        _string_field(2, "All Lights Off"),
    )
    device = _device("scene_configuration", {"scene:all_off": Variable("scene:all_off", value)})

    with pytest.raises(SceneCodecError, match="scene definition has invalid icon"):
        decode_scene_catalog(device)


def test_decodes_redacted_scene_execution_using_embedded_timestamp() -> None:
    fixture = _fixture("scene_execution_all_off.json")
    name = cast(str, fixture["variable_name"])
    value = cast(str, fixture["value"])
    expected = cast(dict[str, object], fixture["expected"])
    device = _device(
        "execution_peripheral",
        {name: Variable(name, value, timestamp_ms=9_999_999_999_999)},
    )

    result = decode_scene_execution(device)

    assert expected == {
        "scene_id": "all_off",
        "executed_at_ms": 1_683_501_714_715,
        "payload_sha256": "7dfdd1938a70c8a9a95586546d8f7d6c1bba7a5b207a68fb8754ebba4ac99572",
    }
    assert hashlib.sha256(value.encode()).hexdigest() == expected["payload_sha256"]
    assert result == (
        SceneExecution(
            scene_id=cast(str, expected["scene_id"]),
            executed_at_ms=cast(int, expected["executed_at_ms"]),
            payload_sha256=expected["payload_sha256"],
        ),
    )
    assert [field.name for field in dataclasses.fields(SceneExecution)] == [
        "scene_id",
        "executed_at_ms",
        "payload_sha256",
    ]


def test_scene_execution_ignores_non_scene_variables_and_other_peripherals() -> None:
    non_scene = {"manual_mode_id": Variable("manual_mode_id", "eco", timestamp_ms=123)}
    assert decode_scene_execution(_device("execution_peripheral", non_scene)) == ()
    assert decode_scene_execution(_device("other", non_scene)) == ()


def test_scene_execution_sorts_by_embedded_timestamp_then_id() -> None:
    later = _struct_value(_i64_field(1, 200))
    earlier_b = _struct_value(_i64_field(1, 100))
    earlier_a = _struct_value(_i64_field(1, 100))
    prefix = "execution_state:scene_execution_handler:scene:"
    device = _device(
        "execution_peripheral",
        {
            f"{prefix}later": Variable(f"{prefix}later", later),
            f"{prefix}b": Variable(f"{prefix}b", earlier_b),
            f"{prefix}a": Variable(f"{prefix}a", earlier_a),
        },
    )

    assert [(item.executed_at_ms, item.scene_id) for item in decode_scene_execution(device)] == [
        (100, "a"),
        (100, "b"),
        (200, "later"),
    ]


@pytest.mark.parametrize(
    "value",
    [
        _struct_value(),
        _struct_value(_string_field(1, "not-an-integer")),
        _struct_value(_bool_field(1, True)),
        _struct_value(_i64_field(1, -1)),
    ],
)
def test_scene_execution_requires_non_negative_integer_embedded_timestamp(value: str) -> None:
    name = "execution_state:scene_execution_handler:scene:test"
    device = _device("execution_peripheral", {name: Variable(name, value, timestamp_ms=123)})

    with pytest.raises(SceneCodecError, match="scene execution is missing its timestamp"):
        decode_scene_execution(device)


def test_decodes_synthetic_mode_definitions() -> None:
    eco = _struct_value(_string_field(1, "eco"), _string_field(2, "Eco"))
    away = _struct_value(_string_field(1, "away"), _string_field(2, "Away"))
    device = _device(
        "mode_configuration",
        {
            "mode:eco": Variable("mode:eco", eco),
            "mode:away": Variable("mode:away", away),
            "active_mode": Variable("active_mode", "eco"),
        },
    )

    assert decode_mode_catalog(device) == (
        ModeDefinition(mode_id="away", display_name="Away"),
        ModeDefinition(mode_id="eco", display_name="Eco"),
    )


def test_mode_catalog_rejects_definition_name_mismatch() -> None:
    value = _struct_value(_string_field(1, "away"), _string_field(2, "Away"))
    device = _device("mode_configuration", {"mode:eco": Variable("mode:eco", value)})

    with pytest.raises(SceneCodecError, match="mode definition id does not match variable name"):
        decode_mode_catalog(device)


def test_empty_mode_configuration_does_not_invent_defaults() -> None:
    assert decode_mode_catalog(_device("mode_configuration", {})) == ()
    assert decode_mode_catalog(_device("other", {})) == ()


def test_nonempty_manual_mode_id_with_bus_timestamp_is_execution() -> None:
    device = _device(
        "execution_peripheral",
        {"manual_mode_id": Variable("manual_mode_id", "eco", timestamp_ms=1_234)},
    )

    assert decode_mode_execution(device) == (ModeExecution(mode_id="eco", executed_at_ms=1_234),)


@pytest.mark.parametrize(
    "variable",
    [
        None,
        Variable("manual_mode_id", "", timestamp_ms=1_234),
    ],
)
def test_absent_or_empty_manual_mode_id_is_not_execution(variable: Variable | None) -> None:
    variables = {} if variable is None else {"manual_mode_id": variable}

    assert decode_mode_execution(_device("execution_peripheral", variables)) == ()


@pytest.mark.parametrize("timestamp_ms", [None, -1])
def test_manual_mode_execution_requires_non_negative_bus_timestamp(
    timestamp_ms: int | None,
) -> None:
    device = _device(
        "execution_peripheral",
        {"manual_mode_id": Variable("manual_mode_id", "eco", timestamp_ms=timestamp_ms)},
    )

    with pytest.raises(SceneCodecError, match="mode execution has no valid bus timestamp"):
        decode_mode_execution(device)


def test_mode_execution_ignores_other_peripherals() -> None:
    variable = Variable("manual_mode_id", "eco", timestamp_ms=1_234)
    assert decode_mode_execution(_device("other", {"manual_mode_id": variable})) == ()
