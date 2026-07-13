"""Tests for the bounded, off-panel TBinaryProtocol decoder."""

from __future__ import annotations

import base64
import json
import struct
from pathlib import Path
from typing import cast

import pytest

from brilliant_mqtt.thrift_binary import ThriftDecodeError, decode_struct_base64

_FIXTURES = Path(__file__).parent / "fixtures"


def _fixture_value(name: str) -> str:
    data = cast(dict[str, object], json.loads((_FIXTURES / name).read_text()))
    return cast(str, data["value"])


def _encoded(raw: bytes) -> str:
    return base64.b64encode(raw).decode("ascii")


def _field(field_type: int, field_id: int, value: bytes) -> bytes:
    return bytes([field_type]) + struct.pack(">h", field_id) + value


def test_decodes_redacted_all_off_definition() -> None:
    decoded = decode_struct_base64(_fixture_value("scene_all_off.json"))

    assert decoded[1] == "all_off"
    assert decoded[2] == "All Lights Off"
    assert decoded[3] == "qrc:/icons/scenes/all_lights_off.png"


def test_decodes_binary_protocol_primitives_and_collections() -> None:
    nested = _field(8, 1, struct.pack(">i", 7)) + b"\x00"
    mapping = (
        bytes([11, 10]) + struct.pack(">i", 1) + struct.pack(">i", 1) + b"k" + struct.pack(">q", 9)
    )
    values = bytes([6]) + struct.pack(">i", 2) + struct.pack(">hh", -2, 3)
    raw = b"".join(
        (
            _field(2, 1, b"\x01"),
            _field(3, 2, struct.pack(">b", -1)),
            _field(4, 3, struct.pack(">d", 1.5)),
            _field(6, 4, struct.pack(">h", -2)),
            _field(8, 5, struct.pack(">i", -3)),
            _field(10, 6, struct.pack(">q", -4)),
            _field(11, 7, struct.pack(">i", 2) + b"ok"),
            _field(12, 8, nested),
            _field(13, 9, mapping),
            _field(14, 10, values),
            _field(15, 11, values),
            b"\x00",
        )
    )

    assert decode_struct_base64(_encoded(raw)) == {
        1: True,
        2: -1,
        3: 1.5,
        4: -2,
        5: -3,
        6: -4,
        7: "ok",
        8: {1: 7},
        9: {"k": 9},
        10: [-2, 3],
        11: [-2, 3],
    }


@pytest.mark.parametrize("value", ["not base64!", "AA=A", "\N{SNOWMAN}"])
def test_rejects_invalid_base64_without_echoing_input(value: str) -> None:
    with pytest.raises(ThriftDecodeError) as exc_info:
        decode_struct_base64(value)

    assert str(exc_info.value) == "invalid base64 thrift value"
    assert value not in str(exc_info.value)


def test_rejects_truncated_value() -> None:
    value = _fixture_value("scene_all_off.json")

    with pytest.raises(ThriftDecodeError, match="truncated thrift value"):
        decode_struct_base64(value[:-4])


@pytest.mark.parametrize(
    "field_type,payload",
    [
        (11, struct.pack(">i", -1)),
        (13, bytes([11, 11]) + struct.pack(">i", -1)),
        (14, bytes([8]) + struct.pack(">i", -1)),
        (15, bytes([8]) + struct.pack(">i", -1)),
    ],
)
def test_rejects_negative_string_and_collection_sizes(field_type: int, payload: bytes) -> None:
    raw = _field(field_type, 1, payload) + b"\x00"

    with pytest.raises(ThriftDecodeError, match="negative thrift (length|collection size)"):
        decode_struct_base64(_encoded(raw))


def test_enforces_byte_limit_before_decoding_struct() -> None:
    value = _fixture_value("scene_all_off.json")
    decoded_size = len(base64.b64decode(value))

    with pytest.raises(ThriftDecodeError, match="exceeds byte limit"):
        decode_struct_base64(value, max_bytes=decoded_size - 1)


def test_enforces_depth_limit() -> None:
    deepest = _field(8, 1, struct.pack(">i", 1)) + b"\x00"
    nested = _field(12, 1, deepest) + b"\x00"
    raw = _field(12, 1, nested) + b"\x00"

    with pytest.raises(ThriftDecodeError, match="exceeds depth limit"):
        decode_struct_base64(_encoded(raw), max_depth=1)


def test_enforces_item_limit_before_large_collection_is_read() -> None:
    hostile_count = 2_000_000_000
    raw = _field(15, 1, bytes([8]) + struct.pack(">i", hostile_count)) + b"\x00"

    with pytest.raises(ThriftDecodeError, match="exceeds item limit"):
        decode_struct_base64(_encoded(raw), max_items=10)


def test_rejects_trailing_bytes() -> None:
    with pytest.raises(ThriftDecodeError, match="trailing bytes"):
        decode_struct_base64(_encoded(b"\x00\x00"))


def test_rejects_unsupported_wire_type() -> None:
    raw = _field(5, 1, b"") + b"\x00"

    with pytest.raises(ThriftDecodeError, match="unsupported thrift type") as exc_info:
        decode_struct_base64(_encoded(raw))

    assert _encoded(raw) not in str(exc_info.value)


def test_rejects_unhashable_map_keys_as_malformed() -> None:
    empty_list_key = bytes([8]) + struct.pack(">i", 0)
    mapping = bytes([15, 8]) + struct.pack(">i", 1) + empty_list_key + struct.pack(">i", 1)
    raw = _field(13, 1, mapping) + b"\x00"

    with pytest.raises(ThriftDecodeError, match="invalid thrift map key"):
        decode_struct_base64(_encoded(raw))


@pytest.mark.parametrize("limit_name", ["max_bytes", "max_depth", "max_items"])
def test_rejects_negative_limits(limit_name: str) -> None:
    kwargs = {limit_name: -1}

    with pytest.raises(ThriftDecodeError, match="limits must be non-negative"):
        decode_struct_base64(_encoded(b"\x00"), **kwargs)
