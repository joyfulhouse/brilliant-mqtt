import pytest
from thrift.Thrift import TType

from brilliant_protocol_lab.thrift_wire import (
    WireField,
    WireMessage,
    decode_message,
    encode_message,
)


def test_recursive_binary_round_trip() -> None:
    message = WireMessage(
        name="join_home",
        message_type=1,
        sequence_id=7,
        fields=(
            WireField(1, 11, b"synthetic-client"),
            WireField(2, 12, (WireField(1, 2, True), WireField(2, 8, 42))),
            WireField(3, 15, (WireField(0, 8, 10), WireField(1, 8, 20))),
        ),
    )
    assert decode_message(encode_message(message)) == message


def test_empty_collection_without_schema_type_is_rejected() -> None:
    message = WireMessage("empty", 1, 1, (WireField(1, TType.LIST, ()),))
    with pytest.raises(ValueError, match="element type"):
        encode_message(message)


def test_heterogeneous_collection_is_rejected() -> None:
    message = WireMessage(
        "mixed",
        1,
        1,
        (
            WireField(
                1,
                TType.LIST,
                (WireField(0, TType.I32, 1), WireField(1, TType.STRING, b"two")),
            ),
        ),
    )
    with pytest.raises(ValueError, match="heterogeneous"):
        encode_message(message)


def test_map_round_trip() -> None:
    message = WireMessage(
        "map_test",
        1,
        42,
        (
            WireField(
                1,
                TType.MAP,
                (
                    (WireField(0, TType.I32, 10), WireField(0, TType.STRING, b"value_one")),
                    (WireField(1, TType.I32, 20), WireField(1, TType.STRING, b"value_two")),
                ),
            ),
        ),
    )
    decoded = decode_message(encode_message(message))
    assert decoded == message
    # Verify the structure of the decoded MAP field
    map_field = decoded.fields[0]
    assert map_field.field_id == 1
    assert map_field.thrift_type == TType.MAP
    assert len(map_field.value) == 2
    assert map_field.value[0][0].value == 10
    assert map_field.value[0][1].value == b"value_one"
    assert map_field.value[1][0].value == 20
    assert map_field.value[1][1].value == b"value_two"


def test_set_round_trip() -> None:
    message = WireMessage(
        "set_test",
        1,
        99,
        (
            WireField(
                1,
                TType.SET,
                (
                    WireField(0, TType.I32, 100),
                    WireField(1, TType.I32, 200),
                    WireField(2, TType.I32, 300),
                ),
            ),
        ),
    )
    decoded = decode_message(encode_message(message))
    assert decoded == message
    # Verify the structure of the decoded SET field
    set_field = decoded.fields[0]
    assert set_field.field_id == 1
    assert set_field.thrift_type == TType.SET
    assert len(set_field.value) == 3
    assert set_field.value[0].value == 100
    assert set_field.value[1].value == 200
    assert set_field.value[2].value == 300


def test_empty_map_rejected() -> None:
    message = WireMessage(
        "empty_map",
        1,
        1,
        (WireField(1, TType.MAP, ()),),
    )
    with pytest.raises(ValueError, match="key/value types"):
        encode_message(message)


def test_heterogeneous_map_keys_rejected() -> None:
    message = WireMessage(
        "mixed_keys",
        1,
        1,
        (
            WireField(
                1,
                TType.MAP,
                (
                    (WireField(0, TType.I32, 10), WireField(0, TType.STRING, b"value1")),
                    (WireField(1, TType.STRING, b"key2"), WireField(1, TType.STRING, b"value2")),
                ),
            ),
        ),
    )
    with pytest.raises(ValueError, match="heterogeneous"):
        encode_message(message)


def test_heterogeneous_map_values_rejected() -> None:
    message = WireMessage(
        "mixed_values",
        1,
        1,
        (
            WireField(
                1,
                TType.MAP,
                (
                    (WireField(0, TType.I32, 10), WireField(0, TType.STRING, b"value1")),
                    (WireField(1, TType.I32, 20), WireField(1, TType.I32, 99)),
                ),
            ),
        ),
    )
    with pytest.raises(ValueError, match="heterogeneous"):
        encode_message(message)
