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
