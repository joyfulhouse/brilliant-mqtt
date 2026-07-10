from __future__ import annotations

from dataclasses import dataclass
from typing import Any, cast

from thrift.protocol.TBinaryProtocol import TBinaryProtocol  # type: ignore[import-untyped]
from thrift.Thrift import TType  # type: ignore[import-untyped]
from thrift.transport.TTransport import TMemoryBuffer  # type: ignore[import-untyped]


@dataclass(frozen=True)
class WireField:
    field_id: int
    thrift_type: int
    value: Any


@dataclass(frozen=True)
class WireMessage:
    name: str
    message_type: int
    sequence_id: int
    fields: tuple[WireField, ...]


def _read_struct(protocol: TBinaryProtocol) -> tuple[WireField, ...]:
    protocol.readStructBegin()
    fields: list[WireField] = []
    while True:
        _, thrift_type, field_id = protocol.readFieldBegin()
        if thrift_type == TType.STOP:
            break
        fields.append(WireField(field_id, thrift_type, _read_value(protocol, thrift_type)))
        protocol.readFieldEnd()
    protocol.readStructEnd()
    return tuple(fields)


def _read_value(protocol: TBinaryProtocol, thrift_type: int) -> Any:
    scalar = {
        TType.BOOL: protocol.readBool,
        TType.BYTE: protocol.readByte,
        TType.I16: protocol.readI16,
        TType.I32: protocol.readI32,
        TType.I64: protocol.readI64,
        TType.DOUBLE: protocol.readDouble,
        TType.STRING: protocol.readBinary,
    }
    if thrift_type in scalar:
        return scalar[thrift_type]()
    if thrift_type == TType.STRUCT:
        return _read_struct(protocol)
    if thrift_type in (TType.LIST, TType.SET):
        element_type, size = (
            protocol.readListBegin() if thrift_type == TType.LIST else protocol.readSetBegin()
        )
        elements: Any = tuple(
            WireField(index, element_type, _read_value(protocol, element_type))
            for index in range(size)
        )
        protocol.readListEnd() if thrift_type == TType.LIST else protocol.readSetEnd()
        return elements
    if thrift_type == TType.MAP:
        key_type, value_type, size = protocol.readMapBegin()
        pairs: Any = tuple(
            (
                WireField(index, key_type, _read_value(protocol, key_type)),
                WireField(index, value_type, _read_value(protocol, value_type)),
            )
            for index in range(size)
        )
        protocol.readMapEnd()
        return pairs
    raise ValueError(f"unsupported thrift type {thrift_type}")


def decode_message(payload: bytes) -> WireMessage:
    protocol = TBinaryProtocol(TMemoryBuffer(payload))
    name, message_type, sequence_id = protocol.readMessageBegin()
    fields = _read_struct(protocol)
    protocol.readMessageEnd()
    return WireMessage(name, message_type, sequence_id, fields)


def _write_struct(protocol: TBinaryProtocol, fields: tuple[WireField, ...]) -> None:
    protocol.writeStructBegin("anonymous")
    for field in fields:
        protocol.writeFieldBegin("", field.thrift_type, field.field_id)
        _write_value(protocol, field.thrift_type, field.value)
        protocol.writeFieldEnd()
    protocol.writeFieldStop()
    protocol.writeStructEnd()


def _write_value(protocol: TBinaryProtocol, thrift_type: int, value: Any) -> None:
    scalar = {
        TType.BOOL: protocol.writeBool,
        TType.BYTE: protocol.writeByte,
        TType.I16: protocol.writeI16,
        TType.I32: protocol.writeI32,
        TType.I64: protocol.writeI64,
        TType.DOUBLE: protocol.writeDouble,
    }
    if thrift_type in scalar:
        scalar[thrift_type](value)
        return
    if thrift_type == TType.STRING:
        protocol.writeBinary(value if isinstance(value, bytes) else str(value).encode())
        return
    if thrift_type == TType.STRUCT:
        _write_struct(protocol, tuple(value))
        return
    if thrift_type in (TType.LIST, TType.SET):
        elements = tuple(value)
        if not elements:
            raise ValueError("empty collection requires schema-supplied element type")
        element_type = elements[0].thrift_type
        if any(element.thrift_type != element_type for element in elements):
            raise ValueError("heterogeneous Thrift collection")
        if thrift_type == TType.LIST:
            protocol.writeListBegin(element_type, len(elements))
        else:
            protocol.writeSetBegin(element_type, len(elements))
        for element in elements:
            _write_value(protocol, element_type, element.value)
        protocol.writeListEnd() if thrift_type == TType.LIST else protocol.writeSetEnd()
        return
    if thrift_type == TType.MAP:
        pairs = tuple(value)
        if not pairs:
            raise ValueError("empty map requires schema-supplied key/value types")
        key_type, value_type = pairs[0][0].thrift_type, pairs[0][1].thrift_type
        if any(
            key.thrift_type != key_type or item.thrift_type != value_type for key, item in pairs
        ):
            raise ValueError("heterogeneous Thrift map")
        protocol.writeMapBegin(key_type, value_type, len(pairs))
        for key, item in pairs:
            _write_value(protocol, key_type, key.value)
            _write_value(protocol, value_type, item.value)
        protocol.writeMapEnd()
        return
    raise ValueError(f"unsupported thrift type {thrift_type}")


def encode_message(message: WireMessage) -> bytes:
    transport = TMemoryBuffer()
    protocol = TBinaryProtocol(transport)
    protocol.writeMessageBegin(message.name, message.message_type, message.sequence_id)
    _write_struct(protocol, message.fields)
    protocol.writeMessageEnd()
    return cast(bytes, transport.getvalue())
