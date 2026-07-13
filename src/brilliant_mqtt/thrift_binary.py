"""Bounded decoder for base64-encoded Thrift TBinaryProtocol structs.

Brilliant stores several configuration and execution records as encoded Thrift
values on the bus. This module deliberately has no dependency on generated
Thrift types, so the records can be reduced off-panel. Inputs are untrusted:
all reads and collection growth are checked against explicit budgets.
"""

from __future__ import annotations

import base64
import binascii
import struct
from typing import cast

_STOP = 0
_BOOL = 2
_BYTE = 3
_DOUBLE = 4
_I16 = 6
_I32 = 8
_I64 = 10
_STRING = 11
_STRUCT = 12
_MAP = 13
_SET = 14
_LIST = 15
_SUPPORTED_TYPES = frozenset(
    {_BOOL, _BYTE, _DOUBLE, _I16, _I32, _I64, _STRING, _STRUCT, _MAP, _SET, _LIST}
)


class ThriftDecodeError(ValueError):
    """A malformed or over-budget encoded Thrift value."""


class _Cursor:
    def __init__(self, data: bytes, *, max_depth: int, max_items: int) -> None:
        self._data = data
        self.position = 0
        self._max_depth = max_depth
        self._remaining_items = max_items

    def _read_exact(self, size: int) -> bytes:
        end = self.position + size
        if end > len(self._data):
            raise ThriftDecodeError("truncated thrift value")
        value = self._data[self.position : end]
        self.position = end
        return value

    def _read_u8(self) -> int:
        return self._read_exact(1)[0]

    def _read_i8(self) -> int:
        return cast(int, struct.unpack(">b", self._read_exact(1))[0])

    def _read_i16(self) -> int:
        return cast(int, struct.unpack(">h", self._read_exact(2))[0])

    def _read_i32(self) -> int:
        return cast(int, struct.unpack(">i", self._read_exact(4))[0])

    def _read_i64(self) -> int:
        return cast(int, struct.unpack(">q", self._read_exact(8))[0])

    def _read_double(self) -> float:
        return cast(float, struct.unpack(">d", self._read_exact(8))[0])

    def _check_depth(self, depth: int) -> None:
        if depth > self._max_depth:
            raise ThriftDecodeError("thrift value exceeds depth limit")

    def _consume_items(self, count: int) -> None:
        if count > self._remaining_items:
            raise ThriftDecodeError("thrift value exceeds item limit")
        self._remaining_items -= count

    @staticmethod
    def _validate_type(field_type: int) -> None:
        if field_type not in _SUPPORTED_TYPES:
            raise ThriftDecodeError(f"unsupported thrift type {field_type}")

    def read_struct(self, *, depth: int) -> dict[int, object]:
        self._check_depth(depth)
        result: dict[int, object] = {}
        while True:
            field_type = self._read_u8()
            if field_type == _STOP:
                return result
            self._validate_type(field_type)
            field_id = self._read_i16()
            self._consume_items(1)
            result[field_id] = self._read_value(field_type, depth=depth)

    def _read_value(self, field_type: int, *, depth: int) -> object:
        if field_type == _BOOL:
            return self._read_i8() == 1
        if field_type == _BYTE:
            return self._read_i8()
        if field_type == _DOUBLE:
            return self._read_double()
        if field_type == _I16:
            return self._read_i16()
        if field_type == _I32:
            return self._read_i32()
        if field_type == _I64:
            return self._read_i64()
        if field_type == _STRING:
            return self._read_string()
        if field_type == _STRUCT:
            return self.read_struct(depth=depth + 1)
        if field_type == _MAP:
            return self._read_map(depth=depth + 1)
        if field_type in {_SET, _LIST}:
            return self._read_sequence(depth=depth + 1)
        raise ThriftDecodeError(f"unsupported thrift type {field_type}")

    def _read_string(self) -> str | bytes:
        length = self._read_i32()
        if length < 0:
            raise ThriftDecodeError("negative thrift length")
        raw = self._read_exact(length)
        try:
            return raw.decode("utf-8")
        except UnicodeDecodeError:
            return raw

    def _read_map(self, *, depth: int) -> dict[object, object]:
        self._check_depth(depth)
        key_type = self._read_u8()
        value_type = self._read_u8()
        self._validate_type(key_type)
        self._validate_type(value_type)
        count = self._read_i32()
        if count < 0:
            raise ThriftDecodeError("negative thrift collection size")
        self._consume_items(count)

        result: dict[object, object] = {}
        for _ in range(count):
            key = self._read_value(key_type, depth=depth)
            try:
                hash(key)
            except TypeError as exc:
                raise ThriftDecodeError("invalid thrift map key") from exc
            value = self._read_value(value_type, depth=depth)
            result[key] = value
        return result

    def _read_sequence(self, *, depth: int) -> list[object]:
        self._check_depth(depth)
        element_type = self._read_u8()
        self._validate_type(element_type)
        count = self._read_i32()
        if count < 0:
            raise ThriftDecodeError("negative thrift collection size")
        self._consume_items(count)

        result: list[object] = []
        for _ in range(count):
            result.append(self._read_value(element_type, depth=depth))
        return result


def decode_struct_base64(
    value: str,
    *,
    max_bytes: int = 262_144,
    max_depth: int = 16,
    max_items: int = 10_000,
) -> dict[int, object]:
    """Decode one base64 TBinaryProtocol struct within caller-supplied limits."""
    limits = (max_bytes, max_depth, max_items)
    if any(isinstance(limit, bool) or not isinstance(limit, int) or limit < 0 for limit in limits):
        raise ThriftDecodeError("thrift limits must be non-negative integers")
    if not isinstance(value, str):
        raise ThriftDecodeError("invalid base64 thrift value")

    # Reject obviously over-budget input before base64 decoding allocates its
    # output. With validation enabled, every four encoded bytes produce at most
    # three decoded bytes.
    max_encoded_bytes = 4 * ((max_bytes + 2) // 3)
    if len(value) > max_encoded_bytes:
        raise ThriftDecodeError("thrift value exceeds byte limit")
    try:
        raw = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError) as exc:
        raise ThriftDecodeError("invalid base64 thrift value") from exc
    if len(raw) > max_bytes:
        raise ThriftDecodeError("thrift value exceeds byte limit")

    cursor = _Cursor(raw, max_depth=max_depth, max_items=max_items)
    try:
        result = cursor.read_struct(depth=0)
    except RecursionError as exc:
        raise ThriftDecodeError("thrift value exceeds depth limit") from exc
    if cursor.position != len(raw):
        raise ThriftDecodeError("trailing bytes after thrift struct")
    return result
