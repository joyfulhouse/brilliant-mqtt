"""Pure, bounded Thrift slider configuration codec shared with recovery checks."""

from __future__ import annotations

import base64
import binascii
import struct

_T_STOP = 0
_T_BOOL = 2
_T_BYTE = 3
_T_DOUBLE = 4
_T_I16 = 6
_T_I32 = 8
_T_I64 = 10
_T_STRING = 11
_T_STRUCT = 12
_T_MAP = 13
_T_SET = 14
_T_LIST = 15

_MAX_SLIDER_INDEX = 255
_MAX_IDENTIFIER_BYTES = 1024
_MAX_ENCODED_BYTES = 64 * 1024


def encode_slider_config(
    device_id: str,
    peripheral_id: str,
    *,
    slider_index: int = 0,
    double_tap_scene_id: str = "",
) -> str:
    """Build a base64-encoded Thrift ``TBinaryProtocol`` ``CapTouchSliderConfig`` string.

    Wire layout verified against the panel's ``CapTouchSliderConfig`` Thrift struct
    and ``tools/brilliant_vc/slider_binding.py``:
    - field 1 (``i32``): ``slider_index`` (0..255)
    - field 2 (``string``): primary target ``device_id``
    - field 3 (``string``): primary target ``peripheral_id``
    - field 5 (``string``, optional): ``double_tap_scene_id``
    - field 6 (``i32``): ``action`` = 0 (``LIGHTS``)
    - field 7 (``struct`` -> field 1 ``list<struct>`` of length 1): ``devices``
    """
    if not device_id and not peripheral_id:
        return ""
    if not device_id or not peripheral_id:
        raise ValueError("device_id and peripheral_id must both be non-empty")
    if not 0 <= slider_index <= _MAX_SLIDER_INDEX:
        raise ValueError(f"slider_index must be 0..{_MAX_SLIDER_INDEX}, got {slider_index}")

    dev_bytes = device_id.encode("utf-8")
    per_bytes = peripheral_id.encode("utf-8")
    buf = bytearray()
    # Field 1 (i32): slider_index
    buf += b"\x08\x00\x01" + slider_index.to_bytes(4, "big", signed=True)
    # Field 2 (string): primary target device_id
    buf += b"\x0b\x00\x02" + len(dev_bytes).to_bytes(4, "big") + dev_bytes
    # Field 3 (string): primary target peripheral_id
    buf += b"\x0b\x00\x03" + len(per_bytes).to_bytes(4, "big") + per_bytes
    # Field 5 (string, optional): double_tap_scene_id
    if double_tap_scene_id:
        dt_bytes = double_tap_scene_id.encode("utf-8")
        buf += b"\x0b\x00\x05" + len(dt_bytes).to_bytes(4, "big") + dt_bytes
    # Field 6 (i32): action = 0 (LIGHTS)
    buf += b"\x08\x00\x06\x00\x00\x00\x00"
    # Field 7 (struct -> field 1 list<struct> of length 1): devices = [(device_id, peripheral_id)]
    buf += b"\x0c\x00\x07\x0f\x00\x01\x0c\x00\x00\x00\x01"
    buf += b"\x0b\x00\x01" + len(dev_bytes).to_bytes(4, "big") + dev_bytes
    buf += b"\x0b\x00\x02" + len(per_bytes).to_bytes(4, "big") + per_bytes
    # Stop bytes for inner struct, outer struct, and root CapTouchSliderConfig
    buf += b"\x00\x00\x00"
    return base64.b64encode(bytes(buf)).decode("ascii")


def decode_slider_config_target(payload_b64: str) -> tuple[str, str] | None:
    """Decode ``(device_id, peripheral_id)`` from a base64 Thrift ``CapTouchSliderConfig``.

    Returns ``None`` if the payload is empty, malformed, or lacks required target fields.
    """
    if not isinstance(payload_b64, str) or not payload_b64 or len(payload_b64) > _MAX_ENCODED_BYTES:
        return None
    try:
        raw = base64.b64decode(payload_b64, validate=True)
    except (binascii.Error, ValueError):
        return None
    if not raw or len(raw) > _MAX_ENCODED_BYTES:
        return None

    offset = 0

    def _take(n: int) -> bytes:
        nonlocal offset
        if n < 0 or offset + n > len(raw):
            raise ValueError("truncated slider_config")
        chunk = raw[offset : offset + n]
        offset += n
        return chunk

    def _read_i32() -> int:
        return int(struct.unpack(">i", _take(4))[0])

    def _read_identifier() -> str:
        length = _read_i32()
        if not 0 < length <= _MAX_IDENTIFIER_BYTES:
            raise ValueError("invalid identifier length")
        val = _take(length).decode("utf-8")
        if any(ord(ch) < 32 or ord(ch) == 127 for ch in val):
            raise ValueError("control character in identifier")
        return val

    def _read_count() -> int:
        count = _read_i32()
        if not 0 <= count <= 4096:
            raise ValueError("invalid collection count")
        return count

    def _skip(ftype: int, depth: int = 0) -> None:
        if depth > 16:
            raise ValueError("nesting too deep")
        if ftype in (_T_BOOL, _T_BYTE):
            _take(1)
        elif ftype == _T_I16:
            _take(2)
        elif ftype == _T_I32:
            _take(4)
        elif ftype in (_T_DOUBLE, _T_I64):
            _take(8)
        elif ftype == _T_STRING:
            _take(_read_count())
        elif ftype == _T_STRUCT:
            while True:
                nested_type = int(struct.unpack(">b", _take(1))[0])
                if nested_type == _T_STOP:
                    break
                _take(2)
                _skip(nested_type, depth + 1)
        elif ftype == _T_MAP:
            ktype = int(struct.unpack(">b", _take(1))[0])
            vtype = int(struct.unpack(">b", _take(1))[0])
            for _ in range(_read_count()):
                _skip(ktype, depth + 1)
                _skip(vtype, depth + 1)
        elif ftype in (_T_SET, _T_LIST):
            etype = int(struct.unpack(">b", _take(1))[0])
            for _ in range(_read_count()):
                _skip(etype, depth + 1)
        else:
            raise ValueError(f"unknown Thrift field type {ftype}")

    try:
        seen_fields: set[int] = set()
        slider_index: int | None = None
        device_id: str | None = None
        peripheral_id: str | None = None

        while True:
            field_type = int(struct.unpack(">b", _take(1))[0])
            if field_type == _T_STOP:
                break
            field_id = int(struct.unpack(">h", _take(2))[0])
            if field_id in seen_fields:
                return None
            seen_fields.add(field_id)
            if field_id == 1:
                if field_type != _T_I32:
                    return None
                slider_index = _read_i32()
            elif field_id == 2:
                if field_type != _T_STRING:
                    return None
                device_id = _read_identifier()
            elif field_id == 3:
                if field_type != _T_STRING:
                    return None
                peripheral_id = _read_identifier()
            elif field_id == 6:
                if field_type != _T_I32:
                    return None
                _read_i32()
            else:
                _skip(field_type, 0)

        if offset != len(raw):
            return None
        if slider_index is None or device_id is None or peripheral_id is None:
            return None
        if not 0 <= slider_index <= _MAX_SLIDER_INDEX:
            return None
        return (device_id, peripheral_id)
    except Exception:
        return None
