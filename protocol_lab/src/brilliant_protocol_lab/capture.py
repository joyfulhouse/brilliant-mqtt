from __future__ import annotations

from dataclasses import dataclass

_STRICT_BINARY_VERSION = 0x80010000
_VERSION_MASK = 0xFFFF0000


@dataclass(frozen=True)
class TransportClassification:
    framing: str
    protocol: str
    tls: bool | str


def _is_binary_header(data: bytes) -> bool:
    return (
        len(data) >= 4
        and (int.from_bytes(data[:4], "big") & _VERSION_MASK) == _STRICT_BINARY_VERSION
    )


def classify_transport(data: bytes) -> TransportClassification:
    if len(data) >= 3 and data[0] == 0x16 and data[1] == 0x03:
        return TransportClassification("unknown", "unknown", True)
    if len(data) >= 8:
        frame_size = int.from_bytes(data[:4], "big")
        if frame_size == len(data) - 4 and _is_binary_header(data[4:]):
            return TransportClassification("framed", "binary", False)
    if _is_binary_header(data):
        return TransportClassification("unframed", "binary", False)
    if data[:1] == b"\x82":
        return TransportClassification("unknown", "compact", False)
    return TransportClassification("unknown", "unknown", "unknown")
