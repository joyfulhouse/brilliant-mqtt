"""Shared MQTT payload decoding helpers."""

from __future__ import annotations


def decode_mqtt_payload(payload: object) -> str:
    """Return an MQTT payload as strict UTF-8 text."""
    if isinstance(payload, (bytes, bytearray)):
        return payload.decode("utf-8", errors="strict")
    if isinstance(payload, str):
        return payload
    raise TypeError("MQTT payload must be text or bytes")
