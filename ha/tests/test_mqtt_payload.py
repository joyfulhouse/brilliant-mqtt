"""Shared MQTT payload decoding contract."""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt.mqtt_payload import decode_mqtt_payload


def test_decode_mqtt_payload_handles_text_and_bytes_strictly() -> None:
    assert decode_mqtt_payload("online") == "online"
    assert decode_mqtt_payload(b"online") == "online"
    assert decode_mqtt_payload(bytearray(b"online")) == "online"
    with pytest.raises(UnicodeDecodeError):
        decode_mqtt_payload(b"\xff")
