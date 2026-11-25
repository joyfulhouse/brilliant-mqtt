"""Broker hosts that a panel on the LAN cannot resolve."""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt.broker_host import (
    format_broker_endpoint,
    panel_unreachable_broker_host_reason,
)


@pytest.mark.parametrize(
    ("host", "reason"),
    [
        ("core-mosquitto", "addon_internal_name"),
        (" Core-Mosquitto ", "addon_internal_name"),
        ("core-ssh", "addon_internal_name"),
        ("a0d7b954-mosquitto", "addon_internal_name"),
        ("A0D7B954-esphome", "addon_internal_name"),
        ("homeassistant.local.hass.io", "hass_internal_dns"),
        ("LOCAL.HASS.IO", "hass_internal_dns"),
        ("localhost", "loopback"),
        ("localhost.", "loopback"),
        ("127.0.0.1", "loopback"),
        ("127.1.2.3", "loopback"),
        ("::1", "loopback"),
        ("[::1]", "loopback"),
    ],
)
def test_panel_unreachable_broker_hosts(host: str, reason: str) -> None:
    assert panel_unreachable_broker_host_reason(host) == reason


@pytest.mark.parametrize(
    "host",
    [
        "192.0.2.10",
        "mqtt.iot.example",
        "broker.local",
        "mqtt",
        "broker",
        "mqtt-broker",
        "my-broker",
        "b0c1d2e3-addon",
        "core-mosquitto.lan",
        "2001:db8::1",
    ],
)
def test_lan_broker_hosts_are_not_classified_unreachable(host: str) -> None:
    assert panel_unreachable_broker_host_reason(host) is None


def test_format_broker_endpoint_redacts_nothing_but_rejects_unsafe_text() -> None:
    assert format_broker_endpoint("mqtt.iot.example", 1883) == "mqtt.iot.example:1883"
    assert format_broker_endpoint("mqtt-broker", 1883) == "mqtt-broker:1883"
    assert format_broker_endpoint("2001:db8::10", 8883) == "[2001:db8::10]:8883"
    assert format_broker_endpoint("user:secret@mqtt.example", 1883) == ""
    assert format_broker_endpoint("mqtt.example", 0) == ""


def test_format_broker_endpoint_omits_malformed_diagnostic_text() -> None:
    """Query, fragment, and extra-colon host text must not survive into diagnostics."""
    from custom_components.brilliant_mqtt.errors import OperationError, OperationStage

    sentinel = "broker.example?token=synthetic-sentinel"
    malformed = (
        sentinel,
        "broker.example#fragment",
        "broker.example:1883",
        "http://broker.example",
        "broker.example/path",
    )
    for host in malformed:
        assert format_broker_endpoint(host, 1883) == ""
        error = OperationError.for_code(
            OperationStage.FLEET_AUTH,
            "broker_connect_failed",
        ).with_connect_context(
            broker_endpoint=format_broker_endpoint(host, 1883),
            connect_cause="dns",
        )
        surface = f"{error.redacted_dict()!r}"
        assert host not in surface
        assert error.redacted_dict()["broker_endpoint"] == ""
    assert sentinel not in format_broker_endpoint("mqtt.iot.example", 1883)
