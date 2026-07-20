"""Environment contract tests for the Brilliant BLE observer."""

from __future__ import annotations

import json
from collections.abc import Mapping

import pytest

from brilliant_ble_observer.config import MAX_EVENTS_PER_SECOND, Settings
from brilliant_ble_observer.model import AllowlistEntry


def _required_env(**overrides: str) -> dict[str, str]:
    values = {
        "BRILLIANT_PANEL": "shed",
        "MQTT_HOST": "mqtt.iot.joyful.house",
        "MQTT_USERNAME": "brilliant-shed",
        "MQTT_PASSWORD": "not-a-real-password",
    }
    values.update(overrides)
    return values


def test_defaults_are_safe_and_observer_is_off() -> None:
    settings = Settings.from_env(_required_env())

    assert settings.enabled is False
    assert settings.allowlist == ()
    assert settings.adapter == "hci0"
    assert settings.max_events_per_second == 10.0
    assert settings.log_level == "INFO"
    assert settings.mqtt_port == 1883


def test_all_environment_values_are_parsed() -> None:
    allowlist = [
        {"address": "aa-bb-cc-dd-ee-ff"},
        {
            "ibeacon_uuid": "00112233-4455-6677-8899-aabbccddeeff",
            "ibeacon_major": 66,
            "ibeacon_minor": 7,
        },
    ]
    settings = Settings.from_env(
        _required_env(
            MQTT_PORT="8883",
            BLE_OBSERVER_ENABLED="yes",
            BLE_OBSERVER_ALLOWLIST_JSON=json.dumps(allowlist),
            BLE_OBSERVER_ADAPTER="hci12",
            BLE_OBSERVER_MAX_EVENTS_PER_SECOND="2.5",
            BLE_OBSERVER_LOG_LEVEL="debug",
        )
    )

    assert settings.enabled is True
    assert settings.allowlist == (
        AllowlistEntry(address="AA:BB:CC:DD:EE:FF"),
        AllowlistEntry(
            ibeacon_uuid="00112233-4455-6677-8899-aabbccddeeff",
            ibeacon_major=66,
            ibeacon_minor=7,
        ),
    )
    assert settings.adapter == "hci12"
    assert settings.max_events_per_second == 2.5
    assert settings.log_level == "DEBUG"
    assert settings.mqtt_port == 8883


def test_settings_repr_redacts_password_and_private_allowlist() -> None:
    password = "SENTINEL-BROKER-PASSWORD"
    address = "AA:BB:CC:DD:EE:FF"
    ibeacon_uuid = "00112233-4455-6677-8899-aabbccddeeff"
    settings = Settings.from_env(
        _required_env(
            MQTT_PASSWORD=password,
            BLE_OBSERVER_ALLOWLIST_JSON=json.dumps(
                [
                    {"address": address},
                    {
                        "ibeacon_uuid": ibeacon_uuid,
                        "ibeacon_major": 66,
                        "ibeacon_minor": 7,
                    },
                ]
            ),
        )
    )

    rendered = repr(settings)
    assert "Settings(" in rendered
    assert password not in rendered
    assert address not in rendered
    assert ibeacon_uuid not in rendered
    assert "mqtt_password=" not in rendered
    assert "allowlist=" not in rendered


@pytest.mark.parametrize(
    "missing", ["BRILLIANT_PANEL", "MQTT_HOST", "MQTT_USERNAME", "MQTT_PASSWORD"]
)
def test_required_broker_contract_is_enforced(missing: str) -> None:
    env = _required_env()
    del env[missing]

    with pytest.raises(KeyError, match=missing):
        Settings.from_env(env)


@pytest.mark.parametrize("raw", ["", "enabled", "2", "truthy"])
def test_invalid_enable_boolean_fails_loudly(raw: str) -> None:
    with pytest.raises(ValueError, match="BLE_OBSERVER_ENABLED"):
        Settings.from_env(_required_env(BLE_OBSERVER_ENABLED=raw))


@pytest.mark.parametrize(
    "raw",
    [
        "not-json",
        "{}",
        '[{"address":"AA:BB:CC:DD:EE:FF","address":"11:22:33:44:55:66"}]',
        '[{"address":"not-an-address"}]',
    ],
)
def test_invalid_allowlist_json_fails_loudly(raw: str) -> None:
    with pytest.raises(ValueError, match="BLE_OBSERVER_ALLOWLIST_JSON"):
        Settings.from_env(_required_env(BLE_OBSERVER_ALLOWLIST_JSON=raw))


@pytest.mark.parametrize("slug", ["", "Shed", "shed/loft", "mesh", "a" * 64])
def test_invalid_panel_slug_fails_loudly(slug: str) -> None:
    with pytest.raises(ValueError, match="BRILLIANT_PANEL"):
        Settings.from_env(_required_env(BRILLIANT_PANEL=slug))


@pytest.mark.parametrize("adapter", ["", "hci", "hci-0", "wlan0", "hci0/../../x"])
def test_invalid_adapter_name_fails_loudly(adapter: str) -> None:
    with pytest.raises(ValueError, match="BLE_OBSERVER_ADAPTER"):
        Settings.from_env(_required_env(BLE_OBSERVER_ADAPTER=adapter))


@pytest.mark.parametrize("rate", ["0", "-1", "nan", "inf", "101", "not-a-number"])
def test_invalid_event_rate_fails_loudly(rate: str) -> None:
    with pytest.raises(ValueError, match="BLE_OBSERVER_MAX_EVENTS_PER_SECOND"):
        Settings.from_env(_required_env(BLE_OBSERVER_MAX_EVENTS_PER_SECOND=rate))


def test_maximum_event_rate_is_accepted() -> None:
    settings = Settings.from_env(
        _required_env(BLE_OBSERVER_MAX_EVENTS_PER_SECOND=str(MAX_EVENTS_PER_SECOND))
    )
    assert settings.max_events_per_second == MAX_EVENTS_PER_SECOND


@pytest.mark.parametrize("log_level", ["", "TRACE", "verbose", "INFO\nDEBUG"])
def test_invalid_log_level_fails_loudly(log_level: str) -> None:
    with pytest.raises(ValueError, match="BLE_OBSERVER_LOG_LEVEL"):
        Settings.from_env(_required_env(BLE_OBSERVER_LOG_LEVEL=log_level))


@pytest.mark.parametrize("port", ["0", "65536", "not-a-port"])
def test_invalid_mqtt_port_fails_loudly(port: str) -> None:
    with pytest.raises(ValueError, match="MQTT_PORT"):
        Settings.from_env(_required_env(MQTT_PORT=port))


@pytest.mark.parametrize("field", ["MQTT_HOST", "MQTT_USERNAME", "MQTT_PASSWORD"])
def test_empty_required_broker_value_fails_loudly(field: str) -> None:
    with pytest.raises(ValueError, match=field):
        Settings.from_env(_required_env(**{field: ""}))


def test_from_env_uses_process_environment_when_mapping_is_omitted(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    env: Mapping[str, str] = _required_env()
    for key, value in env.items():
        monkeypatch.setenv(key, value)
    for key in (
        "BLE_OBSERVER_ENABLED",
        "BLE_OBSERVER_ALLOWLIST_JSON",
        "BLE_OBSERVER_ADAPTER",
        "BLE_OBSERVER_MAX_EVENTS_PER_SECOND",
        "BLE_OBSERVER_LOG_LEVEL",
    ):
        monkeypatch.delenv(key, raising=False)

    assert Settings.from_env().panel == "shed"
