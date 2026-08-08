"""Focused contracts for fleet-onboarding form schemas and allocators."""

from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, cast

import pytest
import voluptuous as vol
import voluptuous_serialize
from homeassistant.const import CONF_NAME
from homeassistant.helpers import config_validation as cv

from custom_components.brilliant_mqtt.broker import BrokerKind
from custom_components.brilliant_mqtt.const import (
    CONF_BROKER_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HOST,
    CONF_HUE_CA_CERT,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DEFAULT_VOICE_WAKE_WORD,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
)
from custom_components.brilliant_mqtt.flow import schemas as flow_schemas
from custom_components.brilliant_mqtt.flow.schemas import (
    ADVANCED_SECTION,
    BROKER_MENU_OPTIONS,
    DEFAULT_SSH_USERNAME,
    SECRET_UNCHANGED,
    FlowInputError,
    _has_c0_control_char,
    allocate_mesh_priority,
    allocate_panel_slug,
    broker_advanced_schema,
    broker_schema,
    control_char_errors,
    normalize_broker_input,
    normalize_panel_connect_input,
    normalize_panel_name,
    panel_confirm_schema,
    panel_connect_schema,
)


def _serialized(schema: vol.Schema) -> list[dict[str, Any]]:
    return cast(
        list[dict[str, Any]],
        voluptuous_serialize.convert(schema, custom_serializer=cv.custom_serializer),
    )


def _field(schema: vol.Schema, name: str) -> tuple[vol.Marker, object]:
    for marker, validator in schema.schema.items():
        if str(marker) == name:
            assert isinstance(marker, vol.Marker)
            return marker, validator
    raise AssertionError(f"missing schema field: {name}")


def _field_default(schema: vol.Schema, name: str) -> object:
    marker, _ = _field(schema, name)
    default = getattr(marker, "default", vol.UNDEFINED)
    return default() if callable(default) else default


def _valid_broker_input(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        CONF_MQTT_HOST: "broker.local",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "mqtt-secret",
        ADVANCED_SECTION: {
            CONF_MQTT_TLS_ENABLED: False,
        },
    }
    data.update(overrides)
    return data


def _valid_control_input(**overrides: object) -> dict[str, object]:
    data: dict[str, object] = {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "Brilliant",
        CONF_ROOM_OVERRIDES: "{}",
        CONF_HA_CONTROL_DOMAINS: ["light", "switch"],
        CONF_MAX_MIRRORED_ENTITIES: 50,
    }
    data.update(overrides)
    return data


def _scene_action() -> dict[str, object]:
    return {
        "domain": "light",
        "service": "turn_on",
        "target": {"entity_id": "light.office"},
        "data": {"brightness_pct": 60},
    }


def test_broker_menu_keeps_recommended_and_existing_paths_equal() -> None:
    assert BROKER_MENU_OPTIONS == (
        BrokerKind.OFFICIAL_MOSQUITTO.value,
        BrokerKind.EXISTING_BROKER.value,
    )


def test_official_broker_prefills_editable_reachable_host_and_port() -> None:
    schema = broker_schema(
        BrokerKind.OFFICIAL_MOSQUITTO,
        default_host="192.0.2.10",
    )

    assert _field_default(schema, CONF_MQTT_HOST) == "192.0.2.10"
    assert _field_default(schema, CONF_MQTT_PORT) == 1883
    submitted = schema(
        {
            CONF_MQTT_HOST: "mqtt.example.test",
            CONF_MQTT_PORT: 2883,
            CONF_MQTT_USERNAME: "fleet",
            CONF_MQTT_PASSWORD: "secret",
        }
    )
    assert submitted[CONF_MQTT_HOST] == "mqtt.example.test"
    assert submitted[CONF_MQTT_PORT] == 2883


def test_existing_broker_has_the_same_normal_fields_without_assuming_a_host() -> None:
    official = broker_schema(
        BrokerKind.OFFICIAL_MOSQUITTO,
        default_host="192.0.2.10",
    )
    existing = broker_schema(BrokerKind.EXISTING_BROKER)

    expected = {
        CONF_MQTT_HOST,
        CONF_MQTT_PORT,
        CONF_MQTT_USERNAME,
        CONF_MQTT_PASSWORD,
        ADVANCED_SECTION,
    }
    assert {str(marker) for marker in official.schema} == expected
    assert {str(marker) for marker in existing.schema} == expected
    existing_host, _ = _field(existing, CONF_MQTT_HOST)
    assert getattr(existing_host, "default", vol.UNDEFINED) is vol.UNDEFINED
    assert _field_default(existing, CONF_MQTT_PORT) == 1883


@pytest.mark.parametrize(
    "kind",
    [BrokerKind.OFFICIAL_MOSQUITTO, BrokerKind.EXISTING_BROKER],
)
def test_broker_advanced_section_is_shared_and_collapsed(kind: BrokerKind) -> None:
    schema = broker_schema(kind)
    serialized = {field["name"]: field for field in _serialized(schema)}
    advanced = serialized[ADVANCED_SECTION]

    assert advanced["type"] == "expandable"
    assert advanced["expanded"] is False
    assert {field["name"] for field in advanced["schema"]} == {
        CONF_MQTT_TLS_ENABLED,
        CONF_MQTT_TLS_CA,
    }
    password = serialized[CONF_MQTT_PASSWORD]
    assert password["selector"]["text"]["type"] == "password"


def test_advanced_schema_accepts_strict_tls_and_a_multiline_public_ca() -> None:
    schema = broker_advanced_schema()
    serialized = {field["name"]: field for field in _serialized(schema)}

    assert _field_default(schema, CONF_MQTT_TLS_ENABLED) is False
    assert serialized[CONF_MQTT_TLS_CA]["selector"]["text"]["multiline"] is True
    assert (
        schema(
            {
                CONF_MQTT_TLS_ENABLED: True,
                CONF_MQTT_TLS_CA: "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----",
            }
        )[CONF_MQTT_TLS_ENABLED]
        is True
    )
    with pytest.raises(vol.Invalid):
        schema({CONF_MQTT_TLS_ENABLED: 1})


def test_reconfigure_sentinel_only_appears_in_masked_password_field() -> None:
    source = {
        CONF_MQTT_HOST: "broker.local",
        CONF_MQTT_PORT: 8883,
        CONF_MQTT_USERNAME: "fleet",
        CONF_MQTT_PASSWORD: "RAW-STORED-SECRET",
        CONF_MQTT_TLS_ENABLED: True,
    }
    serialized = _serialized(broker_schema(BrokerKind.EXISTING_BROKER, source, reconfigure=True))
    encoded = json.dumps(serialized, sort_keys=True)
    fields = {field["name"]: field for field in serialized}

    assert "RAW-STORED-SECRET" not in encoded
    assert encoded.count(SECRET_UNCHANGED) == 1
    assert fields[CONF_MQTT_PASSWORD]["description"]["suggested_value"] == SECRET_UNCHANGED
    assert fields[CONF_MQTT_PASSWORD]["selector"]["text"]["type"] == "password"


def test_nested_advanced_retry_uses_the_same_outer_and_inner_defaults() -> None:
    ca = "-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----"
    source = _valid_broker_input(
        **{
            ADVANCED_SECTION: {
                CONF_MQTT_TLS_ENABLED: True,
                CONF_MQTT_TLS_CA: ca,
            }
        }
    )
    schema = broker_schema(BrokerKind.EXISTING_BROKER, source)
    serialized = {field["name"]: field for field in _serialized(schema)}
    advanced = serialized[ADVANCED_SECTION]

    assert advanced["default"] == {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: ca,
    }
    assert {field["name"]: field.get("default") for field in advanced["schema"]} == {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: ca,
    }
    assert schema(
        {
            CONF_MQTT_HOST: "broker.local",
            CONF_MQTT_PORT: 1883,
            CONF_MQTT_USERNAME: "fleet",
            CONF_MQTT_PASSWORD: "secret",
        }
    )[ADVANCED_SECTION] == {
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: ca,
    }


def test_new_broker_schema_never_serializes_a_supplied_password() -> None:
    serialized = _serialized(
        broker_schema(
            BrokerKind.EXISTING_BROKER,
            {CONF_MQTT_PASSWORD: "IN-PROGRESS-SECRET"},
        )
    )

    encoded = json.dumps(serialized, sort_keys=True)
    assert "IN-PROGRESS-SECRET" not in encoded
    assert SECRET_UNCHANGED not in encoded


def test_normalize_broker_input_flattens_shared_advanced_values() -> None:
    ca = "-----BEGIN CERTIFICATE-----\nCA\n-----END CERTIFICATE-----"
    normalized = normalize_broker_input(
        BrokerKind.OFFICIAL_MOSQUITTO,
        _valid_broker_input(
            **{
                CONF_MQTT_HOST: " Broker.Local ",
                ADVANCED_SECTION: {
                    CONF_MQTT_TLS_ENABLED: True,
                    CONF_MQTT_TLS_CA: ca,
                },
            }
        ),
    )

    assert normalized == {
        CONF_BROKER_KIND: BrokerKind.OFFICIAL_MOSQUITTO.value,
        CONF_MQTT_HOST: "broker.local",
        CONF_MQTT_PORT: 1883,
        CONF_MQTT_USERNAME: "brilliant",
        CONF_MQTT_PASSWORD: "mqtt-secret",
        CONF_MQTT_TLS_ENABLED: True,
        CONF_MQTT_TLS_CA: ca,
    }
    assert ADVANCED_SECTION not in normalized


def test_blank_custom_ca_is_not_persisted() -> None:
    normalized = normalize_broker_input(
        BrokerKind.EXISTING_BROKER,
        _valid_broker_input(
            **{
                ADVANCED_SECTION: {
                    CONF_MQTT_TLS_ENABLED: True,
                    CONF_MQTT_TLS_CA: "   ",
                }
            }
        ),
    )

    assert CONF_MQTT_TLS_CA not in normalized


def test_custom_ca_accepts_a_public_chain_with_crlf_and_blank_separator() -> None:
    ca = (
        "-----BEGIN CERTIFICATE-----\r\nPUBLIC-ONE\r\n"
        "-----END CERTIFICATE-----\r\n\r\n"
        "-----BEGIN CERTIFICATE-----\r\nPUBLIC-TWO\r\n"
        "-----END CERTIFICATE-----"
    )

    normalized = normalize_broker_input(
        BrokerKind.EXISTING_BROKER,
        _valid_broker_input(
            **{
                ADVANCED_SECTION: {
                    CONF_MQTT_TLS_ENABLED: True,
                    CONF_MQTT_TLS_CA: ca,
                }
            }
        ),
    )

    assert normalized[CONF_MQTT_TLS_CA] == ca


def test_private_key_ca_is_rejected_without_echoing_transient_secret_bytes() -> None:
    unsafe_ca = (
        "-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----\n"
        "-----BEGIN PRIVATE KEY-----\nTRANSIENT-PRIVATE-SECRET\n"
        "-----END PRIVATE KEY-----"
    )
    source = _valid_broker_input(
        **{
            ADVANCED_SECTION: {
                CONF_MQTT_TLS_ENABLED: True,
                CONF_MQTT_TLS_CA: unsafe_ca,
            }
        }
    )

    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(BrokerKind.EXISTING_BROKER, source)

    assert dict(raised.value.errors) == {CONF_MQTT_TLS_CA: "invalid_value"}
    assert "TRANSIENT-PRIVATE-SECRET" not in repr(raised.value)
    assert "TRANSIENT-PRIVATE-SECRET" not in str(raised.value)
    retry_schema = json.dumps(
        _serialized(broker_schema(BrokerKind.EXISTING_BROKER, source)),
        sort_keys=True,
    )
    assert "TRANSIENT-PRIVATE-SECRET" not in retry_schema
    assert "PRIVATE KEY" not in retry_schema


@pytest.mark.parametrize(
    "unsafe_ca",
    [
        "not a PEM certificate",
        "-----BEGIN CERTIFICATE-----\nmissing-end",
        "-----END CERTIFICATE-----\nmissing-begin",
        "-----BEGIN CERTIFICATE-----\nPUBLIC\tCA\n-----END CERTIFICATE-----",
        "-----BEGIN CERTIFICATE-----\nPUBLIC\x00CA\n-----END CERTIFICATE-----",
    ],
)
def test_custom_ca_requires_certificate_only_pem_syntax(unsafe_ca: str) -> None:
    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(
            BrokerKind.EXISTING_BROKER,
            _valid_broker_input(
                **{
                    ADVANCED_SECTION: {
                        CONF_MQTT_TLS_ENABLED: True,
                        CONF_MQTT_TLS_CA: unsafe_ca,
                    }
                }
            ),
        )

    assert dict(raised.value.errors) == {CONF_MQTT_TLS_CA: "invalid_value"}
    assert unsafe_ca not in repr(raised.value)


def test_custom_ca_requires_tls() -> None:
    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(
            BrokerKind.EXISTING_BROKER,
            _valid_broker_input(
                **{
                    ADVANCED_SECTION: {
                        CONF_MQTT_TLS_ENABLED: False,
                        CONF_MQTT_TLS_CA: "-----BEGIN CERTIFICATE-----\nCA",
                    }
                }
            ),
        )

    assert dict(raised.value.errors) == {CONF_MQTT_TLS_CA: "invalid_value"}


def test_reconfigure_sentinel_resolves_to_stored_password_not_itself() -> None:
    normalized = normalize_broker_input(
        BrokerKind.EXISTING_BROKER,
        _valid_broker_input(**{CONF_MQTT_PASSWORD: SECRET_UNCHANGED}),
        stored_password="stored-secret",
    )

    assert normalized[CONF_MQTT_PASSWORD] == "stored-secret"
    assert SECRET_UNCHANGED not in normalized.values()


@pytest.mark.parametrize("stored_password", [None, "", SECRET_UNCHANGED])
def test_sentinel_cannot_be_persisted_without_a_real_stored_password(
    stored_password: str | None,
) -> None:
    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(
            BrokerKind.EXISTING_BROKER,
            _valid_broker_input(**{CONF_MQTT_PASSWORD: SECRET_UNCHANGED}),
            stored_password=stored_password,
        )

    assert dict(raised.value.errors) == {CONF_MQTT_PASSWORD: "invalid_value"}
    assert SECRET_UNCHANGED not in repr(raised.value)


@pytest.mark.parametrize("password", ["", "   "])
def test_blank_broker_password_is_rejected(password: str) -> None:
    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(
            BrokerKind.EXISTING_BROKER,
            _valid_broker_input(**{CONF_MQTT_PASSWORD: password}),
        )

    assert dict(raised.value.errors) == {CONF_MQTT_PASSWORD: "invalid_value"}


def test_control_characters_are_rejected_before_host_trimming() -> None:
    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(
            BrokerKind.EXISTING_BROKER,
            _valid_broker_input(
                **{
                    CONF_MQTT_HOST: "broker.local\n",
                    CONF_MQTT_USERNAME: "fleet\x7f",
                    CONF_MQTT_PASSWORD: "secret\x00",
                }
            ),
        )

    assert dict(raised.value.errors) == {
        CONF_MQTT_HOST: "invalid_value",
        CONF_MQTT_USERNAME: "invalid_value",
        CONF_MQTT_PASSWORD: "invalid_value",
    }


def test_control_char_errors_is_field_scoped_and_value_safe() -> None:
    values: Mapping[str, object] = {
        "bad": "unsafe\u0085",
        "safe": "ordinary value",
        "not_text": 1,
    }

    assert control_char_errors(values, ("missing", "bad", "safe", "not_text")) == {
        "bad": "invalid_value"
    }


def test_control_char_detectors_split_c0_only_from_the_full_cc_category() -> None:
    """Onboarding rejects DEL/C1; the legacy reconfigure detector accepts them."""
    values: Mapping[str, object] = {
        "del": "value\u007f",
        "c1": "value\u0085",
        "c0": "value\u001f",
    }
    keys = ("del", "c1", "c0")

    assert control_char_errors(values, keys) == {
        "del": "invalid_value",
        "c1": "invalid_value",
        "c0": "invalid_value",
    }
    assert control_char_errors(values, keys, detector=_has_c0_control_char) == {
        "c0": "invalid_value"
    }


def test_broker_port_rejects_boolean_and_out_of_range_values() -> None:
    schema = broker_schema(BrokerKind.EXISTING_BROKER)
    for value in (True, 0, 65536):
        with pytest.raises(vol.Invalid):
            schema(
                {
                    CONF_MQTT_HOST: "broker.local",
                    CONF_MQTT_PORT: value,
                    CONF_MQTT_USERNAME: "fleet",
                    CONF_MQTT_PASSWORD: "secret",
                }
            )


def test_broker_port_rejects_decimal_beyond_python_integer_digit_limit() -> None:
    oversized_decimal = "9" * 5_000
    schema = broker_schema(BrokerKind.EXISTING_BROKER)
    submitted = {
        CONF_MQTT_HOST: "broker.local",
        CONF_MQTT_PORT: oversized_decimal,
        CONF_MQTT_USERNAME: "fleet",
        CONF_MQTT_PASSWORD: "secret",
    }

    with pytest.raises(vol.Invalid):
        schema(submitted)
    with pytest.raises(FlowInputError) as raised:
        normalize_broker_input(BrokerKind.EXISTING_BROKER, submitted)
    assert dict(raised.value.errors) == {CONF_MQTT_PORT: "invalid_value"}


def test_panel_connect_has_only_host_and_masked_root_password() -> None:
    schema = panel_connect_schema(
        {
            CONF_HOST: "192.0.2.20",
            CONF_ROOT_PASSWORD: "RAW-ROOT-SECRET",
            CONF_SSH_USERNAME: "admin",
        }
    )
    serialized = _serialized(schema)
    fields = {field["name"]: field for field in serialized}

    assert set(fields) == {CONF_HOST, CONF_ROOT_PASSWORD}
    assert fields[CONF_ROOT_PASSWORD]["selector"]["text"]["type"] == "password"
    assert _field_default(schema, CONF_HOST) == "192.0.2.20"
    assert "RAW-ROOT-SECRET" not in json.dumps(serialized, sort_keys=True)
    assert DEFAULT_SSH_USERNAME == "root"


def test_panel_connect_normalization_fixes_root_and_preserves_password_bytes() -> None:
    normalized = normalize_panel_connect_input(
        {
            CONF_HOST: " 192.0.2.20 ",
            CONF_ROOT_PASSWORD: " password with spaces ",
        }
    )

    assert normalized == {
        CONF_HOST: "192.0.2.20",
        CONF_SSH_USERNAME: "root",
        CONF_ROOT_PASSWORD: " password with spaces ",
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (CONF_HOST, "192.0.2.20\n"),
        (CONF_ROOT_PASSWORD, "secret\r"),
        (CONF_ROOT_PASSWORD, ""),
    ],
)
def test_panel_connect_rejects_invalid_values(field: str, value: str) -> None:
    user_input = {
        CONF_HOST: "192.0.2.20",
        CONF_ROOT_PASSWORD: "secret",
        field: value,
    }

    with pytest.raises(FlowInputError) as raised:
        normalize_panel_connect_input(user_input)

    assert dict(raised.value.errors) == {field: "invalid_value"}


def test_panel_confirmation_exposes_only_the_editable_discovered_name() -> None:
    schema = panel_confirm_schema("Office Panel")

    assert {str(marker) for marker in schema.schema} == {CONF_NAME}
    assert _field_default(schema, CONF_NAME) == "Office Panel"
    assert schema({})[CONF_NAME] == "Office Panel"


def test_panel_name_rejects_control_chars_before_trimming() -> None:
    with pytest.raises(FlowInputError) as raised:
        normalize_panel_name({CONF_NAME: " Office\n"})

    assert dict(raised.value.errors) == {CONF_NAME: "invalid_value"}


def test_panel_name_is_trimmed_and_bounded_for_the_provisioner() -> None:
    assert normalize_panel_name({CONF_NAME: " Office Panel "}) == "Office Panel"

    with pytest.raises(FlowInputError) as raised:
        normalize_panel_name({CONF_NAME: "é" * 129})
    assert dict(raised.value.errors) == {CONF_NAME: "invalid_value"}


@pytest.mark.parametrize(
    ("name", "expected"),
    [
        ("Office Bath", "office-bath"),
        ("Office___Bath", "office-bath"),
        ("Office ... Bath", "office-bath"),
        ("Café Office", "caf-office"),
        ("Panel 2", "panel-2"),
    ],
)
def test_slug_allocation_normalizes_non_alphanumerics(
    name: str,
    expected: str,
) -> None:
    assert allocate_panel_slug(name, ()) == expected


@pytest.mark.parametrize(
    ("name", "error"),
    [
        ("!!!", "invalid_name"),
        ("mesh", "reserved_panel"),
        (" MESH ", "reserved_panel"),
        ("unsafe\nname", "invalid_name"),
    ],
)
def test_slug_allocation_rejects_invalid_and_reserved_names(
    name: str,
    error: str,
) -> None:
    with pytest.raises(FlowInputError) as raised:
        allocate_panel_slug(name, ())

    assert dict(raised.value.errors) == {CONF_NAME: error}


def test_slug_allocation_truncates_and_preserves_the_64_character_bound_on_collision() -> None:
    base = "p" * 64
    existing = {base, ("p" * 62) + "-2"}
    before = set(existing)

    allocated = allocate_panel_slug(("P" * 80) + " ignored", existing)

    assert allocated == ("p" * 62) + "-3"
    assert len(allocated) == 64
    assert existing == before


def test_slug_allocation_uses_first_available_suffix() -> None:
    assert (
        allocate_panel_slug(
            "Office",
            {"office", "office-2", "office-4"},
        )
        == "office-3"
    )


@pytest.mark.parametrize(
    ("used", "expected"),
    [
        ((), 1),
        ((0,), 1),
        ((1, 2, 4), 3),
        ((3, 1), 2),
    ],
)
def test_mesh_priority_allocation_uses_smallest_unused_positive(
    used: tuple[int, ...],
    expected: int,
) -> None:
    assert allocate_mesh_priority(used) == expected


def test_mesh_priority_allocation_does_not_mutate_existing_values() -> None:
    used = [1, 3]

    assert allocate_mesh_priority(used) == 2
    assert used == [1, 3]


def test_mesh_priority_allocation_fails_when_supported_range_is_exhausted() -> None:
    with pytest.raises(FlowInputError) as raised:
        allocate_mesh_priority(range(1, 100))

    assert dict(raised.value.errors) == {"base": "mesh_priority_exhausted"}


def test_fleet_control_surface_owns_only_five_typed_globals() -> None:
    source = {
        CONF_HA_CONTROL_ENABLED: DEFAULT_HA_CONTROL_ENABLED,
        CONF_HA_CONTROL_LABEL: DEFAULT_HA_CONTROL_LABEL,
        CONF_ROOM_OVERRIDES: {"Kitchen": "Upstairs"},
        CONF_HA_CONTROL_DOMAINS: DEFAULT_HA_CONTROL_DOMAINS,
        CONF_MAX_MIRRORED_ENTITIES: DEFAULT_MAX_MIRRORED_ENTITIES,
        CONF_SCENE_PANEL: "panel-subentry-office",
        CONF_SCENE_ACTIONS: {"office:movie": _scene_action()},
    }

    schema = flow_schemas.fleet_control_schema(source)

    assert {str(marker) for marker in schema.schema} == {
        CONF_HA_CONTROL_ENABLED,
        CONF_HA_CONTROL_LABEL,
        CONF_ROOM_OVERRIDES,
        CONF_HA_CONTROL_DOMAINS,
        CONF_MAX_MIRRORED_ENTITIES,
    }
    assert _field_default(schema, CONF_ROOM_OVERRIDES) == '{"Kitchen":"Upstairs"}'
    assert flow_schemas.normalize_fleet_control_input(
        _valid_control_input(
            **{
                CONF_HA_CONTROL_LABEL: " Whole Home ",
                CONF_ROOM_OVERRIDES: '{" Kitchen ":" Upstairs "}',
                CONF_HA_CONTROL_DOMAINS: ["cover", "light"],
                CONF_MAX_MIRRORED_ENTITIES: "75",
            }
        )
    ) == {
        CONF_HA_CONTROL_ENABLED: True,
        CONF_HA_CONTROL_LABEL: "Whole Home",
        CONF_ROOM_OVERRIDES: {"Kitchen": "Upstairs"},
        CONF_HA_CONTROL_DOMAINS: ["light", "cover"],
        CONF_MAX_MIRRORED_ENTITIES: 75,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (CONF_HA_CONTROL_ENABLED, 1),
        (CONF_HA_CONTROL_LABEL, "é" * 129),
        (CONF_ROOM_OVERRIDES, "{" + (" " * (64 * 1024)) + "}"),
        (CONF_HA_CONTROL_DOMAINS, ["light", "light"]),
        (CONF_HA_CONTROL_DOMAINS, ["light", "sensor"]),
        (CONF_MAX_MIRRORED_ENTITIES, True),
        (CONF_MAX_MIRRORED_ENTITIES, 201),
    ],
)
def test_fleet_control_rejects_untyped_or_unbounded_values(
    field: str,
    value: object,
) -> None:
    submitted = _valid_control_input(**{field: value})

    with pytest.raises(FlowInputError) as raised:
        flow_schemas.normalize_fleet_control_input(submitted)

    assert dict(raised.value.errors) == {field: "invalid_value"}
    assert repr(value) not in repr(raised.value)


@pytest.mark.parametrize(
    "marker",
    ("\u001f", "\u007f", "\u0085", "\u009b"),
    ids=("c0-unit-separator", "del", "c1-nel", "c1-csi"),
)
def test_fleet_control_json_keeps_the_strict_cc_default_for_onboarding(
    marker: str,
) -> None:
    """The shared JSON decoders stay full-Cc for every non-reconfigure caller.

    The legacy reconfigure step opts into the narrower C0-only detector; nothing
    else does, so DEL and C1 must still be rejected here.
    """
    submitted = _valid_control_input(
        **{CONF_ROOM_OVERRIDES: json.dumps({"Office": f"Off{marker}ice"})}
    )

    with pytest.raises(FlowInputError) as raised:
        flow_schemas.normalize_fleet_control_input(submitted)

    assert dict(raised.value.errors) == {CONF_ROOM_OVERRIDES: "invalid_value"}


def test_fleet_scenes_separates_subentry_owners_from_action_panel_slugs() -> None:
    source = {
        CONF_SCENE_PANEL: "panel-subentry-office",
        CONF_SCENE_ACTIONS: {"office:movie": _scene_action()},
    }
    schema = flow_schemas.fleet_scenes_schema(
        source,
        panel_subentry_ids=("panel-subentry-office", "panel-subentry-kitchen"),
    )

    assert {str(marker) for marker in schema.schema} == {
        CONF_SCENE_PANEL,
        CONF_SCENE_ACTIONS,
    }
    assert _field_default(schema, CONF_SCENE_PANEL) == "panel-subentry-office"
    normalized = flow_schemas.normalize_fleet_scenes_input(
        {
            CONF_SCENE_PANEL: "panel-subentry-office",
            CONF_SCENE_ACTIONS: json.dumps({"office:movie": _scene_action()}),
        },
        panel_subentry_ids=("panel-subentry-office", "panel-subentry-kitchen"),
        panel_slugs=("office", "kitchen"),
    )
    assert normalized == source

    with pytest.raises(FlowInputError) as owner_error:
        flow_schemas.normalize_fleet_scenes_input(
            {
                CONF_SCENE_PANEL: "office",
                CONF_SCENE_ACTIONS: "{}",
            },
            panel_subentry_ids=("panel-subentry-office",),
            panel_slugs=("office",),
        )
    assert dict(owner_error.value.errors) == {CONF_SCENE_PANEL: "invalid_value"}

    with pytest.raises(FlowInputError) as action_error:
        flow_schemas.normalize_fleet_scenes_input(
            {
                CONF_SCENE_PANEL: "panel-subentry-office",
                CONF_SCENE_ACTIONS: json.dumps({"panel-subentry-office:movie": _scene_action()}),
            },
            panel_subentry_ids=("panel-subentry-office",),
            panel_slugs=("office",),
        )
    assert dict(action_error.value.errors) == {CONF_SCENE_ACTIONS: "invalid_value"}


def test_fleet_scene_actions_reject_unbounded_nested_data_without_echoing_it() -> None:
    deeply_nested: object = "TRANSIENT-SCENE-VALUE"
    for _ in range(14):
        deeply_nested = {"nested": deeply_nested}
    actions = {
        "office:movie": {
            **_scene_action(),
            "data": deeply_nested,
        }
    }

    with pytest.raises(FlowInputError) as raised:
        flow_schemas.normalize_fleet_scenes_input(
            {
                CONF_SCENE_PANEL: "panel-subentry-office",
                CONF_SCENE_ACTIONS: json.dumps(actions),
            },
            panel_subentry_ids=("panel-subentry-office",),
            panel_slugs=("office",),
        )

    assert dict(raised.value.errors) == {CONF_SCENE_ACTIONS: "invalid_value"}
    assert "TRANSIENT-SCENE-VALUE" not in repr(raised.value)


def test_fleet_defaults_are_strict_and_keep_existing_ranges() -> None:
    schema = flow_schemas.fleet_defaults_schema()

    assert {str(marker) for marker in schema.schema} == {
        OPT_AUTO_REPAIR,
        OPT_OFFLINE_GRACE_MINUTES,
        OPT_REPAIR_COOLDOWN_MINUTES,
    }
    assert _field_default(schema, OPT_AUTO_REPAIR) is DEFAULT_AUTO_REPAIR
    assert _field_default(schema, OPT_OFFLINE_GRACE_MINUTES) == DEFAULT_OFFLINE_GRACE_MINUTES
    assert _field_default(schema, OPT_REPAIR_COOLDOWN_MINUTES) == DEFAULT_REPAIR_COOLDOWN_MINUTES
    assert flow_schemas.normalize_fleet_defaults_input(
        {
            OPT_AUTO_REPAIR: False,
            OPT_OFFLINE_GRACE_MINUTES: "2",
            OPT_REPAIR_COOLDOWN_MINUTES: "1440",
        }
    ) == {
        OPT_AUTO_REPAIR: False,
        OPT_OFFLINE_GRACE_MINUTES: 2,
        OPT_REPAIR_COOLDOWN_MINUTES: 1440,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        (OPT_AUTO_REPAIR, 1),
        (OPT_OFFLINE_GRACE_MINUTES, 1),
        (OPT_OFFLINE_GRACE_MINUTES, 121),
        (OPT_REPAIR_COOLDOWN_MINUTES, 4),
        (OPT_REPAIR_COOLDOWN_MINUTES, 1441),
    ],
)
def test_fleet_defaults_reject_invalid_types_and_ranges(
    field: str,
    value: object,
) -> None:
    submitted: dict[str, object] = {
        OPT_AUTO_REPAIR: True,
        OPT_OFFLINE_GRACE_MINUTES: 10,
        OPT_REPAIR_COOLDOWN_MINUTES: 60,
        field: value,
    }

    with pytest.raises(FlowInputError) as raised:
        flow_schemas.normalize_fleet_defaults_input(submitted)

    assert dict(raised.value.errors) == {field: "invalid_value"}


def test_panel_rename_and_address_are_focused_bounded_inputs() -> None:
    rename = flow_schemas.panel_rename_schema(
        {
            CONF_NAME: "Office Panel",
            CONF_HOST: "192.0.2.20",
            CONF_ROOT_PASSWORD: "RAW-ROOT-SECRET",
        }
    )
    address = flow_schemas.panel_address_schema(
        {
            CONF_NAME: "Office Panel",
            CONF_HOST: "192.0.2.20",
            CONF_ROOT_PASSWORD: "RAW-ROOT-SECRET",
        }
    )

    assert {str(marker) for marker in rename.schema} == {CONF_NAME}
    assert {str(marker) for marker in address.schema} == {CONF_HOST}
    assert _field_default(rename, CONF_NAME) == "Office Panel"
    assert _field_default(address, CONF_HOST) == "192.0.2.20"
    assert flow_schemas.normalize_panel_rename_input({CONF_NAME: " Office "}) == {
        CONF_NAME: "Office"
    }
    assert flow_schemas.normalize_panel_address_input({CONF_HOST: " 192.0.2.21 "}) == {
        CONF_HOST: "192.0.2.21"
    }
    assert "RAW-ROOT-SECRET" not in repr(rename)
    assert "RAW-ROOT-SECRET" not in repr(address)


def test_panel_credential_and_rebind_passwords_are_masked_without_suggestions() -> None:
    credentials = flow_schemas.panel_ssh_credentials_schema()
    rebind = flow_schemas.panel_rebind_schema(
        {
            CONF_HOST: "192.0.2.20",
            CONF_ROOT_PASSWORD: "RAW-ROOT-SECRET",
            CONF_IDENTITY_FINGERPRINT: "SHA256:OLD-FINGERPRINT",
            CONF_SSH_HOST_KEY: "ssh-ed25519 RAW-PUBLIC-KEY",
        }
    )
    serialized = _serialized(credentials) + _serialized(rebind)
    fields = {field["name"]: field for field in serialized}
    encoded = json.dumps(serialized, sort_keys=True)

    assert {str(marker) for marker in credentials.schema} == {CONF_ROOT_PASSWORD}
    assert {str(marker) for marker in rebind.schema} == {CONF_HOST, CONF_ROOT_PASSWORD}
    assert fields[CONF_ROOT_PASSWORD]["selector"]["text"]["type"] == "password"
    assert "suggested_value" not in fields[CONF_ROOT_PASSWORD].get("description", {})
    assert "RAW-ROOT-SECRET" not in encoded
    assert "SHA256:OLD-FINGERPRINT" not in encoded
    assert "RAW-PUBLIC-KEY" not in encoded
    assert SECRET_UNCHANGED not in encoded
    assert flow_schemas.normalize_panel_ssh_credentials_input(
        {CONF_ROOT_PASSWORD: " password with spaces "}
    ) == {CONF_ROOT_PASSWORD: " password with spaces "}
    assert flow_schemas.normalize_panel_rebind_input(
        {
            CONF_HOST: " 192.0.2.21 ",
            CONF_ROOT_PASSWORD: " replacement secret ",
        }
    ) == {
        CONF_HOST: "192.0.2.21",
        CONF_ROOT_PASSWORD: " replacement secret ",
    }


@pytest.mark.parametrize(
    "identity_field",
    [CONF_IDENTITY_FINGERPRINT, CONF_SSH_HOST_KEY],
)
def test_panel_rebind_never_accepts_user_asserted_identity(
    identity_field: str,
) -> None:
    with pytest.raises(FlowInputError) as raised:
        flow_schemas.normalize_panel_rebind_input(
            {
                CONF_HOST: "192.0.2.21",
                CONF_ROOT_PASSWORD: "replacement secret",
                identity_field: "USER-ASSERTED-IDENTITY",
            }
        )

    assert dict(raised.value.errors) == {"base": "invalid_value"}
    assert "USER-ASSERTED-IDENTITY" not in repr(raised.value)


def test_panel_feature_overrides_are_typed_and_allowlisted() -> None:
    certificate = "-----BEGIN CERTIFICATE-----\nPUBLIC-CA\n-----END CERTIFICATE-----"
    schema = flow_schemas.panel_feature_overrides_schema(
        {
            CONF_VOICE_WAKE_WORD: "hey_jarvis",
            CONF_VOICE_HA_HOST: "ha.internal",
            CONF_HUE_CA_CERT: certificate,
            CONF_FEATURE_OVERRIDES: {"unexpected": True},
        }
    )

    assert {str(marker) for marker in schema.schema} == {
        CONF_VOICE_WAKE_WORD,
        CONF_VOICE_HA_HOST,
        CONF_HUE_CA_CERT,
    }
    assert _field_default(schema, CONF_VOICE_WAKE_WORD) == "hey_jarvis"
    assert _field_default(schema, CONF_VOICE_HA_HOST) == "ha.internal"
    assert flow_schemas.normalize_panel_feature_overrides_input(
        {
            CONF_VOICE_WAKE_WORD: "hey_mycroft",
            CONF_VOICE_HA_HOST: " ha.internal ",
            CONF_HUE_CA_CERT: certificate,
        }
    ) == {
        CONF_VOICE_WAKE_WORD: "hey_mycroft",
        CONF_VOICE_HA_HOST: "ha.internal",
        CONF_HUE_CA_CERT: certificate,
    }


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("unexpected", True),
        (CONF_VOICE_WAKE_WORD, "user_supplied_wake_word"),
        (CONF_VOICE_HA_HOST, "ha.internal\n"),
        (
            CONF_HUE_CA_CERT,
            "-----BEGIN PRIVATE KEY-----\nTRANSIENT-PRIVATE-SECRET\n-----END PRIVATE KEY-----",
        ),
    ],
)
def test_panel_feature_overrides_reject_unknown_or_unsafe_values(
    field: str,
    value: object,
) -> None:
    submitted: dict[str, object] = {
        CONF_VOICE_WAKE_WORD: DEFAULT_VOICE_WAKE_WORD,
        CONF_VOICE_HA_HOST: "",
        CONF_HUE_CA_CERT: "",
        field: value,
    }

    with pytest.raises(FlowInputError) as raised:
        flow_schemas.normalize_panel_feature_overrides_input(submitted)

    expected_field = field if field != "unexpected" else "base"
    assert dict(raised.value.errors) == {expected_field: "invalid_value"}
    assert repr(value) not in repr(raised.value)
