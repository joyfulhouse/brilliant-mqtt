"""Pure legacy-to-fleet migration planner: eligibility, conflicts, redaction."""

from __future__ import annotations

import ast
import datetime
import inspect
import itertools
import json
from functools import cache
from pathlib import Path
from types import MappingProxyType
from typing import Any

import asyncssh
import pytest
from cryptography import x509
from cryptography.hazmat.primitives import hashes, serialization
from cryptography.hazmat.primitives.asymmetric import ec
from cryptography.x509.oid import NameOID
from homeassistant.config_entries import ConfigSubentry
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt import migration
from custom_components.brilliant_mqtt.const import (
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_ROOM_OVERRIDES,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONFIG_ENTRY_VERSION,
    DOMAIN,
    ENTRY_KIND_FLEET,
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    FLEET_UNIQUE_ID,
    SUBENTRY_TYPE_PANEL,
)
from custom_components.brilliant_mqtt.entry_data import FleetConfig, PanelConfig
from custom_components.brilliant_mqtt.migration import (
    LegacySnapshot,
    MigrationConflictReport,
    MigrationPlan,
    plan_migration,
    planned_subentry_id,
)

FLEET_PASSWORD = "test-fleet-password"
FLEET_USERNAME = "test-fleet-username"
ROOT_PASSWORD = "test-root-password"

# Entry IDs deliberately sort differently from titles and input order.
KITCHEN_ID = "01KITCHEN000000000000000000"
OFFICE_ID = "01OFFICE0000000000000000000"
BEDROOM_ID = "01BEDROOM000000000000000000"


@cache
def host_key(name: str, algorithm: str = "ssh-ed25519") -> str:
    """A real, canonical OpenSSH public key, generated once per name."""
    key = (
        asyncssh.generate_private_key(algorithm, key_size=2048)
        if algorithm == "ssh-rsa"
        else (asyncssh.generate_private_key(algorithm))
    )
    del name
    return key.export_public_key().decode().strip()


def fingerprint(public_key: str) -> str:
    return str(asyncssh.import_public_key(public_key).get_fingerprint("sha256"))


@cache
def ca_pem(name: str) -> str:
    """A self-signed public CA certificate (never a private key)."""
    key = ec.generate_private_key(ec.SECP256R1())
    subject = x509.Name([x509.NameAttribute(NameOID.COMMON_NAME, name)])
    now = datetime.datetime(2026, 1, 1, tzinfo=datetime.UTC)
    certificate = (
        x509.CertificateBuilder()
        .subject_name(subject)
        .issuer_name(subject)
        .public_key(key.public_key())
        .serial_number(1)
        .not_valid_before(now)
        .not_valid_after(now + datetime.timedelta(days=3650))
        .add_extension(x509.BasicConstraints(ca=True, path_length=None), critical=True)
        .sign(key, hashes.SHA256())
    )
    return certificate.public_bytes(serialization.Encoding.PEM).decode()


def legacy_data(
    panel: str,
    host: str,
    mesh_priority: int,
    **overrides: Any,
) -> dict[str, Any]:
    """One current-version legacy entry in the production layout."""
    data: dict[str, Any] = {
        "components": {
            "bridge": True,
            "bus_watchdog": True,
            "ha_mirror": False,
            "hue_ca": False,
            "voice": False,
            "wifi_watchdog": False,
        },
        "entry_kind": ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
        "ha_control_domains": ["light", "switch"],
        "ha_control_enabled": False,
        "ha_control_label": "brilliant",
        "host": host,
        "max_mirrored_entities": 50,
        "mesh_priority": mesh_priority,
        "mqtt_host": "192.0.2.1",
        "mqtt_password": FLEET_PASSWORD,
        "mqtt_port": 1883,
        "mqtt_username": FLEET_USERNAME,
        "panel": panel,
        "room_overrides": {},
        "root_password": ROOT_PASSWORD,
        "scene_actions": {},
        "scene_panel": "office",
        "ssh_host_key": host_key(panel),
        "voice_ha_host": "",
        "voice_wake_word": "okay_nabu",
    }
    data.update(overrides)
    return data


def legacy_options(**overrides: Any) -> dict[str, Any]:
    return {
        "auto_repair": True,
        "offline_grace_minutes": 10,
        "repair_cooldown_minutes": 60,
        "trust_host_key_changes": False,
        **overrides,
    }


def snapshot(
    entry_id: str,
    title: str,
    data: dict[str, Any],
    options: dict[str, Any] | None = None,
    *,
    version: int = CONFIG_ENTRY_VERSION,
    disabled: bool = False,
) -> LegacySnapshot:
    return LegacySnapshot(
        entry_id=entry_id,
        title=title,
        unique_id=str(data.get("panel")),
        version=version,
        disabled=disabled,
        data=MappingProxyType(data),
        options=MappingProxyType(legacy_options() if options is None else options),
    )


def fixture() -> list[LegacySnapshot]:
    """Kitchen, Office, Bedroom: documentation hosts, mesh priorities 1/2/3."""
    return [
        snapshot(KITCHEN_ID, "Brilliant kitchen", legacy_data("kitchen", "192.0.2.10", 1)),
        snapshot(OFFICE_ID, "Brilliant office", legacy_data("office", "192.0.2.11", 2)),
        snapshot(BEDROOM_ID, "Brilliant bedroom", legacy_data("bedroom", "192.0.2.12", 3)),
    ]


def replace_data(snapshots: list[LegacySnapshot], entry_id: str, **changes: Any) -> None:
    for index, item in enumerate(snapshots):
        if item.entry_id == entry_id:
            snapshots[index] = snapshot(
                item.entry_id,
                item.title,
                {**item.data, **changes},
                dict(item.options),
            )


def replace_options(snapshots: list[LegacySnapshot], entry_id: str, **changes: Any) -> None:
    for index, item in enumerate(snapshots):
        if item.entry_id == entry_id:
            snapshots[index] = snapshot(
                item.entry_id, item.title, dict(item.data), {**item.options, **changes}
            )


def eligible(snapshots: list[LegacySnapshot]) -> MigrationPlan:
    result = plan_migration(snapshots)
    assert isinstance(result, MigrationPlan), (
        result.summary() if isinstance(result, MigrationConflictReport) else result
    )
    return result


def conflicts(snapshots: list[LegacySnapshot]) -> list[tuple[str, str, tuple[str, ...]]]:
    result = plan_migration(snapshots)
    assert isinstance(result, MigrationConflictReport)
    return [(item.code, item.field, item.entry_ids) for item in result.conflicts]


def plan_entry(plan: MigrationPlan) -> MockConfigEntry:
    """Materialize a plan exactly as the coordinator writes it."""
    return MockConfigEntry(
        domain=DOMAIN,
        title=migration.FLEET_TITLE,
        unique_id=FLEET_UNIQUE_ID,
        version=CONFIG_ENTRY_VERSION,
        data=dict(plan.fleet_data),
        options=dict(plan.fleet_options),
        subentries_data=[
            ConfigSubentry(
                data=MappingProxyType(dict(panel.data)),
                subentry_id=panel.subentry_id,
                subentry_type=SUBENTRY_TYPE_PANEL,
                title=panel.title,
                unique_id=panel.fingerprint,
            ).as_dict()
            for panel in plan.panels
        ],
    )


def test_eligible_fleet_preserves_identity_and_round_trips_fleet_parsers() -> None:
    plan = eligible(fixture())

    assert plan.anchor_entry_id == BEDROOM_ID == min(KITCHEN_ID, OFFICE_ID, BEDROOM_ID)
    assert plan.candidate_ids == (BEDROOM_ID, KITCHEN_ID, OFFICE_ID)
    assert plan.sibling_ids == (KITCHEN_ID, OFFICE_ID)
    office = planned_subentry_id(OFFICE_ID)
    assert dict(plan.fleet_data) == {
        "entry_kind": ENTRY_KIND_FLEET,
        "broker_kind": "existing_broker",
        "mqtt_host": "192.0.2.1",
        "mqtt_port": 1883,
        "mqtt_username": FLEET_USERNAME,
        "mqtt_password": FLEET_PASSWORD,
        "mqtt_tls_enabled": False,
        "next_mesh_priority": 4,
        "ha_control_enabled": False,
        "ha_control_label": "brilliant",
        "room_overrides": {},
        "ha_control_domains": ["light", "switch"],
        "max_mirrored_entities": 50,
        "scene_panel": office,
        "scene_actions": {},
        "schema_version": CONFIG_ENTRY_VERSION,
    }
    assert dict(plan.fleet_options) == {
        "auto_repair": True,
        "offline_grace_minutes": 10,
        "repair_cooldown_minutes": 60,
    }
    by_entry = {panel.entry_id: panel for panel in plan.panels}
    kitchen = by_entry[KITCHEN_ID]
    assert dict(kitchen.data) == {
        "identity_fingerprint": fingerprint(host_key("kitchen")),
        "ssh_host_key": host_key("kitchen"),
        "host": "192.0.2.10",
        "ssh_username": "root",
        "root_password": ROOT_PASSWORD,
        "name": "Brilliant kitchen",
        "panel": "kitchen",
        "management_id": KITCHEN_ID,
        "components": {
            "bridge": True,
            "bus_watchdog": True,
            "ha_mirror": False,
            "hue_ca": False,
            "voice": False,
            "wifi_watchdog": False,
        },
        "feature_overrides": {"voice_ha_host": "", "voice_wake_word": "okay_nabu"},
        "mesh_priority": 1,
    }
    assert {panel.entry_id: panel.mesh_priority for panel in plan.panels} == {
        KITCHEN_ID: 1,
        OFFICE_ID: 2,
        BEDROOM_ID: 3,
    }
    assert all(panel.subentry_id == planned_subentry_id(panel.entry_id) for panel in plan.panels)

    entry = plan_entry(plan)
    fleet = FleetConfig.from_entry(entry)
    assert fleet.scene_panel == office
    assert fleet.offline_grace_minutes == 10
    parsed = {
        subentry_id: PanelConfig.from_subentry(subentry)
        for subentry_id, subentry in entry.subentries.items()
    }
    assert {config.management_id for config in parsed.values()} == {
        KITCHEN_ID,
        OFFICE_ID,
        BEDROOM_ID,
    }
    assert {config.panel for config in parsed.values()} == {"kitchen", "office", "bedroom"}


def test_snapshot_from_entry_detaches_entry_values() -> None:
    data = legacy_data("kitchen", "192.0.2.10", 1)
    entry = MockConfigEntry(
        domain=DOMAIN,
        entry_id=KITCHEN_ID,
        title="Brilliant kitchen",
        unique_id="kitchen",
        version=CONFIG_ENTRY_VERSION,
        data=data,
        options=legacy_options(),
    )
    taken = LegacySnapshot.from_entry(entry)
    data["components"]["bridge"] = False

    assert taken.entry_id == KITCHEN_ID
    assert taken.title == "Brilliant kitchen"
    assert taken.unique_id == "kitchen"
    assert taken.disabled is False
    assert taken.data["components"]["bridge"] is True


def test_every_input_order_produces_the_identical_plan_and_digest() -> None:
    reference = eligible(fixture())
    for permutation in itertools.permutations(fixture()):
        plan = eligible(list(permutation))
        assert plan.plan_digest == reference.plan_digest
        assert dict(plan.fleet_data) == dict(reference.fleet_data)
        assert plan.panels == reference.panels
        assert plan.summary() == reference.summary()


def test_digest_changes_with_structure_but_never_encodes_secret_values() -> None:
    reference = eligible(fixture())
    changed = fixture()
    for item in changed:
        replace_data(changed, item.entry_id, mqtt_password="another-fleet-password")
    assert eligible(changed).plan_digest == reference.plan_digest

    moved = fixture()
    replace_data(moved, KITCHEN_ID, host="192.0.2.99")
    assert eligible(moved).plan_digest != reference.plan_digest


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("mqtt_host", "192.0.2.2"),
        ("mqtt_port", 1884),
        ("mqtt_username", "other-username"),
        ("mqtt_password", "other-password"),
        ("broker_kind", "official_mosquitto"),
        (CONF_HA_CONTROL_ENABLED, True),
        (CONF_HA_CONTROL_LABEL, "panels"),
        (CONF_ROOM_OVERRIDES, {"light.desk": "Office"}),
        (CONF_HA_CONTROL_DOMAINS, ["light", "lock"]),
        (CONF_MAX_MIRRORED_ENTITIES, 20),
        (CONF_SCENE_PANEL, "kitchen"),
        (
            CONF_SCENE_ACTIONS,
            {
                "office:movie": {
                    "domain": "light",
                    "service": "turn_off",
                    "target": {},
                    "data": {},
                }
            },
        ),
    ],
)
def test_each_global_conflicts_independently(field: str, value: object) -> None:
    snapshots = fixture()
    replace_data(snapshots, KITCHEN_ID, **{field: value})

    assert conflicts(snapshots) == [("global_mismatch", field, (KITCHEN_ID,))]


def test_tls_enabled_and_ca_conflict_independently() -> None:
    tls = {CONF_MQTT_TLS_ENABLED: True, "mqtt_port": 8883, CONF_MQTT_TLS_CA: ca_pem("fleet")}
    snapshots = fixture()
    for item in list(snapshots):
        replace_data(snapshots, item.entry_id, **tls)
    plan = eligible(snapshots)
    assert plan.fleet_data[CONF_MQTT_TLS_CA] == ca_pem("fleet")
    assert plan.summary()["fleet"]["has_custom_ca"] is True

    ca_changed = list(snapshots)
    replace_data(ca_changed, OFFICE_ID, **{CONF_MQTT_TLS_CA: ca_pem("other")})
    assert conflicts(ca_changed) == [("global_mismatch", CONF_MQTT_TLS_CA, (OFFICE_ID,))]

    tls_changed = fixture()
    replace_data(tls_changed, OFFICE_ID, **{CONF_MQTT_TLS_ENABLED: True})
    assert conflicts(tls_changed) == [("global_mismatch", CONF_MQTT_TLS_ENABLED, (OFFICE_ID,))]


def test_ca_line_endings_are_normalized_before_comparison() -> None:
    lf = ca_pem("fleet")
    crlf = lf.replace("\n", "\r\n") + "\r\n"
    snapshots = fixture()
    for item in list(snapshots):
        replace_data(
            snapshots,
            item.entry_id,
            **{
                CONF_MQTT_TLS_ENABLED: True,
                CONF_MQTT_TLS_CA: crlf if item.entry_id == OFFICE_ID else lf,
            },
        )
    plan = eligible(snapshots)
    assert plan.fleet_data[CONF_MQTT_TLS_CA] == lf


def test_invalid_or_private_ca_is_rejected_without_echoing_it() -> None:
    snapshots = fixture()
    for item in list(snapshots):
        replace_data(
            snapshots,
            item.entry_id,
            **{CONF_MQTT_TLS_ENABLED: True, CONF_MQTT_TLS_CA: "-----BEGIN PRIVATE KEY-----x"},
        )
    found = conflicts(snapshots)
    assert {code for code, _field, _entries in found} == {"invalid_value"}
    assert {field for _code, field, _entries in found} == {CONF_MQTT_TLS_CA}


def test_duplicate_mesh_priority_is_reported_not_fixed() -> None:
    snapshots = fixture()
    replace_data(snapshots, BEDROOM_ID, mesh_priority=2)
    result = plan_migration(snapshots)

    assert isinstance(result, MigrationConflictReport)
    assert [(item.code, item.field, item.entry_ids, item.detail) for item in result.conflicts] == [
        ("duplicate_mesh_priority", "mesh_priority", (BEDROOM_ID, OFFICE_ID), 2)
    ]
    summary = result.summary()
    assert summary["conflicts"][0]["entries"] == [
        {"entry_id": BEDROOM_ID, "title": "Brilliant bedroom"},
        {"entry_id": OFFICE_ID, "title": "Brilliant office"},
    ]


def test_zero_mesh_priorities_may_repeat() -> None:
    snapshots = fixture()
    for item in list(snapshots):
        replace_data(snapshots, item.entry_id, mesh_priority=0)
    replace_data(snapshots, OFFICE_ID, mesh_priority=1)
    plan = eligible(snapshots)

    assert plan.fleet_data["next_mesh_priority"] == 2


@pytest.mark.parametrize(
    ("change", "expected"),
    [
        ({"ssh_host_key": None}, ("missing_host_key", "ssh_host_key")),
        ({"ssh_host_key": ""}, ("missing_host_key", "ssh_host_key")),
        ({"ssh_host_key": "ssh-ed25519 AAAAnotakey"}, ("invalid_host_key", "ssh_host_key")),
        ({"ssh_host_key": "ecdsa-sha2-nistp256 AAAA"}, ("invalid_host_key", "ssh_host_key")),
        ({"host": "192.0.2.10,evil"}, ("invalid_value", "host")),
        ({"root_password": ""}, ("invalid_value", "root_password")),
        ({"panel": "Kitchen Panel"}, ("invalid_panel_slug", "panel")),
        ({"panel": "mesh"}, ("invalid_panel_slug", "panel")),
        ({"ssh_username": "admin"}, ("unsupported_ssh_username", "ssh_username")),
        ({"mesh_priority": -1}, ("invalid_value", "mesh_priority")),
        ({"hot_poll_seconds": 61}, ("invalid_value", "hot_poll_seconds")),
        ({"components": {"bridge": "yes"}}, ("invalid_value", "components")),
        (
            {"ha_mirror_ws_url": "ws://192.0.2.5"},
            ("ha_mirror_retirement_pending", "ha_mirror_ws_url"),
        ),
        (
            {"provisioning_transaction_id": "x"},
            ("provisioning_marker_present", "provisioning_transaction_id"),
        ),
        ({"mystery_field": 1}, ("unsupported_legacy_field", "mystery_field")),
    ],
)
def test_panel_defects_are_reported_per_entry(
    change: dict[str, Any], expected: tuple[str, str]
) -> None:
    snapshots = fixture()
    replace_data(snapshots, KITCHEN_ID, **change)

    assert (*expected, (KITCHEN_ID,)) in conflicts(snapshots)


def test_missing_host_key_key_absent_is_a_conflict() -> None:
    snapshots = fixture()
    data = dict(snapshots[0].data)
    del data["ssh_host_key"]
    snapshots[0] = snapshot(KITCHEN_ID, "Brilliant kitchen", data)

    assert conflicts(snapshots) == [("missing_host_key", "ssh_host_key", (KITCHEN_ID,))]


def test_ha_mirror_component_enabled_blocks_until_retired() -> None:
    snapshots = fixture()
    replace_data(
        snapshots,
        KITCHEN_ID,
        components={"bridge": True, "ha_mirror": True},
    )
    assert conflicts(snapshots) == [("ha_mirror_retirement_pending", "components", (KITCHEN_ID,))]


def test_duplicate_fingerprint_and_duplicate_slug() -> None:
    snapshots = fixture()
    replace_data(snapshots, OFFICE_ID, ssh_host_key=host_key("kitchen"))
    assert conflicts(snapshots) == [
        ("duplicate_fingerprint", "ssh_host_key", (KITCHEN_ID, OFFICE_ID)),
    ]

    snapshots = fixture()
    replace_data(snapshots, BEDROOM_ID, panel="kitchen")
    result = plan_migration(snapshots)
    assert isinstance(result, MigrationConflictReport)
    assert [(item.code, item.entry_ids, item.detail) for item in result.conflicts] == [
        ("duplicate_slug", (BEDROOM_ID, KITCHEN_ID), "kitchen"),
    ]


def test_trust_host_key_changes_true_anywhere_is_a_conflict() -> None:
    snapshots = fixture()
    replace_options(snapshots, BEDROOM_ID, trust_host_key_changes=True)

    assert conflicts(snapshots) == [
        ("trust_host_key_changes_enabled", "options.trust_host_key_changes", (BEDROOM_ID,)),
    ]


def test_unknown_scene_panel_is_a_conflict() -> None:
    snapshots = fixture()
    for item in list(snapshots):
        replace_data(snapshots, item.entry_id, scene_panel="garage")

    assert conflicts(snapshots) == [
        ("unknown_scene_panel", "scene_panel", (BEDROOM_ID, KITCHEN_ID, OFFICE_ID)),
    ]


def test_per_panel_resilience_differences_become_feature_overrides() -> None:
    snapshots = fixture()
    replace_options(snapshots, OFFICE_ID, offline_grace_minutes=30, auto_repair=False)
    plan = eligible(snapshots)

    assert dict(plan.fleet_options) == {
        "auto_repair": True,
        "offline_grace_minutes": 10,
        "repair_cooldown_minutes": 60,
    }
    by_entry = {panel.entry_id: panel for panel in plan.panels}
    assert dict(by_entry[OFFICE_ID].data[CONF_FEATURE_OVERRIDES]) == {
        "auto_repair": False,
        "offline_grace_minutes": 30,
        "voice_ha_host": "",
        "voice_wake_word": "okay_nabu",
    }
    assert "offline_grace_minutes" not in by_entry[KITCHEN_ID].data[CONF_FEATURE_OVERRIDES]
    entry = plan_entry(plan)
    office = PanelConfig.from_subentry(entry.subentries[planned_subentry_id(OFFICE_ID)])
    assert office.feature_overrides["offline_grace_minutes"] == 30


def test_fleet_resilience_value_is_the_majority_with_deterministic_tie() -> None:
    snapshots = fixture()
    replace_options(snapshots, KITCHEN_ID, repair_cooldown_minutes=90)
    replace_options(snapshots, OFFICE_ID, repair_cooldown_minutes=90)
    assert eligible(snapshots).fleet_options["repair_cooldown_minutes"] == 90

    tied = fixture()[:2]
    replace_options(tied, OFFICE_ID, repair_cooldown_minutes=90)
    plan = eligible(tied)
    assert plan.fleet_options["repair_cooldown_minutes"] == 60  # KITCHEN_ID < OFFICE_ID


def test_invalid_resilience_option_is_reported() -> None:
    snapshots = fixture()
    replace_options(snapshots, KITCHEN_ID, offline_grace_minutes=1)

    assert conflicts(snapshots) == [
        ("invalid_value", "options.offline_grace_minutes", (KITCHEN_ID,)),
    ]


def test_missing_globals_use_the_legacy_runtime_defaults() -> None:
    snapshots = fixture()
    bare = {
        key: value
        for key, value in snapshots[0].data.items()
        if key
        not in {
            CONF_HA_CONTROL_ENABLED,
            CONF_HA_CONTROL_LABEL,
            CONF_ROOM_OVERRIDES,
            CONF_HA_CONTROL_DOMAINS,
            CONF_MAX_MIRRORED_ENTITIES,
            CONF_SCENE_ACTIONS,
            "voice_ha_host",
            "voice_wake_word",
        }
    }
    snapshots[0] = snapshot(KITCHEN_ID, "Brilliant kitchen", bare, {})
    plan = eligible(snapshots)

    by_entry = {panel.entry_id: panel for panel in plan.panels}
    assert dict(by_entry[KITCHEN_ID].data[CONF_FEATURE_OVERRIDES]) == {}


def test_missing_scene_panel_defaults_to_each_entry_slug_and_conflicts() -> None:
    snapshots = fixture()
    for index, item in enumerate(list(snapshots)):
        data = {key: value for key, value in item.data.items() if key != CONF_SCENE_PANEL}
        snapshots[index] = snapshot(item.entry_id, item.title, data)

    found = conflicts(snapshots)
    assert [(code, field) for code, field, _entries in found] == [
        ("global_mismatch", CONF_SCENE_PANEL)
    ]


def test_production_shaped_entries_keep_panel_values_and_drop_runtime_caches() -> None:
    hue_ca = ca_pem("hue")
    office = legacy_data(
        "office",
        "192.0.2.21",
        1,
        hot_poll_seconds=5,
        resync_seconds=900,
        ha_mirror_retire_verified=True,
        last_firmware="v26.10.21.1",
        hue_ca_cert=hue_ca,
        ssh_host_key=host_key("office-rsa", "ssh-rsa"),
    )
    office["components"] = {**office["components"], "hue_ca": True}
    bath = legacy_data(
        "master-bath",
        "192.0.2.22",
        0,
        last_firmware="v26.10.21.1",
        hue_ca_cert=hue_ca,
    )
    bath["components"] = {**bath["components"], "wifi_watchdog": True}
    plan = eligible(
        [
            snapshot("01OFFICE", "Brilliant office", office),
            snapshot("01BATH", "Brilliant master-bath", bath),
        ]
    )

    by_entry = {panel.entry_id: panel for panel in plan.panels}
    office_data = dict(by_entry["01OFFICE"].data)
    assert office_data["hot_poll_seconds"] == 5
    assert office_data["resync_seconds"] == 900
    assert office_data["ssh_host_key"] == host_key("office-rsa", "ssh-rsa")
    assert office_data["identity_fingerprint"] == fingerprint(host_key("office-rsa", "ssh-rsa"))
    assert "last_firmware" not in office_data
    assert "ha_mirror_retire_verified" not in office_data
    assert office_data[CONF_FEATURE_OVERRIDES]["hue_ca_cert"] == hue_ca
    assert office_data["components"]["hue_ca"] is True
    assert by_entry["01BATH"].data["components"]["wifi_watchdog"] is True
    assert plan.anchor_entry_id == "01BATH"
    assert plan.fleet_data["next_mesh_priority"] == 2
    assert plan.fleet_data[CONF_SCENE_PANEL] == planned_subentry_id("01OFFICE")


def test_production_mesh_duplicate_office_and_shed_is_reported() -> None:
    entries = [
        snapshot("01OFFICE", "Brilliant office", legacy_data("office", "192.0.2.21", 1)),
        snapshot("01SHED", "Brilliant shed", legacy_data("shed", "192.0.2.22", 1)),
        snapshot("01STARRY", "Brilliant starry", legacy_data("starry", "192.0.2.23", 0)),
    ]
    result = plan_migration(entries)

    assert isinstance(result, MigrationConflictReport)
    assert result.summary()["conflicts"] == [
        {
            "code": "duplicate_mesh_priority",
            "field": "mesh_priority",
            "entries": [
                {"entry_id": "01OFFICE", "title": "Brilliant office"},
                {"entry_id": "01SHED", "title": "Brilliant shed"},
            ],
            "detail": 1,
        }
    ]


def test_entry_level_ineligibility() -> None:
    assert conflicts([]) == [("no_legacy_entries", "entry_kind", ())]

    single = eligible(fixture()[1:2])
    assert single.anchor_entry_id == OFFICE_ID
    assert single.sibling_ids == ()

    with_fleet = [*fixture(), snapshot("00FLEET", "Brilliant MQTT", {"entry_kind": "fleet"})]
    assert ("fleet_entry_present", "entry_kind", ("00FLEET",)) in conflicts(with_fleet)

    old = fixture()
    old[0] = snapshot(KITCHEN_ID, "Brilliant kitchen", dict(old[0].data), version=3)
    assert ("unsupported_entry_version", "version", (KITCHEN_ID,)) in conflicts(old)

    disabled = fixture()
    disabled[0] = snapshot(KITCHEN_ID, "Brilliant kitchen", dict(disabled[0].data), disabled=True)
    assert conflicts(disabled) == [("entry_disabled", "disabled_by", (KITCHEN_ID,))]


def test_conflicts_are_sorted_and_deterministic() -> None:
    snapshots = fixture()
    replace_data(snapshots, KITCHEN_ID, mqtt_password="other", ha_control_label="x")
    replace_options(snapshots, OFFICE_ID, trust_host_key_changes=True)
    reference = plan_migration(snapshots)
    for permutation in itertools.permutations(snapshots):
        assert plan_migration(list(permutation)) == reference
    assert isinstance(reference, MigrationConflictReport)
    assert [item.field for item in reference.conflicts] == sorted(
        item.field for item in reference.conflicts
    )


def _secret_values() -> list[str]:
    return [
        FLEET_PASSWORD,
        FLEET_USERNAME,
        ROOT_PASSWORD,
        "other-password",
        "other-username",
        *(host_key(name).split()[1] for name in ("kitchen", "office", "bedroom")),
        ca_pem("fleet").splitlines()[1],
        ca_pem("other").splitlines()[1],
    ]


def test_reports_and_summaries_never_contain_secret_values() -> None:
    plan = eligible(fixture())
    rendered = [json.dumps(plan.summary()), repr(plan), repr(plan.panels)]

    tls = {CONF_MQTT_TLS_ENABLED: True, CONF_MQTT_TLS_CA: ca_pem("fleet")}
    snapshots = fixture()
    for item in list(snapshots):
        replace_data(snapshots, item.entry_id, **tls)
    replace_data(
        snapshots,
        KITCHEN_ID,
        mqtt_password="other-password",
        mqtt_username="other-username",
        mqtt_tls_ca=ca_pem("other"),
        root_password="",
    )
    replace_data(snapshots, OFFICE_ID, ssh_host_key=host_key("kitchen"))
    report = plan_migration(snapshots)
    assert isinstance(report, MigrationConflictReport)
    rendered += [json.dumps(report.summary()), repr(report), repr(fixture())]

    for text in rendered:
        for secret in _secret_values():
            assert secret not in text


def test_planner_is_pure_and_synchronous() -> None:
    assert not inspect.iscoroutinefunction(plan_migration)
    source = Path(inspect.getfile(migration)).read_text(encoding="utf-8")
    tree = ast.parse(source)
    assert not any(isinstance(node, ast.AsyncFunctionDef) for node in ast.walk(tree))
    names = {node.id for node in ast.walk(tree) if isinstance(node, ast.Name)}
    attributes = {node.attr for node in ast.walk(tree) if isinstance(node, ast.Attribute)}
    assert "hass" not in names
    assert not attributes & {
        "async_update_entry",
        "async_remove",
        "async_add_subentry",
        "async_update_device",
        "async_update_entity",
        "open",
        "read_text",
        "write_text",
    }
