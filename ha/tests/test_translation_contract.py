"""Translation contracts for code-owned Brilliant MQTT flow keys."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from custom_components.brilliant_mqtt import errors as operation_errors
from custom_components.brilliant_mqtt import panel_inspection, panel_provisioner, shell
from custom_components.brilliant_mqtt.flow.schemas import BROKER_MENU_OPTIONS

_REPOSITORY_ROOT = Path(__file__).parents[2]
_STRINGS_PATH = _REPOSITORY_ROOT / "custom_components/brilliant_mqtt/strings.json"
_EN_PATH = _REPOSITORY_ROOT / "custom_components/brilliant_mqtt/translations/en.json"


def _strings() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(_STRINGS_PATH.read_text(encoding="utf-8")))


def _assert_nonempty_keys(
    values: dict[str, Any],
    required: set[str],
) -> None:
    """Require every code-owned key to have nonempty English copy."""
    missing = required - values.keys()

    assert not missing
    assert all(isinstance(values[key], str) and values[key].strip() for key in required)


def test_panel_subentry_translates_every_reachable_onboarding_error() -> None:
    """Every stable panel onboarding failure has user-facing recovery copy."""
    errors = _strings()["config_subentries"]["panel"]["error"]
    required = (
        shell._IDENTITY_ERROR_CODES
        | panel_inspection._ERROR_CODES
        | panel_provisioner._ERROR_CODES
        | operation_errors._ERROR_METADATA.keys()
    )
    missing = required - errors.keys()

    assert not missing


def test_broker_menu_and_fallback_error_have_complete_translation_contract() -> None:
    """Every code-owned menu/error result has a nonempty user-facing label."""
    config = _strings()["config"]
    labels = config["step"]["user"]["menu_options"]

    assert set(BROKER_MENU_OPTIONS) <= labels.keys()
    menu_labels = [labels[option] for option in BROKER_MENU_OPTIONS]
    assert all(isinstance(label, str) and label.strip() for label in menu_labels)
    assert "recommended" in labels["official_mosquitto"].casefold()
    assert "external" in labels["existing_broker"].casefold()
    assert "fully supported" in labels["existing_broker"].casefold()
    assert config["error"]["broker_validation_failed"].strip()


def test_fleet_options_translate_every_reachable_surface() -> None:
    """Fleet-owned menus, forms, fields, and progress all have usable copy."""
    options = _strings()["options"]
    steps = options["step"]
    required_fields = {
        "broker_profile": {
            "mqtt_host",
            "mqtt_port",
            "mqtt_username",
            "mqtt_password",
        },
        "ha_control": {
            "ha_control_enabled",
            "ha_control_label",
            "room_overrides",
            "ha_control_domains",
            "max_mirrored_entities",
        },
        "scenes": {"scene_panel", "scene_actions"},
        "fleet_defaults": {
            "auto_repair",
            "offline_grace_minutes",
            "repair_cooldown_minutes",
        },
    }
    required_steps = {
        "init",
        "broker",
        "official_mosquitto",
        "existing_broker",
        "broker_profile",
        "broker_validation",
        "broker_commit",
        "ha_control",
        "scenes",
        "fleet_defaults",
        "advanced",
        "mesh_priorities",
    }

    assert not required_steps - steps.keys()
    for step_id in required_steps:
        _assert_nonempty_keys(steps[step_id], {"title", "description"})
    assert set(steps["init"]["menu_options"]) == {
        "broker",
        "ha_control",
        "scenes",
        "fleet_defaults",
        "advanced",
    }
    assert set(steps["broker"]["menu_options"]) == set(BROKER_MENU_OPTIONS)
    assert set(steps["advanced"]["menu_options"]) == {"mesh_priorities"}
    for step_id, fields in required_fields.items():
        _assert_nonempty_keys(steps[step_id]["data"], fields)
        _assert_nonempty_keys(steps[step_id]["data_description"], fields)

    advanced = steps["broker_profile"]["sections"]["advanced"]
    _assert_nonempty_keys(advanced, {"name"})
    _assert_nonempty_keys(
        advanced["data"],
        {"mqtt_tls_enabled", "mqtt_tls_ca"},
    )
    _assert_nonempty_keys(
        advanced["data_description"],
        {"mqtt_tls_enabled", "mqtt_tls_ca"},
    )
    _assert_nonempty_keys(options["progress"], {"broker_validation"})


def test_fleet_options_translate_every_reachable_failure() -> None:
    """Every fleet options error and abort gives a concrete next action."""
    options = _strings()["options"]
    errors = options["error"]
    required_errors = set(operation_errors._ERROR_METADATA) | {
        "invalid_value",
        "broker_validation_failed",
    }
    required_aborts = {
        "invalid_parent",
        "invalid_flow_state",
        "parent_changed",
        "broker_change_blocked_by_panel_onboarding",
        "broker_change_requires_guided_flow",
        "reconfigure_successful",
        "ha_control_change_requires_agent_rollout",
        "no_panels_configured",
        "mesh_priority_change_requires_agent_rollout",
    }

    _assert_nonempty_keys(errors, required_errors)
    _assert_nonempty_keys(options["abort"], required_aborts)
    for code in required_errors - {"invalid_value"}:
        copy = errors[code].casefold()
        assert any(action in copy for action in ("check", "confirm", "review", "retry", "restore"))
    for code in required_aborts - {"reconfigure_successful"}:
        copy = options["abort"][code].casefold()
        assert any(
            action in copy for action in ("add", "change", "check", "open", "reload", "retry")
        )


def test_panel_day_two_actions_translate_every_reachable_surface() -> None:
    """Every focused panel reconfiguration action has complete English copy."""
    panel = _strings()["config_subentries"]["panel"]
    steps = panel["step"]
    required_fields = {
        "rename": {"name"},
        "address": {"host"},
        "repair_credentials": {"root_password"},
        "overrides": {
            "voice_wake_word",
            "voice_ha_host",
            "hue_ca_cert",
            "hot_poll_seconds",
            "resync_seconds",
        },
        "rebind": {"host", "root_password"},
        "rebind_confirm": {"confirm"},
    }
    required_steps = {
        "reconfigure",
        "rename",
        "address",
        "repair_credentials",
        "components",
        "overrides",
        "rebind",
        "rebind_confirm",
    }

    assert not required_steps - steps.keys()
    for step_id in required_steps:
        _assert_nonempty_keys(steps[step_id], {"title", "description"})
    assert set(steps["reconfigure"]["menu_options"]) == {
        "rename",
        "address",
        "repair_credentials",
        "components",
        "overrides",
        "rebind",
    }
    assert "advanced" in steps["reconfigure"]["menu_options"]["overrides"].casefold()
    assert "advanced" in steps["overrides"]["title"].casefold()
    description = steps["overrides"]["description"].casefold()
    assert "hot-poll" in description
    assert "resync" in description
    for step_id, fields in required_fields.items():
        _assert_nonempty_keys(steps[step_id]["data"], fields)
        _assert_nonempty_keys(steps[step_id]["data_description"], fields)


def test_panel_day_two_actions_translate_every_reachable_failure() -> None:
    """Day-two identity and persistence failures explain safe recovery."""
    panel = _strings()["config_subentries"]["panel"]

    _assert_nonempty_keys(
        panel["error"],
        {
            "panel_identity_mismatch",
            "rebind_identity_unchanged",
            "rebind_identity_changed",
            "confirmation_required",
        },
    )
    _assert_nonempty_keys(
        panel["abort"],
        {
            "already_configured",
            "invalid_parent",
            "invalid_flow_state",
            "parent_changed",
            "reconfigure_successful",
            "manage_components_with_panel_entities",
            "feature_override_change_requires_agent_rollout",
            "runtime_unavailable",
            "rebind_blocked_by_panel_onboarding",
            "config_entry_storage_unavailable",
            "rebind_identity_changed",
            "cannot_connect",
            "rebind_failed",
        },
    )


def test_panel_rebind_copy_requires_explicit_identity_confirmation() -> None:
    """Replacement-panel onboarding cannot look like an ordinary address edit."""
    steps = _strings()["config_subentries"]["panel"]["step"]
    rebind = steps["rebind"]["description"].casefold()
    confirmation = steps["rebind_confirm"]["description"].casefold()

    assert "replacement" in rebind
    assert "ssh identity" in rebind
    assert "no panel settings" in rebind
    assert "{old_fingerprint}" in confirmation
    assert "{new_fingerprint}" in confirmation
    assert "confirm" in confirmation
    assert "mqtt slug" in confirmation
    assert "entities" in confirmation


def test_shared_host_key_failure_leads_fleet_users_to_explicit_rebind() -> None:
    """Shared safety copy prefers explicit fleet rebind over automatic trust."""
    strings = _strings()
    copies = (
        strings["config"]["error"]["host_key_changed"],
        strings["config_subentries"]["panel"]["error"]["host_key_changed"],
        strings["exceptions"]["host_key_changed"]["message"],
    )

    for message in copies:
        normalized = message.casefold()
        assert "replace physical panel" in normalized
        assert "explicit" in normalized
        assert "unexpected" in normalized
    assert "legacy" in copies[2].casefold()


def test_broker_copy_states_addon_prerequisite_without_forcing_external_users() -> None:
    """The shared broker form distinguishes the recommended and external paths."""
    description = _strings()["config"]["step"]["broker"]["description"].casefold()

    assert "core_mosquitto" in description
    assert "install" in description
    assert "start" in description
    assert "credential" in description
    assert "external" in description
    assert "do not need" in description
    assert "{documentation_url}" in description


def test_onboarding_descriptions_render_direct_troubleshooting_links() -> None:
    """Failure metadata must be visible to users, not hidden in flow results."""
    strings = _strings()
    descriptions = (
        strings["config"]["step"]["broker"]["description"],
        strings["config_subentries"]["panel"]["step"]["panel_connect"]["description"],
        strings["config_subentries"]["panel"]["step"]["panel_confirm"]["description"],
        strings["options"]["step"]["broker_profile"]["description"],
    )

    assert all("[Open troubleshooting]({documentation_url})" in copy for copy in descriptions)


def test_fleet_broker_copy_keeps_external_brokers_first_class() -> None:
    """Day-two broker guidance recommends core_mosquitto without requiring it."""
    steps = _strings()["options"]["step"]
    broker_menu = steps["broker"]["menu_options"]
    profile = steps["broker_profile"]["description"].casefold()
    external = steps["existing_broker"]["description"].casefold()

    assert "recommended" in broker_menu["official_mosquitto"].casefold()
    assert "fully supported" in broker_menu["existing_broker"].casefold()
    assert "core_mosquitto" in profile
    assert "external" in profile
    assert "not required" in profile
    assert "validate" in external


def test_english_catalog_is_byte_identical_to_source_strings() -> None:
    """English cannot silently drift from the source translation catalog."""
    assert _EN_PATH.read_bytes() == _STRINGS_PATH.read_bytes()
