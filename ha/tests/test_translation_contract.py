"""Translation contracts for code-owned Brilliant MQTT flow keys."""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any, cast

from custom_components.brilliant_mqtt import errors as operation_errors
from custom_components.brilliant_mqtt import panel_inspection, panel_provisioner, shell
from custom_components.brilliant_mqtt.flow_schemas import BROKER_MENU_OPTIONS

_REPOSITORY_ROOT = Path(__file__).parents[2]
_STRINGS_PATH = _REPOSITORY_ROOT / "custom_components/brilliant_mqtt/strings.json"
_EN_PATH = _REPOSITORY_ROOT / "custom_components/brilliant_mqtt/translations/en.json"


def _strings() -> dict[str, Any]:
    return cast(dict[str, Any], json.loads(_STRINGS_PATH.read_text(encoding="utf-8")))


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


def test_broker_copy_states_addon_prerequisite_without_forcing_external_users() -> None:
    """The shared broker form distinguishes the recommended and external paths."""
    description = _strings()["config"]["step"]["broker"]["description"].casefold()

    assert "core_mosquitto" in description
    assert "install" in description
    assert "start" in description
    assert "credential" in description
    assert "external" in description
    assert "do not need" in description


def test_english_catalog_is_byte_identical_to_source_strings() -> None:
    """English cannot silently drift from the source translation catalog."""
    assert _EN_PATH.read_bytes() == _STRINGS_PATH.read_bytes()
