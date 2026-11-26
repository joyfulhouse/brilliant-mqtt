"""Reviewed catalog identity, revocation and feature gate contracts."""

import hashlib
from typing import Any

import pytest
from homeassistant.core import HomeAssistant
from pytest_homeassistant_custom_component.common import MockConfigEntry

from custom_components.brilliant_mqtt.fleet_manager import FleetManager
from custom_components.brilliant_mqtt.plugin_catalog import (
    configured_plugin_catalog,
    parse_catalog,
    verify_artifact_bytes,
)
from custom_components.brilliant_mqtt.plugin_manifest import PluginValidationError, parse_manifest
from tests.test_plugin_manifest import encoded, manifest_document, policy


def catalog_document(raw: bytes) -> dict[str, Any]:
    return {
        "schema_version": 1,
        "revision": 1,
        "revoked_manifest_sha256": [],
        "revoked_artifact_sha256": [],
        "revoked_publisher_keys": [],
        "entries": [
            {
                "id": "demo",
                "version": "1.2.3",
                "publisher": "maintainer",
                "publisher_key_id": "integration_release",
                "review_revision": 1,
                "manifest_sha256": hashlib.sha256(raw).hexdigest(),
                "artifact_sha256": hashlib.sha256(b"artifact").hexdigest(),
                "artifact_size_bytes": 8,
                "license": "MIT",
                "source_url": "https://example.invalid/source",
                "support_owner": "maintainer",
                "firmware_evidence": ["https://example.invalid/evidence"],
                "minimum_safe_version": "1.2.0",
            }
        ],
    }


def test_catalog_pins_manifest_and_artifact() -> None:
    raw = encoded(manifest_document())
    catalog = parse_catalog(
        encoded(catalog_document(raw)), {hashlib.sha256(raw).hexdigest(): raw}, policy()
    )
    assert catalog.revision == 1
    assert catalog.manifests[0].id == "demo"
    verify_artifact_bytes(catalog.manifests[0], b"artifact")
    with pytest.raises(PluginValidationError):
        verify_artifact_bytes(catalog.manifests[0], b"tampered")


def test_equivalent_json_does_not_match_pinned_raw_manifest() -> None:
    raw = encoded(manifest_document())
    catalog = catalog_document(raw)
    with pytest.raises(PluginValidationError):
        parse_catalog(encoded(catalog), {hashlib.sha256(raw).hexdigest(): raw + b" "}, policy())


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("artifact_sha256", "0" * 64),
        ("artifact_size_bytes", 9),
        ("publisher", "other"),
        ("id", "other"),
        ("version", "1.2.4"),
        ("review_revision", True),
        ("minimum_safe_version", "2.0.0"),
        ("unexpected", "value"),
    ],
)
def test_catalog_record_disagreement_refused(field: str, value: object) -> None:
    raw = encoded(manifest_document())
    catalog = catalog_document(raw)
    catalog["entries"][0][field] = value
    with pytest.raises(PluginValidationError):
        parse_catalog(encoded(catalog), {hashlib.sha256(raw).hexdigest(): raw}, policy())


@pytest.mark.parametrize("kind", ["manifest_sha256", "artifact_sha256", "publisher_keys"])
def test_revoked_candidate_refused(kind: str) -> None:
    raw = encoded(manifest_document())
    catalog = catalog_document(raw)
    record = catalog["entries"][0]
    catalog["revoked_" + kind] = [record["publisher_key_id" if kind == "publisher_keys" else kind]]
    with pytest.raises(PluginValidationError):
        parse_catalog(encoded(catalog), {hashlib.sha256(raw).hexdigest(): raw}, policy())


def test_duplicate_catalog_release_refused() -> None:
    raw = encoded(manifest_document())
    catalog = catalog_document(raw)
    catalog["entries"].append(catalog["entries"][0])
    with pytest.raises(PluginValidationError):
        parse_catalog(encoded(catalog), {hashlib.sha256(raw).hexdigest(): raw}, policy())


@pytest.mark.parametrize("enabled", [None, False, 1, "true", [], {}])
def test_feature_flag_is_off_unless_exact_true(enabled: object) -> None:
    assert configured_plugin_catalog({"plugins_enabled": enabled}) is None


def test_opt_in_has_empty_catalog_and_no_panel_actions() -> None:
    catalog = configured_plugin_catalog({"plugins_enabled": True})
    assert catalog is not None
    assert catalog.manifests == ()
    assert configured_plugin_catalog({}) is None


def test_artifact_size_is_checked_before_digest() -> None:
    manifest = parse_manifest(encoded(manifest_document()), policy())
    with pytest.raises(PluginValidationError):
        verify_artifact_bytes(manifest, b"artifact extra")


@pytest.mark.parametrize("raw_value", [b" " * 65537, "not bytes", None])
def test_catalog_bounds_manifest_input_before_hashing(raw_value: Any) -> None:
    raw = encoded(manifest_document())
    catalog = catalog_document(raw)
    with pytest.raises(PluginValidationError):
        parse_catalog(encoded(catalog), {hashlib.sha256(raw).hexdigest(): raw_value}, policy())


@pytest.mark.parametrize("enabled", [False, True])
async def test_fleet_manager_plumbs_internal_flag_without_panel_operations(
    hass: HomeAssistant,
    enabled: bool,
) -> None:
    entry = MockConfigEntry(domain="brilliant_mqtt", options={"plugins_enabled": enabled})
    manager = FleetManager(hass, entry)
    assert (manager.plugin_catalog is not None) is enabled
    assert manager.panels == {}


@pytest.mark.parametrize("key", ["revision", "review_revision"])
def test_duplicate_key_in_otherwise_valid_catalog_is_refused(key: str) -> None:
    raw = encoded(manifest_document())
    catalog_raw = encoded(catalog_document(raw))
    needle = f'"{key}": 1'.encode()
    catalog_raw = catalog_raw.replace(needle, needle + b", " + needle, 1)
    with pytest.raises(PluginValidationError):
        parse_catalog(catalog_raw, {hashlib.sha256(raw).hexdigest(): raw}, policy())


@pytest.mark.parametrize("case", ["unknown", "missing", "version", "boolean_version"])
def test_catalog_root_schema_is_strict(case: str) -> None:
    raw = encoded(manifest_document())
    catalog = catalog_document(raw)
    if case == "unknown":
        catalog["unexpected"] = True
    elif case == "missing":
        del catalog["revision"]
    elif case == "version":
        catalog["schema_version"] = 2
    else:
        catalog["schema_version"] = True
    with pytest.raises(PluginValidationError):
        parse_catalog(encoded(catalog), {hashlib.sha256(raw).hexdigest(): raw}, policy())


def test_valid_catalog_accepts_exact_document_limit_and_refuses_one_more_before_hashing() -> None:
    from unittest.mock import patch

    raw = encoded(manifest_document())
    manifests = {hashlib.sha256(raw).hexdigest(): raw}
    catalog_raw = encoded(catalog_document(raw))
    padded = catalog_raw + b" " * (65536 - len(catalog_raw))
    assert parse_catalog(padded, manifests, policy()).manifests[0].id == "demo"
    with patch("hashlib.sha256", side_effect=AssertionError("oversized input was hashed")):
        with pytest.raises(PluginValidationError):
            parse_catalog(padded + b" ", manifests, policy())


def test_catalog_rejects_valid_but_oversized_manifest_before_hashing() -> None:
    from unittest.mock import patch

    raw = encoded(manifest_document())
    oversized = raw + b" " * (65537 - len(raw))
    manifests = {hashlib.sha256(oversized).hexdigest(): oversized}
    catalog_raw = encoded(catalog_document(oversized))
    with patch("hashlib.sha256", side_effect=AssertionError("oversized input was hashed")):
        with pytest.raises(PluginValidationError):
            parse_catalog(catalog_raw, manifests, policy())


def test_oversized_artifact_is_not_hashed_before_refusal() -> None:
    from unittest.mock import patch

    manifest = parse_manifest(encoded(manifest_document()), policy())
    oversized = b"artifact extra"
    with patch("hashlib.sha256", side_effect=AssertionError("oversized input was hashed")):
        with pytest.raises(PluginValidationError):
            verify_artifact_bytes(manifest, oversized)
