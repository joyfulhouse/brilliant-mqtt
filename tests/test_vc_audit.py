from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tools.brilliant_vc.audit import (
    SAFE_STAT_FIELDS,
    SENSITIVE_PATHS,
    AuditInputError,
    audit_prior_state,
    collect_stat_inventory,
)


def _snapshot(**overrides: object) -> dict[str, object]:
    snapshot: dict[str, object] = {
        "firmware_version": "v26.06.03.1",
        "bus_home_id_sha256": "a" * 64,
        "physical_control_count": 14,
        "bus_device_type_6_ids": [],
        "app_device_type_6_ids": [],
        "known_preexisting_device_type_6_ids": [],
        "july9_app_inventory_confirms_no_new_vc": True,
        "july9_bus_inventory_confirms_no_new_vc": True,
    }
    snapshot.update(overrides)
    return snapshot


def _missing_stats() -> list[dict[str, object]]:
    return [
        {
            "path": path,
            "exists": False,
            "uid": None,
            "gid": None,
            "mode": None,
            "size": None,
            "mtime_ns": None,
        }
        for path in SENSITIVE_PATHS
    ]


def test_audit_reports_required_counts_and_hashes() -> None:
    result = audit_prior_state(_snapshot(), _missing_stats())

    assert result.passed is True
    assert result.report["firmware_version"] == "v26.06.03.1"
    assert result.report["bus_home_id_sha256"] == "a" * 64
    assert result.report["physical_control_count"] == 14
    assert result.report["device_type_6_count"] == 0
    assert result.report["july9_no_vc_confirmed"] is True


def test_unexplained_type_6_device_fails_vc0() -> None:
    result = audit_prior_state(
        _snapshot(
            bus_device_type_6_ids=["abcd…1234"],
            app_device_type_6_ids=["abcd…1234"],
        ),
        _missing_stats(),
    )

    assert result.passed is False
    assert result.report["unexplained_device_type_6_count"] == 1
    assert "unexplained DeviceType 6 identity" in result.reasons[0]


def test_known_preexisting_type_6_device_is_explained() -> None:
    result = audit_prior_state(
        _snapshot(
            bus_device_type_6_ids=["abcd…1234"],
            app_device_type_6_ids=["abcd…1234"],
            known_preexisting_device_type_6_ids=["abcd…1234"],
        ),
        _missing_stats(),
    )

    assert result.passed is True
    assert result.report["device_type_6_count"] == 1
    assert result.report["unexplained_device_type_6_count"] == 0


def test_july9_no_vc_requires_app_and_bus_confirmation() -> None:
    result = audit_prior_state(
        _snapshot(july9_app_inventory_confirms_no_new_vc=False),
        _missing_stats(),
    )

    assert result.passed is False
    assert result.report["july9_no_vc_confirmed"] is False
    assert any("independent app and bus confirmation" in reason for reason in result.reasons)


def test_stat_inventory_rejects_extra_or_content_fields() -> None:
    stats = _missing_stats()
    stats[0]["content"] = "not allowed"

    with pytest.raises(AuditInputError, match="stat fields"):
        audit_prior_state(_snapshot(), stats)


@pytest.mark.parametrize("mode", [0o640, 0o604, 0o644])
def test_group_or_world_readable_credential_shaped_file_fails(mode: int) -> None:
    stats = _missing_stats()
    stats[0] = {
        "path": SENSITIVE_PATHS[0],
        "exists": True,
        "uid": 0,
        "gid": 0,
        "mode": mode,
        "size": 32,
        "mtime_ns": 1,
    }

    result = audit_prior_state(_snapshot(), stats)

    assert result.passed is False
    assert any("group/world permissions" in reason for reason in result.reasons)


def test_root_only_prior_credential_may_be_retained_with_reason() -> None:
    stats = _missing_stats()
    stats[0] = {
        "path": SENSITIVE_PATHS[0],
        "exists": True,
        "uid": 0,
        "gid": 0,
        "mode": 0o600,
        "size": 32,
        "mtime_ns": 1,
    }

    result = audit_prior_state(
        _snapshot(),
        stats,
        retained_paths={SENSITIVE_PATHS[0]: "needed for claims-only VC1 audit"},
    )

    assert result.passed is True
    assert result.report["retained_root_only_count"] == 1


def test_existing_prior_credential_requires_explicit_action() -> None:
    stats = _missing_stats()
    stats[0] = {
        "path": SENSITIVE_PATHS[0],
        "exists": True,
        "uid": 0,
        "gid": 0,
        "mode": 0o600,
        "size": 32,
        "mtime_ns": 1,
    }

    result = audit_prior_state(_snapshot(), stats)

    assert result.passed is False
    assert any("explicit delete-or-retain action" in reason for reason in result.reasons)


@pytest.mark.parametrize(
    "unsafe_value",
    [
        "eyJhbGciOiJIUzI1NiJ9.abcdefgh.signature",
        "-----BEGIN CERTIFICATE-----",
        "A" * 300,
    ],
)
def test_secret_shaped_snapshot_values_are_rejected(unsafe_value: str) -> None:
    with pytest.raises(AuditInputError, match="unsafe value"):
        audit_prior_state(_snapshot(firmware_version=unsafe_value), _missing_stats())


def test_credential_shaped_snapshot_key_is_rejected() -> None:
    with pytest.raises(AuditInputError, match="unsafe field"):
        audit_prior_state(_snapshot(access_token="redacted"), _missing_stats())


def test_collect_stat_inventory_reads_metadata_not_contents(tmp_path: Path) -> None:
    credential = tmp_path / ".access"
    credential.write_text("do-not-read")
    credential.chmod(0o600)

    inventory = collect_stat_inventory((str(credential), str(tmp_path / "missing")))

    assert set(inventory[0]) == set(SAFE_STAT_FIELDS)
    assert inventory[0]["exists"] is True
    assert inventory[0]["mode"] == 0o600
    assert inventory[0]["size"] == len("do-not-read")
    assert inventory[1]["exists"] is False
    assert "do-not-read" not in json.dumps(inventory)


def test_collect_stat_inventory_uses_lstat_for_symlink(tmp_path: Path) -> None:
    target = tmp_path / "target"
    target.write_text("secret")
    link = tmp_path / "link"
    link.symlink_to(target)

    inventory = collect_stat_inventory((str(link),))

    assert inventory[0]["exists"] is True
    assert inventory[0]["mode"] == os.lstat(link).st_mode & 0o7777
