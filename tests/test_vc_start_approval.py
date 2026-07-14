from __future__ import annotations

import json
import os
from pathlib import Path

import pytest

from tools.brilliant_vc.start_approval import (
    StartApprovalError,
    validate_start_approval,
)

NOW_S = 1_800_000_000


def _approval(path: Path) -> None:
    example = (
        Path(__file__).parents[1]
        / "docs/brilliant-panel/virtual-control-start-approval.example.json"
    )
    payload = json.loads(example.read_text(encoding="utf-8"))
    payload["approved_at_s"] = NOW_S
    payload["run_id"] = "office-vc-bootstrap-01"
    path.write_text(json.dumps(payload), encoding="utf-8")
    path.chmod(0o640)
    os.chown(path, os.getuid(), os.getgid())


def _control_directory(path: Path) -> None:
    path.mkdir(mode=0o750)
    os.chown(path, os.getuid(), os.getgid())


def test_atomically_renamed_single_link_marker_validates(tmp_path: Path) -> None:
    control = tmp_path / "control"
    source = control / "start-approval.json"
    marker = control / "start-approval-consumed.json"
    _control_directory(control)
    _approval(source)

    source.rename(marker)

    consumed = validate_start_approval(
        marker,
        now_s=NOW_S,
        credential_uid=os.getuid(),
        runtime_gid=os.getgid(),
        allowed_paths=(marker,),
    )

    assert source.exists() is False
    assert marker.stat().st_nlink == 1
    assert marker.stat().st_mode & 0o777 == 0o640
    assert marker.parent.stat().st_mode & 0o777 == 0o750
    assert consumed.runtime_credential_bundle_sha256 == "0" * 64


def test_multi_link_marker_fails_closed(tmp_path: Path) -> None:
    control = tmp_path / "control"
    source = control / "start-approval.json"
    marker = control / "start-approval-consumed.json"
    _control_directory(control)
    _approval(source)
    os.link(source, marker)

    assert source.exists()
    assert marker.exists()
    assert marker.stat().st_nlink == 2
    with pytest.raises(StartApprovalError, match="link count"):
        validate_start_approval(
            marker,
            now_s=NOW_S,
            credential_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_paths=(marker,),
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("schema_version", True),
        ("approved", 1),
        ("runtime_limit_s", 600.0),
        ("physical_device_actions_permitted", 0),
        ("hosted_light_permitted", 0),
    ],
)
def test_approval_rejects_json_type_confusion(
    tmp_path: Path,
    field: str,
    value: object,
) -> None:
    control = tmp_path / "control"
    source = control / "start-approval.json"
    marker = control / "start-approval-consumed.json"
    _control_directory(control)
    _approval(source)
    payload = json.loads(source.read_text(encoding="utf-8"))
    payload[field] = value
    source.write_text(json.dumps(payload), encoding="utf-8")
    source.chmod(0o640)
    source.rename(marker)

    with pytest.raises(StartApprovalError, match="bootstrap-only run"):
        validate_start_approval(
            marker,
            now_s=NOW_S,
            credential_uid=os.getuid(),
            runtime_gid=os.getgid(),
            allowed_paths=(marker,),
        )
