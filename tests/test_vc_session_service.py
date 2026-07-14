from __future__ import annotations

import configparser
from pathlib import Path


class _CaseConfigParser(configparser.ConfigParser):
    def optionxform(self, optionstr: str) -> str:
        return optionstr


def test_reference_session_service_is_bounded_nonroot_and_non_enableable() -> None:
    unit_path = Path(__file__).parents[1] / "deploy/brilliant-vc-session.service"
    source = unit_path.read_text(encoding="utf-8")
    parser = _CaseConfigParser(strict=False, interpolation=None)
    parser.read_string(source)

    assert "Install" not in parser
    unit = parser["Unit"]
    service = parser["Service"]
    assert unit["ConditionPathExists"] == (
        "/run/brilliant-vc-session-approval/session-approval.json"
    )
    assert service["Type"] == "simple"
    assert service["User"] == service["Group"] == "brilliant-vc"
    assert service["Restart"] == "no"
    assert service["TimeoutStartSec"] == "2580"
    assert service["RuntimeMaxSec"] == "1"
    assert service["KillMode"] == "control-group"
    assert service["UMask"] == "0077"

    prestarts = source.count("ExecStartPre=")
    assert prestarts == 3
    assert "ExecStartPre=!/usr/bin/mv.coreutils --no-clobber --no-target-directory" in source
    assert "-m tools.brilliant_vc.staged_runtime" in source
    assert "-m tools.brilliant_vc.session_prepare --apply" in source
    assert "ExecStart=/data/switch-embedded/env/bin/uwsgi " in source
    assert "--pidfile /run/brilliant-vc/emperor.pid" in source
    assert (
        "ExecStartPost=/usr/bin/python3.10 -m tools.brilliant_vc.session_coordinator --apply"
    ) in source
    assert "/var/brilliant-vc/vendor" in service["Environment"]
    assert "/data/brilliant-vc-session-input" in service["ReadOnlyPaths"]
    assert "/var/brilliant-vc/app" in service["ReadOnlyPaths"]
    assert "/var/brilliant-vc/vendor" in service["ReadOnlyPaths"]
    assert "/var/brilliant-vc/session-app-manifest.sha256" in service["ReadOnlyPaths"]
    assert "/data/brilliant-vc-session" in service["ReadWritePaths"]
    assert "/run/brilliant-vc-session" in service["ReadWritePaths"]
    assert "/var/run/brilliant" in service["InaccessiblePaths"]
    assert "CapabilityBoundingSet" in service and service["CapabilityBoundingSet"] == ""
    assert service["NoNewPrivileges"] == "yes"
    assert "ExecStart=+" not in source and "ExecStartPost=!" not in source
