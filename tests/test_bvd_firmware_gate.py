"""Firmware gate for the BVD pilot, parsed from real ``ostree admin status`` text.

Panel ostree is 2022.2, which has no ``--json`` (``error: Unknown option
--json``), so the gate must parse the human output.  The fixture is a verbatim
read-only capture from a v26.10.21.1 panel (no device or home identifiers).
"""

from __future__ import annotations

import io
import json
from pathlib import Path

import pytest

from tools.brilliant_bvd.firmware_gate import (
    PINNED_FIRMWARE,
    FirmwareGateError,
    FirmwarePin,
    OstreeDeployment,
    main,
    parse_ostree_admin_status,
    validate_firmware,
)

FIXTURES = Path(__file__).parent / "fixtures" / "brilliant_bvd"
FIXTURE = FIXTURES / "ostree-admin-status-v26.10.21.1.txt"
REAL_STATUS = FIXTURE.read_text(encoding="utf-8")
BOOTED = "aae2bfc59cee9b3a36af252c1e90c7898f41a59f5033567f665ca807eba82ac0"
ROLLBACK = "2346695584090613927922e35249a37474103ef46c3d7696fceaa7bd7662811c"
EMBEDDED = "4329f9ae9c20c3d50c80a5f44a818d23602b9b30"
STAGED = "15d05a7a" + "0" * 56


def test_pin_matches_the_reviewed_fleet_firmware() -> None:
    assert PINNED_FIRMWARE == FirmwarePin(
        version="v26.10.21.1",
        ostree_commit=BOOTED,
        embedded_revision=EMBEDDED,
    )


def test_parser_reads_real_panel_status_with_booted_and_rollback() -> None:
    deployments = parse_ostree_admin_status(REAL_STATUS)

    assert deployments == (
        OstreeDeployment(
            osname="brilliant-linux",
            checksum=BOOTED,
            serial=0,
            booted=True,
            status="",
            version="v26.10.21.1",
        ),
        OstreeDeployment(
            osname="brilliant-linux",
            checksum=ROLLBACK,
            serial=0,
            booted=False,
            status="rollback",
            version="v26.09.23.1",
        ),
    )


def test_real_panel_status_passes_the_pinned_gate() -> None:
    result = validate_firmware(REAL_STATUS, embedded_version=f"{EMBEDDED}\n")

    assert result.version == "v26.10.21.1"
    assert result.booted_commit == BOOTED
    assert result.embedded_revision == EMBEDDED
    assert result.rollback_versions == ("v26.09.23.1",)


def _with_header_before_booted(header: str, version: str = "v26.11.04.1") -> str:
    return f"{header}\n    Version: {version}\n{REAL_STATUS}"


@pytest.mark.parametrize("status", ["staged", "pending", "finalization locked"])
def test_staged_or_pending_next_deployment_is_no_go(status: str) -> None:
    text = _with_header_before_booted(f"  brilliant-linux {STAGED}.0 ({status})")

    assert parse_ostree_admin_status(text)[0].status == status
    with pytest.raises(FirmwareGateError, match="staged or pending"):
        validate_firmware(text, embedded_version=EMBEDDED)


def test_unknown_deployment_annotation_fails_closed() -> None:
    text = _with_header_before_booted(f"  brilliant-linux {STAGED}.0 (soft-reboot)")

    with pytest.raises(FirmwareGateError, match="unrecognized"):
        parse_ostree_admin_status(text)


@pytest.mark.parametrize(
    ("text", "message"),
    [
        ("", "no deployments"),
        ("error: Unknown option --json\n", "unrecognized"),
        (REAL_STATUS.replace("* brilliant-linux", "  brilliant-linux", 1), "exactly one booted"),
        (
            REAL_STATUS.replace(f"  brilliant-linux {ROLLBACK}", f"* brilliant-linux {ROLLBACK}"),
            "exactly one booted",
        ),
        (REAL_STATUS.replace(BOOTED, ROLLBACK, 1), "booted OSTree commit"),
        (REAL_STATUS.replace("Version: v26.10.21.1", "Version: v26.11.04.1"), "booted version"),
        (REAL_STATUS.replace("    Version: v26.10.21.1\n", "", 1), "booted version"),
        (
            REAL_STATUS.replace(
                "    Version: v26.10.21.1\n",
                "    Version: v26.10.21.1\n    Version: v26.10.21.1\n",
                1,
            ),
            "more than one Version",
        ),
        ("    Version: v26.10.21.1\n" + REAL_STATUS, "before any deployment"),
    ],
)
def test_gate_rejects_malformed_or_mismatched_status(text: str, message: str) -> None:
    with pytest.raises(FirmwareGateError, match=message):
        validate_firmware(text, embedded_version=EMBEDDED)


@pytest.mark.parametrize(
    "embedded",
    ["", "2332ced103d755d48d2302b592f95f8e7b6c66f5", f"{EMBEDDED}\n\n", f" {EMBEDDED}", "x"],
)
def test_gate_requires_the_exact_embedded_revision(embedded: str) -> None:
    with pytest.raises(FirmwareGateError, match="embedded"):
        validate_firmware(REAL_STATUS, embedded_version=embedded)


def test_cli_reads_status_from_stdin_and_emits_ok_json(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(REAL_STATUS))

    assert main(["--embedded-version", EMBEDDED]) == 0

    assert json.loads(capsys.readouterr().out) == {
        "booted_commit": BOOTED,
        "embedded_revision": EMBEDDED,
        "event": "FIRMWARE_GATE_OK",
        "rollback_versions": ["v26.09.23.1"],
        "version": "v26.10.21.1",
    }


def test_cli_exits_nonzero_without_ok_on_mismatch(
    monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    monkeypatch.setattr("sys.stdin", io.StringIO(REAL_STATUS))

    with pytest.raises(SystemExit) as raised:
        main(["--embedded-version", "2332ced103d755d48d2302b592f95f8e7b6c66f5"])

    assert raised.value.code != 0
    assert "FIRMWARE_GATE_OK" not in capsys.readouterr().out
