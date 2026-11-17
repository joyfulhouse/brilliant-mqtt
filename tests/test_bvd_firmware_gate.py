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
OTHER = "0123abcd" + "0" * 56
REFSPEC = "brilliant-linux:linux/brilliant-imx6/brilliant-embedded-image/stable"
SIGNING_KEY = "949B1682BC2E58A4"
PRIMARY_KEY = "426FB14BC543CDF3"
SIGNER = "Brilliant Deploy <deploy@brilliant.tech>"
BOOTED_VERSION_LINE = "    Version: v26.10.21.1\n"
ROLLBACK_HEADER = f"  brilliant-linux {ROLLBACK}.0 (rollback)\n"


def _complete_block(header: str, version: str = "v26.11.04.1") -> str:
    """A deployment block with every field the real capture carries."""

    return (
        f"{header}\n"
        f"    Version: {version}\n"
        f"    origin refspec: {REFSPEC}\n"
        f"    GPG: Signature made Wed Nov 04 01:00:00 2026 using RSA key ID {SIGNING_KEY}\n"
        f'    GPG: Good signature from "{SIGNER}"\n'
        f"    GPG: Primary key ID {PRIMARY_KEY}\n"
    )


def _insert_after_booted_version(line: str) -> str:
    return REAL_STATUS.replace(BOOTED_VERSION_LINE, BOOTED_VERSION_LINE + line, 1)


def test_pin_matches_the_reviewed_fleet_firmware() -> None:
    assert PINNED_FIRMWARE == FirmwarePin(
        version="v26.10.21.1",
        ostree_commit=BOOTED,
        embedded_revision=EMBEDDED,
        origin_refspec=REFSPEC,
        gpg_primary_key_id=PRIMARY_KEY,
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
            origin_refspec=REFSPEC,
            gpg_signing_key_id=SIGNING_KEY,
            gpg_good_signer=SIGNER,
            gpg_primary_key_id=PRIMARY_KEY,
        ),
        OstreeDeployment(
            osname="brilliant-linux",
            checksum=ROLLBACK,
            serial=0,
            booted=False,
            status="rollback",
            version="v26.09.23.1",
            origin_refspec=REFSPEC,
            gpg_signing_key_id=SIGNING_KEY,
            gpg_good_signer=SIGNER,
            gpg_primary_key_id=PRIMARY_KEY,
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
        (REAL_STATUS.replace(BOOTED, OTHER, 1), "booted OSTree commit"),
        (REAL_STATUS.replace("Version: v26.10.21.1", "Version: v26.11.04.1"), "booted version"),
        (REAL_STATUS.replace("    Version: v26.10.21.1\n", "", 1), "incomplete"),
        (
            REAL_STATUS.replace(
                "    Version: v26.10.21.1\n",
                "    Version: v26.10.21.1\n    Version: v26.10.21.1\n",
                1,
            ),
            "duplicate Version",
        ),
        ("    Version: v26.10.21.1\n" + REAL_STATUS, "before any deployment"),
    ],
    ids=[
        "empty",
        "json-error",
        "no-booted",
        "two-booted",
        "wrong-commit",
        "wrong-version",
        "missing-version",
        "duplicate-version",
        "detail-before-header",
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


def test_booted_deployment_alone_without_rollback_passes() -> None:
    booted_only = REAL_STATUS[: REAL_STATUS.index(ROLLBACK_HEADER)]

    assert validate_firmware(booted_only, embedded_version=EMBEDDED).rollback_versions == ()


# Offline mutations of the captured fixture that an independent review showed
# the first parser accepted.  Each must now fail closed.
@pytest.mark.parametrize(
    ("text", "message"),
    [
        (_insert_after_booted_version("    Pinned: yes\n"), "unrecognized"),
        (_insert_after_booted_version("    Version: not-a-version\n"), "malformed Version"),
        (
            REAL_STATUS.replace("Version: v26.10.21.1", "Version: v26.10.21.1 (local)", 1),
            "malformed Version",
        ),
        (
            _insert_after_booted_version(f"    brilliant-linux {STAGED}.0 (staged)\n"),
            "unrecognized",
        ),
        (
            _complete_block(f"  brilliant-linux {STAGED}.0") + REAL_STATUS,
            "deployment structure",
        ),
        (
            REAL_STATUS + f"  brilliant-linux {OTHER}.0 (rollback)\n",
            "deployment structure",
        ),
    ],
    ids=[
        "unknown-indented-field",
        "malformed-second-version",
        "malformed-version",
        "indented-staged-header",
        "unannotated-before-booted",
        "bare-extra-rollback",
    ],
)
def test_reviewed_fixture_mutations_fail_closed(text: str, message: str) -> None:
    with pytest.raises(FirmwareGateError, match=message):
        validate_firmware(text, embedded_version=EMBEDDED)


def _rollback_without(field_prefix: str) -> str:
    head, rollback = REAL_STATUS.split(ROLLBACK_HEADER)
    kept = "".join(
        line for line in rollback.splitlines(keepends=True) if not line.startswith(field_prefix)
    )
    return head + ROLLBACK_HEADER + kept


@pytest.mark.parametrize(
    ("text", "message"),
    [
        (REAL_STATUS[: REAL_STATUS.index(ROLLBACK_HEADER) + len(ROLLBACK_HEADER)], "incomplete"),
        (_rollback_without("    GPG: Good signature"), "incomplete"),
        (_rollback_without("    Version:"), "incomplete"),
        (
            REAL_STATUS.replace('    GPG: Good signature from "Brilliant Deploy', "    GPG: X", 1),
            "unrecognized",
        ),
        (
            REAL_STATUS.replace(
                f"    GPG: Primary key ID {PRIMARY_KEY}\n",
                f"    GPG: Primary key ID {PRIMARY_KEY}\n" * 2,
                1,
            ),
            "duplicate GPG primary key",
        ),
        (
            _insert_after_booted_version(f"    GPG: Primary key ID {PRIMARY_KEY}\n"),
            "out of order",
        ),
        (
            _insert_after_booted_version(f"    origin refspec: {REFSPEC}\n"),
            "duplicate origin refspec",
        ),
        (
            REAL_STATUS.replace(
                BOOTED_VERSION_LINE + f"    origin refspec: {REFSPEC}\n",
                f"    origin refspec: {REFSPEC}\n" + BOOTED_VERSION_LINE,
                1,
            ),
            "order",
        ),
        (REAL_STATUS.replace(ROLLBACK_HEADER, "\n" + ROLLBACK_HEADER), "blank"),
        (
            REAL_STATUS.replace(f"{ROLLBACK}.0 (rollback)", f"{OTHER}.0"),
            "deployment structure",
        ),
        (REAL_STATUS.replace(ROLLBACK, BOOTED), "deployment structure"),
        (
            REAL_STATUS.replace(REFSPEC, "brilliant-linux:linux/brilliant-imx6/canary", 1),
            "origin refspec",
        ),
        (
            REAL_STATUS.replace(f"Primary key ID {PRIMARY_KEY}", "Primary key ID 0000000000000000"),
            "primary key",
        ),
        (
            REAL_STATUS.replace("RSA key ID 949B1682BC2E58A4", "RSA key ID 949b1682bc2e58a4", 1),
            "malformed GPG signature",
        ),
    ],
    ids=[
        "rollback-header-only",
        "rollback-no-good-signature",
        "rollback-no-version",
        "mangled-good-signature",
        "duplicate-primary-key",
        "primary-key-out-of-order",
        "duplicate-origin",
        "version-after-origin",
        "blank-line",
        "unannotated-second",
        "rollback-equals-booted",
        "wrong-refspec",
        "wrong-primary-key",
        "lowercase-signing-key",
    ],
)
def test_block_grammar_is_explicit_and_complete(text: str, message: str) -> None:
    with pytest.raises(FirmwareGateError, match=message):
        validate_firmware(text, embedded_version=EMBEDDED)


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
