"""Fail-closed firmware gate for the BVD single-light pilot.

Brilliant panels ship ostree 2022.2, whose ``ostree admin status`` has no
``--json`` option, so this parses the stable human-readable format::

    * <osname> <64-hex commit>.<serial>[ (staged|pending|rollback|...)]
        Version: <version>
        <other indented detail lines>

Only the standard library is used so the gate runs both on the panel
interpreter and on the operator workstation.  It reads the status text from
stdin; it never invokes ostree or writes anything itself.
"""

from __future__ import annotations

import argparse
import json
import re
import sys
from collections.abc import Sequence
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class FirmwarePin:
    """The one reviewed firmware identity the pilot may run on."""

    version: str
    ostree_commit: str
    embedded_revision: str
    origin_refspec: str
    gpg_primary_key_id: str


PINNED_FIRMWARE = FirmwarePin(
    version="v26.10.21.1",
    ostree_commit="aae2bfc59cee9b3a36af252c1e90c7898f41a59f5033567f665ca807eba82ac0",
    embedded_revision="4329f9ae9c20c3d50c80a5f44a818d23602b9b30",
    origin_refspec="brilliant-linux:linux/brilliant-imx6/brilliant-embedded-image/stable",
    gpg_primary_key_id="426FB14BC543CDF3",
)

_HEADER = re.compile(
    r"(?P<booted>[* ]) (?P<osname>[A-Za-z0-9_.-]+) (?P<checksum>[0-9a-f]{64})\.(?P<serial>[0-9]+)"
    r"(?: \((?P<status>staged|pending|rollback|finalization locked)\))?"
)
# The complete, ordered detail grammar of one deployment block as printed by
# ostree 2022.2 for a GPG-verified remote.  Each field is recognised by its
# prefix and must then match its full pattern; anything else fails closed.
_FIELDS: tuple[tuple[str, str, re.Pattern[str]], ...] = (
    (
        "Version",
        "    Version:",
        re.compile(r"    Version: (?P<value>v[0-9]+\.[0-9]+\.[0-9]+\.[0-9]+)"),
    ),
    (
        "origin refspec",
        "    origin refspec:",
        re.compile(r"    origin refspec: (?P<value>[A-Za-z0-9_.-]+:[A-Za-z0-9_./-]+)"),
    ),
    (
        "GPG signature",
        "    GPG: Signature made ",
        re.compile(
            r"    GPG: Signature made [A-Za-z0-9 :]+ using RSA key ID (?P<value>[0-9A-F]{16})"
        ),
    ),
    (
        "GPG good signature",
        "    GPG: Good signature from ",
        re.compile(r'    GPG: Good signature from "(?P<value>[^"]+)"'),
    ),
    (
        "GPG primary key",
        "    GPG: Primary key ID ",
        re.compile(r"    GPG: Primary key ID (?P<value>[0-9A-F]{16})"),
    ),
)
_EMBEDDED_REVISION = re.compile(r"[0-9a-f]{40}\n?")
_NEXT_BOOT_STATUSES = frozenset({"staged", "pending", "finalization locked"})


class FirmwareGateError(ValueError):
    """Raised when the panel is not provably on the pinned firmware."""


@dataclass(frozen=True, slots=True)
class OstreeDeployment:
    """One deployment block from ``ostree admin status``."""

    osname: str
    checksum: str
    serial: int
    booted: bool
    status: str
    version: str | None = None
    origin_refspec: str | None = None
    gpg_signing_key_id: str | None = None
    gpg_good_signer: str | None = None
    gpg_primary_key_id: str | None = None

    @property
    def complete(self) -> bool:
        return None not in (
            self.version,
            self.origin_refspec,
            self.gpg_signing_key_id,
            self.gpg_good_signer,
            self.gpg_primary_key_id,
        )


_FIELD_ATTRIBUTES = (
    "version",
    "origin_refspec",
    "gpg_signing_key_id",
    "gpg_good_signer",
    "gpg_primary_key_id",
)


@dataclass(frozen=True, slots=True)
class FirmwareGateResult:
    """Sanitized facts proving the pinned firmware is booted with nothing queued."""

    version: str
    booted_commit: str
    embedded_revision: str
    rollback_versions: tuple[str, ...]


def parse_ostree_admin_status(text: str) -> tuple[OstreeDeployment, ...]:
    """Parse ``ostree admin status`` with an explicit block grammar.

    Blank, unknown, malformed, duplicated, or out-of-order lines fail closed.
    Block completeness and the allowed deployment structure are enforced by
    :func:`validate_firmware`.
    """

    headers: list[re.Match[str]] = []
    details: list[dict[str, str]] = []
    last_field_index = -1
    for line in text.splitlines():
        if not line.strip():
            raise FirmwareGateError("blank line in ostree admin status output")
        header = _HEADER.fullmatch(line)
        if header is not None:
            headers.append(header)
            details.append({})
            last_field_index = -1
            continue
        field_index = next(
            (index for index, (_, prefix, _) in enumerate(_FIELDS) if line.startswith(prefix)),
            None,
        )
        if field_index is None:
            raise FirmwareGateError("unrecognized ostree admin status line")
        if not details:
            raise FirmwareGateError("ostree detail line appeared before any deployment")
        name, _, pattern = _FIELDS[field_index]
        attribute = _FIELD_ATTRIBUTES[field_index]
        match = pattern.fullmatch(line)
        if match is None:
            raise FirmwareGateError(f"malformed {name} line")
        if attribute in details[-1]:
            raise FirmwareGateError(f"deployment has a duplicate {name} line")
        if field_index <= last_field_index:
            raise FirmwareGateError(f"deployment {name} line is out of order")
        last_field_index = field_index
        details[-1][attribute] = match["value"]
    if not headers:
        raise FirmwareGateError("ostree admin status listed no deployments")
    return tuple(
        OstreeDeployment(
            osname=header["osname"],
            checksum=header["checksum"],
            serial=int(header["serial"]),
            booted=header["booted"] == "*",
            status=header["status"] or "",
            version=fields.get("version"),
            origin_refspec=fields.get("origin_refspec"),
            gpg_signing_key_id=fields.get("gpg_signing_key_id"),
            gpg_good_signer=fields.get("gpg_good_signer"),
            gpg_primary_key_id=fields.get("gpg_primary_key_id"),
        )
        for header, fields in zip(headers, details, strict=True)
    )


def validate_firmware(
    status_text: str,
    *,
    embedded_version: str,
    pin: FirmwarePin = PINNED_FIRMWARE,
) -> FirmwareGateResult:
    """Require the pinned booted deployment, at most one rollback, and no queued OTA."""

    if _EMBEDDED_REVISION.fullmatch(embedded_version) is None:
        raise FirmwareGateError("embedded-stack revision is malformed")
    embedded_revision = embedded_version.rstrip("\n")
    if embedded_revision != pin.embedded_revision:
        raise FirmwareGateError("embedded-stack revision does not match the pin")
    deployments = parse_ostree_admin_status(status_text)
    if any(deployment.status in _NEXT_BOOT_STATUSES for deployment in deployments):
        raise FirmwareGateError("a staged or pending OSTree deployment is queued")
    booted = [deployment for deployment in deployments if deployment.booted]
    if len(booted) != 1 or booted[0].status:
        raise FirmwareGateError("ostree must report exactly one booted deployment")
    # The only reviewed shapes: the booted deployment first, optionally followed
    # by exactly one distinct rollback.  Any other deployment has no proven role.
    current = deployments[0]
    rollbacks = deployments[1:]
    if (
        not current.booted
        or len(rollbacks) > 1
        or any(
            deployment.booted
            or deployment.status != "rollback"
            or deployment.checksum == current.checksum
            for deployment in rollbacks
        )
    ):
        raise FirmwareGateError("unexpected OSTree deployment structure")
    if not all(deployment.complete for deployment in deployments):
        raise FirmwareGateError("an OSTree deployment block is incomplete")
    if current.checksum != pin.ostree_commit:
        raise FirmwareGateError("booted OSTree commit does not match the pin")
    if current.version != pin.version:
        raise FirmwareGateError("booted version does not match the pin")
    if current.origin_refspec != pin.origin_refspec:
        raise FirmwareGateError("booted origin refspec does not match the pin")
    if any(deployment.gpg_primary_key_id != pin.gpg_primary_key_id for deployment in deployments):
        raise FirmwareGateError("deployment GPG primary key does not match the pin")
    return FirmwareGateResult(
        version=pin.version,
        booted_commit=current.checksum,
        embedded_revision=embedded_revision,
        rollback_versions=tuple(str(deployment.version) for deployment in rollbacks),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--embedded-version",
        required=True,
        help="contents of /data/switch-embedded/.version",
    )
    args = parser.parse_args(argv)
    try:
        result = validate_firmware(sys.stdin.read(), embedded_version=args.embedded_version)
    except FirmwareGateError as error:
        raise SystemExit(f"FIRMWARE_GATE_NO_GO: {error}") from None
    print(
        json.dumps(
            {
                "event": "FIRMWARE_GATE_OK",
                "version": result.version,
                "booted_commit": result.booted_commit,
                "embedded_revision": result.embedded_revision,
                "rollback_versions": list(result.rollback_versions),
            },
            sort_keys=True,
        )
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
