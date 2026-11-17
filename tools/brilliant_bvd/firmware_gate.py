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
from dataclasses import dataclass, replace


@dataclass(frozen=True, slots=True)
class FirmwarePin:
    """The one reviewed firmware identity the pilot may run on."""

    version: str
    ostree_commit: str
    embedded_revision: str


PINNED_FIRMWARE = FirmwarePin(
    version="v26.10.21.1",
    ostree_commit="aae2bfc59cee9b3a36af252c1e90c7898f41a59f5033567f665ca807eba82ac0",
    embedded_revision="4329f9ae9c20c3d50c80a5f44a818d23602b9b30",
)

_HEADER = re.compile(
    r"(?P<booted>[* ]) (?P<osname>[A-Za-z0-9_.-]+) (?P<checksum>[0-9a-f]{64})\.(?P<serial>[0-9]+)"
    r"(?: \((?P<status>staged|pending|rollback|finalization locked)\))?"
)
_DETAIL = re.compile(r"    \S.*")
_VERSION = re.compile(r"    Version: (?P<version>\S+)")
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
    version: str | None


@dataclass(frozen=True, slots=True)
class FirmwareGateResult:
    """Sanitized facts proving the pinned firmware is booted with nothing queued."""

    version: str
    booted_commit: str
    embedded_revision: str
    rollback_versions: tuple[str, ...]


def parse_ostree_admin_status(text: str) -> tuple[OstreeDeployment, ...]:
    """Parse ``ostree admin status`` text; unrecognized lines fail closed."""

    deployments: list[OstreeDeployment] = []
    for line in text.splitlines():
        if not line.strip():
            continue
        header = _HEADER.fullmatch(line)
        if header is not None:
            deployments.append(
                OstreeDeployment(
                    osname=header["osname"],
                    checksum=header["checksum"],
                    serial=int(header["serial"]),
                    booted=header["booted"] == "*",
                    status=header["status"] or "",
                    version=None,
                )
            )
            continue
        if _DETAIL.fullmatch(line) is None:
            raise FirmwareGateError("unrecognized ostree admin status line")
        if not deployments:
            raise FirmwareGateError("ostree detail line appeared before any deployment")
        version = _VERSION.fullmatch(line)
        if version is not None:
            if deployments[-1].version is not None:
                raise FirmwareGateError("deployment has more than one Version line")
            deployments[-1] = replace(deployments[-1], version=version["version"])
    if not deployments:
        raise FirmwareGateError("ostree admin status listed no deployments")
    return tuple(deployments)


def validate_firmware(
    status_text: str,
    *,
    embedded_version: str,
    pin: FirmwarePin = PINNED_FIRMWARE,
) -> FirmwareGateResult:
    """Require the pinned booted commit/version/embedded stack and no queued OTA."""

    if _EMBEDDED_REVISION.fullmatch(embedded_version) is None:
        raise FirmwareGateError("embedded-stack revision is malformed")
    embedded_revision = embedded_version.rstrip("\n")
    if embedded_revision != pin.embedded_revision:
        raise FirmwareGateError("embedded-stack revision does not match the pin")
    deployments = parse_ostree_admin_status(status_text)
    booted = [deployment for deployment in deployments if deployment.booted]
    if len(booted) != 1 or booted[0].status:
        raise FirmwareGateError("ostree must report exactly one booted deployment")
    if any(deployment.status in _NEXT_BOOT_STATUSES for deployment in deployments):
        raise FirmwareGateError("a staged or pending OSTree deployment is queued")
    current = booted[0]
    if current.checksum != pin.ostree_commit:
        raise FirmwareGateError("booted OSTree commit does not match the pin")
    if current.version != pin.version:
        raise FirmwareGateError("booted version does not match the pin")
    return FirmwareGateResult(
        version=current.version,
        booted_commit=current.checksum,
        embedded_revision=embedded_revision,
        rollback_versions=tuple(
            deployment.version or "unknown"
            for deployment in deployments
            if deployment.status == "rollback"
        ),
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
