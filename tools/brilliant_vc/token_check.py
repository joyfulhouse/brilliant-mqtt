"""Claims-only inspection of an official Virtual Control bootstrap token."""

from __future__ import annotations

import argparse
import base64
import binascii
import hashlib
import json
import os
import re
import stat
import sys
import tempfile
import time
from collections.abc import Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import cast

SELF_BOOTSTRAP_PATH = "/provisioning/virtual-control-self-bootstrap"
MAX_TOKEN_BYTES = 64 * 1024
_JWT_SEGMENT = re.compile(r"^[A-Za-z0-9_-]+$")


class TokenCheckError(ValueError):
    """Raised when a token or its storage fails a claims-only guard."""


@dataclass(frozen=True, slots=True)
class TokenReport:
    """Sanitized token facts; never includes token or raw claim values."""

    jwt_shape: bool
    expires_at: int
    issued_at: int
    time_valid: bool
    allows_self_bootstrap: bool
    fingerprint8: str
    issuer_sha256: str | None
    audience_sha256: str | None
    claims_only: bool = True

    @property
    def vc1_claims_pass(self) -> bool:
        """Whether the time and exact allow-path claims satisfy VC1."""

        return self.time_valid and self.allows_self_bootstrap

    def to_public_dict(self) -> dict[str, object]:
        """Return only sanitized, explicitly allowlisted report fields."""

        return {
            "verification": "claims-only",
            "jwt_shape": self.jwt_shape,
            "issued_at": self.issued_at,
            "expires_at": self.expires_at,
            "time_valid": self.time_valid,
            "allows_self_bootstrap": self.allows_self_bootstrap,
            "vc1_claims_pass": self.vc1_claims_pass,
            "fingerprint8": self.fingerprint8,
            "issuer_sha256": self.issuer_sha256,
            "audience_sha256": self.audience_sha256,
        }


def inspect_token(raw: bytes, now_s: int) -> TokenReport:
    """Parse JWT claims without asserting signature authenticity."""

    try:
        text = raw.decode("ascii")
    except UnicodeDecodeError as exc:
        raise TokenCheckError("bootstrap token is not ASCII JWT data") from exc
    parts = text.split(".")
    if len(parts) != 3 or any(not part or not _JWT_SEGMENT.fullmatch(part) for part in parts):
        raise TokenCheckError("bootstrap token is not JWT-shaped")
    header = _decode_json_segment(parts[0], "header")
    claims = _decode_json_segment(parts[1], "claims")
    if not isinstance(header, dict) or not isinstance(claims, dict):
        raise TokenCheckError("JWT header and claims must be objects")
    claim_map = cast(dict[str, object], claims)

    issued_at = _required_int_claim(claim_map, "iat")
    expires_at = _required_int_claim(claim_map, "exp")
    if issued_at > now_s:
        raise TokenCheckError("bootstrap token iat is in the future")
    if expires_at <= now_s:
        raise TokenCheckError("bootstrap token is expired")
    if expires_at <= issued_at:
        raise TokenCheckError("bootstrap token exp must follow iat")

    raw_allowed = claim_map.get("allowed_paths", ())
    if not isinstance(raw_allowed, (list, tuple)) or not all(
        isinstance(item, str) for item in raw_allowed
    ):
        raise TokenCheckError("allowed_paths must be a string list")
    allowed_paths = cast(Sequence[str], raw_allowed)

    return TokenReport(
        jwt_shape=True,
        expires_at=expires_at,
        issued_at=issued_at,
        time_valid=True,
        allows_self_bootstrap=SELF_BOOTSTRAP_PATH in allowed_paths,
        fingerprint8=hashlib.sha256(raw).hexdigest()[:8],
        issuer_sha256=_hash_scalar_claim(claim_map.get("iss"), "iss"),
        audience_sha256=_hash_audience(claim_map.get("aud")),
    )


def inspect_token_file(
    path: Path,
    *,
    now_s: int,
    required_uid: int = 0,
) -> TokenReport:
    """Open a private regular token file without following links."""

    before = path.lstat()
    if stat.S_ISLNK(before.st_mode):
        raise TokenCheckError("bootstrap token file must not be a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise TokenCheckError("bootstrap token path must be a regular file")
    if before.st_uid != required_uid:
        raise TokenCheckError(f"bootstrap token file must be owned by UID {required_uid}")
    mode = stat.S_IMODE(before.st_mode)
    if mode & 0o077 or not mode & 0o400:
        raise TokenCheckError("bootstrap token file must use private mode 0600 or 0400")
    if before.st_size > MAX_TOKEN_BYTES:
        raise TokenCheckError("bootstrap token file exceeds 64 KiB")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError as exc:
        if path.is_symlink():
            raise TokenCheckError("bootstrap token file must not be a symlink") from exc
        raise TokenCheckError("could not safely open bootstrap token file") from exc

    buffer = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise TokenCheckError("bootstrap token file changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, MAX_TOKEN_BYTES + 1 - len(buffer)))
            if not chunk:
                break
            buffer.extend(chunk)
            if len(buffer) > MAX_TOKEN_BYTES:
                raise TokenCheckError("bootstrap token file exceeds 64 KiB")
        token = bytes(buffer).strip()
        return inspect_token(token, now_s)
    finally:
        os.close(descriptor)
        for index in range(len(buffer)):
            buffer[index] = 0


def _decode_json_segment(segment: str, name: str) -> object:
    padding = "=" * (-len(segment) % 4)
    try:
        raw = base64.b64decode(segment + padding, altchars=b"-_", validate=True)
        return json.loads(raw)
    except (binascii.Error, UnicodeDecodeError, json.JSONDecodeError) as exc:
        raise TokenCheckError(f"JWT {name} is not valid base64url JSON") from exc


def _required_int_claim(claims: dict[str, object], name: str) -> int:
    value = claims.get(name)
    if isinstance(value, bool) or not isinstance(value, int):
        raise TokenCheckError(f"bootstrap token requires integer {name}")
    return value


def _hash_scalar_claim(value: object, name: str) -> str | None:
    if value is None:
        return None
    if not isinstance(value, str):
        raise TokenCheckError(f"{name} must be a string")
    return hashlib.sha256(value.encode()).hexdigest()


def _hash_audience(value: object) -> str | None:
    if value is None:
        return None
    if isinstance(value, str):
        canonical = value
    elif isinstance(value, list) and all(isinstance(item, str) for item in value):
        audiences = cast(list[str], value)
        canonical = json.dumps(sorted(audiences), separators=(",", ":"))
    else:
        raise TokenCheckError("aud must be a string or string list")
    return hashlib.sha256(canonical.encode()).hexdigest()


def _atomic_report(path: Path, report: TokenReport) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    descriptor, temp_name = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
    try:
        os.fchmod(descriptor, 0o600)
        with os.fdopen(descriptor, "w", encoding="utf-8") as handle:
            descriptor = -1
            json.dump(report.to_public_dict(), handle, indent=2, sort_keys=True)
            handle.write("\n")
            handle.flush()
            os.fsync(handle.fileno())
        os.replace(temp_name, path)
    finally:
        if descriptor >= 0:
            os.close(descriptor)
        Path(temp_name).unlink(missing_ok=True)


def main(argv: Sequence[str] | None = None) -> int:
    """Inspect a root-only token and emit a sanitized claims-only report."""

    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--report", type=Path, required=True)
    args = parser.parse_args(argv)
    report = inspect_token_file(args.token_file, now_s=int(time.time()))
    _atomic_report(args.report, report)
    return 0 if report.vc1_claims_pass else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except TokenCheckError as exc:
        print(f"VC1 token check failed: {exc}", file=sys.stderr)
        sys.exit(2)
