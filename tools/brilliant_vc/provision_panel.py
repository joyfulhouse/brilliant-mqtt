"""One-shot, approval-gated Brilliant Virtual Control provisioner."""

from __future__ import annotations

import argparse
import asyncio
import base64
import json
import os
import re
import stat
import sys
import time
from collections.abc import Callable, Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Protocol, cast

from tools.brilliant_vc._common import redact as _redact_id
from tools.brilliant_vc._common import wipe as _wipe
from tools.brilliant_vc.gates import GateLedger, GateName, GateStatus
from tools.brilliant_vc.token_check import TokenCheckError, inspect_token

_ID = re.compile(r"^[0-9a-f]{32}$")
_MAX_PRIVATE_FILE_BYTES = 64 * 1024
_LEGACY_RECORD = Path("/tmp/mirror_poc/.vc_record.json")


class ProvisioningGuardError(RuntimeError):
    """Raised when any precondition or response validation fails closed."""


@dataclass(frozen=True, slots=True)
class ProvisionRequest:
    """Exact scope of one dry-run or approved provisioning request."""

    run_id: str
    panel: str
    property_id: str
    expected_home_id: str
    identity_dir: Path
    token_file: Path
    ledger_path: Path
    apply: bool
    approval_file: Path | None = None


@dataclass(frozen=True, slots=True)
class ProvisioningResponse:
    """Minimal response adapter that keeps body handling out of errors/logs."""

    http_status: int
    payload: Mapping[str, object]


@dataclass(frozen=True, slots=True)
class ProvisionResult:
    """Sanitized result suitable for an ignored gate artifact."""

    dry_run: bool
    http_status: int | None
    identity_written: bool
    target_home_match: bool | None
    device_id_redacted: str | None
    duration_ms: int

    def to_public_dict(self) -> dict[str, object]:
        return {
            "dry_run": self.dry_run,
            "http_status": self.http_status,
            "identity_written": self.identity_written,
            "target_home_match": self.target_home_match,
            "device_id_redacted": self.device_id_redacted,
            "duration_ms": self.duration_ms,
        }


class ProvisioningClient(Protocol):
    """Narrow adapter around the shipped Brilliant provisioning client."""

    async def get_virtual_control_self_bootstrap(
        self, property_id: str, token: str
    ) -> ProvisioningResponse: ...


async def provision_virtual_control(
    request: ProvisionRequest,
    *,
    client: ProvisioningClient,
    decode_target_home: Callable[[object], str],
    now_s: int,
    required_uid: int = 0,
    legacy_record_path: Path = _LEGACY_RECORD,
) -> ProvisionResult:
    """Validate every guard and, only with approval, provision exactly once."""

    started = time.monotonic()
    _validate_identifier(request.property_id, "property_id")
    _validate_identifier(request.expected_home_id, "expected_home_id")
    _validate_label(request.run_id, "run_id")
    _validate_label(request.panel, "panel")

    ledger = GateLedger.load(request.ledger_path)
    if ledger.run_id != request.run_id:
        raise ProvisioningGuardError("ledger run_id does not match request")
    for gate in (GateName.VC0, GateName.VC1):
        if ledger.status(gate) is not GateStatus.PASS:
            raise ProvisioningGuardError(f"{gate.value} must pass before VC2")

    _validate_no_legacy_record(legacy_record_path)
    _validate_identity_directory(request.identity_dir, required_uid=required_uid)

    token_buffer = _read_private_regular(
        request.token_file,
        required_uid=required_uid,
        description="bootstrap token",
        exact_mode_0600=False,
    )
    try:
        try:
            token_report = inspect_token(bytes(token_buffer).strip(), now_s)
        except TokenCheckError as exc:
            raise ProvisioningGuardError(f"bootstrap token guard failed: {exc}") from None
        if not token_report.vc1_claims_pass:
            raise ProvisioningGuardError("bootstrap token does not allow exact self-bootstrap path")

        if not request.apply:
            return ProvisionResult(
                dry_run=True,
                http_status=None,
                identity_written=False,
                target_home_match=None,
                device_id_redacted=None,
                duration_ms=_duration_ms(started),
            )

        if request.approval_file is None:
            raise ProvisioningGuardError("--apply requires an approval file")
        validate_approval(
            request.approval_file,
            run_id=request.run_id,
            property_id=request.property_id,
            panel=request.panel,
            now_s=now_s,
            required_uid=required_uid,
        )

        try:
            token_text = bytes(token_buffer).strip().decode("ascii")
            response = await client.get_virtual_control_self_bootstrap(
                request.property_id, token_text
            )
        except Exception:
            raise ProvisioningGuardError(
                "provisioning request ended without a confirmed response; "
                "inspect account and home graph before any retry"
            ) from None
    finally:
        _wipe(token_buffer)

    if response.http_status != 200:
        return ProvisionResult(
            dry_run=False,
            http_status=response.http_status,
            identity_written=False,
            target_home_match=None,
            device_id_redacted=None,
            duration_ms=_duration_ms(started),
        )

    identity = _validated_identity_payload(response.payload)
    device_id = cast(str, identity["device_id"])
    _validate_identifier(device_id, "response device_id")
    try:
        target_home_id = decode_target_home(identity["bootstrap"])
    except Exception:
        raise ProvisioningGuardError("could not safely decode bootstrap target home") from None
    if target_home_id != request.expected_home_id:
        raise ProvisioningGuardError("bootstrap target home mismatch; no identity written")

    redacted_id = _redact_id(device_id)
    _store_identity(request.identity_dir, identity, redacted_id=redacted_id)
    return ProvisionResult(
        dry_run=False,
        http_status=200,
        identity_written=True,
        target_home_match=True,
        device_id_redacted=redacted_id,
        duration_ms=_duration_ms(started),
    )


def validate_approval(
    path: Path,
    *,
    run_id: str,
    property_id: str,
    panel: str,
    now_s: int,
    required_uid: int = 0,
) -> None:
    """Validate a private approval document with an exact ten-minute scope."""

    raw = _read_private_regular(
        path,
        required_uid=required_uid,
        description="approval file",
        exact_mode_0600=True,
    )
    try:
        try:
            parsed: object = json.loads(raw)
        except (UnicodeDecodeError, json.JSONDecodeError):
            raise ProvisioningGuardError("approval file is not valid JSON") from None
    finally:
        _wipe(raw)
    if not isinstance(parsed, dict):
        raise ProvisioningGuardError("approval file must contain an object")
    approval = cast(dict[str, object], parsed)
    expected_fields = {"approved", "approved_at_s", "run_id", "property_id", "panel"}
    if set(approval) != expected_fields:
        raise ProvisioningGuardError("approval scope fields do not match this request")
    approved_at = approval.get("approved_at_s")
    if isinstance(approved_at, bool) or not isinstance(approved_at, int):
        raise ProvisioningGuardError("approval timestamp must be an integer")
    if approved_at > now_s + 30:
        raise ProvisioningGuardError("approval timestamp is in the future")
    if now_s - approved_at > 600:
        raise ProvisioningGuardError("approval is older than 10 minutes")
    expected: dict[str, object] = {
        "approved": True,
        "approved_at_s": approved_at,
        "run_id": run_id,
        "property_id": property_id,
        "panel": panel,
    }
    if approval != expected:
        raise ProvisioningGuardError("approval scope does not match this request")


def _read_private_regular(
    path: Path,
    *,
    required_uid: int,
    description: str,
    exact_mode_0600: bool,
) -> bytearray:
    try:
        before = path.lstat()
    except FileNotFoundError:
        raise ProvisioningGuardError(f"{description} does not exist") from None
    if stat.S_ISLNK(before.st_mode):
        raise ProvisioningGuardError(f"{description} must not be a symlink")
    if not stat.S_ISREG(before.st_mode):
        raise ProvisioningGuardError(f"{description} must be a regular file")
    if before.st_uid != required_uid:
        raise ProvisioningGuardError(f"{description} must be owned by UID {required_uid}")
    mode = stat.S_IMODE(before.st_mode)
    if exact_mode_0600:
        if mode != 0o600:
            raise ProvisioningGuardError(f"{description} must be mode 0600")
    elif mode & 0o077 or not mode & 0o400:
        raise ProvisioningGuardError(f"{description} must be private mode 0600 or 0400")
    if before.st_size > _MAX_PRIVATE_FILE_BYTES:
        raise ProvisioningGuardError(f"{description} exceeds 64 KiB")

    flags = os.O_RDONLY | getattr(os, "O_NOFOLLOW", 0)
    try:
        descriptor = os.open(path, flags)
    except OSError:
        raise ProvisioningGuardError(f"could not safely open {description}") from None
    data = bytearray()
    try:
        opened = os.fstat(descriptor)
        if (opened.st_dev, opened.st_ino) != (before.st_dev, before.st_ino):
            raise ProvisioningGuardError(f"{description} changed during open")
        while True:
            chunk = os.read(descriptor, min(8192, _MAX_PRIVATE_FILE_BYTES + 1 - len(data)))
            if not chunk:
                break
            data.extend(chunk)
            if len(data) > _MAX_PRIVATE_FILE_BYTES:
                raise ProvisioningGuardError(f"{description} exceeds 64 KiB")
        return data
    except Exception:
        _wipe(data)
        raise
    finally:
        os.close(descriptor)


def _validate_identity_directory(path: Path, *, required_uid: int) -> None:
    if path.is_symlink():
        raise ProvisioningGuardError("identity directory must not be a symlink")
    if not path.exists():
        return
    metadata = path.stat()
    if not stat.S_ISDIR(metadata.st_mode):
        raise ProvisioningGuardError("identity directory path is not a directory")
    if metadata.st_uid != required_uid:
        raise ProvisioningGuardError("identity directory has the wrong owner")
    if any(path.iterdir()):
        raise ProvisioningGuardError("identity directory must not contain prior identity data")


def _validate_no_legacy_record(path: Path) -> None:
    if path.exists() or path.is_symlink():
        raise ProvisioningGuardError("prior VC record exists; inspect before retrying")


def _validated_identity_payload(payload: Mapping[str, object]) -> dict[str, str | bytes]:
    required = ("device_id", "pkcs12_certificate", "bootstrap")
    if any(name not in payload for name in required):
        raise ProvisioningGuardError("HTTP 200 response lacks required identity fields")
    result: dict[str, str | bytes] = {}
    for name in required:
        value = payload[name]
        if not isinstance(value, (str, bytes)) or not value:
            raise ProvisioningGuardError("HTTP 200 response has invalid required identity fields")
        result[name] = value
    return result


def _store_identity(
    identity_dir: Path,
    identity: Mapping[str, str | bytes],
    *,
    redacted_id: str,
) -> None:
    created_dir = not identity_dir.exists()
    if created_dir:
        identity_dir.mkdir(mode=0o700, parents=True)
    os.chmod(identity_dir, 0o700)
    written: list[Path] = []
    try:
        for name in ("device_id", "pkcs12_certificate", "bootstrap"):
            value = identity[name]
            data = value.encode() if isinstance(value, str) else value
            path = identity_dir / name
            _exclusive_write(path, data)
            written.append(path)
        metadata_path = identity_dir / "metadata.json"
        metadata = (
            json.dumps(
                {"device_id_redacted": redacted_id, "target_home_match": True},
                indent=2,
                sort_keys=True,
            ).encode()
            + b"\n"
        )
        _exclusive_write(metadata_path, metadata)
        written.append(metadata_path)
        directory_fd = os.open(identity_dir, os.O_RDONLY)
        try:
            os.fsync(directory_fd)
        finally:
            os.close(directory_fd)
    except Exception:
        for path in written:
            path.unlink(missing_ok=True)
        if created_dir:
            identity_dir.rmdir()
        raise


def _exclusive_write(path: Path, data: bytes) -> None:
    flags = os.O_WRONLY | os.O_CREAT | os.O_EXCL | getattr(os, "O_NOFOLLOW", 0)
    descriptor = os.open(path, flags, 0o600)
    try:
        os.fchmod(descriptor, 0o600)
        view = memoryview(data)
        written = 0
        while written < len(view):
            written += os.write(descriptor, view[written:])
        os.fsync(descriptor)
    finally:
        os.close(descriptor)


def _validate_identifier(value: str, name: str) -> None:
    if not _ID.fullmatch(value):
        raise ProvisioningGuardError(f"{name} must be 32 lowercase hex characters")


def _validate_label(value: str, name: str) -> None:
    if not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]{0,127}", value):
        raise ProvisioningGuardError(f"{name} is not a safe label")


def _duration_ms(started: float) -> int:
    return max(0, round((time.monotonic() - started) * 1000))


class _NoNetworkClient:
    async def get_virtual_control_self_bootstrap(
        self, property_id: str, token: str
    ) -> ProvisioningResponse:
        raise AssertionError("dry run attempted a network request")


class _LiveClient:
    def __init__(self, loop: asyncio.AbstractEventLoop) -> None:
        from lib.clients.web_api.client import WebAPIClientSession
        from lib.clients.web_api.provisioning import WebAPIProvisioningClient

        session = WebAPIClientSession(
            cert_dir="/var/device_variables/pki",
            cert_file_prefix="device",
            web_api_server_prefix="https://web-api.brilliant.tech",
            cert_port=443,
            non_cert_port=443,
            loop=loop,
            use_ssl=True,
        )
        self._client = WebAPIProvisioningClient(session)

    async def get_virtual_control_self_bootstrap(
        self, property_id: str, token: str
    ) -> ProvisioningResponse:
        response = await self._client.get_virtual_control_self_bootstrap(property_id, token)
        status = int(getattr(response, "http_status", 0))
        if status != 200:
            return ProvisioningResponse(http_status=status, payload={})
        payload: object = response.json()
        if not isinstance(payload, dict):
            raise ProvisioningGuardError("HTTP 200 response is not a JSON object")
        return ProvisioningResponse(http_status=status, payload=cast(dict[str, object], payload))


def _decode_target_home(bootstrap: object) -> str:
    if isinstance(bootstrap, str):
        try:
            raw = base64.b64decode(bootstrap, validate=True)
        except ValueError:
            raise ProvisioningGuardError("bootstrap is not valid base64") from None
    elif isinstance(bootstrap, bytes):
        raw = bootstrap
    else:
        raise ProvisioningGuardError("bootstrap has an invalid type")

    from thrift.protocol import TBinaryProtocol
    from thrift.transport import TTransport
    from thrift_types.bootstrap.ttypes import BootstrapParameters

    parameters = BootstrapParameters()
    transport = TTransport.TMemoryBuffer(raw)
    parameters.read(TBinaryProtocol.TBinaryProtocol(transport))
    target = parameters.target_home_id
    if not isinstance(target, str):
        raise ProvisioningGuardError("bootstrap target_home_id is missing")
    return target


async def _run_cli(args: argparse.Namespace) -> ProvisionResult:
    request = ProvisionRequest(
        run_id=cast(str, args.run_id),
        panel=cast(str, args.panel),
        property_id=cast(str, args.property_id),
        expected_home_id=cast(str, args.expected_home_id),
        identity_dir=cast(Path, args.identity_dir),
        token_file=cast(Path, args.token_file),
        ledger_path=cast(Path, args.ledger),
        apply=cast(bool, args.apply),
        approval_file=cast(Path | None, args.approval_file),
    )
    client: ProvisioningClient
    if request.apply:
        client = _LiveClient(asyncio.get_running_loop())
    else:
        client = _NoNetworkClient()
    return await provision_virtual_control(
        request,
        client=client,
        decode_target_home=_decode_target_home,
        now_s=int(time.time()),
    )


def main(argv: Sequence[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-id", required=True)
    parser.add_argument("--panel", default="office")
    parser.add_argument("--ledger", type=Path, required=True)
    parser.add_argument("--token-file", type=Path, required=True)
    parser.add_argument("--property-id", required=True)
    parser.add_argument("--expected-home-id", required=True)
    parser.add_argument("--identity-dir", type=Path, required=True)
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--approval-file", type=Path)
    args = parser.parse_args(argv)
    result = asyncio.run(_run_cli(args))
    print(json.dumps(result.to_public_dict(), sort_keys=True))
    if result.dry_run:
        print("DRY RUN — no provisioning request sent")
    return 0 if result.dry_run or result.identity_written else 2


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ProvisioningGuardError as exc:
        print(f"VC2 provisioning blocked: {exc}", file=sys.stderr)
        sys.exit(2)
