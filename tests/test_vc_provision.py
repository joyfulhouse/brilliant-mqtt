from __future__ import annotations

import base64
import json
import os
import stat
from dataclasses import replace
from pathlib import Path

import pytest

from tools.brilliant_vc.gates import GateLedger, GateName, GateStatus
from tools.brilliant_vc.provision_panel import (
    ProvisioningGuardError,
    ProvisioningResponse,
    ProvisionRequest,
    provision_virtual_control,
)

NOW = 2_000
PROPERTY_ID = "a" * 32
HOME_ID = "b" * 32
DEVICE_ID = "c" * 32


def _segment(value: object) -> str:
    raw = json.dumps(value, separators=(",", ":")).encode()
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


def _token(*, allowed: bool = True) -> bytes:
    paths = ["/provisioning/virtual-control-self-bootstrap"] if allowed else ["/graphql"]
    claims = {"iat": NOW - 100, "exp": NOW + 100, "allowed_paths": paths}
    return f"{_segment({'alg': 'RS256'})}.{_segment(claims)}.signature".encode()


class FakeClient:
    def __init__(self, response: ProvisioningResponse) -> None:
        self.response = response
        self.calls: list[tuple[str, str]] = []

    async def get_virtual_control_self_bootstrap(
        self, property_id: str, token: str
    ) -> ProvisioningResponse:
        self.calls.append((property_id, token))
        return self.response


def _response(**overrides: object) -> ProvisioningResponse:
    payload: dict[str, object] = {
        "device_id": DEVICE_ID,
        "pkcs12_certificate": "synthetic-certificate-value",
        "bootstrap": "synthetic-bootstrap-value",
    }
    payload.update(overrides)
    return ProvisioningResponse(http_status=200, payload=payload)


def _write_private(path: Path, content: bytes) -> None:
    path.write_bytes(content)
    path.chmod(0o600)


def _request(tmp_path: Path, *, apply: bool) -> ProvisionRequest:
    ledger_path = tmp_path / "ledger.json"
    ledger = GateLedger.new(run_id="run-01")
    ledger.record(GateName.VC0, GateStatus.PASS, "prior state audited", [])
    ledger.record(GateName.VC1, GateStatus.PASS, "official token observed", [])
    ledger.save(ledger_path)

    token_path = tmp_path / "bootstrap.token"
    _write_private(token_path, _token())

    approval_path: Path | None = None
    if apply:
        approval_path = tmp_path / "approval.json"
        _write_private(
            approval_path,
            json.dumps(
                {
                    "approved": True,
                    "approved_at_s": NOW - 30,
                    "run_id": "run-01",
                    "property_id": PROPERTY_ID,
                    "panel": "office",
                }
            ).encode(),
        )

    return ProvisionRequest(
        run_id="run-01",
        panel="office",
        property_id=PROPERTY_ID,
        expected_home_id=HOME_ID,
        identity_dir=tmp_path / "identity",
        token_file=token_path,
        ledger_path=ledger_path,
        apply=apply,
        approval_file=approval_path,
    )


@pytest.mark.asyncio
async def test_dry_run_never_calls_network_or_writes_identity(tmp_path: Path) -> None:
    client = FakeClient(_response())
    request = _request(tmp_path, apply=False)

    result = await provision_virtual_control(
        request,
        client=client,
        decode_target_home=lambda _: HOME_ID,
        now_s=NOW,
        required_uid=os.getuid(),
        legacy_record_path=tmp_path / "legacy-record",
    )

    assert result.dry_run is True
    assert result.http_status is None
    assert client.calls == []
    assert not request.identity_dir.exists()


@pytest.mark.asyncio
async def test_apply_requires_vc0_and_vc1_pass(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    ledger = GateLedger.new(run_id="run-01")
    ledger.record(GateName.VC0, GateStatus.PASS, "prior state audited", [])
    ledger.save(request.ledger_path)
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="VC1 must pass"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("change", "message"),
    [
        ({"approved": False}, "scope"),
        ({"approved_at_s": NOW - 601}, "older than 10 minutes"),
        ({"run_id": "other"}, "scope"),
        ({"property_id": "d" * 32}, "scope"),
        ({"panel": "kitchen"}, "scope"),
    ],
)
async def test_apply_requires_fresh_exact_approval(
    tmp_path: Path, change: dict[str, object], message: str
) -> None:
    request = _request(tmp_path, apply=True)
    assert request.approval_file is not None
    approval = json.loads(request.approval_file.read_text())
    approval.update(change)
    _write_private(request.approval_file, json.dumps(approval).encode())
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match=message):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_approval_must_be_private_regular_file(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    assert request.approval_file is not None
    request.approval_file.chmod(0o644)
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="mode 0600"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_non_provisioning_token_never_reaches_network(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    _write_private(request.token_file, _token(allowed=False))
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="self-bootstrap"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_nonempty_identity_directory_blocks_network(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    request.identity_dir.mkdir()
    (request.identity_dir / "existing").write_text("present")
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="identity directory"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_legacy_vc_record_blocks_network(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    legacy_record = tmp_path / "legacy-record"
    legacy_record.write_text("exists")
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="prior VC record"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=legacy_record,
        )

    assert client.calls == []


@pytest.mark.asyncio
@pytest.mark.parametrize("field", ["property_id", "expected_home_id"])
async def test_ids_must_be_32_lowercase_hex(tmp_path: Path, field: str) -> None:
    valid_request = _request(tmp_path, apply=True)
    request = (
        replace(valid_request, property_id="NOT-AN-ID")
        if field == "property_id"
        else replace(valid_request, expected_home_id="NOT-AN-ID")
    )
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="32 lowercase hex"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert client.calls == []


@pytest.mark.asyncio
async def test_non_200_response_writes_no_identity(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    client = FakeClient(ProvisioningResponse(http_status=403, payload={"ignored": "body"}))

    result = await provision_virtual_control(
        request,
        client=client,
        decode_target_home=lambda _: HOME_ID,
        now_s=NOW,
        required_uid=os.getuid(),
        legacy_record_path=tmp_path / "legacy-record",
    )

    assert result.http_status == 403
    assert result.identity_written is False
    assert len(client.calls) == 1
    assert not request.identity_dir.exists()
    assert "ignored" not in json.dumps(result.to_public_dict())


@pytest.mark.asyncio
@pytest.mark.parametrize("missing", ["device_id", "pkcs12_certificate", "bootstrap"])
async def test_200_response_requires_all_identity_fields(tmp_path: Path, missing: str) -> None:
    request = _request(tmp_path, apply=True)
    payload = dict(_response().payload)
    payload.pop(missing)
    client = FakeClient(ProvisioningResponse(http_status=200, payload=payload))

    with pytest.raises(ProvisioningGuardError, match="required identity fields"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: HOME_ID,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert not request.identity_dir.exists()


@pytest.mark.asyncio
async def test_target_home_mismatch_writes_no_identity(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    client = FakeClient(_response())

    with pytest.raises(ProvisioningGuardError, match="target home mismatch"):
        await provision_virtual_control(
            request,
            client=client,
            decode_target_home=lambda _: "d" * 32,
            now_s=NOW,
            required_uid=os.getuid(),
            legacy_record_path=tmp_path / "legacy-record",
        )

    assert not request.identity_dir.exists()


@pytest.mark.asyncio
async def test_success_writes_root_only_identity_once(tmp_path: Path) -> None:
    request = _request(tmp_path, apply=True)
    client = FakeClient(_response())

    result = await provision_virtual_control(
        request,
        client=client,
        decode_target_home=lambda _: HOME_ID,
        now_s=NOW,
        required_uid=os.getuid(),
        legacy_record_path=tmp_path / "legacy-record",
    )

    assert result.http_status == 200
    assert result.target_home_match is True
    assert result.identity_written is True
    assert result.device_id_redacted == "cccc…cccc"
    assert len(client.calls) == 1
    assert stat.S_IMODE(request.identity_dir.stat().st_mode) == 0o700
    assert {path.name for path in request.identity_dir.iterdir()} == {
        "bootstrap",
        "device_id",
        "metadata.json",
        "pkcs12_certificate",
    }
    assert all(
        stat.S_IMODE(path.stat().st_mode) == 0o600 for path in request.identity_dir.iterdir()
    )
    metadata = json.loads((request.identity_dir / "metadata.json").read_text())
    assert metadata == {
        "device_id_redacted": "cccc…cccc",
        "target_home_match": True,
    }
    public = json.dumps(result.to_public_dict())
    assert DEVICE_ID not in public
    assert "synthetic-certificate-value" not in public
    assert "synthetic-bootstrap-value" not in public
