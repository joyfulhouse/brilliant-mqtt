from __future__ import annotations

from dataclasses import replace
from pathlib import Path

from compile_profile import compile_profile

from brilliant_protocol_lab.profile import Evidence, ProtocolProfile


def _known(value: object) -> Evidence:
    return Evidence.known(value, "synthetic-test")


def _complete_profile() -> ProtocolProfile:
    return ProtocolProfile(
        init_service=_known("_init-brilliant._tcp.local."),
        provisioning_methods=_known(
            (
                "search_for_available_homes",
                "knock_on_home",
                "request_provisioning_with_code",
                "join_home",
            )
        ),
        thrift_type_graph=_known({"join_home_args": {"fields": []}}),
        framing=_known("framed"),
        protocol=_known("binary"),
        tls=_known(False),
        commitment=_known("hmac-sha256"),
        hardware_attestation=_known(False),
        removal_path=_known("app-remove-then-local-delete"),
    )


def test_unknown_transport_blocks_pairing_plan() -> None:
    profile = _complete_profile()
    profile = replace(profile, framing=Evidence.unknown("no loopback capture"))
    assert profile.ready_for_pairing() is False
    assert profile.blockers() == ("framing: no loopback capture",)


def test_complete_standard_profile_allows_pairing_plan() -> None:
    profile = _complete_profile()
    assert profile.ready_for_pairing() is True
    assert profile.blockers() == ()


def test_non_standard_commitment_blocks_pairing_plan() -> None:
    profile = _complete_profile()
    profile = replace(profile, commitment=_known("rot13"))
    assert profile.ready_for_pairing() is False
    assert profile.blockers() == ("commitment: no matched standard primitive",)


def test_active_hardware_attestation_blocks_pairing_plan() -> None:
    profile = _complete_profile()
    profile = replace(profile, hardware_attestation=_known(True))
    assert profile.ready_for_pairing() is False
    assert profile.blockers() == ("hardware_attestation: proprietary attestation required",)


_STRUCTURE_FIXTURE: object = {
    "format": 1,
    "modules": {
        "peripherals.bootstrap.device_provisioning_client": {
            "classes": {
                "DeviceProvisioningClient": {
                    "signature": "(self)",
                    "fields": [],
                    "methods": {
                        "search_for_available_homes": "(self)",
                        "knock_on_home": "(self, home_id)",
                        "request_provisioning_with_code": "(self, home_id, authentication_code)",
                        "join_home": "(self, client_device_id, home_id, client_commitment_secret)",
                    },
                }
            },
            "callables": {},
        },
        "thrift_types.bootstrap.ttypes": {
            "classes": {
                "join_home_args": {
                    "signature": "(self)",
                    "fields": [],
                    "methods": {},
                }
            },
            "callables": {},
        },
    },
}

_MDNS_FIXTURE: object = [
    {
        "service_type": "_init-brilliant._tcp.local.",
        "instance": "_init-brilliant._tcp.local.",
        "addresses": ["127.0.0.1"],
        "port": 8080,
        "properties": {},
    }
]

_CAPTURE_FIXTURE: object = {"framing": "framed", "protocol": "binary", "tls": False}

_COMMITMENT_FIXTURE: object = {
    "commitment": "hmac-sha256",
    "hardware_attestation": False,
    "vector_count": 3,
}

_REMOVAL_FIXTURE: object = {"removal_path": "app-remove-then-local-delete"}


def test_compiler_marks_missing_capture_as_unknown_and_blocks(tmp_path: Path) -> None:
    profile = compile_profile(
        structure=_STRUCTURE_FIXTURE,
        structure_source=str(tmp_path / "structure.json"),
        mdns=_MDNS_FIXTURE,
        mdns_source=str(tmp_path / "mdns.json"),
        capture=None,
        capture_source=str(tmp_path / "capture.json"),
        commitment=_COMMITMENT_FIXTURE,
        commitment_source=str(tmp_path / "commitment.json"),
        removal=_REMOVAL_FIXTURE,
        removal_source=str(tmp_path / "removal.json"),
    )
    assert profile.ready_for_pairing() is False
    blockers = profile.blockers()
    capture_source = str(tmp_path / "capture.json")
    assert any("framing" in blocker and capture_source in blocker for blocker in blockers)
    assert any("protocol" in blocker and capture_source in blocker for blocker in blockers)
    assert any("tls" in blocker and capture_source in blocker for blocker in blockers)


def test_compiler_fully_known_synthetic_set_is_ready(tmp_path: Path) -> None:
    profile = compile_profile(
        structure=_STRUCTURE_FIXTURE,
        structure_source=str(tmp_path / "structure.json"),
        mdns=_MDNS_FIXTURE,
        mdns_source=str(tmp_path / "mdns.json"),
        capture=_CAPTURE_FIXTURE,
        capture_source=str(tmp_path / "capture.json"),
        commitment=_COMMITMENT_FIXTURE,
        commitment_source=str(tmp_path / "commitment.json"),
        removal=_REMOVAL_FIXTURE,
        removal_source=str(tmp_path / "removal.json"),
    )
    assert profile.ready_for_pairing() is True
    assert profile.blockers() == ()


def test_compiler_never_invents_a_default_when_all_input_is_absent() -> None:
    profile = compile_profile(
        structure=None,
        structure_source="/private/tmp/brilliant-structure.oracle.local.json",
        mdns=None,
        mdns_source="/private/tmp/brilliant-mdns.oracle.local.json",
        capture=None,
        capture_source="/private/tmp/brilliant-loopback-capture.oracle.local.json",
        commitment=None,
        commitment_source="/private/tmp/brilliant-commitment.oracle.local.json",
        removal=None,
        removal_source="/private/tmp/brilliant-removal.oracle.local.json",
    )
    assert profile.ready_for_pairing() is False
    for field_name in (
        "init_service",
        "provisioning_methods",
        "thrift_type_graph",
        "framing",
        "protocol",
        "tls",
        "commitment",
        "hardware_attestation",
        "removal_path",
    ):
        evidence = getattr(profile, field_name)
        assert evidence.status.value == "unknown"
        assert evidence.value is None


def test_compiler_treats_unresolved_commitment_as_unknown() -> None:
    profile = compile_profile(
        structure=_STRUCTURE_FIXTURE,
        structure_source="/private/tmp/brilliant-structure.oracle.local.json",
        mdns=_MDNS_FIXTURE,
        mdns_source="/private/tmp/brilliant-mdns.oracle.local.json",
        capture=_CAPTURE_FIXTURE,
        capture_source="/private/tmp/brilliant-loopback-capture.oracle.local.json",
        commitment={"commitment": None, "hardware_attestation": None, "vector_count": 3},
        commitment_source="/private/tmp/brilliant-commitment.oracle.local.json",
        removal=_REMOVAL_FIXTURE,
        removal_source="/private/tmp/brilliant-removal.oracle.local.json",
    )
    assert profile.ready_for_pairing() is False
    assert profile.commitment.status.value == "unknown"
    assert profile.hardware_attestation.status.value == "unknown"


def test_compiler_treats_recorded_unknown_removal_as_unknown() -> None:
    profile = compile_profile(
        structure=_STRUCTURE_FIXTURE,
        structure_source="/private/tmp/brilliant-structure.oracle.local.json",
        mdns=_MDNS_FIXTURE,
        mdns_source="/private/tmp/brilliant-mdns.oracle.local.json",
        capture=_CAPTURE_FIXTURE,
        capture_source="/private/tmp/brilliant-loopback-capture.oracle.local.json",
        commitment=_COMMITMENT_FIXTURE,
        commitment_source="/private/tmp/brilliant-commitment.oracle.local.json",
        removal={"removal_path": "unknown"},
        removal_source="/private/tmp/brilliant-removal.oracle.local.json",
    )
    assert profile.ready_for_pairing() is False
    assert profile.removal_path.status.value == "unknown"


def test_compiler_missing_required_method_blocks_with_named_gap() -> None:
    incomplete_structure = {
        "format": 1,
        "modules": {
            "peripherals.bootstrap.device_provisioning_client": {
                "classes": {
                    "DeviceProvisioningClient": {
                        "signature": "(self)",
                        "fields": [],
                        "methods": {"search_for_available_homes": "(self)"},
                    }
                },
                "callables": {},
            }
        },
    }
    profile = compile_profile(
        structure=incomplete_structure,
        structure_source="/private/tmp/brilliant-structure.oracle.local.json",
        mdns=_MDNS_FIXTURE,
        mdns_source="/private/tmp/brilliant-mdns.oracle.local.json",
        capture=_CAPTURE_FIXTURE,
        capture_source="/private/tmp/brilliant-loopback-capture.oracle.local.json",
        commitment=_COMMITMENT_FIXTURE,
        commitment_source="/private/tmp/brilliant-commitment.oracle.local.json",
        removal=_REMOVAL_FIXTURE,
        removal_source="/private/tmp/brilliant-removal.oracle.local.json",
    )
    assert profile.ready_for_pairing() is False
    blocker = next(b for b in profile.blockers() if b.startswith("provisioning_methods"))
    assert "knock_on_home" in blocker
    assert "request_provisioning_with_code" in blocker
    assert "join_home" in blocker
