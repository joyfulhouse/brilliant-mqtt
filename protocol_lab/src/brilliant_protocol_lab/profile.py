from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum
from typing import cast


class EvidenceStatus(StrEnum):
    KNOWN = "known"
    UNKNOWN = "unknown"


@dataclass(frozen=True)
class Evidence:
    status: EvidenceStatus
    value: object | None
    source: str

    @classmethod
    def known(cls, value: object, source: str) -> Evidence:
        return cls(EvidenceStatus.KNOWN, value, source)

    @classmethod
    def unknown(cls, reason: str) -> Evidence:
        return cls(EvidenceStatus.UNKNOWN, None, reason)


@dataclass(frozen=True)
class ProtocolProfile:
    init_service: Evidence
    provisioning_methods: Evidence
    thrift_type_graph: Evidence
    framing: Evidence
    protocol: Evidence
    tls: Evidence
    commitment: Evidence
    hardware_attestation: Evidence
    removal_path: Evidence

    def blockers(self) -> tuple[str, ...]:
        blocked: list[str] = []
        for field in fields(self):
            evidence = cast(Evidence, getattr(self, field.name))
            if evidence.status is EvidenceStatus.UNKNOWN:
                blocked.append(f"{field.name}: {evidence.source}")
        if (
            self.hardware_attestation.status is EvidenceStatus.KNOWN
            and self.hardware_attestation.value is not False
        ):
            blocked.append("hardware_attestation: proprietary attestation required")
        allowed_commitments = {"hmac-sha256", "sha256-client-server", "sha256-server-client"}
        if (
            self.commitment.status is EvidenceStatus.KNOWN
            and self.commitment.value not in allowed_commitments
        ):
            blocked.append("commitment: no matched standard primitive")
        return tuple(blocked)

    def ready_for_pairing(self) -> bool:
        return not self.blockers()
