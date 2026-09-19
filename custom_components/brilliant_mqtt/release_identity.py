"""Pure release identity policy. Ordinals are authority, digests only equality."""

from __future__ import annotations

import re
from collections.abc import Mapping
from dataclasses import asdict, dataclass
from types import MappingProxyType


@dataclass(frozen=True, repr=False)
class ReleaseIdentity:
    version: str
    release_ordinal: int | None
    digest: str
    deployment_id: str | None
    layout: str

    def __post_init__(self) -> None:
        if (
            re.fullmatch(r"[0-9A-Za-z][0-9A-Za-z._+-]{0,127}", self.version) is None
            or re.fullmatch(r"[0-9a-f]{64}", self.digest) is None
            or self.layout not in {"candidate", "legacy_fixed", "release_link"}
            or self.release_ordinal is not None
            and (type(self.release_ordinal) is not int or not 0 < self.release_ordinal < 10**16)
            or self.deployment_id is not None
            and re.fullmatch(r"[0-9A-Za-z_-]{1,128}", self.deployment_id) is None
        ):
            raise ValueError("invalid_release_identity")

    def __repr__(self) -> str:
        return "ReleaseIdentity(<redacted>)"

    def as_dict(self) -> dict[str, object]:
        return asdict(self)

    @classmethod
    def from_dict(cls, value: object) -> ReleaseIdentity:
        if not isinstance(value, dict) or set(value) != {
            "version",
            "release_ordinal",
            "digest",
            "deployment_id",
            "layout",
        }:
            raise ValueError("invalid_release_identity")
        version, digest, layout = value["version"], value["digest"], value["layout"]
        ordinal, deployment = value["release_ordinal"], value["deployment_id"]
        if (
            not all(isinstance(item, str) for item in (version, digest, layout))
            or ordinal is not None
            and type(ordinal) is not int
            or deployment is not None
            and not isinstance(deployment, str)
        ):
            raise ValueError("invalid_release_identity")
        return cls(version, ordinal, digest, deployment, layout)


@dataclass(frozen=True, repr=False)
class RollbackBaseline:
    """A content-bound reference to a durably completed private panel capture."""

    identifier: str
    digest: str
    identities: Mapping[str, ReleaseIdentity]

    def __post_init__(self) -> None:
        if (
            re.fullmatch(r"[0-9a-f]{32}", self.identifier) is None
            or re.fullmatch(r"[0-9a-f]{64}", self.digest) is None
            or not set(self.identities) <= {"bridge", "wifi_watchdog", "bus_watchdog"}
            or not all(isinstance(value, ReleaseIdentity) for value in self.identities.values())
        ):
            raise ValueError("baseline_invalid")
        object.__setattr__(self, "identities", MappingProxyType(dict(self.identities)))

    def __repr__(self) -> str:
        return "RollbackBaseline(<redacted>)"

    def as_dict(self) -> dict[str, object]:
        return {
            "identifier": self.identifier,
            "digest": self.digest,
            "identities": {key: value.as_dict() for key, value in self.identities.items()},
        }

    @classmethod
    def from_dict(cls, value: object) -> RollbackBaseline:
        if (
            not isinstance(value, dict)
            or set(value) != {"identifier", "digest", "identities"}
            or not isinstance(value["identifier"], str)
            or not isinstance(value["digest"], str)
            or not isinstance(value["identities"], dict)
        ):
            raise ValueError("baseline_invalid")
        return cls(
            value["identifier"],
            value["digest"],
            {
                key: ReleaseIdentity.from_dict(item)
                for key, item in value["identities"].items()
                if item is not None
            },
        )


def admit_identity(incumbent: ReleaseIdentity | None, candidate: ReleaseIdentity) -> bool:
    """Return whether code changes; never infer release age from version/digest."""
    if incumbent is None:
        return True
    if incumbent.digest == candidate.digest:
        return False
    if (
        incumbent.release_ordinal is not None
        and candidate.release_ordinal is not None
        and candidate.release_ordinal > incumbent.release_ordinal
    ):
        return True
    raise ValueError("release_identity_blocked")
