"""Validated ownership metadata shared by config and provisioning storage."""

from __future__ import annotations

from collections.abc import Mapping
from dataclasses import dataclass, field
from uuid import UUID


def canonical_commit_id(value: object) -> str:
    """Require a canonical UUID, without interpreting absent metadata as proof."""
    if not isinstance(value, str) or str(UUID(value)) != value:
        raise ValueError("invalid_connection_commit_id")
    return value


def _text(value: object) -> str:
    if not isinstance(value, str) or not value or not value.isascii() or not value.isprintable():
        raise ValueError("invalid_connection_state")
    return value


def _mapping(value: object, keys: set[str]) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or set(value) != keys:
        raise ValueError("invalid_connection_state")
    return value


@dataclass(frozen=True, slots=True)
class PanelOwner:
    """Original config IDs and immutable panel identity; never an address alias."""

    entry_id: str
    subentry_id: str | None
    slug: str
    public_key: str = field(repr=False)

    def __post_init__(self) -> None:
        for value in (self.entry_id, self.slug, self.public_key):
            _text(value)
        if self.subentry_id is not None:
            _text(self.subentry_id)

    def to_storage(self) -> dict[str, object]:
        return {
            "entry_id": self.entry_id,
            "subentry_id": self.subentry_id,
            "slug": self.slug,
            "public_key": self.public_key,
        }

    @classmethod
    def from_storage(cls, value: object) -> PanelOwner:
        raw = _mapping(value, {"entry_id", "subentry_id", "slug", "public_key"})
        return cls(
            _text(raw["entry_id"]),
            None if raw["subentry_id"] is None else _text(raw["subentry_id"]),
            _text(raw["slug"]),
            _text(raw["public_key"]),
        )


@dataclass(frozen=True, slots=True, repr=False)
class PanelConnection:
    host: str
    username: str
    password: str = field(repr=False)
    public_key: str = field(repr=False)

    def __post_init__(self) -> None:
        for value in (self.host, self.username, self.public_key):
            _text(value)
        if (
            not isinstance(self.password, str)
            or not self.password
            or "\x00" in self.password
            or len(self.password.encode()) > 16384
        ):
            raise ValueError("invalid_connection_state")

    def to_storage(self) -> dict[str, object]:
        return {
            "host": self.host,
            "username": self.username,
            "password": self.password,
            "public_key": self.public_key,
        }

    @classmethod
    def from_storage(cls, value: object) -> PanelConnection:
        raw = _mapping(value, {"host", "username", "password", "public_key"})
        password = raw["password"]
        if not isinstance(password, str):
            raise ValueError("invalid_connection_state")
        return cls(_text(raw["host"]), _text(raw["username"]), password, _text(raw["public_key"]))


@dataclass(frozen=True, slots=True, repr=False)
class ConnectionIntent:
    commit_id: str
    owner: PanelOwner
    prior: PanelConnection
    proposed: PanelConnection
    transactions: tuple[str, ...]
    active_transaction: str | None = None
    retained_transactions: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        canonical_commit_id(self.commit_id)
        if (
            self.prior.public_key != self.owner.public_key
            or self.proposed.public_key != self.owner.public_key
        ):
            raise ValueError("invalid_connection_state")
        for value in self.transactions:
            canonical_commit_id(value)
        if self.active_transaction is not None:
            canonical_commit_id(self.active_transaction)
        for value in self.retained_transactions:
            canonical_commit_id(value)

    def to_storage(self) -> dict[str, object]:
        return {
            "commit_id": self.commit_id,
            "owner": self.owner.to_storage(),
            "prior": self.prior.to_storage(),
            "proposed": self.proposed.to_storage(),
            "transactions": list(self.transactions),
            "active_transaction": self.active_transaction,
            "retained_transactions": list(self.retained_transactions),
        }

    @classmethod
    def from_storage(cls, value: object) -> ConnectionIntent:
        raw = _mapping(
            value,
            {
                "commit_id",
                "owner",
                "prior",
                "proposed",
                "transactions",
                "active_transaction",
                "retained_transactions",
            },
        )
        transactions = raw["transactions"]
        if not isinstance(transactions, list):
            raise ValueError("invalid_connection_state")
        retained = raw["retained_transactions"]
        if not isinstance(retained, list):
            raise ValueError("invalid_connection_state")
        return cls(
            canonical_commit_id(raw["commit_id"]),
            PanelOwner.from_storage(raw["owner"]),
            PanelConnection.from_storage(raw["prior"]),
            PanelConnection.from_storage(raw["proposed"]),
            tuple(canonical_commit_id(item) for item in transactions),
            None
            if raw["active_transaction"] is None
            else canonical_commit_id(raw["active_transaction"]),
            tuple(canonical_commit_id(item) for item in retained),
        )
