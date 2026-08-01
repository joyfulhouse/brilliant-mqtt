"""Durable, redaction-safe state for one in-flight panel provisioning operation."""

from __future__ import annotations

import asyncio
import base64
import binascii
import re
from collections.abc import Coroutine, Mapping
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from typing import Any, Self
from uuid import RFC_4122, UUID

from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import Store as HomeAssistantStore
from homeassistant.util import json as json_util

from .broker import BrokerKind
from .shell import HostIdentity, PanelIdentityError, known_hosts_line

JOURNAL_STORAGE_VERSION = 1
JOURNAL_STORAGE_KEY = "brilliant_mqtt.provisioning"
_JOURNAL_COORDINATOR_KEY = "brilliant_mqtt.provisioning_journal.coordinator"
_STORE_LOAD_TIMEOUT_SECONDS = 30.0
_MISSING = object()

_MAX_SECRET_BYTES = 16 * 1024
_MAX_FILE_BYTES = 1024 * 1024
_MAX_PUBLIC_KEY_BYTES = 16 * 1024
_MAX_VERSION_BYTES = 128
_COMPONENT_ID = re.compile(r"^[a-z][a-z0-9_]{0,63}$")
_ERROR_TOKEN = re.compile(r"^[a-z][a-z0-9_]{0,127}$")
_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_SSH_USERNAME = re.compile(r"^[A-Za-z0-9._-]{1,64}$")
_VERSION_PATTERN = r"[0-9A-Za-z][0-9A-Za-z._+-]{0,127}"
_VERSION = re.compile(rf"^{_VERSION_PATTERN}$")
_RELEASE_TARGET = re.compile(rf"^/var/brilliant-mqtt/releases/{_VERSION_PATTERN}--[0-9a-f]{{32}}$")


def _load_json_or_missing(path: str) -> object:
    return json_util.load_json(path, _MISSING)  # type: ignore[arg-type]


class Store(HomeAssistantStore[dict[str, Any]]):
    """Strict Store variant for safety-critical provisioning state."""

    async def async_load(self) -> dict[str, Any] | None:
        """Read exact on-disk state and fail closed on malformed storage."""
        raw: object = await self.hass.async_add_executor_job(
            _load_json_or_missing,
            self.path,
        )
        if raw is _MISSING:
            return None
        if (
            not isinstance(raw, dict)
            or frozenset(raw) != frozenset({"version", "minor_version", "key", "data"})
            or raw["version"] != self.version
            or raw["minor_version"] != self.minor_version
            or raw["key"] != self.key
            or not isinstance(raw["data"], dict)
        ):
            raise ValueError("invalid_provisioning_store_envelope")
        return raw["data"]

    async def async_save(self, data: dict[str, Any]) -> None:
        """Atomically write once and propagate serialization or I/O failures."""
        envelope = {
            "version": self.version,
            "minor_version": self.minor_version,
            "key": self.key,
            "data": data,
        }
        # Store.async_save logs and swallows WriteError. This protected primitive
        # retains Store's atomic/private encoding while preserving failure.
        await self._async_write_data(envelope)


_RECORD_KEYS = frozenset(
    {
        "transaction_id",
        "operation",
        "phase",
        "setup_id",
        "panel_request",
        "fleet_profile",
        "staged_version",
        "prior_snapshot",
        "started_at",
        "last_error",
    }
)
_PANEL_REQUEST_KEYS = frozenset(
    {
        "host",
        "ssh_username",
        "root_password",
        "public_key",
        "fingerprint",
        "slug",
        "selected_components",
    }
)
_FLEET_PROFILE_KEYS = frozenset(
    {
        "kind",
        "host",
        "port",
        "tls_enabled",
    }
)
_FILE_KEYS = frozenset({"content", "mode"})
_SERVICE_KEYS = frozenset({"unit_file", "enabled", "active"})
_SNAPSHOT_KEYS = frozenset(
    {
        "layout",
        "active_release_target",
        "environment_file",
        "version_file",
        "bridge_service",
        "wifi_watchdog_service",
        "bus_watchdog_service",
        "selected_components",
    }
)
_ERROR_KEYS = frozenset({"stage", "code"})
_JOURNAL_ERROR_CODES = frozenset(
    {
        "invalid_provisioning_journal",
        "invalid_journal_transition",
        "journal_load_failed",
        "journal_remove_failed",
        "journal_save_failed",
        "journal_transaction_in_progress",
        "journal_transaction_mismatch",
        "journal_transaction_missing",
        "rollback_verification_required",
        "subentry_creation_not_proven",
    }
)


class ProvisioningJournalError(ValueError):
    """One stable error code which never retains persisted data or an exception."""

    __slots__ = ("code",)

    def __init__(self, code: str) -> None:
        if code not in _JOURNAL_ERROR_CODES:
            raise ValueError("invalid_provisioning_journal_error_code")
        self.code = code
        super().__init__(code)

    def __repr__(self) -> str:
        return f"ProvisioningJournalError({self.code!r})"


class ProvisioningOperation(StrEnum):
    """Provisioning mutations whose recovery shape is journaled."""

    INSTALL = "install"
    UPGRADE = "upgrade"


class ProvisioningPhase(StrEnum):
    """Exact durable provisioning phases."""

    STAGED = "staged"
    ACTIVATION_PENDING = "activation_pending"
    ACTIVATED = "activated"
    VERIFYING = "verifying"
    PENDING_CONFIG_COMMIT = "pending_config_commit"
    COMMITTED = "committed"
    ROLLBACK_PENDING = "rollback_pending"
    ROLLED_BACK = "rolled_back"


_TRANSITIONS: Mapping[ProvisioningPhase, frozenset[ProvisioningPhase]] = {
    ProvisioningPhase.STAGED: frozenset(
        {
            ProvisioningPhase.ACTIVATION_PENDING,
            ProvisioningPhase.ROLLBACK_PENDING,
        }
    ),
    ProvisioningPhase.ACTIVATION_PENDING: frozenset(
        {
            ProvisioningPhase.ACTIVATED,
            ProvisioningPhase.ROLLBACK_PENDING,
        }
    ),
    ProvisioningPhase.ACTIVATED: frozenset(
        {
            ProvisioningPhase.VERIFYING,
            ProvisioningPhase.ROLLBACK_PENDING,
        }
    ),
    ProvisioningPhase.VERIFYING: frozenset(
        {
            ProvisioningPhase.PENDING_CONFIG_COMMIT,
            ProvisioningPhase.ROLLBACK_PENDING,
        }
    ),
    ProvisioningPhase.PENDING_CONFIG_COMMIT: frozenset({ProvisioningPhase.ROLLBACK_PENDING}),
    ProvisioningPhase.COMMITTED: frozenset(),
    ProvisioningPhase.ROLLBACK_PENDING: frozenset(),
    ProvisioningPhase.ROLLED_BACK: frozenset(),
}


def _invalid() -> ProvisioningJournalError:
    return ProvisioningJournalError("invalid_provisioning_journal")


def _exact_keys(value: object, expected: frozenset[str]) -> Mapping[str, object]:
    if not isinstance(value, Mapping) or frozenset(value) != expected:
        raise _invalid()
    if any(not isinstance(key, str) for key in value):
        raise _invalid()
    return value


def _uuid4(value: object) -> UUID:
    parse_failed = False
    parsed: UUID | None = None
    try:
        parsed = value if isinstance(value, UUID) else UUID(str(value))
    except (AttributeError, TypeError, ValueError):
        parse_failed = True
    if parse_failed or parsed is None or parsed.version != 4 or parsed.variant != RFC_4122:
        raise _invalid()
    return parsed


def _utf8_text(value: object, *, maximum_bytes: int, allow_empty: bool = False) -> str:
    if not isinstance(value, str) or (not allow_empty and not value) or "\x00" in value:
        raise _invalid()
    encode_failed = False
    encoded = b""
    try:
        encoded = value.encode("utf-8")
    except UnicodeEncodeError:
        encode_failed = True
    if encode_failed or len(encoded) > maximum_bytes:
        raise _invalid()
    return value


def _safe_ascii_text(value: object, *, maximum_bytes: int) -> str:
    text = _utf8_text(value, maximum_bytes=maximum_bytes)
    if not text.isascii() or not text.isprintable():
        raise _invalid()
    return text


def _secret_text(value: object) -> str:
    return _utf8_text(value, maximum_bytes=_MAX_SECRET_BYTES)


def _string(value: object) -> str:
    if not isinstance(value, str):
        raise _invalid()
    return value


def _optional_string(value: object) -> str | None:
    if value is None:
        return None
    return _string(value)


def _integer(value: object) -> int:
    if type(value) is not int:
        raise _invalid()
    return value


def _boolean(value: object) -> bool:
    if type(value) is not bool:
        raise _invalid()
    return value


def _components(value: object, *, allow_empty: bool = False) -> tuple[str, ...]:
    if not isinstance(value, (list, tuple)) or (not allow_empty and not value) or len(value) > 64:
        raise _invalid()
    normalized: list[str] = []
    for component_id in value:
        if not isinstance(component_id, str) or _COMPONENT_ID.fullmatch(component_id) is None:
            raise _invalid()
        normalized.append(component_id)
    result = tuple(normalized)
    if len(set(result)) != len(result):
        raise _invalid()
    return result


def _optional_bytes(value: object) -> bytes | None:
    if value is None:
        return None
    if type(value) is not bytes or len(value) > _MAX_FILE_BYTES:
        raise _invalid()
    return bytes(value)


def _encode_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return base64.b64encode(value).decode("ascii")


def _decode_bytes(value: object) -> bytes | None:
    if value is None:
        return None
    if not isinstance(value, str) or len(value) > ((_MAX_FILE_BYTES + 2) // 3) * 4:
        raise _invalid()
    decode_failed = False
    decoded = b""
    try:
        decoded = base64.b64decode(value, validate=True)
    except (binascii.Error, ValueError):
        decode_failed = True
    if decode_failed or len(decoded) > _MAX_FILE_BYTES or _encode_bytes(decoded) != value:
        raise _invalid()
    return decoded


@dataclass(frozen=True, slots=True)
class StoredJournalError:
    """Redacted error identity safe to retain across restart."""

    stage: str
    code: str

    def __post_init__(self) -> None:
        if (
            not isinstance(self.stage, str)
            or _ERROR_TOKEN.fullmatch(self.stage) is None
            or not isinstance(self.code, str)
            or _ERROR_TOKEN.fullmatch(self.code) is None
        ):
            raise _invalid()

    def _to_storage(self) -> dict[str, object]:
        return {"stage": self.stage, "code": self.code}

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _ERROR_KEYS)
        return cls(stage=_string(value["stage"]), code=_string(value["code"]))


@dataclass(frozen=True, slots=True, repr=False)
class StoredPanelRequest:
    """Panel recovery inputs, including the temporary root credential."""

    host: str
    ssh_username: str
    root_password: str = field(repr=False)
    public_key: str = field(repr=False)
    fingerprint: str
    slug: str
    selected_components: tuple[str, ...]

    def __post_init__(self) -> None:
        public_key = _safe_ascii_text(
            self.public_key,
            maximum_bytes=_MAX_PUBLIC_KEY_BYTES,
        )
        fingerprint = _safe_ascii_text(self.fingerprint, maximum_bytes=128)
        identity_failed = False
        identity: HostIdentity | None = None
        try:
            identity = HostIdentity(public_key, fingerprint)
            known_hosts_line(self.host, identity.public_key)
        except (PanelIdentityError, TypeError, ValueError):
            identity_failed = True
        if identity_failed or identity is None:
            raise _invalid()
        if (
            not isinstance(self.ssh_username, str)
            or _SSH_USERNAME.fullmatch(self.ssh_username) is None
            or self.ssh_username != "root"
        ):
            raise _invalid()
        root_password = _secret_text(self.root_password)
        if not isinstance(self.slug, str) or _PANEL_SLUG.fullmatch(self.slug) is None:
            raise _invalid()
        object.__setattr__(self, "root_password", root_password)
        object.__setattr__(self, "public_key", identity.public_key)
        object.__setattr__(self, "fingerprint", identity.fingerprint)
        object.__setattr__(self, "selected_components", _components(self.selected_components))

    def __repr__(self) -> str:
        return "StoredPanelRequest(<redacted>)"

    def _to_storage(self) -> dict[str, object]:
        return {
            "host": self.host,
            "ssh_username": self.ssh_username,
            "root_password": self.root_password,
            "public_key": self.public_key,
            "fingerprint": self.fingerprint,
            "slug": self.slug,
            "selected_components": list(self.selected_components),
        }

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _PANEL_REQUEST_KEYS)
        return cls(
            host=_string(value["host"]),
            ssh_username=_string(value["ssh_username"]),
            root_password=_string(value["root_password"]),
            public_key=_string(value["public_key"]),
            fingerprint=_string(value["fingerprint"]),
            slug=_string(value["slug"]),
            selected_components=_components(value["selected_components"]),
        )


@dataclass(frozen=True, slots=True, repr=False)
class StoredFleetProfile:
    """Non-secret broker identity needed to bind recovery to the fleet."""

    kind: BrokerKind
    host: str
    port: int
    tls_enabled: bool

    def __post_init__(self) -> None:
        if not isinstance(self.kind, BrokerKind):
            raise _invalid()
        raw_host = _string(self.host)
        host = _safe_ascii_text(raw_host.strip().lower(), maximum_bytes=253)
        if any(char.isspace() for char in host):
            raise _invalid()
        if type(self.port) is not int or not 1 <= self.port <= 65535:
            raise _invalid()
        if type(self.tls_enabled) is not bool:
            raise _invalid()
        object.__setattr__(self, "host", host)

    def __repr__(self) -> str:
        return "StoredFleetProfile(<redacted>)"

    def _to_storage(self) -> dict[str, object]:
        return {
            "kind": self.kind.value,
            "host": self.host,
            "port": self.port,
            "tls_enabled": self.tls_enabled,
        }

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _FLEET_PROFILE_KEYS)
        raw_kind = _string(value["kind"])
        kind_failed = False
        kind: BrokerKind | None = None
        try:
            kind = BrokerKind(raw_kind)
        except ValueError:
            kind_failed = True
        if kind_failed or kind is None:
            raise _invalid()
        return cls(
            kind=kind,
            host=_string(value["host"]),
            port=_integer(value["port"]),
            tls_enabled=_boolean(value["tls_enabled"]),
        )


class StoredPanelLayout(StrEnum):
    """Panel installation layout captured before activation."""

    ABSENT = "absent"
    LEGACY_FIXED = "legacy_fixed"
    RELEASE_LINK = "release_link"


@dataclass(frozen=True, slots=True, repr=False)
class StoredFileSnapshot:
    """Exact optional owned-file bytes and permission mode."""

    content: bytes | None = field(repr=False)
    mode: int | None

    def __post_init__(self) -> None:
        content = _optional_bytes(self.content)
        if content is None:
            if self.mode is not None:
                raise _invalid()
        elif type(self.mode) is not int or not 0 <= self.mode <= 0o777:
            raise _invalid()
        object.__setattr__(self, "content", content)

    def __repr__(self) -> str:
        return "StoredFileSnapshot(<redacted>)"

    def _to_storage(self) -> dict[str, object]:
        return {
            "content": _encode_bytes(self.content),
            "mode": self.mode,
        }

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _FILE_KEYS)
        raw_mode = value["mode"]
        if raw_mode is None:
            mode = None
        else:
            mode = _integer(raw_mode)
        return cls(
            content=_decode_bytes(value["content"]),
            mode=mode,
        )


@dataclass(frozen=True, slots=True, repr=False)
class StoredServiceSnapshot:
    """Exact owned unit file and its systemd state."""

    unit_file: StoredFileSnapshot = field(repr=False)
    enabled: bool
    active: bool

    def __post_init__(self) -> None:
        if not isinstance(self.unit_file, StoredFileSnapshot):
            raise _invalid()
        if type(self.enabled) is not bool or type(self.active) is not bool:
            raise _invalid()
        if self.unit_file.content is None and (self.enabled or self.active):
            raise _invalid()

    def __repr__(self) -> str:
        return "StoredServiceSnapshot(<redacted>)"

    def _to_storage(self) -> dict[str, object]:
        return {
            "unit_file": self.unit_file._to_storage(),
            "enabled": self.enabled,
            "active": self.active,
        }

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _SERVICE_KEYS)
        return cls(
            unit_file=StoredFileSnapshot._from_storage(value["unit_file"]),
            enabled=_boolean(value["enabled"]),
            active=_boolean(value["active"]),
        )


@dataclass(frozen=True, slots=True, repr=False)
class StoredPanelSnapshot:
    """Exact core-owned panel state restored by a first-install or upgrade rollback."""

    layout: StoredPanelLayout
    active_release_target: str | None
    environment_file: StoredFileSnapshot = field(repr=False)
    version_file: StoredFileSnapshot = field(repr=False)
    bridge_service: StoredServiceSnapshot = field(repr=False)
    wifi_watchdog_service: StoredServiceSnapshot = field(repr=False)
    bus_watchdog_service: StoredServiceSnapshot = field(repr=False)
    selected_components: tuple[str, ...]

    def __post_init__(self) -> None:
        if not isinstance(self.layout, StoredPanelLayout):
            raise _invalid()
        if not all(
            isinstance(snapshot, expected)
            for snapshot, expected in (
                (self.environment_file, StoredFileSnapshot),
                (self.version_file, StoredFileSnapshot),
                (self.bridge_service, StoredServiceSnapshot),
                (self.wifi_watchdog_service, StoredServiceSnapshot),
                (self.bus_watchdog_service, StoredServiceSnapshot),
            )
        ):
            raise _invalid()
        selected_components = _components(
            self.selected_components,
            allow_empty=True,
        )
        if self.layout is StoredPanelLayout.RELEASE_LINK:
            if (
                not isinstance(self.active_release_target, str)
                or _RELEASE_TARGET.fullmatch(self.active_release_target) is None
                or self.version_file.content is None
            ):
                raise _invalid()
        elif self.active_release_target is not None:
            raise _invalid()

        selected_from_units = tuple(
            component_id
            for component_id, service in (
                ("bridge", self.bridge_service),
                ("wifi_watchdog", self.wifi_watchdog_service),
                ("bus_watchdog", self.bus_watchdog_service),
            )
            if service.unit_file.content is not None
        )
        files_absent = (
            self.environment_file.content is None
            and self.version_file.content is None
            and not selected_from_units
        )
        if self.layout is StoredPanelLayout.ABSENT:
            if not files_absent or selected_components:
                raise _invalid()
        elif set(selected_components) != set(selected_from_units):
            raise _invalid()
        elif self.layout is StoredPanelLayout.RELEASE_LINK and (
            self.environment_file.content is None
            or self.bridge_service.unit_file.content is None
            or not selected_components
            or "bridge" not in selected_components
        ):
            raise _invalid()
        object.__setattr__(self, "selected_components", selected_components)

    def __repr__(self) -> str:
        return "StoredPanelSnapshot(<redacted>)"

    def _to_storage(self) -> dict[str, object]:
        return {
            "layout": self.layout.value,
            "active_release_target": self.active_release_target,
            "environment_file": self.environment_file._to_storage(),
            "version_file": self.version_file._to_storage(),
            "bridge_service": self.bridge_service._to_storage(),
            "wifi_watchdog_service": self.wifi_watchdog_service._to_storage(),
            "bus_watchdog_service": self.bus_watchdog_service._to_storage(),
            "selected_components": list(self.selected_components),
        }

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _SNAPSHOT_KEYS)
        raw_layout = _string(value["layout"])
        layout_failed = False
        layout: StoredPanelLayout | None = None
        try:
            layout = StoredPanelLayout(raw_layout)
        except ValueError:
            layout_failed = True
        if layout_failed or layout is None:
            raise _invalid()
        return cls(
            layout=layout,
            active_release_target=_optional_string(value["active_release_target"]),
            environment_file=StoredFileSnapshot._from_storage(value["environment_file"]),
            version_file=StoredFileSnapshot._from_storage(value["version_file"]),
            bridge_service=StoredServiceSnapshot._from_storage(value["bridge_service"]),
            wifi_watchdog_service=StoredServiceSnapshot._from_storage(
                value["wifi_watchdog_service"]
            ),
            bus_watchdog_service=StoredServiceSnapshot._from_storage(value["bus_watchdog_service"]),
            selected_components=_components(
                value["selected_components"],
                allow_empty=True,
            ),
        )


@dataclass(frozen=True, slots=True)
class ProvisioningRecord:
    """One immutable provisioning transaction and everything recovery requires."""

    transaction_id: UUID
    operation: ProvisioningOperation
    phase: ProvisioningPhase
    setup_id: UUID
    panel_request: StoredPanelRequest = field(repr=False)
    fleet_profile: StoredFleetProfile = field(repr=False)
    staged_version: str
    prior_snapshot: StoredPanelSnapshot = field(repr=False)
    started_at: datetime
    last_error: StoredJournalError | None

    def __post_init__(self) -> None:
        object.__setattr__(self, "transaction_id", _uuid4(self.transaction_id))
        object.__setattr__(self, "setup_id", _uuid4(self.setup_id))
        if not isinstance(self.operation, ProvisioningOperation):
            raise _invalid()
        if not isinstance(self.phase, ProvisioningPhase):
            raise _invalid()
        if not isinstance(self.panel_request, StoredPanelRequest):
            raise _invalid()
        if not isinstance(self.fleet_profile, StoredFleetProfile):
            raise _invalid()
        if (
            not isinstance(self.staged_version, str)
            or _VERSION.fullmatch(self.staged_version) is None
            or len(self.staged_version.encode("ascii")) > _MAX_VERSION_BYTES
        ):
            raise _invalid()
        if not isinstance(self.prior_snapshot, StoredPanelSnapshot):
            raise _invalid()
        if (
            not isinstance(self.started_at, datetime)
            or self.started_at.tzinfo is None
            or self.started_at.utcoffset() is None
        ):
            raise _invalid()
        datetime_failed = False
        normalized_started_at: datetime | None = None
        try:
            normalized_started_at = self.started_at.astimezone(UTC)
        except (OverflowError, ValueError):
            datetime_failed = True
        if datetime_failed or normalized_started_at is None:
            raise _invalid()
        if self.last_error is not None and not isinstance(self.last_error, StoredJournalError):
            raise _invalid()
        object.__setattr__(self, "started_at", normalized_started_at)

    def __repr__(self) -> str:
        return (
            "ProvisioningRecord("
            f"transaction_id={self.transaction_id!r}, "
            f"operation={self.operation.value!r}, "
            f"phase={self.phase.value!r})"
        )

    def _to_storage(self) -> dict[str, object]:
        return {
            "transaction_id": str(self.transaction_id),
            "operation": self.operation.value,
            "phase": self.phase.value,
            "setup_id": str(self.setup_id),
            "panel_request": self.panel_request._to_storage(),
            "fleet_profile": self.fleet_profile._to_storage(),
            "staged_version": self.staged_version,
            "prior_snapshot": self.prior_snapshot._to_storage(),
            "started_at": self.started_at.isoformat(),
            "last_error": (self.last_error._to_storage() if self.last_error is not None else None),
        }

    @classmethod
    def _from_storage(cls, raw: object) -> Self:
        value = _exact_keys(raw, _RECORD_KEYS)
        raw_operation = _string(value["operation"])
        raw_phase = _string(value["phase"])
        raw_started_at = _string(value["started_at"])
        parse_failed = False
        operation: ProvisioningOperation | None = None
        phase: ProvisioningPhase | None = None
        started_at: datetime | None = None
        try:
            operation = ProvisioningOperation(raw_operation)
            phase = ProvisioningPhase(raw_phase)
            started_at = datetime.fromisoformat(raw_started_at)
            if (
                started_at.tzinfo is None
                or started_at.utcoffset() is None
                or started_at.astimezone(UTC).isoformat() != raw_started_at
            ):
                raise ValueError
        except (OverflowError, TypeError, ValueError):
            parse_failed = True
        if parse_failed or operation is None or phase is None or started_at is None:
            raise _invalid()
        raw_last_error = value["last_error"]
        return cls(
            transaction_id=_uuid4(value["transaction_id"]),
            operation=operation,
            phase=phase,
            setup_id=_uuid4(value["setup_id"]),
            panel_request=StoredPanelRequest._from_storage(value["panel_request"]),
            fleet_profile=StoredFleetProfile._from_storage(value["fleet_profile"]),
            staged_version=_string(value["staged_version"]),
            prior_snapshot=StoredPanelSnapshot._from_storage(value["prior_snapshot"]),
            started_at=started_at,
            last_error=(
                None if raw_last_error is None else StoredJournalError._from_storage(raw_last_error)
            ),
        )


@dataclass(slots=True)
class _JournalCoordinator:
    """One in-process transaction view shared by every flow for this HA instance."""

    lock: asyncio.Lock = field(default_factory=asyncio.Lock)
    loaded: bool = False
    record: ProvisioningRecord | None = None


@dataclass(frozen=True, slots=True)
class _StorageSettlement:
    """One storage operation settled without losing caller cancellation."""

    result: object
    error: BaseException | None
    cancellation: asyncio.CancelledError | None


async def _async_settle_storage(
    hass: HomeAssistant,
    operation: Coroutine[Any, Any, object],
) -> _StorageSettlement:
    """Keep the journal lock until an executor-backed side effect has settled."""
    task = hass.async_create_task(
        operation,
        "brilliant_mqtt provisioning storage",
        eager_start=False,
    )
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            if cancellation is None:
                cancellation = error
            if task.done():
                break
        except BaseException:
            break
    result: object = None
    failure: BaseException | None = None
    try:
        result = task.result()
    except BaseException as error:
        failure = error
    return _StorageSettlement(
        result=result,
        error=failure,
        cancellation=cancellation,
    )


class ProvisioningJournal:
    """Serialize one provisioning transaction before every observable phase change."""

    __slots__ = ("_coordinator", "_hass", "_store")

    def __init__(self, hass: HomeAssistant) -> None:
        self._hass = hass
        self._store: Store = Store(
            hass,
            JOURNAL_STORAGE_VERSION,
            JOURNAL_STORAGE_KEY,
            atomic_writes=True,
            private=True,
        )
        coordinator = hass.data.setdefault(
            _JOURNAL_COORDINATOR_KEY,
            _JournalCoordinator(),
        )
        if not isinstance(coordinator, _JournalCoordinator):
            raise RuntimeError("invalid_provisioning_journal_coordinator")
        self._coordinator = coordinator

    async def async_load(self) -> ProvisioningRecord | None:
        """Load and strictly validate the single durable transaction."""
        async with self._coordinator.lock:
            await self._async_reload()
            return self._coordinator.record

    async def async_create(self, record: ProvisioningRecord) -> ProvisioningRecord:
        """Persist a new staged transaction, refusing concurrent provisioning."""
        if (
            not isinstance(record, ProvisioningRecord)
            or record.phase is not ProvisioningPhase.STAGED
        ):
            raise ProvisioningJournalError("invalid_journal_transition")
        async with self._coordinator.lock:
            await self._async_reload()
            if self._coordinator.record is not None:
                raise ProvisioningJournalError("journal_transaction_in_progress")
            await self._async_save(record)
            return record

    async def async_transition(
        self,
        transaction_id: UUID,
        phase: ProvisioningPhase,
        *,
        last_error: StoredJournalError | None = None,
    ) -> ProvisioningRecord:
        """Persist one approved non-terminal phase edge before returning it."""
        if not isinstance(phase, ProvisioningPhase):
            raise ProvisioningJournalError("invalid_journal_transition")
        if last_error is not None and not isinstance(last_error, StoredJournalError):
            raise _invalid()
        async with self._coordinator.lock:
            record = await self._async_current(transaction_id)
            if phase not in _TRANSITIONS[record.phase]:
                raise ProvisioningJournalError("invalid_journal_transition")
            updated = replace(record, phase=phase, last_error=last_error)
            await self._async_save(updated)
            return updated

    async def async_record_error(
        self,
        transaction_id: UUID,
        last_error: StoredJournalError,
    ) -> ProvisioningRecord:
        """Durably replace the redacted failure identity without changing phase."""
        if not isinstance(last_error, StoredJournalError):
            raise _invalid()
        async with self._coordinator.lock:
            record = await self._async_current(transaction_id)
            updated = replace(record, last_error=last_error)
            await self._async_save(updated)
            return updated

    async def async_complete_commit(
        self,
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> ProvisioningRecord:
        """Persist committed, then clear only with proof of subentry creation."""
        if (
            not isinstance(subentry_id, str)
            or not subentry_id
            or len(subentry_id) > 128
            or not subentry_id.isascii()
            or not subentry_id.isprintable()
        ):
            raise ProvisioningJournalError("subentry_creation_not_proven")
        async with self._coordinator.lock:
            record = await self._async_current(transaction_id)
            if record.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT:
                record = replace(record, phase=ProvisioningPhase.COMMITTED)
                await self._async_save(record)
            elif record.phase is not ProvisioningPhase.COMMITTED:
                raise ProvisioningJournalError("invalid_journal_transition")
            await self._async_remove()
            return record

    async def async_clear_committed(
        self,
        transaction_id: UUID,
    ) -> ProvisioningRecord:
        """Retry removal after the durable committed phase already proves creation."""
        async with self._coordinator.lock:
            record = await self._async_current(transaction_id)
            if record.phase is not ProvisioningPhase.COMMITTED:
                raise ProvisioningJournalError("invalid_journal_transition")
            await self._async_remove()
            return record

    async def async_complete_rollback(
        self,
        transaction_id: UUID,
        *,
        verified: bool,
    ) -> ProvisioningRecord:
        """Persist rolled_back, then clear only after rollback verification."""
        if verified is not True:
            raise ProvisioningJournalError("rollback_verification_required")
        async with self._coordinator.lock:
            record = await self._async_current(transaction_id)
            if record.phase is ProvisioningPhase.ROLLBACK_PENDING:
                record = replace(record, phase=ProvisioningPhase.ROLLED_BACK)
                await self._async_save(record)
            elif record.phase is not ProvisioningPhase.ROLLED_BACK:
                raise ProvisioningJournalError("invalid_journal_transition")
            await self._async_remove()
            return record

    async def async_diagnostics(self) -> dict[str, int | str | None]:
        """Expose only transaction count and phase, never journal contents."""
        if not self._coordinator.loaded:
            await self.async_load()
        record = self._coordinator.record
        return {
            "count": 0 if record is None else 1,
            "phase": None if record is None else record.phase.value,
        }

    async def _async_reload(self) -> None:
        """Refresh the shared committed view while holding the HA-wide lock."""
        self._coordinator.loaded = False
        load_failed = False
        raw: dict[str, Any] | None = None
        try:
            async with asyncio.timeout(_STORE_LOAD_TIMEOUT_SECONDS):
                raw = await self._store.async_load()
        except Exception:
            load_failed = True
        if load_failed:
            raise ProvisioningJournalError("journal_load_failed")
        if raw is None:
            record = None
        else:
            parse_failed = False
            record = None
            try:
                envelope = _exact_keys(raw, frozenset({"record"}))
                record = ProvisioningRecord._from_storage(envelope["record"])
            except ProvisioningJournalError:
                raise
            except Exception:
                parse_failed = True
            if parse_failed or record is None:
                raise _invalid()
        self._coordinator.record = record
        self._coordinator.loaded = True

    async def _async_current(self, transaction_id: UUID) -> ProvisioningRecord:
        candidate_id = _uuid4(transaction_id)
        await self._async_reload()
        record = self._coordinator.record
        if record is None:
            raise ProvisioningJournalError("journal_transaction_missing")
        if record.transaction_id != candidate_id:
            raise ProvisioningJournalError("journal_transaction_mismatch")
        return record

    async def _async_save(self, record: ProvisioningRecord) -> None:
        payload = {"record": record._to_storage()}
        save_outcome = await _async_settle_storage(
            self._hass,
            self._store.async_save(payload),
        )
        cancellation = save_outcome.cancellation
        save_failed = save_outcome.error is not None
        del save_outcome
        verification_failed = True
        if not save_failed:
            verification = await _async_settle_storage(
                self._hass,
                self._store.async_load(),
            )
            if cancellation is None:
                cancellation = verification.cancellation
            verification_failed = verification.error is not None or verification.result != payload
            del verification
        if save_failed or verification_failed:
            if cancellation is not None:
                raise cancellation from None
            raise ProvisioningJournalError("journal_save_failed")
        self._coordinator.record = record
        self._coordinator.loaded = True
        if cancellation is not None:
            raise cancellation from None

    async def _async_remove(self) -> None:
        remove_outcome = await _async_settle_storage(
            self._hass,
            self._store.async_remove(),
        )
        cancellation = remove_outcome.cancellation
        remove_failed = remove_outcome.error is not None
        del remove_outcome
        verification_failed = True
        if not remove_failed:
            verification = await _async_settle_storage(
                self._hass,
                self._store.async_load(),
            )
            if cancellation is None:
                cancellation = verification.cancellation
            verification_failed = verification.error is not None or verification.result is not None
            del verification
        if remove_failed or verification_failed:
            if cancellation is not None:
                raise cancellation from None
            raise ProvisioningJournalError("journal_remove_failed")
        self._coordinator.record = None
        self._coordinator.loaded = True
        if cancellation is not None:
            raise cancellation from None
