"""Thin orchestration boundary for one durable staged panel installation."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import AsyncIterator, Awaitable, Callable, Iterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass, field, replace
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol
from uuid import RFC_4122, UUID, uuid4

from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant

from . import connect_retry
from .broker import BrokerProfile
from .broker_validation import BrokerValidationResult
from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_FEATURE_OVERRIDES,
    CONF_HOST,
    CONF_HOT_POLL_SECONDS,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_RESYNC_SECONDS,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    MESH_PANEL,
    PANEL_RELEASES_DIR,
)
from .entry_data import FleetConfig
from .errors import OperationError, OperationStage
from .panel_health import PanelHealthError, PanelHealthEvidence
from .panel_inspection import (
    PANEL_TOOLCHAIN_CAPABILITIES,
    PanelCompatibilityError,
    PanelFacts,
)
from .panel_ops import (
    CLEANUP_FINALIZE_SECONDS,
    MAX_MQTT_CA_BYTES,
    MAX_SNAPSHOT_FILE_BYTES,
    FileSnapshot,
    PanelLayout,
    PanelOpError,
    PanelSnapshot,
    ReleaseAdmission,
    ReleaseIdentityBlocked,
    ServiceSnapshot,
    Settlement,
    settle,
)
from .panel_ops import (
    StagedRelease as StagedRelease,
)
from .provisioning_journal import (
    PROVISIONING_ERROR_CODES,
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    RetainedRollback,
    StoredFileSnapshot,
    StoredFleetProfile,
    StoredJournalError,
    StoredPanelLayout,
    StoredPanelRequest,
    StoredPanelSnapshot,
    StoredServiceSnapshot,
    ownerless_journal_recovery_allowed,
)
from .shell import HostIdentity, PanelProcess, PanelShell, recovery_connect_failure_code

type JsonScalar = None | bool | int | float | str
type JsonValue = JsonScalar | list["JsonValue"] | Mapping[str, "JsonValue"]
type ProgressReporter = Callable[["ProvisioningProgress"], Awaitable[None]]
type PanelPreflightLauncher = Callable[[str], Awaitable[PanelProcess]]

_CORE_COMPONENTS = (
    COMPONENT_BRIDGE,
    COMPONENT_WIFI_WATCHDOG,
    COMPONENT_BUS_WATCHDOG,
)
_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")
_VERSION = re.compile(r"^[0-9A-Za-z][0-9A-Za-z._+-]{0,127}$")
# Snapshot-stage panel-op codes surfaced as-is because each tells the operator
# what to do; any other snapshot-stage failure stays the generic snapshot_failed.
_ACTIONABLE_SNAPSHOT_CODES = frozenset(
    {"snapshot_probe_failed", "service_crash_looping", "baseline_capture_interrupted"}
)
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 4096
_PROGRESS_TIMEOUT_SECONDS = 5.0
RECOVERY_TIMEOUT_SECONDS = 300.0
# A managed update whose session was lost reconnects (pinned) within this bound,
# which is part of the RECOVERY_TIMEOUT_SECONDS budget for the whole rollback.
RECONNECT_TIMEOUT_SECONDS = 120.0
INSTALL_LOCK_TIMEOUT_SECONDS = 30.0


@dataclass(frozen=True, slots=True, repr=False)
class ProvisioningFailureDetail:
    """Detached allowlisted MQTT failure metadata for actionable onboarding UI."""

    stage: OperationStage
    code: str
    retryable: bool
    summary_key: str
    documentation_slug: str
    redacted_detail: str
    cleanup_stage: OperationStage | None = None
    cleanup_code: str | None = None

    def __post_init__(self) -> None:
        valid = False
        try:
            canonical = OperationError.for_code(self.stage, self.code)
            valid = (
                self.retryable == canonical.retryable
                and self.summary_key == canonical.summary_key
                and self.documentation_slug == canonical.documentation_slug
                and self.redacted_detail == canonical.redacted_detail
            )
            if self.cleanup_stage is None or self.cleanup_code is None:
                valid = valid and self.cleanup_stage is None and self.cleanup_code is None
            else:
                OperationError.for_code(self.cleanup_stage, self.cleanup_code)
        except (TypeError, ValueError):
            valid = False
        if not valid:
            raise ValueError("invalid_provisioning_failure_detail")

    @classmethod
    def from_operation_error(cls, error: OperationError) -> ProvisioningFailureDetail:
        """Copy safe metadata without retaining an exception or its traceback."""
        if not isinstance(error, OperationError):
            raise TypeError("error must be an OperationError")
        cleanup = error.cleanup_error
        return cls(
            stage=error.stage,
            code=error.code,
            retryable=error.retryable,
            summary_key=error.summary_key,
            documentation_slug=error.documentation_slug,
            redacted_detail=error.redacted_detail,
            cleanup_stage=cleanup.stage if cleanup is not None else None,
            cleanup_code=cleanup.code if cleanup is not None else None,
        )

    def redacted_dict(self) -> dict[str, object]:
        """Return the complete stable UI/diagnostic representation."""
        return {
            "stage": self.stage.value,
            "code": self.code,
            "retryable": self.retryable,
            "summary_key": self.summary_key,
            "documentation_slug": self.documentation_slug,
            "redacted_detail": self.redacted_detail,
            "cleanup_error": (
                None
                if self.cleanup_stage is None
                else {
                    "stage": self.cleanup_stage.value,
                    "code": self.cleanup_code,
                }
            ),
        }

    def __repr__(self) -> str:
        return (
            "ProvisioningFailureDetail("
            f"stage={self.stage.value!r}, "
            f"code={self.code!r}, "
            f"retryable={self.retryable!r}, "
            f"cleanup_code={self.cleanup_code!r})"
        )


class PanelProvisioningError(RuntimeError):
    """One allowlisted orchestration failure without raw dependency context."""

    __slots__ = ("capability", "cleanup_code", "code", "detail", "release_override")

    def __init__(
        self,
        code: str,
        *,
        detail: ProvisioningFailureDetail | None = None,
        cleanup_code: str | None = None,
        capability: str | None = None,
        release_override: Mapping[str, object] | None = None,
    ) -> None:
        valid_toolchain_capability = (
            isinstance(capability, str) and capability in PANEL_TOOLCHAIN_CAPABILITIES
        )
        if (
            code not in PROVISIONING_ERROR_CODES
            or (
                detail is not None
                and (
                    code != "preflight_failed" or not isinstance(detail, ProvisioningFailureDetail)
                )
            )
            or cleanup_code not in {None, "cleanup_failed"}
            or (code == "unsupported_panel_toolchain" and not valid_toolchain_capability)
            or code != "unsupported_panel_toolchain"
            and capability is not None
        ):
            raise ValueError("invalid_panel_provisioning_error_code")
        self.release_override = release_override
        self.code = code
        self.detail = detail
        self.cleanup_code = cleanup_code
        self.capability = capability
        super().__init__(code)
        self.__suppress_context__ = True

    def __repr__(self) -> str:
        fields = [f"code={self.code!r}"]
        if self.capability is not None:
            fields.append(f"capability={self.capability!r}")
        if self.cleanup_code is not None:
            fields.append(f"cleanup_code={self.cleanup_code!r}")
        return f"PanelProvisioningError({', '.join(fields)})"


def _invalid_request() -> PanelProvisioningError:
    return PanelProvisioningError("invalid_panel_install_request")


def _copy_json(value: object, *, depth: int, nodes: list[int]) -> JsonValue:
    nodes[0] += 1
    if depth > _MAX_JSON_DEPTH or nodes[0] > _MAX_JSON_NODES:
        raise _invalid_request()
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise _invalid_request()
        return value
    if isinstance(value, list):
        return [_copy_json(item, depth=depth + 1, nodes=nodes) for item in value]
    if isinstance(value, Mapping):
        copied: dict[str, JsonValue] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise _invalid_request()
            copied[key] = _copy_json(item, depth=depth + 1, nodes=nodes)
        return copied
    raise _invalid_request()


def _copy_json_mapping(value: object) -> MappingProxyType[str, JsonValue]:
    copied = _copy_json(value, depth=0, nodes=[0])
    if not isinstance(copied, dict):
        raise _invalid_request()
    return MappingProxyType(copied)


def _valid_text(value: object, *, maximum_bytes: int) -> bool:
    if not isinstance(value, str) or not value or "\x00" in value:
        return False
    try:
        return len(value.encode("utf-8")) <= maximum_bytes
    except UnicodeEncodeError:
        return False


@dataclass(frozen=True, slots=True, repr=False)
class PanelInstallRequest:
    """Validated panel-owned inputs; repr never reveals onboarding data."""

    host: str
    ssh_username: str
    root_password: str = field(repr=False)
    display_name: str
    slug: str
    mesh_priority: int
    selected_components: tuple[str, ...]
    feature_overrides: Mapping[str, JsonValue] = field(repr=False)
    hot_poll_seconds: int | None = None
    resync_seconds: int | None = None

    def __post_init__(self) -> None:
        if (
            not _valid_text(self.host, maximum_bytes=253)
            or not self.host.isascii()
            or any(not character.isprintable() or character.isspace() for character in self.host)
            or any(character in ",*?![]|#@\\" for character in self.host)
            or self.ssh_username != "root"
            or not _valid_text(self.root_password, maximum_bytes=16 * 1024)
            or not _valid_text(self.display_name, maximum_bytes=256)
            or not isinstance(self.slug, str)
            or _PANEL_SLUG.fullmatch(self.slug) is None
            or self.slug == MESH_PANEL
            or type(self.mesh_priority) is not int
            or self.mesh_priority < 0
            or self.mesh_priority > 99
            or not isinstance(self.selected_components, tuple)
            or COMPONENT_BRIDGE not in self.selected_components
            or len(set(self.selected_components)) != len(self.selected_components)
            or any(component not in _CORE_COMPONENTS for component in self.selected_components)
            or self.hot_poll_seconds is not None
            and (type(self.hot_poll_seconds) is not int or not 0 <= self.hot_poll_seconds <= 60)
            or self.resync_seconds is not None
            and (type(self.resync_seconds) is not int or not 60 <= self.resync_seconds <= 86400)
        ):
            raise _invalid_request()
        normalized_components = tuple(
            component for component in _CORE_COMPONENTS if component in self.selected_components
        )
        object.__setattr__(self, "selected_components", normalized_components)
        object.__setattr__(
            self,
            "feature_overrides",
            _copy_json_mapping(self.feature_overrides),
        )

    def __repr__(self) -> str:
        return "PanelInstallRequest(<redacted>)"


class ProvisioningProgressStage(StrEnum):
    """Fixed, non-sensitive progress states exposed to a config-flow task."""

    RECOVERING = "recovering"
    FETCHING_IDENTITY = "fetching_identity"
    INSPECTING = "inspecting"
    SNAPSHOTTING = "snapshotting"
    STAGING = "staging"
    PREFLIGHT = "preflight"
    ACTIVATING = "activating"
    VERIFYING = "verifying"
    PENDING_CONFIG_COMMIT = "pending_config_commit"
    ROLLING_BACK = "rolling_back"
    COMPLETE = "complete"


@dataclass(frozen=True, slots=True)
class ProvisioningProgress:
    """One bounded progress notification without request or credential data."""

    stage: ProvisioningProgressStage
    transaction_id: UUID | None = None
    setup_id: UUID | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.stage, ProvisioningProgressStage):
            raise ValueError("invalid_provisioning_progress")
        for candidate in (self.transaction_id, self.setup_id):
            if candidate is not None and not _is_uuid4(candidate):
                raise ValueError("invalid_provisioning_progress")


class TransactionLookupState(StrEnum):
    """Authoritative config-flow/subentry state for one transaction."""

    MATCHED = "matched"
    FLOW_PENDING = "flow_pending"
    FLOW_ABORTED_OR_ABSENT = "flow_aborted_or_absent"


@dataclass(frozen=True, slots=True)
class TransactionLookup:
    """Tri-state lookup result; only a match may carry a subentry identifier."""

    state: TransactionLookupState
    subentry_id: str | None = None

    def __post_init__(self) -> None:
        matched = self.state is TransactionLookupState.MATCHED
        if (
            not isinstance(self.state, TransactionLookupState)
            or matched
            and (
                not isinstance(self.subentry_id, str)
                or not self.subentry_id
                or len(self.subentry_id) > 128
                or not self.subentry_id.isascii()
                or not self.subentry_id.isprintable()
            )
            or not matched
            and self.subentry_id is not None
        ):
            raise ValueError("invalid_transaction_lookup")


@dataclass(frozen=True, slots=True, repr=False)
class PanelReleaseBundle:
    """Locally prepared release inputs; environment contents stay redacted."""

    local_payload_dir: str
    version: str
    transaction_id: UUID
    environment: str = field(repr=False)
    mqtt_ca: bytes | None = field(default=None, repr=False)
    mqtt_ca_path: str | None = None

    def __post_init__(self) -> None:
        ca_prefix = "MQTT_TLS_CA_FILE="
        deployment_prefix = "BRILLIANT_DEPLOYMENT_ID="
        ca_lines = (
            tuple(line for line in self.environment.split("\n") if line.startswith(ca_prefix))
            if isinstance(self.environment, str)
            else ()
        )
        deployment_lines = (
            tuple(
                line for line in self.environment.split("\n") if line.startswith(deployment_prefix)
            )
            if isinstance(self.environment, str)
            else ()
        )
        if self.mqtt_ca is None:
            valid_ca = self.mqtt_ca_path is None and not ca_lines
        elif type(self.mqtt_ca) is not bytes:
            valid_ca = False
        else:
            expected_path = (
                f"{PANEL_RELEASES_DIR}/{self.version}--{self.transaction_id.hex}/mqtt-ca.pem"
                if _is_uuid4(self.transaction_id)
                else ""
            )
            valid_ca = (
                type(self.mqtt_ca) is bytes
                and bool(self.mqtt_ca)
                and len(self.mqtt_ca) <= MAX_MQTT_CA_BYTES
                and b"PRIVATE KEY" not in self.mqtt_ca.upper()
                and self.mqtt_ca_path == expected_path
                and ca_lines == (f"{ca_prefix}{expected_path}",)
            )
        if (
            not _valid_text(self.local_payload_dir, maximum_bytes=4096)
            or not isinstance(self.version, str)
            or _VERSION.fullmatch(self.version) is None
            or not _is_uuid4(self.transaction_id)
            or deployment_lines != (f"{deployment_prefix}{self.transaction_id.hex}",)
            or not _valid_text(
                self.environment,
                maximum_bytes=MAX_SNAPSHOT_FILE_BYTES,
            )
            or not valid_ca
        ):
            raise ValueError("invalid_panel_release_bundle")

    def __repr__(self) -> str:
        return "PanelReleaseBundle(<redacted>)"


type PanelReleaseProvider = Callable[
    [PanelInstallRequest, FleetConfig, UUID, UUID],
    Awaitable[PanelReleaseBundle],
]


def panel_release_provider(hass: HomeAssistant) -> PanelReleaseProvider:
    """Build the concrete provider for the integration's bundled panel payload."""
    if not isinstance(hass, HomeAssistant):
        raise ValueError("invalid_panel_release_provider")
    payload_dir = Path(__file__).parent / "agent_payload"

    async def provide(
        request: PanelInstallRequest,
        fleet: FleetConfig,
        transaction_id: UUID,
        setup_id: UUID,
    ) -> PanelReleaseBundle:
        if (
            not isinstance(request, PanelInstallRequest)
            or not isinstance(fleet, FleetConfig)
            or not _is_uuid4(transaction_id)
            or not _is_uuid4(setup_id)
        ):
            raise PanelProvisioningError("release_prepare_failed")
        settings = None
        version = ""
        mqtt_ca_path: str | None = None
        failed = False
        try:
            version = (
                await hass.async_add_executor_job(
                    (payload_dir / "VERSION").read_text,
                    "ascii",
                )
            ).strip()
            if _VERSION.fullmatch(version) is None:
                raise ValueError
            if fleet.broker.has_custom_ca:
                mqtt_ca_path = f"{PANEL_RELEASES_DIR}/{version}--{transaction_id.hex}/mqtt-ca.pem"
            settings = fleet.broker.panel_release_settings(
                panel=request.slug,
                mesh_priority=request.mesh_priority,
                scene_bridge_enabled=fleet.ha_control_enabled,
                mqtt_ca_path=mqtt_ca_path,
                deployment_id=transaction_id.hex,
                hot_poll_seconds=request.hot_poll_seconds,
                resync_seconds=request.resync_seconds,
            )
            bundle = PanelReleaseBundle(
                local_payload_dir=str(payload_dir),
                version=version,
                transaction_id=transaction_id,
                environment=settings.environment,
                mqtt_ca=settings.mqtt_ca,
                mqtt_ca_path=settings.mqtt_ca_path,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
            bundle = None
        settings = None
        version = "<redacted>"
        mqtt_ca_path = None
        if failed or bundle is None:
            raise PanelProvisioningError("release_prepare_failed")
        return bundle

    return provide


@dataclass(frozen=True, slots=True, repr=False)
class CanonicalPanelData(Mapping[str, Any]):
    """Immutable config-subentry mapping whose repr never exposes credentials."""

    _data: MappingProxyType[str, Any] = field(repr=False)

    def __post_init__(self) -> None:
        if not isinstance(self._data, MappingProxyType):
            raise ValueError("invalid_canonical_panel_data")
        try:
            detached = _copy_json_mapping(self._data)
        except PanelProvisioningError:
            raise ValueError("invalid_canonical_panel_data") from None
        object.__setattr__(self, "_data", detached)

    def __getitem__(self, key: str) -> Any:
        return _copy_json(self._data[key], depth=0, nodes=[0])

    def __iter__(self) -> Iterator[str]:
        return iter(self._data)

    def __len__(self) -> int:
        return len(self._data)

    def as_dict(self) -> dict[str, Any]:
        """Return the detached data only for Home Assistant subentry creation."""
        copied = _copy_json(self._data, depth=0, nodes=[0])
        if not isinstance(copied, dict):
            raise ValueError("invalid_canonical_panel_data")
        return copied

    def __repr__(self) -> str:
        return "CanonicalPanelData(<redacted>)"


@dataclass(frozen=True, slots=True, repr=False)
class ProvisionedPanel:
    """Successful candidate verification plus canonical panel subentry data."""

    identity: HostIdentity
    facts: PanelFacts
    version: str
    health: PanelHealthEvidence
    transaction_id: UUID
    setup_id: UUID
    panel_data: CanonicalPanelData = field(repr=False)

    def __post_init__(self) -> None:
        if (
            not isinstance(self.identity, HostIdentity)
            or not isinstance(self.facts, PanelFacts)
            or not isinstance(self.health, PanelHealthEvidence)
            or not isinstance(self.version, str)
            or _VERSION.fullmatch(self.version) is None
            or not _is_uuid4(self.transaction_id)
            or not _is_uuid4(self.setup_id)
            or not isinstance(self.panel_data, CanonicalPanelData)
        ):
            raise ValueError("invalid_provisioned_panel")

    def __repr__(self) -> str:
        return (
            "ProvisionedPanel("
            f"transaction_id={str(self.transaction_id)!r}, "
            f"version={self.version!r})"
        )


class _Journal(Protocol):
    async def async_begin_ownerless_finalize(
        self, expected: RetainedRollback
    ) -> RetainedRollback: ...
    async def async_retained_records(self) -> tuple[RetainedRollback, ...]: ...
    async def async_retained(self, transaction_id: UUID) -> RetainedRollback | None: ...
    async def async_retained_state(
        self,
        transaction_id: UUID,
        state: str,
        *,
        evidence: str | None = None,
    ) -> None: ...
    async def async_begin_restore(self, transaction_id: UUID, root_password: str) -> None: ...
    async def async_remove_retained(self, transaction_id: UUID) -> None: ...
    async def async_complete_cleanup(self, transaction_id: UUID) -> None: ...
    async def async_complete_absent_rollback(self, transaction_id: UUID) -> None: ...
    async def async_load(self) -> ProvisioningRecord | None: ...

    async def async_create(self, record: ProvisioningRecord) -> ProvisioningRecord: ...

    async def async_transition(
        self,
        transaction_id: UUID,
        phase: ProvisioningPhase,
        *,
        last_error: StoredJournalError | None = None,
    ) -> ProvisioningRecord: ...

    async def async_record_error(
        self,
        transaction_id: UUID,
        last_error: StoredJournalError,
    ) -> ProvisioningRecord: ...

    async def async_complete_commit(
        self,
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> ProvisioningRecord: ...

    async def async_clear_committed(
        self,
        transaction_id: UUID,
    ) -> ProvisioningRecord: ...

    async def async_complete_rollback(
        self,
        transaction_id: UUID,
        *,
        verified: bool,
    ) -> ProvisioningRecord: ...


class PanelBrokerValidator(Protocol):
    async def async_validate_panel(
        self,
        profile: BrokerProfile,
        launcher: PanelPreflightLauncher,
        setup_id: UUID | None = None,
    ) -> BrokerValidationResult: ...


class PanelOperations(Protocol):
    def release_transaction(
        self,
        shell: PanelShell,
        local_payload_dir: str,
        *,
        panel: str,
        components: tuple[str, ...] = (),
        override: Mapping[str, object] | None = None,
        transaction_id: UUID | None = None,
        allow_pre_ordinal_upgrade: bool = False,
    ) -> AbstractAsyncContextManager[ReleaseAdmission]: ...

    async def capture_baseline(
        self, shell: PanelShell, snapshot: PanelSnapshot
    ) -> PanelSnapshot: ...
    async def verify_baseline(self, shell: PanelShell, snapshot: PanelSnapshot) -> None: ...
    async def finalize_baseline(self, shell: PanelShell, snapshot: PanelSnapshot) -> None: ...
    async def restart_restored(
        self,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        deployment_id: str,
        *,
        on_service_stopped: Callable[[], None],
    ) -> None: ...
    async def snapshot_panel(self, shell: PanelShell) -> PanelSnapshot: ...
    async def rehearse_restart(
        self,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        deployment_id: str,
        *,
        before_restart: Callable[[], None],
    ) -> None: ...
    async def start_bridge(self, shell: PanelShell) -> None: ...
    async def bridge_running(self, shell: PanelShell) -> bool: ...
    async def rebind_rehearsed_deployment(self, shell: PanelShell, deployment_id: str) -> None: ...

    async def stage_release(
        self,
        shell: PanelShell,
        local_payload_dir: str,
        version: str,
        environment: str,
        selected_components: tuple[str, ...],
        transaction_id: UUID,
        mqtt_ca: bytes | None = None,
    ) -> StagedRelease: ...

    async def activate_staged(
        self,
        shell: PanelShell,
        staged: StagedRelease,
        *,
        on_services_stopped: Callable[[], None],
    ) -> None: ...

    async def restart_candidate(
        self,
        shell: PanelShell,
        staged: StagedRelease,
        *,
        on_service_stopped: Callable[[], None],
    ) -> None: ...

    async def rollback_snapshot(
        self,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        staged: StagedRelease,
    ) -> None: ...

    async def cleanup_staged(
        self,
        shell: PanelShell,
        staged: StagedRelease,
    ) -> None: ...


class _HealthObserver(Protocol):
    async def async_subscribe(self) -> None: ...
    def mark_activation_started(
        self,
        expected_version: str,
        expected_deployment_id: str,
    ) -> None: ...

    async def async_wait(
        self,
        expected_version: str,
        timeout: float = 90.0,  # noqa: ASYNC109 - mirrors observer API
    ) -> PanelHealthEvidence: ...

    async def async_close(self) -> None: ...


class _RepairReporter(Protocol):
    async def async_report_rollback_failure(
        self,
        transaction_id: UUID,
        *,
        original_code: str,
        rollback_code: str,
    ) -> None: ...

    async def async_clear_rollback_failure(
        self,
        transaction_id: UUID,
    ) -> None: ...


@dataclass(slots=True, repr=False)
class _InstallState:
    shell: PanelShell | None = None
    observer: _HealthObserver | None = None
    snapshot: PanelSnapshot | None = None
    stored_snapshot: StoredPanelSnapshot | None = None
    staged: StagedRelease | None = None
    transaction_id: UUID | None = None
    setup_id: UUID | None = None
    journal_phase: ProvisioningPhase | None = None
    failure_code: str = "invalid_provisioning_dependency"
    failure_stage: str = "provisioning"


class _RehearsalOutcome(StrEnum):
    """How one incumbent health rehearsal ended (before mapping to a code)."""

    HEALTHY = "healthy"
    PROOF_FAILED = "proof_failed"
    RESTART_FAILED = "restart_failed"
    UNOBSERVABLE = "unobservable"
    CANCELLED = "cancelled"


def rehearsal_failure_code(
    outcome: _RehearsalOutcome, *, transport_lost: bool, bridge_running: bool
) -> str | None:
    """The refusal code for a rehearsal outcome; None when nothing is refused.

    A cancellation is re-raised rather than coded. An HA-side observer failure
    never touched the bridge. A lost session is never reconnected, even after a
    healthy proof: the install or update cannot continue on it. Otherwise the
    code says whether the bridge is verified running: an unhealthy but running
    incumbent, a rehearsal that never restarted it, or one whose state could not
    be verified.
    """
    if outcome is _RehearsalOutcome.CANCELLED:
        return None
    if outcome is _RehearsalOutcome.UNOBSERVABLE:
        return "incumbent_health_unobservable"
    if transport_lost:
        return "incumbent_rehearsal_interrupted"
    if outcome is _RehearsalOutcome.HEALTHY:
        return None
    if not bridge_running:
        return "incumbent_unhealthy_unverified"
    if outcome is _RehearsalOutcome.RESTART_FAILED:
        return "incumbent_rehearsal_failed"
    return "incumbent_unhealthy"


# Refusals whose operator action is about the bridge's own state: never replaced
# by baseline_capture_interrupted when the baseline cannot be removed.
_BRIDGE_STATE_CODES = frozenset(
    {"incumbent_unhealthy_unverified", "incumbent_rehearsal_interrupted"}
)

# The rehearsal waits at most this long for the restarted incumbent's evidence.
REHEARSAL_HEALTH_SECONDS = 90.0


@dataclass(slots=True)
class _BaselineGuard:
    """Whether a Home Assistant record that could own the baseline was attempted."""

    create_attempted: bool = False


# How often a rollback reconnect backoff re-checks for an HA stop.
_STOP_POLL_SECONDS = 0.5


async def _sleep_unless_stopping(delay: float, stopping: Callable[[], bool]) -> bool:
    """Back off for *delay*, returning False as soon as *stopping* reports a stop.

    The predicate is polled every _STOP_POLL_SECONDS, so an HA shutdown is never
    held for a whole backoff.
    """
    loop = asyncio.get_running_loop()
    deadline = loop.time() + delay
    while True:
        if stopping():
            return False
        remaining = deadline - loop.time()
        if remaining <= 0:
            return True
        await asyncio.sleep(min(_STOP_POLL_SECONDS, remaining))


# Indirected so tests drive the rollback reconnect backoff without real waits.
_reconnect_backoff: Callable[[float, Callable[[], bool]], Awaitable[bool]] = _sleep_unless_stopping


def _not_stopping() -> bool:
    return False


async def _wait_unless_lost(
    observer: _HealthObserver, shell: PanelShell, version: str, bound_seconds: float
) -> PanelHealthEvidence | None:
    """Wait for health evidence; None as soon as the SSH session is seen lost.

    A lost session makes the outcome interrupted whatever the proof says, so the
    wait is cancelled instead of running out its whole bound.
    """
    waiting = asyncio.ensure_future(observer.async_wait(version, bound_seconds))
    try:
        while not waiting.done():
            if shell.transport_lost():
                return None
            await asyncio.wait({waiting}, timeout=_STOP_POLL_SECONDS)
        return waiting.result()
    finally:
        if not waiting.done():
            waiting.cancel()
            await asyncio.gather(waiting, return_exceptions=True)


async def _noop_progress(update: ProvisioningProgress) -> None:
    del update


class PanelProvisioner:
    """Serialize and durably orchestrate one panel release transaction."""

    def __init__(
        self,
        *,
        operation_lock: asyncio.Lock,
        journal: _Journal,
        identity_fetcher: Callable[[str], Awaitable[HostIdentity]],
        shell_factory: Callable[[str, str, str], PanelShell],
        inspector: Callable[[PanelShell, HostIdentity], Awaitable[PanelFacts]],
        duplicate_fingerprint: Callable[[str], Awaitable[bool]],
        operations: PanelOperations,
        snapshot_to_stored: Callable[[PanelSnapshot], StoredPanelSnapshot],
        snapshot_from_stored: Callable[[StoredPanelSnapshot], PanelSnapshot],
        staged_release_factory: Callable[
            [str, UUID, tuple[str, ...]],
            StagedRelease,
        ],
        release_provider: Callable[
            [PanelInstallRequest, FleetConfig, UUID, UUID],
            Awaitable[PanelReleaseBundle],
        ],
        preflight_launcher_factory: Callable[
            [PanelShell, StagedRelease],
            PanelPreflightLauncher,
        ],
        broker_validator: PanelBrokerValidator,
        health_observer_factory: Callable[[str], _HealthObserver],
        transaction_lookup: Callable[[UUID], Awaitable[TransactionLookup]],
        repair_reporter: _RepairReporter,
        id_factory: Callable[[], UUID] = uuid4,
        clock: Callable[[], datetime] = lambda: datetime.now(UTC),
        health_timeout: float = 90.0,
        credential_resolver: Callable[[StoredPanelRequest], Awaitable[str | None]] | None = None,
        retained_owner_exists: Callable[[RetainedRollback], Awaitable[bool]] | None = None,
    ) -> None:
        self._lock = operation_lock
        self._journal = journal
        self._identity_fetcher = identity_fetcher
        self._shell_factory = shell_factory
        self._inspector = inspector
        self._duplicate_fingerprint = duplicate_fingerprint
        self._operations = operations
        self._snapshot_to_stored = snapshot_to_stored
        self._snapshot_from_stored = snapshot_from_stored
        self._staged_release_factory = staged_release_factory
        self._release_provider = release_provider
        self._preflight_launcher_factory = preflight_launcher_factory
        self._broker_validator = broker_validator
        self._health_observer_factory = health_observer_factory
        self._transaction_lookup = transaction_lookup
        self._repair_reporter = repair_reporter
        self._id_factory = id_factory
        self._clock = clock
        self._credential_resolver = credential_resolver
        self._retained_owner_exists = retained_owner_exists
        if (
            not isinstance(operation_lock, asyncio.Lock)
            or not isinstance(health_timeout, (int, float))
            or isinstance(health_timeout, bool)
            or not math.isfinite(health_timeout)
            or health_timeout <= 0
        ):
            raise ValueError("invalid_panel_provisioner")
        self._health_timeout = float(health_timeout)

    async def async_install(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        progress: ProgressReporter,
        *,
        release_override: Mapping[str, object] | None = None,
    ) -> ProvisionedPanel:
        """Install and verify one inactive release, retaining the journal."""
        if not isinstance(request, PanelInstallRequest) or not isinstance(fleet, FleetConfig):
            raise PanelProvisioningError("invalid_panel_install_request")
        async with self._async_install_lock():
            state = _InstallState()
            result: ProvisionedPanel | None = None
            primary: PanelProvisioningError | None = None
            cancellation: asyncio.CancelledError | None = None
            try:
                result = await self._async_install_body(
                    request,
                    fleet,
                    progress,
                    state,
                    release_override,
                )
            except ReleaseIdentityBlocked as error:
                primary = PanelProvisioningError(
                    "release_identity_blocked", release_override=error.override
                )
            except asyncio.CancelledError as error:
                cancellation = error
            except PanelCompatibilityError as error:
                primary = (
                    PanelProvisioningError(
                        "unsupported_panel_toolchain",
                        capability=error.capability,
                    )
                    if error.code == "unsupported_panel_toolchain"
                    else PanelProvisioningError(state.failure_code)
                )
            except PanelProvisioningError as error:
                primary = error
            except PanelOpError as error:
                code = str(error).partition(":")[0]
                primary = PanelProvisioningError(
                    code
                    if state.failure_code == "snapshot_failed"
                    and code in _ACTIONABLE_SNAPSHOT_CODES
                    else state.failure_code
                )
            except OperationError as error:
                detail = (
                    ProvisioningFailureDetail.from_operation_error(error)
                    if state.failure_code == "preflight_failed"
                    else None
                )
                primary = PanelProvisioningError(
                    state.failure_code,
                    detail=detail,
                )
            except Exception:
                primary = PanelProvisioningError(state.failure_code)

            observer_outcome = await _settle_close(
                state.observer.async_close if state.observer is not None else None
            )
            if primary is None and cancellation is None:
                if observer_outcome.cancellation is not None:
                    cancellation = observer_outcome.cancellation
                elif observer_outcome.error is not None:
                    primary = PanelProvisioningError("cleanup_failed")
            if primary is not None or cancellation is not None:
                original_code = (
                    "operation_cancelled"
                    if cancellation is not None
                    else _required_primary_code(primary)
                )
                compensation = await settle(self._async_compensate(state, original_code, progress))
                await self._async_report_compensation_failure(
                    state,
                    original_code,
                    compensation,
                )
                if (
                    compensation.error is not None
                    and state.journal_phase is ProvisioningPhase.STAGED
                    and cancellation is None
                    and primary is not None
                ):
                    primary = PanelProvisioningError(
                        primary.code,
                        detail=primary.detail,
                        cleanup_code="cleanup_failed",
                        capability=primary.capability,
                    )

            shell_outcome = await _settle_close(
                state.shell.close if state.shell is not None else None
            )
            if primary is None and cancellation is None and shell_outcome.cancellation is not None:
                cancellation = shell_outcome.cancellation
                state.failure_stage = "cleanup"
                original_code = "operation_cancelled"
                if result is None:
                    compensation = Settlement(
                        error=PanelProvisioningError("rollback_failed"),
                        cancellation=None,
                    )
                else:
                    compensation = await settle(
                        self._async_reopen_compensate_and_close(
                            state,
                            request,
                            result.identity.public_key,
                            original_code,
                            progress,
                        )
                    )
                await self._async_report_compensation_failure(
                    state,
                    original_code,
                    compensation,
                )
            if cancellation is not None:
                raise cancellation from None
            if primary is not None:
                raise primary from None
            if result is None:
                raise PanelProvisioningError("invalid_provisioning_dependency")
            return result

    @asynccontextmanager
    async def _async_install_lock(self) -> AsyncIterator[None]:
        try:
            async with asyncio.timeout(INSTALL_LOCK_TIMEOUT_SECONDS):
                await self._lock.acquire()
        except TimeoutError:
            raise PanelProvisioningError("transaction_in_progress") from None
        try:
            yield
        finally:
            self._lock.release()

    @asynccontextmanager
    async def _unjournaled_baseline_guard(
        self, shell: PanelShell, snapshot: PanelSnapshot, transaction_id: UUID
    ) -> AsyncIterator[_BaselineGuard]:
        """Retire a published baseline if this region fails before a record owns it.

        Wraps the region after ``capture_baseline`` and before the journal exists,
        under the fleet lock. On any failure (including cancellation) the baseline
        is finalized, bounded, unless a retained record or the active journal names
        this transaction once its creation was attempted. An unreadable journal
        counts as owned. A cancellation always wins; otherwise the original error is
        re-raised unchanged after a successful finalize. A lost session is never
        reconnected: like a failed or timed-out finalize it raises
        ``baseline_capture_interrupted`` (an unowned baseline may remain), except
        that a bridge-state code (``_BRIDGE_STATE_CODES``) is kept, so the operator
        is still told to check and start the bridge; its text also says a baseline
        may remain.

        A settle bound that expired earlier in the region (a hung probe, start or
        finalize) fences the shell by closing it: the shell is then closed but NOT
        lost (``transport_lost()`` stays False), so this finalize is attempted and
        fails, and the code follows the failed-finalize rule above.
        """
        guard = _BaselineGuard()
        try:
            yield guard
        except (Exception, asyncio.CancelledError) as error:
            await self._async_retire_unjournaled(shell, snapshot, transaction_id, guard, error)
            raise

    async def _async_retire_unjournaled(
        self,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        transaction_id: UUID,
        guard: _BaselineGuard,
        error: BaseException,
    ) -> None:
        """Return to re-raise *error*; raise to replace it (see the guard)."""
        cancellation = error if isinstance(error, asyncio.CancelledError) else None
        # These codes already say a baseline may remain, and must keep telling the
        # operator to check (and start) the bridge.
        keeps_code = isinstance(error, PanelProvisioningError) and error.code in _BRIDGE_STATE_CODES
        unowned_cleanup_possible = not shell.transport_lost()
        if guard.create_attempted:
            ownership = await settle(self._async_baseline_owned(transaction_id))
            cancellation = cancellation or ownership.cancellation
            if ownership.error is None and ownership.result is True:
                if cancellation is not None:
                    raise cancellation from None
                return  # a durable record owns the baseline; its recovery settles it
            unowned_cleanup_possible = unowned_cleanup_possible and ownership.error is None
        finalized = False
        if unowned_cleanup_possible:
            outcome = await settle(
                self._operations.finalize_baseline(shell, snapshot),
                bound_seconds=CLEANUP_FINALIZE_SECONDS,
            )
            cancellation = cancellation or outcome.cancellation
            finalized = outcome.error is None
        if cancellation is not None:
            raise cancellation from None
        if not finalized and not keeps_code:
            raise PanelProvisioningError("baseline_capture_interrupted") from None

    async def _async_rehearse_incumbent(
        self, shell: PanelShell, snapshot: PanelSnapshot, slug: str
    ) -> None:
        """Prove the installed bridge can come back healthy before replacing it.

        Installed layouts only. Restarts the incumbent ONCE under a fresh,
        memory-only deployment id and waits for its MQTT health evidence, so a
        bridge that stays ``active`` but cannot reach MQTT is refused here, where
        its baseline is still unjournaled (the caller's guard removes it), instead
        of after the candidate fails with a rollback that can never be proven.
        On success the release admission is re-bound to the rotated id.
        """
        if snapshot.layout is PanelLayout.ABSENT:
            return
        baseline = snapshot.baseline
        if baseline is None or COMPONENT_BRIDGE not in baseline.identities:
            raise PanelProvisioningError("baseline_incomplete")
        version = baseline.identities[COMPONENT_BRIDGE].version
        deployment_id = uuid4().hex
        outcome, cancellation = await self._async_run_rehearsal(
            shell, snapshot, slug, version, deployment_id
        )
        if cancellation is not None:
            raise cancellation from None
        if outcome is _RehearsalOutcome.HEALTHY and not shell.transport_lost():
            try:
                await self._operations.rebind_rehearsed_deployment(shell, deployment_id)
            except PanelOpError:
                # Typed on both routes; the caller's guard still retires the baseline.
                raise PanelProvisioningError("release_identity_changed") from None
            return
        running = False
        if (
            outcome
            not in {
                _RehearsalOutcome.UNOBSERVABLE,
                _RehearsalOutcome.HEALTHY,
            }
            and not shell.transport_lost()
        ):
            running = await self._async_probe_bridge(shell)
            if (
                not running
                and outcome is _RehearsalOutcome.PROOF_FAILED
                and not shell.transport_lost()
            ):
                # The restarted bridge is down: never leave it so. Best effort,
                # then the re-probe decides the code.
                await self._async_start_bridge(shell)
                if not shell.transport_lost():
                    running = await self._async_probe_bridge(shell)
        code = rehearsal_failure_code(
            outcome, transport_lost=shell.transport_lost(), bridge_running=running
        )
        raise PanelProvisioningError(code or "invalid_provisioning_dependency")

    async def _async_run_rehearsal(
        self,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        slug: str,
        version: str,
        deployment_id: str,
    ) -> tuple[_RehearsalOutcome, asyncio.CancelledError | None]:
        observer: _HealthObserver | None = None
        outcome = _RehearsalOutcome.UNOBSERVABLE
        cancellation: asyncio.CancelledError | None = None
        try:
            observer = self._health_observer_factory(slug)
            await observer.async_subscribe()
        except asyncio.CancelledError as error:
            outcome, cancellation = _RehearsalOutcome.CANCELLED, error
        except Exception:
            outcome = _RehearsalOutcome.UNOBSERVABLE
        else:
            try:
                outcome = await self._async_rehearse_observed(
                    observer, shell, snapshot, slug, version, deployment_id
                )
            except asyncio.CancelledError as error:
                outcome, cancellation = _RehearsalOutcome.CANCELLED, error
        closed = await _settle_close(observer.async_close if observer is not None else None)
        if cancellation is None and closed.cancellation is not None:
            outcome, cancellation = _RehearsalOutcome.CANCELLED, closed.cancellation
        return outcome, cancellation

    async def _async_rehearse_observed(
        self,
        observer: _HealthObserver,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        slug: str,
        version: str,
        deployment_id: str,
    ) -> _RehearsalOutcome:
        def before_restart() -> None:
            # Metadata carrying any other id is ignored, so the still-running old
            # process cannot satisfy the proof opened here.
            observer.mark_activation_started(version, deployment_id)

        try:
            await self._operations.rehearse_restart(
                shell, snapshot, deployment_id, before_restart=before_restart
            )
        except Exception as error:
            code = str(error).partition(":")[0] if isinstance(error, PanelOpError) else ""
            if code == "rehearsal_boundary_failed":
                return _RehearsalOutcome.UNOBSERVABLE
            if code == "rehearsal_restart_failed" and not shell.transport_lost():
                await self._async_start_bridge(shell)
            return _RehearsalOutcome.RESTART_FAILED
        try:
            evidence = await _wait_unless_lost(
                observer, shell, version, min(self._health_timeout, REHEARSAL_HEALTH_SECONDS)
            )
        except Exception:
            # Also HA's own MQTT dropping mid-wait: the observer cannot tell.
            return _RehearsalOutcome.PROOF_FAILED
        if evidence is None:
            return _RehearsalOutcome.PROOF_FAILED  # the session was lost mid-wait
        if (
            evidence.deployment_id != deployment_id
            or evidence.agent_version != version
            or evidence.panel != slug
        ):
            return _RehearsalOutcome.PROOF_FAILED
        return _RehearsalOutcome.HEALTHY

    async def _async_probe_bridge(self, shell: PanelShell) -> bool:
        """Whether the bridge is verified running; any failure counts as not.

        Past its bound the probe fences the shell (closed, not lost): every later
        command fails, so the bridge stays unverified.
        """
        probe = await settle(
            self._operations.bridge_running(shell), bound_seconds=CLEANUP_FINALIZE_SECONDS
        )
        if probe.cancellation is not None:
            raise probe.cancellation from None
        return probe.error is None and probe.result is True

    async def _async_start_bridge(self, shell: PanelShell) -> None:
        """Best-effort, bounded start; its result is judged only by a re-probe."""
        started = await settle(
            self._operations.start_bridge(shell), bound_seconds=CLEANUP_FINALIZE_SECONDS
        )
        if started.cancellation is not None:
            raise started.cancellation from None

    async def _async_baseline_owned(self, transaction_id: UUID) -> bool:
        if await self._journal.async_retained(transaction_id) is not None:
            return True
        current = await self._journal.async_load()
        return current is not None and current.transaction_id == transaction_id

    @asynccontextmanager
    async def async_managed_update(
        self,
        shell: PanelShell,
        request: StoredPanelRequest,
        fleet: StoredFleetProfile,
        version: str,
        *,
        subentry_id: str,
        stopping: Callable[[], bool] = _not_stopping,
    ) -> AsyncIterator[UUID]:
        """Journal the existing manager's admitted update without reacquiring its lock.

        If the update fails after the transaction exists, the journal's verified
        rollback runs. When the update's session was LOST (#219) that rollback runs
        over a fresh session pinned to the journaled host key instead of the dead
        one; *stopping* reports an HA stop/unload so the reconnect gives up with
        ``recovery_interrupted``. Activation is never repeated, and every failure
        keeps the durable restore intent for the background recovery runner.
        """
        if not self._lock.locked():
            raise PanelProvisioningError("transaction_in_progress")
        if await self._journal.async_load() is not None:
            # The shared lock is held, so this journal is not in flight: it awaits
            # recovery, which the operator can see and settle (docs/canary-rollback.md).
            raise PanelProvisioningError("provisioning_recovery_required")
        if any(
            item.record.panel_request.fingerprint == request.fingerprint
            for item in await self._journal.async_retained_records()
        ):
            raise PanelProvisioningError("baseline_retained_finalize_first")
        snapshot = await self._operations.snapshot_panel(shell)
        snapshot = await self._operations.capture_baseline(shell, snapshot)
        if snapshot.baseline is None:
            raise PanelProvisioningError("baseline_incomplete")
        transaction = uuid4()
        async with self._unjournaled_baseline_guard(shell, snapshot, transaction) as guard:
            await self._operations.verify_baseline(shell, snapshot)
            await self._async_rehearse_incumbent(shell, snapshot, request.slug)
            request = replace(
                request,
                selected_components=self._staged_release_factory(
                    version, transaction, request.selected_components
                ).selected_components,
            )
            record = ProvisioningRecord(
                transaction,
                ProvisioningOperation.UPDATE,
                ProvisioningPhase.STAGED,
                uuid4(),
                request,
                fleet,
                version,
                self._snapshot_to_stored(snapshot),
                self._clock(),
                None,
            )
            guard.create_attempted = True
            await self._journal.async_create(record)
        record = await self._journal.async_transition(
            transaction, ProvisioningPhase.ACTIVATION_PENDING
        )
        observer = self._health_observer_factory(request.slug)
        try:
            await observer.async_subscribe()
            observer.mark_activation_started(version, transaction.hex)
            yield transaction
            evidence = await observer.async_wait(version, min(self._health_timeout, 90.0))
            if evidence.deployment_id != transaction.hex or evidence.agent_version != version:
                raise PanelHealthError("panel_health_version_mismatch")
            for phase in (
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
                ProvisioningPhase.PENDING_CONFIG_COMMIT,
            ):
                await self._journal.async_transition(transaction, phase)
            await self._journal.async_complete_commit(transaction, subentry_id=subentry_id)
        except BaseException:
            current = await self._journal.async_load()
            if current is not None and current.phase is not ProvisioningPhase.COMMITTED:
                staged = self._staged_release_factory(
                    version, transaction, request.selected_components
                )
                if shell.transport_lost():
                    outcome = await settle(
                        self._async_rollback_over_fresh_session(current, snapshot, staged, stopping)
                    )
                    # The rollback itself recorded any failure and kept the intent; a
                    # caller cancellation stays a cancellation.
                    if outcome.cancellation is not None:
                        raise outcome.cancellation from None
                    if isinstance(outcome.error, PanelProvisioningError):
                        raise PanelProvisioningError(outcome.error.code) from None
                else:
                    outcome = await settle(
                        self._async_recovery_rollback(
                            current, shell, snapshot, staged, original_code="health_failed"
                        )
                    )
                if outcome.error is not None:
                    raise PanelProvisioningError("rollback_failed") from None
            raise
        finally:
            await _settle_close(observer.async_close)

    async def _async_install_body(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        progress: ProgressReporter,
        state: _InstallState,
        release_override: Mapping[str, object] | None,
    ) -> ProvisionedPanel:
        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(progress, ProvisioningProgressStage.RECOVERING)
        state.failure_code = "journal_failed"
        state.failure_stage = "journal"
        existing = await self._journal.async_load()
        if existing is not None:
            state.failure_code = "recovery_failed"
            state.failure_stage = "recovery"
            recovery = await settle(self._async_recover_bounded(progress))
            if recovery.cancellation is not None:
                raise recovery.cancellation
            if recovery.error is not None:
                if isinstance(recovery.error, PanelProvisioningError):
                    raise PanelProvisioningError(recovery.error.code)
                raise PanelProvisioningError("recovery_failed")
            state.failure_code = "journal_failed"
            existing = await self._journal.async_load()
            if existing is not None:
                raise PanelProvisioningError("transaction_in_progress")

        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(progress, ProvisioningProgressStage.FETCHING_IDENTITY)
        state.failure_code = "identity_failed"
        state.failure_stage = "identity"
        identity = await self._identity_fetcher(request.host)
        if not isinstance(identity, HostIdentity):
            raise PanelProvisioningError("identity_failed")

        state.failure_code = "panel_authentication_failed"
        state.failure_stage = "authentication"
        state.shell = self._shell_factory(
            request.host,
            request.root_password,
            identity.public_key,
        )
        if state.shell.pinned_host_key() != identity.public_key:
            raise PanelProvisioningError("panel_authentication_failed")
        await state.shell.connect()

        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(progress, ProvisioningProgressStage.INSPECTING)
        state.failure_code = "inspection_failed"
        state.failure_stage = "inspection"
        facts = await self._inspector(state.shell, identity)
        if not isinstance(facts, PanelFacts) or facts.fingerprint != identity.fingerprint:
            raise PanelProvisioningError("inspection_failed")
        if await self._duplicate_fingerprint(identity.fingerprint):
            raise PanelProvisioningError("duplicate_panel")

        state.transaction_id = self._next_id()
        state.setup_id = self._next_id()
        if release_override is not None:
            value = release_override.get("transaction")
            if not isinstance(value, str):
                raise PanelProvisioningError("release_identity_blocked")
            try:
                transaction = UUID(hex=value)
            except ValueError:
                raise PanelProvisioningError("release_identity_blocked") from None
            if not _is_uuid4(transaction):
                raise PanelProvisioningError("release_identity_blocked")
            state.transaction_id = transaction
        state.failure_code = "release_prepare_failed"
        state.failure_stage = "release"
        bundle = await self._release_provider(
            request,
            fleet,
            state.transaction_id,
            state.setup_id,
        )
        if (
            not isinstance(bundle, PanelReleaseBundle)
            or bundle.transaction_id != state.transaction_id
            or fleet.broker.has_custom_ca != (bundle.mqtt_ca is not None)
        ):
            raise PanelProvisioningError("release_prepare_failed")

        async with self._operations.release_transaction(
            state.shell,
            bundle.local_payload_dir,
            panel=request.slug,
            components=request.selected_components,
            override=release_override,
            transaction_id=state.transaction_id,
            allow_pre_ordinal_upgrade=True,
        ) as admission:
            if admission.noop:
                raise PanelProvisioningError("release_unchanged")
            if not all(admission.changes.values()):
                raise PanelProvisioningError("release_selection_conflict")
            return await self._async_install_admitted(
                request, fleet, progress, state, identity, facts, bundle
            )

    async def _async_install_admitted(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        progress: ProgressReporter,
        state: _InstallState,
        identity: HostIdentity,
        facts: PanelFacts,
        bundle: PanelReleaseBundle,
    ) -> ProvisionedPanel:
        assert state.shell is not None
        assert state.transaction_id is not None
        assert state.setup_id is not None
        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(progress, ProvisioningProgressStage.SNAPSHOTTING)
        state.failure_code = "snapshot_failed"
        state.failure_stage = "snapshot"
        state.snapshot = await self._operations.snapshot_panel(state.shell)
        state.snapshot = await self._operations.capture_baseline(state.shell, state.snapshot)
        if state.snapshot.baseline is None:
            raise PanelProvisioningError("snapshot_failed")
        async with self._unjournaled_baseline_guard(
            state.shell, state.snapshot, state.transaction_id
        ) as guard:
            await self._async_prepare_journal(
                request, fleet, state, identity, bundle, state.snapshot, guard
            )
        return await self._async_install_journaled(
            request, fleet, progress, state, identity, facts, bundle
        )

    async def _async_prepare_journal(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        state: _InstallState,
        identity: HostIdentity,
        bundle: PanelReleaseBundle,
        snapshot: PanelSnapshot,
        guard: _BaselineGuard,
    ) -> None:
        """Verify the baseline and create the journal that owns it (install path)."""
        assert state.shell is not None
        assert state.transaction_id is not None
        assert state.setup_id is not None
        await self._operations.verify_baseline(state.shell, snapshot)
        await self._async_rehearse_incumbent(state.shell, snapshot, request.slug)
        state.stored_snapshot = self._snapshot_to_stored(snapshot)
        if not isinstance(state.stored_snapshot, StoredPanelSnapshot):
            raise PanelProvisioningError("snapshot_failed")

        state.staged = self._staged_release_factory(
            bundle.version,
            state.transaction_id,
            request.selected_components,
        )
        if not _matching_staged(
            state.staged,
            bundle.version,
            state.transaction_id,
            request.selected_components,
        ):
            raise PanelProvisioningError("invalid_provisioning_dependency")

        state.failure_code = "journal_failed"
        state.failure_stage = "journal"
        record = ProvisioningRecord(
            transaction_id=state.transaction_id,
            operation=(
                ProvisioningOperation.INSTALL
                if state.stored_snapshot.layout is StoredPanelLayout.ABSENT
                else ProvisioningOperation.UPGRADE
            ),
            phase=ProvisioningPhase.STAGED,
            setup_id=state.setup_id,
            panel_request=StoredPanelRequest(
                host=request.host,
                ssh_username=request.ssh_username,
                root_password=request.root_password,
                public_key=identity.public_key,
                fingerprint=identity.fingerprint,
                slug=request.slug,
                selected_components=request.selected_components,
            ),
            fleet_profile=StoredFleetProfile(
                kind=fleet.broker.kind,
                host=fleet.broker.host,
                port=fleet.broker.port,
                tls_enabled=fleet.broker.tls_enabled,
            ),
            staged_version=bundle.version,
            prior_snapshot=state.stored_snapshot,
            started_at=self._clock(),
            last_error=None,
        )
        guard.create_attempted = True
        create_outcome = await settle(self._journal.async_create(record))
        if create_outcome.error is not None:
            if create_outcome.cancellation is not None:
                raise create_outcome.cancellation from None
            raise PanelProvisioningError("journal_failed")
        state.journal_phase = ProvisioningPhase.STAGED
        if create_outcome.cancellation is not None:
            raise create_outcome.cancellation from None

    async def _async_install_journaled(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        progress: ProgressReporter,
        state: _InstallState,
        identity: HostIdentity,
        facts: PanelFacts,
        bundle: PanelReleaseBundle,
    ) -> ProvisionedPanel:
        assert state.shell is not None
        assert state.transaction_id is not None
        assert state.setup_id is not None
        assert state.staged is not None
        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(
            progress,
            ProvisioningProgressStage.STAGING,
            state.transaction_id,
            state.setup_id,
        )
        state.failure_code = "stage_failed"
        state.failure_stage = "stage"
        staged_result = await self._operations.stage_release(
            state.shell,
            bundle.local_payload_dir,
            bundle.version,
            bundle.environment,
            request.selected_components,
            state.transaction_id,
            mqtt_ca=bundle.mqtt_ca,
        )
        if not _matching_staged(
            staged_result,
            bundle.version,
            state.transaction_id,
            request.selected_components,
        ):
            raise PanelProvisioningError("stage_failed")

        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(
            progress,
            ProvisioningProgressStage.PREFLIGHT,
            state.transaction_id,
            state.setup_id,
        )
        state.failure_code = "preflight_failed"
        state.failure_stage = "preflight"
        launcher = self._preflight_launcher_factory(state.shell, state.staged)
        if not callable(launcher):
            raise PanelProvisioningError("preflight_failed")
        validation = await self._broker_validator.async_validate_panel(
            fleet.broker,
            launcher,
            setup_id=state.setup_id,
        )
        if (
            not isinstance(validation, BrokerValidationResult)
            or validation.setup_id != state.setup_id
        ):
            raise PanelProvisioningError("preflight_failed")

        state.failure_code = "journal_failed"
        state.failure_stage = "journal"
        await self._journal.async_transition(
            state.transaction_id,
            ProvisioningPhase.ACTIVATION_PENDING,
        )
        state.journal_phase = ProvisioningPhase.ACTIVATION_PENDING

        state.failure_code = "health_subscribe_failed"
        state.failure_stage = "health"
        state.observer = self._health_observer_factory(request.slug)
        await state.observer.async_subscribe()
        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(
            progress,
            ProvisioningProgressStage.ACTIVATING,
            state.transaction_id,
            state.setup_id,
        )
        state.failure_code = "activation_failed"
        state.failure_stage = "activation"

        def mark_candidate_boundary() -> None:
            assert state.observer is not None
            assert state.transaction_id is not None
            state.observer.mark_activation_started(
                bundle.version,
                state.transaction_id.hex,
            )

        await self._operations.activate_staged(
            state.shell,
            state.staged,
            on_services_stopped=mark_candidate_boundary,
        )
        state.failure_code = "journal_failed"
        state.failure_stage = "journal"
        await self._journal.async_transition(
            state.transaction_id,
            ProvisioningPhase.ACTIVATED,
        )
        state.journal_phase = ProvisioningPhase.ACTIVATED

        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(
            progress,
            ProvisioningProgressStage.VERIFYING,
            state.transaction_id,
            state.setup_id,
        )
        state.failure_code = "health_failed"
        state.failure_stage = "health"
        health = await state.observer.async_wait(
            bundle.version,
            self._health_timeout,
        )
        state.failure_code = "journal_failed"
        state.failure_stage = "journal"
        await self._journal.async_transition(
            state.transaction_id,
            ProvisioningPhase.VERIFYING,
        )
        state.journal_phase = ProvisioningPhase.VERIFYING
        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(
            progress,
            ProvisioningProgressStage.COMPLETE,
            state.transaction_id,
            state.setup_id,
        )
        return ProvisionedPanel(
            identity=identity,
            facts=facts,
            version=bundle.version,
            health=health,
            transaction_id=state.transaction_id,
            setup_id=state.setup_id,
            panel_data=_panel_data(request, identity, state.transaction_id),
        )

    async def _async_compensate(
        self,
        state: _InstallState,
        original_code: str,
        progress: ProgressReporter,
    ) -> None:
        if (
            state.shell is None
            or state.staged is None
            or state.transaction_id is None
            or state.journal_phase is None
        ):
            return
        if state.journal_phase in {
            ProvisioningPhase.ACTIVATION_PENDING,
            ProvisioningPhase.ACTIVATED,
            ProvisioningPhase.VERIFYING,
            ProvisioningPhase.ROLLBACK_PENDING,
        }:
            if state.journal_phase is not ProvisioningPhase.ROLLBACK_PENDING:
                await self._journal.async_transition(
                    state.transaction_id,
                    ProvisioningPhase.ROLLBACK_PENDING,
                    last_error=StoredJournalError(
                        stage=state.failure_stage,
                        code=original_code,
                    ),
                )
                state.journal_phase = ProvisioningPhase.ROLLBACK_PENDING
            try:
                await _report(
                    progress,
                    ProvisioningProgressStage.ROLLING_BACK,
                    state.transaction_id,
                    state.setup_id,
                )
            except (asyncio.CancelledError, Exception):
                pass
            if state.snapshot is None:
                raise PanelProvisioningError("rollback_failed")
            record = await self._journal.async_load()
            if record is None:
                raise PanelProvisioningError("rollback_failed")
            await self._async_recovery_rollback(
                record, state.shell, state.snapshot, state.staged, original_code=original_code
            )
            state.journal_phase = None
            return

        if state.journal_phase is ProvisioningPhase.STAGED:
            await settle(
                self._journal.async_record_error(
                    state.transaction_id,
                    StoredJournalError(
                        stage=state.failure_stage,
                        code=original_code,
                    ),
                )
            )
        await self._operations.cleanup_staged(state.shell, state.staged)
        if state.journal_phase is ProvisioningPhase.STAGED:
            await self._journal.async_complete_cleanup(state.transaction_id)
            state.journal_phase = None

    async def _async_reopen_compensate_and_close(
        self,
        state: _InstallState,
        request: PanelInstallRequest,
        public_key: str,
        original_code: str,
        progress: ProgressReporter,
    ) -> None:
        """Use a fresh pinned session when cancellation interrupts final close."""
        replacement: PanelShell | None = None
        failed = False
        try:
            replacement = self._shell_factory(
                request.host,
                request.root_password,
                public_key,
            )
            state.shell = replacement
            if replacement.pinned_host_key() != public_key:
                raise PanelProvisioningError("rollback_failed")
            await replacement.connect()
            await self._async_compensate(state, original_code, progress)
        except BaseException:
            failed = True
        close_outcome = await _settle_close(replacement.close if replacement is not None else None)
        if failed or close_outcome.error is not None or close_outcome.cancellation is not None:
            raise PanelProvisioningError("rollback_failed")

    async def _async_report_compensation_failure(
        self,
        state: _InstallState,
        original_code: str,
        compensation: Settlement[None],
    ) -> None:
        if (
            compensation.error is None
            or state.journal_phase
            not in {
                ProvisioningPhase.STAGED,
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
                ProvisioningPhase.ROLLBACK_PENDING,
            }
            or state.transaction_id is None
        ):
            return
        compensation_code = (
            "cleanup_failed"
            if state.journal_phase is ProvisioningPhase.STAGED
            else "rollback_failed"
        )
        await settle(
            self._repair_reporter.async_report_rollback_failure(
                state.transaction_id,
                original_code=original_code,
                rollback_code=compensation_code,
            )
        )

    async def async_mark_pending_config_commit(
        self,
        transaction_id: UUID,
    ) -> None:
        """Persist the config-flow handoff boundary without clearing recovery."""
        if not _is_uuid4(transaction_id):
            raise PanelProvisioningError("commit_failed")
        async with self._lock:

            async def mark_pending() -> None:
                record = await self._journal.async_load()
                if (
                    isinstance(record, ProvisioningRecord)
                    and record.transaction_id == transaction_id
                    and record.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT
                ):
                    return
                if (
                    not isinstance(record, ProvisioningRecord)
                    or record.transaction_id != transaction_id
                    or record.phase is not ProvisioningPhase.VERIFYING
                ):
                    raise PanelProvisioningError("commit_failed")
                await self._journal.async_transition(
                    transaction_id,
                    ProvisioningPhase.PENDING_CONFIG_COMMIT,
                )

            outcome = await settle(mark_pending())
            if outcome.cancellation is not None:
                raise outcome.cancellation from None
            if outcome.error is not None:
                raise PanelProvisioningError("commit_failed")

    async def async_complete_config_commit(
        self,
        transaction_id: UUID,
        *,
        subentry_id: str,
    ) -> None:
        """Clear the journal only after exact subentry creation is proven."""
        if not _is_uuid4(transaction_id):
            raise PanelProvisioningError("commit_failed")
        async with self._lock:

            async def complete_commit() -> None:
                await self._journal.async_complete_commit(
                    transaction_id,
                    subentry_id=subentry_id,
                )
                await self._repair_reporter.async_clear_rollback_failure(transaction_id)

            outcome = await settle(complete_commit())
            if outcome.cancellation is not None:
                raise outcome.cancellation from None
            if outcome.error is not None:
                raise PanelProvisioningError("commit_failed")

    async def async_recover(
        self,
        progress: ProgressReporter = _noop_progress,
    ) -> None:
        """Settle the one durable transaction before accepting new work."""
        async with self._lock:
            outcome = await settle(self._async_recover_bounded(progress))
            if outcome.cancellation is not None:
                raise outcome.cancellation from None
            if outcome.error is not None:
                if isinstance(outcome.error, PanelProvisioningError):
                    raise PanelProvisioningError(outcome.error.code)
                raise PanelProvisioningError("recovery_failed")

    async def _async_recover_bounded(self, progress: ProgressReporter) -> None:
        try:
            async with asyncio.timeout(RECOVERY_TIMEOUT_SECONDS):
                await self._async_recover_current(progress)
        except TimeoutError:
            raise PanelProvisioningError("rollback_deadline_exceeded") from None

    async def _async_recover_current(
        self,
        progress: ProgressReporter,
    ) -> None:
        await _report(progress, ProvisioningProgressStage.RECOVERING)
        for retained in await self._journal.async_retained_records():
            if retained.state == "finalizing":
                if retained.finalization_origin == "ownerless_restored":
                    raise PanelProvisioningError("ownerless_finalize_reauthentication_required")
                await self._async_finalize(retained)
            elif retained.state == "restore_requested":
                password = retained.compensation_password(await self._journal.async_load())
                if password is None:
                    password = await self._resolve_credential(retained.record.panel_request)
                await self._journal.async_begin_restore(retained.record.transaction_id, password)
        record = await self._journal.async_load()
        if record is not None:
            await self._async_recover_record(record, progress)

    async def _async_recover_record(
        self,
        record: ProvisioningRecord,
        progress: ProgressReporter,
        *,
        host: str | None = None,
        password: str | None = None,
    ) -> None:
        if not isinstance(record, ProvisioningRecord):
            raise PanelProvisioningError("recovery_failed")
        if record.phase is ProvisioningPhase.ROLLED_BACK:
            retained = await self._journal.async_retained(record.transaction_id)
            verified = (
                retained is not None
                and retained.state == "restored"
                and retained.recovery_deployment_id is not None
            )
            await self._journal.async_complete_rollback(
                record.transaction_id,
                verified=verified,
            )
            await self._repair_reporter.async_clear_rollback_failure(record.transaction_id)
            return
        if record.phase is ProvisioningPhase.COMMITTED:
            await self._journal.async_clear_committed(record.transaction_id)
            await self._repair_reporter.async_clear_rollback_failure(record.transaction_id)
            return
        if record.phase is ProvisioningPhase.PENDING_CONFIG_COMMIT:
            lookup = await self._transaction_lookup(record.transaction_id)
            if lookup.state is TransactionLookupState.FLOW_PENDING:
                return
            if lookup.state is TransactionLookupState.MATCHED and lookup.subentry_id is not None:
                await self._journal.async_complete_commit(
                    record.transaction_id,
                    subentry_id=lookup.subentry_id,
                )
                await self._repair_reporter.async_clear_rollback_failure(record.transaction_id)
                return

        request = record.panel_request
        # Failures caused by flow-supplied access are shown in that flow; they must
        # not replace the background runner's diagnosis of the recorded access.
        record_failures = host is None and password is None
        staged, snapshot = await self._async_recovery_artifacts(record, record_failures)
        shell = await self._async_recovery_shell(record, host, password, record_failures)
        observer: _HealthObserver | None = None
        primary: PanelProvisioningError | None = None
        cancellation: asyncio.CancelledError | None = None
        restore_started = False

        async def rollback(original_code: str) -> None:
            nonlocal restore_started
            restore_started = True
            await self._async_recovery_rollback(
                record,
                shell,
                snapshot,
                staged,
                original_code=original_code,
                password=password,
            )

        try:
            if shell.pinned_host_key() != request.public_key:
                raise PanelProvisioningError("panel_identity_mismatch")
            try:
                await shell.connect()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise PanelProvisioningError(recovery_connect_failure_code(error)) from None
            if record.phase is ProvisioningPhase.STAGED:
                await self._operations.cleanup_staged(shell, staged)
                await self._journal.async_complete_cleanup(record.transaction_id)
                await self._repair_reporter.async_clear_rollback_failure(record.transaction_id)
            elif (
                record.phase
                in {
                    ProvisioningPhase.ACTIVATION_PENDING,
                    ProvisioningPhase.ROLLBACK_PENDING,
                    ProvisioningPhase.PENDING_CONFIG_COMMIT,
                }
                or record.operation is ProvisioningOperation.UPDATE
            ):
                await rollback("recovery_interrupted")
            elif record.phase in {
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
            }:
                lookup = await self._transaction_lookup(record.transaction_id)
                if lookup.state is TransactionLookupState.FLOW_ABORTED_OR_ABSENT:
                    await rollback("flow_aborted")
                else:
                    observer = self._health_observer_factory(record.panel_request.slug)
                    health_failed = False
                    try:
                        await observer.async_subscribe()

                        def mark_candidate_boundary() -> None:
                            observer.mark_activation_started(
                                record.staged_version,
                                record.transaction_id.hex,
                            )

                        await self._operations.restart_candidate(
                            shell,
                            staged,
                            on_service_stopped=mark_candidate_boundary,
                        )
                        await observer.async_wait(
                            record.staged_version,
                            self._health_timeout,
                        )
                    except asyncio.CancelledError:
                        raise
                    except Exception:
                        health_failed = True
                    if health_failed:
                        await rollback("health_failed")
                    else:
                        if record.phase is ProvisioningPhase.ACTIVATED:
                            record = await self._journal.async_transition(
                                record.transaction_id,
                                ProvisioningPhase.VERIFYING,
                            )
                        lookup = await self._transaction_lookup(record.transaction_id)
                        if lookup.state is TransactionLookupState.FLOW_ABORTED_OR_ABSENT:
                            await rollback("flow_aborted")
                        else:
                            await self._journal.async_transition(
                                record.transaction_id,
                                ProvisioningPhase.PENDING_CONFIG_COMMIT,
                            )
                            if (
                                lookup.state is TransactionLookupState.MATCHED
                                and lookup.subentry_id is not None
                            ):
                                await self._journal.async_complete_commit(
                                    record.transaction_id,
                                    subentry_id=lookup.subentry_id,
                                )
                                await self._repair_reporter.async_clear_rollback_failure(
                                    record.transaction_id
                                )
        except asyncio.CancelledError as error:
            cancellation = error
        except PanelProvisioningError as error:
            primary = error
        except Exception:
            primary = PanelProvisioningError("recovery_failed")

        observer_outcome = await _settle_close(
            observer.async_close if observer is not None else None
        )
        shell_outcome = await _settle_close(shell.close)
        if cancellation is not None:
            raise cancellation from None
        if primary is not None:
            if not restore_started and record_failures:
                # Restore intent never became durable: record why, typed, so the
                # operator sees an actionable code instead of last_error: null.
                await self._async_record_recovery_failure(record, primary.code)
            raise primary from None
        if observer_outcome.cancellation is not None:
            raise observer_outcome.cancellation from None
        if shell_outcome.cancellation is not None:
            raise shell_outcome.cancellation from None
        if observer_outcome.error is not None or shell_outcome.error is not None:
            raise PanelProvisioningError("recovery_failed")

    async def _async_recovery_artifacts(
        self, record: ProvisioningRecord, record_failures: bool
    ) -> tuple[StagedRelease, PanelSnapshot]:
        """Rebuild the candidate and snapshot only from what the journal recorded.

        Never from the currently bundled payload, which may be a newer release.
        """
        request = record.panel_request
        try:
            staged = self._staged_release_factory(
                record.staged_version,
                record.transaction_id,
                request.selected_components,
            )
            if not _matching_staged(
                staged,
                record.staged_version,
                record.transaction_id,
                _canonical_components(request.selected_components),
            ):
                raise PanelProvisioningError("recovery_artifact_invalid")
            snapshot = self._snapshot_from_stored(record.prior_snapshot)
        except (PanelProvisioningError, PanelOpError, ValueError, TypeError):
            if record_failures:
                await self._async_record_recovery_failure(record, "recovery_artifact_invalid")
            raise PanelProvisioningError("recovery_artifact_invalid") from None
        return staged, snapshot

    async def _async_recovery_shell(
        self,
        record: ProvisioningRecord,
        host: str | None,
        password: str | None,
        record_failures: bool,
    ) -> PanelShell:
        request = record.panel_request
        try:
            return self._shell_factory(
                host if host is not None else request.host,
                password if password is not None else request.root_password,
                request.public_key,
            )
        except Exception as error:
            code = recovery_connect_failure_code(error)
            if record_failures:
                await self._async_record_recovery_failure(record, code)
            raise PanelProvisioningError(code) from None

    async def _async_finish_verified_restore(
        self, record: ProvisioningRecord, host: str, password: str
    ) -> None:
        """Complete a rollback that crashed after durable verified-restore evidence.

        The panel already passed fresh restore verification, so it is not restored
        again: only the transaction-owned candidate is pruned before the journal is
        cleared, exactly as the interrupted rollback would have done.
        """
        staged, _snapshot = await self._async_recovery_artifacts(record, False)
        shell = await self._async_recovery_shell(record, host, password, False)
        primary: PanelProvisioningError | None = None
        try:
            if shell.pinned_host_key() != record.panel_request.public_key:
                raise PanelProvisioningError("panel_identity_mismatch")
            try:
                await shell.connect()
            except asyncio.CancelledError:
                raise
            except Exception as error:
                raise PanelProvisioningError(recovery_connect_failure_code(error)) from None
            await self._operations.cleanup_staged(shell, staged)
            if record.prior_snapshot.layout is StoredPanelLayout.ABSENT:
                await self._journal.async_complete_absent_rollback(record.transaction_id)
            else:
                await self._journal.async_complete_rollback(record.transaction_id, verified=True)
            await self._repair_reporter.async_clear_rollback_failure(record.transaction_id)
        except PanelProvisioningError as error:
            primary = error
        except Exception:
            primary = PanelProvisioningError("cleanup_failed")
        closed = await _settle_close(shell.close)
        if closed.cancellation is not None:
            raise closed.cancellation from None
        if primary is not None:
            raise primary from None

    async def _async_record_recovery_failure(self, record: ProvisioningRecord, code: str) -> None:
        """Best-effort durable typed error; never changes phase, baseline or journal."""
        if code not in PROVISIONING_ERROR_CODES:
            code = "recovery_failed"
        await settle(
            self._journal.async_record_error(
                record.transaction_id, StoredJournalError(stage="recovery", code=code)
            )
        )

    async def _async_assert_ownerless_journal(
        self, expected: ProvisioningRecord
    ) -> RetainedRollback:
        current = await self._journal.async_load()
        retained = (
            await self._journal.async_retained(expected.transaction_id)
            if isinstance(expected, ProvisioningRecord)
            else None
        )
        if (
            current is None
            or not _same_journal_transaction(current, expected)
            or not ownerless_journal_recovery_allowed(current, retained)
            or retained is None
            or self._retained_owner_exists is None
            or await self._retained_owner_exists(retained)
            or (await self._transaction_lookup(current.transaction_id)).state
            is not TransactionLookupState.FLOW_ABORTED_OR_ABSENT
        ):
            raise PanelProvisioningError("ownerless_recovery_unavailable")
        return retained

    async def async_recover_ownerless_journal(
        self,
        expected: ProvisioningRecord,
        host: str,
        password: str,
        identity: HostIdentity,
    ) -> None:
        """Roll back one interrupted, ownerless onboarding journal with fresh authority.

        The operator's freshly entered address and password are used only after the
        panel proves the journal's pinned SSH identity. The outcome is the existing
        compensating rollback: a verified restore (or verified prior absence) or a
        durable typed failure. Nothing is finalized, skipped or erased here.
        """
        async with self._async_install_lock():
            await self._async_assert_ownerless_journal(expected)
            request = expected.panel_request
            proven = await self._identity_fetcher(host)
            if (
                identity.public_key != request.public_key
                or identity.fingerprint != request.fingerprint
                or proven != identity
            ):
                raise PanelProvisioningError("panel_identity_mismatch")
            retained = await self._async_assert_ownerless_journal(expected)
            current = await self._journal.async_load()
            if current is None:
                raise PanelProvisioningError("ownerless_recovery_unavailable")

            async def recover() -> None:
                async with asyncio.timeout(RECOVERY_TIMEOUT_SECONDS):
                    if (
                        retained.state == "restored"
                        and current.phase is ProvisioningPhase.ROLLBACK_PENDING
                    ):
                        await self._async_finish_verified_restore(current, host, password)
                    else:
                        await self._async_recover_record(
                            current, _noop_progress, host=host, password=password
                        )

            outcome = await settle(recover())
            if outcome.cancellation is not None:
                raise outcome.cancellation from None
            if isinstance(outcome.error, TimeoutError):
                raise PanelProvisioningError("rollback_deadline_exceeded") from None
            if isinstance(outcome.error, PanelProvisioningError):
                raise PanelProvisioningError(outcome.error.code) from None
            if outcome.error is not None:
                raise PanelProvisioningError("recovery_failed") from None
            if await self._journal.async_load() is not None:
                raise PanelProvisioningError("recovery_failed")

    async def _async_recovery_rollback(
        self,
        record: ProvisioningRecord,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        staged: StagedRelease,
        *,
        original_code: str,
        password: str | None = None,
    ) -> None:
        current = await self._async_persist_rollback_intent(
            record, original_code=original_code, password=password
        )
        await self._async_restore_with_intent(
            current, shell, snapshot, staged, original_code=original_code
        )

    async def _async_persist_rollback_intent(
        self,
        record: ProvisioningRecord,
        *,
        original_code: str,
        password: str | None = None,
    ) -> ProvisioningRecord:
        """Make the restore intent durable before anything touches the panel."""
        current = record
        if current.prior_snapshot.baseline is not None:
            await self._journal.async_retained_state(current.transaction_id, "restore_requested")
            await self._journal.async_begin_restore(
                current.transaction_id,
                password if password is not None else current.panel_request.root_password,
            )
            current = await self._journal.async_record_error(
                current.transaction_id, StoredJournalError(stage="recovery", code=original_code)
            )
        elif current.phase is not ProvisioningPhase.ROLLBACK_PENDING:
            current = await self._journal.async_transition(
                current.transaction_id,
                ProvisioningPhase.ROLLBACK_PENDING,
                last_error=StoredJournalError(
                    stage="recovery",
                    code=original_code,
                ),
            )
        return current

    async def _async_restore_with_intent(
        self,
        current: ProvisioningRecord,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        staged: StagedRelease,
        *,
        original_code: str,
        deadline_seconds: float = RECOVERY_TIMEOUT_SECONDS,
    ) -> None:
        failure_code: str | None = None
        try:
            async with asyncio.timeout(deadline_seconds):
                await self._async_restore_verified(current, shell, snapshot, staged)
        except asyncio.CancelledError:
            raise
        except TimeoutError:
            failure_code = "rollback_deadline_exceeded"
        except PanelHealthError:
            failure_code = "rollback_health_failed"
        except Exception:
            failure_code = "rollback_failed"
        if failure_code is not None:
            await self._journal.async_retained_state(current.transaction_id, "rollback_failed")
            await self._journal.async_record_error(
                current.transaction_id, StoredJournalError(stage="rollback", code=failure_code)
            )
            await settle(
                self._repair_reporter.async_report_rollback_failure(
                    current.transaction_id,
                    original_code=original_code,
                    rollback_code=failure_code,
                )
            )
            raise PanelProvisioningError(failure_code)
        await self._repair_reporter.async_clear_rollback_failure(current.transaction_id)

    async def _async_rollback_over_fresh_session(
        self,
        record: ProvisioningRecord,
        snapshot: PanelSnapshot,
        staged: StagedRelease,
        stopping: Callable[[], bool],
    ) -> None:
        """Run the verified rollback over a new pinned session after a lost one (#219).

        The restore intent is made durable FIRST. A failure to reconnect keeps it
        (ROLLBACK_PENDING, retained ``restore_requested``), records a typed
        ``last_error`` and raises a Repair naming ``connection_lost``; the
        background recovery runner settles it later. Nothing is cleared, finalized
        or re-activated here.
        """
        loop = asyncio.get_running_loop()
        started = loop.time()
        current = await self._async_persist_rollback_intent(record, original_code="connection_lost")
        try:
            shell = await self._async_reconnect_pinned(current, stopping)
        except asyncio.CancelledError:
            await self._async_reconnect_failed(current, "recovery_interrupted")
            raise
        except PanelProvisioningError as error:
            await self._async_reconnect_failed(current, error.code)
            raise
        try:
            await self._async_restore_with_intent(
                current,
                shell,
                snapshot,
                staged,
                original_code="connection_lost",
                deadline_seconds=max(0.0, RECOVERY_TIMEOUT_SECONDS - (loop.time() - started)),
            )
        finally:
            await _settle_close(shell.close)

    async def _async_reconnect_pinned(
        self, record: ProvisioningRecord, stopping: Callable[[], bool]
    ) -> PanelShell:
        """Connect to the journaled host, pinned to the journaled key, with retries.

        Only classified network faults that never reached SSH authentication are
        retried (bounded attempts, shared backoff); authentication, identity and
        post-auth failures stop at once. The whole reconnect is bounded by
        RECONNECT_TIMEOUT_SECONDS (``recovery_connection_failed``); an exhausted
        attempt budget is ``rollback_failed``; an HA stop is ``recovery_interrupted``.
        """
        request = record.panel_request
        try:
            async with asyncio.timeout(RECONNECT_TIMEOUT_SECONDS):
                attempt = 0
                while True:
                    if stopping():
                        raise PanelProvisioningError("recovery_interrupted")
                    attempt += 1
                    try:
                        shell = self._shell_factory(
                            request.host, request.root_password, request.public_key
                        )
                    except Exception as error:
                        raise PanelProvisioningError(recovery_connect_failure_code(error)) from None
                    if shell.pinned_host_key() != request.public_key:
                        await _settle_close(shell.close)
                        raise PanelProvisioningError("panel_identity_mismatch")
                    try:
                        await shell.connect()
                    except asyncio.CancelledError:
                        await _settle_close(shell.close)
                        raise
                    except Exception as error:
                        await _settle_close(shell.close)
                        if (
                            shell.connect_reached_auth()
                            or connect_retry.classify_transient_connect_failure(error) is None
                        ):
                            raise PanelProvisioningError(
                                recovery_connect_failure_code(error)
                            ) from None
                        if attempt >= connect_retry.MAX_ATTEMPTS:
                            raise PanelProvisioningError("rollback_failed") from None
                        if not await _reconnect_backoff(
                            connect_retry.backoff_delay(attempt), stopping
                        ):
                            raise PanelProvisioningError("recovery_interrupted") from None
                        continue
                    return shell
        except TimeoutError:
            raise PanelProvisioningError("recovery_connection_failed") from None

    async def _async_reconnect_failed(self, record: ProvisioningRecord, code: str) -> None:
        """Keep the durable intent; record the typed failure and raise a Repair."""
        if code not in PROVISIONING_ERROR_CODES:
            code = "rollback_failed"
        await settle(
            self._journal.async_record_error(
                record.transaction_id, StoredJournalError(stage="recovery", code=code)
            )
        )
        await settle(
            self._repair_reporter.async_report_rollback_failure(
                record.transaction_id,
                original_code="connection_lost",
                rollback_code=code,
            )
        )

    async def _async_restore_verified(
        self,
        record: ProvisioningRecord,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        staged: StagedRelease,
    ) -> None:
        await self._operations.rollback_snapshot(shell, snapshot, staged)
        if snapshot.layout is PanelLayout.ABSENT:
            await self._operations.cleanup_staged(shell, staged)
            await self._journal.async_complete_absent_rollback(record.transaction_id)
            return
        if snapshot.baseline is None or "bridge" not in snapshot.baseline.identities:
            raise PanelProvisioningError("baseline_incomplete")
        identity = snapshot.baseline.identities["bridge"]
        deployment_id = uuid4().hex
        observer = self._health_observer_factory(record.panel_request.slug)
        try:
            await observer.async_subscribe()
            await self._operations.restart_restored(
                shell,
                snapshot,
                deployment_id,
                on_service_stopped=lambda: observer.mark_activation_started(
                    identity.version, deployment_id
                ),
            )
            evidence = await observer.async_wait(identity.version, min(self._health_timeout, 90.0))
            verified = (
                evidence.agent_version == identity.version
                and evidence.deployment_id == deployment_id
                and evidence.panel == record.panel_request.slug
            )
            if not verified:
                raise PanelHealthError("panel_health_version_mismatch")
            # Durable evidence precedes candidate cleanup and journal removal.
            await self._journal.async_retained_state(
                record.transaction_id, "restored", evidence=deployment_id
            )
            await self._operations.cleanup_staged(shell, staged)
            await self._journal.async_complete_rollback(record.transaction_id, verified=verified)
        finally:
            await _settle_close(observer.async_close)

    async def _resolve_credential(self, request: StoredPanelRequest) -> str:
        password = await self._credential_resolver(request) if self._credential_resolver else None
        if password is None:
            raise PanelProvisioningError("rollback_credentials_unavailable")
        return password

    async def async_rollback(self, transaction_id: UUID) -> None:
        """Persist operator intent before asking the existing runner to execute it."""
        async with self._lock:
            current = await self._journal.async_load()
            if current is not None and current.transaction_id != transaction_id:
                raise PanelProvisioningError("provisioning_recovery_required")
            retained = await self._journal.async_retained(transaction_id)
            if retained is None:
                raise PanelProvisioningError("baseline_incomplete")
            password = await self._resolve_credential(retained.record.panel_request)
            await self._journal.async_retained_state(transaction_id, "restore_requested")
            await self._journal.async_begin_restore(transaction_id, password)
        await self.async_recover()

    async def async_finalize(self, transaction_id: UUID) -> None:
        """Explicit intentional loss of the rollback guarantee, with durable intent."""
        async with self._lock:
            if await self._journal.async_load() is not None:
                raise PanelProvisioningError("provisioning_recovery_required")
            retained = await self._journal.async_retained(transaction_id)
            if retained is None:
                return
            if retained.finalization_origin == "ownerless_restored":
                raise PanelProvisioningError("ownerless_finalize_reauthentication_required")
            await self._journal.async_retained_state(transaction_id, "finalizing")
            await self._async_finalize(retained)

    async def _async_assert_ownerless(self, expected: RetainedRollback) -> None:
        current = await self._journal.async_retained(expected.record.transaction_id)
        if (
            await self._journal.async_load() is not None
            or current != expected
            or not expected.ownerless_finalization_allowed
            or self._retained_owner_exists is None
            or await self._retained_owner_exists(expected)
        ):
            raise PanelProvisioningError("ownerless_recovery_unavailable")

    async def async_finalize_ownerless_restored(
        self,
        expected: RetainedRollback,
        host: str,
        password: str,
        identity: HostIdentity,
    ) -> None:
        """Finalize one verified restore using only freshly authenticated flow authority."""
        async with self._async_install_lock():
            await self._async_assert_ownerless(expected)
            request = expected.record.panel_request
            proven = await self._identity_fetcher(host)
            if (
                identity.public_key != request.public_key
                or identity.fingerprint != request.fingerprint
                or proven != identity
            ):
                raise PanelProvisioningError("panel_identity_mismatch")
            shell = self._shell_factory(host, password, request.public_key)
            try:
                if shell.pinned_host_key() != request.public_key:
                    raise PanelProvisioningError("panel_identity_mismatch")
                await shell.connect()
                await self._async_assert_ownerless(expected)
                marked = await self._journal.async_begin_ownerless_finalize(expected)
                outcome = await settle(self._async_finish_ownerless_finalize(shell, marked))
                if outcome.cancellation is not None:
                    raise outcome.cancellation from None
                if outcome.error is not None:
                    raise PanelProvisioningError("cleanup_failed") from None
            finally:
                closed = await _settle_close(shell.close)
                if closed.cancellation is not None:
                    raise closed.cancellation from None

    async def _async_finish_ownerless_finalize(
        self, shell: PanelShell, marked: RetainedRollback
    ) -> None:
        # A new owner or active journal must not inherit this flow's ephemeral password.
        await self._async_assert_ownerless(marked)
        await self._operations.finalize_baseline(
            shell, self._snapshot_from_stored(marked.record.prior_snapshot)
        )
        await self._journal.async_remove_retained(marked.record.transaction_id)
        await self._repair_reporter.async_clear_rollback_failure(marked.record.transaction_id)

    async def _async_finalize(self, retained: RetainedRollback) -> None:
        request = retained.record.panel_request
        password = await self._resolve_credential(request)
        shell = self._shell_factory(request.host, password, request.public_key)
        try:
            if shell.pinned_host_key() != request.public_key:
                raise PanelProvisioningError("panel_authentication_failed")
            await shell.connect()
            await self._operations.finalize_baseline(
                shell, self._snapshot_from_stored(retained.record.prior_snapshot)
            )
            await self._journal.async_remove_retained(retained.record.transaction_id)
        finally:
            await _settle_close(shell.close)

    def _next_id(self) -> UUID:
        candidate = self._id_factory()
        if not _is_uuid4(candidate):
            raise PanelProvisioningError("invalid_provisioning_dependency")
        return candidate


async def _settle_close(
    operation: Callable[[], Awaitable[object]] | None,
) -> Settlement[object]:
    if operation is None:
        return Settlement(error=None, cancellation=None)
    try:
        return await settle(operation())
    except asyncio.CancelledError as error:
        return Settlement(error=None, cancellation=error)
    except BaseException as error:
        return Settlement(error=error, cancellation=None)


def _required_primary_code(
    primary: PanelProvisioningError | None,
) -> str:
    if primary is None:
        raise RuntimeError("missing_primary_provisioning_error")
    return primary.code


def _is_uuid4(value: object) -> bool:
    return isinstance(value, UUID) and value.version == 4 and value.variant == RFC_4122


def _same_journal_transaction(current: ProvisioningRecord, expected: ProvisioningRecord) -> bool:
    """Compare the fields that define what the operator confirmed, not volatile ones.

    A newly recorded ``last_error`` (or the recorded credential) changing between
    selection and confirmation must not invalidate an otherwise identical journal.
    """
    if not isinstance(expected, ProvisioningRecord):
        return False
    mine, theirs = current.panel_request, expected.panel_request
    return (
        current.transaction_id == expected.transaction_id
        and current.operation is expected.operation
        and current.phase is expected.phase
        and current.setup_id == expected.setup_id
        and current.staged_version == expected.staged_version
        and current.prior_snapshot == expected.prior_snapshot
        and current.fleet_profile == expected.fleet_profile
        and mine.host == theirs.host
        and mine.public_key == theirs.public_key
        and mine.fingerprint == theirs.fingerprint
        and mine.slug == theirs.slug
        and mine.selected_components == theirs.selected_components
    )


def _canonical_components(selected_components: tuple[str, ...]) -> tuple[str, ...]:
    """Order journal-recorded components exactly as the staged DTO normalizes them."""
    chosen = set(selected_components)
    if len(chosen) != len(selected_components) or not chosen <= set(_CORE_COMPONENTS):
        return selected_components
    return tuple(component for component in _CORE_COMPONENTS if component in chosen)


def _matching_staged(
    staged: object,
    version: str,
    transaction_id: UUID,
    selected_components: tuple[str, ...],
) -> bool:
    return (
        isinstance(staged, StagedRelease)
        and staged.version == version
        and staged.transaction_id == transaction_id
        and staged.selected_components == selected_components
        and staged.release_target == f"/var/brilliant-mqtt/releases/{version}--{transaction_id.hex}"
    )


def stored_snapshot_from_panel(snapshot: PanelSnapshot) -> StoredPanelSnapshot:
    """Convert the concrete operational DTO into its durable journal twin."""
    if not isinstance(snapshot, PanelSnapshot):
        raise ValueError("invalid_panel_snapshot_adapter")

    def file(value: FileSnapshot) -> StoredFileSnapshot:
        return StoredFileSnapshot(content=value.content, mode=value.mode)

    def service(value: ServiceSnapshot) -> StoredServiceSnapshot:
        return StoredServiceSnapshot(
            unit_file=file(value.unit_file),
            enabled=value.enabled,
            active=value.active,
        )

    return StoredPanelSnapshot(
        layout=StoredPanelLayout(snapshot.layout.value),
        active_release_target=snapshot.active_release_target,
        environment_file=file(snapshot.environment_file),
        version_file=file(snapshot.version_file),
        bridge_service=service(snapshot.bridge_service),
        wifi_watchdog_service=service(snapshot.wifi_watchdog_service),
        bus_watchdog_service=service(snapshot.bus_watchdog_service),
        selected_components=snapshot.selected_components,
        baseline=snapshot.baseline,
    )


def panel_snapshot_from_stored(snapshot: StoredPanelSnapshot) -> PanelSnapshot:
    """Rebuild the exact operational DTO required by concrete rollback."""
    if not isinstance(snapshot, StoredPanelSnapshot):
        raise ValueError("invalid_panel_snapshot_adapter")

    def file(value: StoredFileSnapshot) -> FileSnapshot:
        return FileSnapshot(content=value.content, mode=value.mode)

    def service(value: StoredServiceSnapshot) -> ServiceSnapshot:
        return ServiceSnapshot(
            unit_file=file(value.unit_file),
            enabled=value.enabled,
            active=value.active,
        )

    return PanelSnapshot(
        layout=PanelLayout(snapshot.layout.value),
        active_release_target=snapshot.active_release_target,
        environment_file=file(snapshot.environment_file),
        version_file=file(snapshot.version_file),
        bridge_service=service(snapshot.bridge_service),
        wifi_watchdog_service=service(snapshot.wifi_watchdog_service),
        bus_watchdog_service=service(snapshot.bus_watchdog_service),
        selected_components=snapshot.selected_components,
        baseline=snapshot.baseline,
    )


def staged_release_from_journal(
    version: str,
    transaction_id: UUID,
    selected_components: tuple[str, ...],
) -> StagedRelease:
    """Reconstruct panel_ops' deterministic staged DTO after an HA restart."""
    return StagedRelease(
        version=version,
        transaction_id=transaction_id,
        release_target=(f"/var/brilliant-mqtt/releases/{version}--{transaction_id.hex}"),
        selected_components=selected_components,
    )


def _panel_data(
    request: PanelInstallRequest,
    identity: HostIdentity,
    transaction_id: UUID,
) -> CanonicalPanelData:
    components = {
        component: component in request.selected_components for component in _CORE_COMPONENTS
    }
    feature_overrides = _copy_json_mapping(request.feature_overrides)
    return CanonicalPanelData(
        MappingProxyType(
            {
                CONF_IDENTITY_FINGERPRINT: identity.fingerprint,
                CONF_SSH_HOST_KEY: identity.public_key,
                CONF_HOST: request.host,
                CONF_SSH_USERNAME: request.ssh_username,
                CONF_ROOT_PASSWORD: request.root_password,
                CONF_NAME: request.display_name,
                CONF_PANEL: request.slug,
                CONF_MANAGEMENT_ID: identity.fingerprint,
                CONF_COMPONENTS: components,
                CONF_FEATURE_OVERRIDES: dict(feature_overrides),
                CONF_MESH_PRIORITY: request.mesh_priority,
                **(
                    {CONF_HOT_POLL_SECONDS: request.hot_poll_seconds}
                    if request.hot_poll_seconds is not None
                    else {}
                ),
                **(
                    {CONF_RESYNC_SECONDS: request.resync_seconds}
                    if request.resync_seconds is not None
                    else {}
                ),
                CONF_PROVISIONING_TRANSACTION_ID: str(transaction_id),
            }
        )
    )


async def _report(
    progress: ProgressReporter,
    stage: ProvisioningProgressStage,
    transaction_id: UUID | None = None,
    setup_id: UUID | None = None,
) -> None:
    async with asyncio.timeout(_PROGRESS_TIMEOUT_SECONDS):
        await progress(
            ProvisioningProgress(
                stage=stage,
                transaction_id=transaction_id,
                setup_id=setup_id,
            )
        )
