"""Thin orchestration boundary for one durable staged panel installation."""

from __future__ import annotations

import asyncio
import math
import re
from collections.abc import Awaitable, Callable, Iterator, Mapping
from dataclasses import dataclass, field
from datetime import UTC, datetime
from enum import StrEnum
from pathlib import Path
from types import MappingProxyType
from typing import Any, Protocol
from uuid import RFC_4122, UUID, uuid4

from homeassistant.const import CONF_NAME
from homeassistant.core import HomeAssistant

from .broker import BrokerProfile
from .broker_validation import BrokerValidationResult
from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
    CONF_FEATURE_OVERRIDES,
    CONF_HOST,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_ROOT_PASSWORD,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    MESH_PANEL,
    PANEL_RELEASES_DIR,
)
from .entry_data import FleetConfig
from .errors import OperationError, OperationStage
from .panel_health import PanelHealthEvidence
from .panel_inspection import PanelFacts
from .panel_ops import (
    MAX_MQTT_CA_BYTES,
    MAX_SNAPSHOT_FILE_BYTES,
    FileSnapshot,
    PanelLayout,
    PanelSnapshot,
    ServiceSnapshot,
)
from .panel_ops import (
    StagedRelease as StagedRelease,
)
from .provisioning_journal import (
    ProvisioningOperation,
    ProvisioningPhase,
    ProvisioningRecord,
    StoredFileSnapshot,
    StoredFleetProfile,
    StoredJournalError,
    StoredPanelLayout,
    StoredPanelRequest,
    StoredPanelSnapshot,
    StoredServiceSnapshot,
)
from .shell import HostIdentity, PanelProcess, PanelShell

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
_ERROR_CODES = frozenset(
    {
        "duplicate_panel",
        "activation_failed",
        "cleanup_failed",
        "commit_failed",
        "health_failed",
        "health_subscribe_failed",
        "identity_failed",
        "inspection_failed",
        "invalid_panel_install_request",
        "invalid_provisioning_dependency",
        "journal_failed",
        "panel_authentication_failed",
        "preflight_failed",
        "progress_failed",
        "recovery_failed",
        "release_prepare_failed",
        "rollback_failed",
        "snapshot_failed",
        "stage_failed",
        "transaction_in_progress",
    }
)
_MAX_JSON_DEPTH = 64
_MAX_JSON_NODES = 4096
_PROGRESS_TIMEOUT_SECONDS = 5.0


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

    __slots__ = ("cleanup_code", "code", "detail")

    def __init__(
        self,
        code: str,
        *,
        detail: ProvisioningFailureDetail | None = None,
        cleanup_code: str | None = None,
    ) -> None:
        if (
            code not in _ERROR_CODES
            or (
                detail is not None
                and (
                    code != "preflight_failed" or not isinstance(detail, ProvisioningFailureDetail)
                )
            )
            or cleanup_code not in {None, "cleanup_failed"}
        ):
            raise ValueError("invalid_panel_provisioning_error_code")
        self.code = code
        self.detail = detail
        self.cleanup_code = cleanup_code
        super().__init__(code)
        self.__suppress_context__ = True

    def __repr__(self) -> str:
        if self.cleanup_code is not None:
            return f"PanelProvisioningError(code={self.code!r}, cleanup_code={self.cleanup_code!r})"
        return f"PanelProvisioningError(code={self.code!r})"


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
    async def snapshot_panel(self, shell: PanelShell) -> PanelSnapshot: ...

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


@dataclass(frozen=True, slots=True, repr=False)
class _Settlement:
    error: BaseException | None
    cancellation: asyncio.CancelledError | None


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
    ) -> ProvisionedPanel:
        """Install and verify one inactive release, retaining the journal."""
        if not isinstance(request, PanelInstallRequest) or not isinstance(fleet, FleetConfig):
            raise PanelProvisioningError("invalid_panel_install_request")
        async with self._lock:
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
                )
            except asyncio.CancelledError as error:
                cancellation = error
            except PanelProvisioningError as error:
                primary = error
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
                compensation = await _settle(self._async_compensate(state, original_code, progress))
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
                    )

            shell_outcome = await _settle_close(
                state.shell.close if state.shell is not None else None
            )
            if primary is None and cancellation is None and shell_outcome.cancellation is not None:
                cancellation = shell_outcome.cancellation
                state.failure_stage = "cleanup"
                original_code = "operation_cancelled"
                if result is None:
                    compensation = _Settlement(
                        error=PanelProvisioningError("rollback_failed"),
                        cancellation=None,
                    )
                else:
                    compensation = await _settle(
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

    async def _async_install_body(
        self,
        request: PanelInstallRequest,
        fleet: FleetConfig,
        progress: ProgressReporter,
        state: _InstallState,
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
            recovery = await _settle(self._async_recover_record(existing, progress))
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

        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(progress, ProvisioningProgressStage.SNAPSHOTTING)
        state.failure_code = "snapshot_failed"
        state.failure_stage = "snapshot"
        state.snapshot = await self._operations.snapshot_panel(state.shell)
        state.stored_snapshot = self._snapshot_to_stored(state.snapshot)
        if not isinstance(state.stored_snapshot, StoredPanelSnapshot):
            raise PanelProvisioningError("snapshot_failed")

        state.transaction_id = self._next_id()
        state.setup_id = self._next_id()
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

        state.failure_code = "progress_failed"
        state.failure_stage = "progress"
        await _report(
            progress,
            ProvisioningProgressStage.STAGING,
            state.transaction_id,
            state.setup_id,
        )
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
        create_outcome = await _settle(self._journal.async_create(record))
        if create_outcome.error is not None:
            if create_outcome.cancellation is not None:
                raise create_outcome.cancellation from None
            raise PanelProvisioningError("journal_failed")
        state.journal_phase = ProvisioningPhase.STAGED
        if create_outcome.cancellation is not None:
            raise create_outcome.cancellation from None

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
            await self._operations.rollback_snapshot(
                state.shell,
                state.snapshot,
                state.staged,
            )
            await self._operations.cleanup_staged(
                state.shell,
                state.staged,
            )
            await self._journal.async_complete_rollback(
                state.transaction_id,
                verified=True,
            )
            state.journal_phase = None
            return

        if state.journal_phase is ProvisioningPhase.STAGED:
            await _settle(
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
            await self._journal.async_transition(
                state.transaction_id,
                ProvisioningPhase.ROLLBACK_PENDING,
                last_error=StoredJournalError(
                    stage=state.failure_stage,
                    code=original_code,
                ),
            )
            state.journal_phase = ProvisioningPhase.ROLLBACK_PENDING
            await self._journal.async_complete_rollback(
                state.transaction_id,
                verified=True,
            )
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
        compensation: _Settlement,
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
        await _settle(
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
            outcome = await _settle(
                self._journal.async_transition(
                    transaction_id,
                    ProvisioningPhase.PENDING_CONFIG_COMMIT,
                )
            )
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
            outcome = await _settle(
                self._journal.async_complete_commit(
                    transaction_id,
                    subentry_id=subentry_id,
                )
            )
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
            outcome = await _settle(self._async_recover_current(progress))
            if outcome.cancellation is not None:
                raise outcome.cancellation from None
            if outcome.error is not None:
                if isinstance(outcome.error, PanelProvisioningError):
                    raise PanelProvisioningError(outcome.error.code)
                raise PanelProvisioningError("recovery_failed")

    async def _async_recover_current(
        self,
        progress: ProgressReporter,
    ) -> None:
        await _report(progress, ProvisioningProgressStage.RECOVERING)
        record = await self._journal.async_load()
        if record is not None:
            await self._async_recover_record(record, progress)

    async def _async_recover_record(
        self,
        record: ProvisioningRecord,
        progress: ProgressReporter,
    ) -> None:
        if not isinstance(record, ProvisioningRecord):
            raise PanelProvisioningError("recovery_failed")
        if record.phase is ProvisioningPhase.ROLLED_BACK:
            await self._journal.async_complete_rollback(
                record.transaction_id,
                verified=True,
            )
            return
        if record.phase is ProvisioningPhase.COMMITTED:
            await self._journal.async_clear_committed(record.transaction_id)
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
                return

        staged = self._staged_release_factory(
            record.staged_version,
            record.transaction_id,
            record.panel_request.selected_components,
        )
        if not _matching_staged(
            staged,
            record.staged_version,
            record.transaction_id,
            record.panel_request.selected_components,
        ):
            raise PanelProvisioningError("recovery_failed")
        snapshot = self._snapshot_from_stored(record.prior_snapshot)
        shell = self._shell_factory(
            record.panel_request.host,
            record.panel_request.root_password,
            record.panel_request.public_key,
        )
        observer: _HealthObserver | None = None
        primary: PanelProvisioningError | None = None
        try:
            if shell.pinned_host_key() != record.panel_request.public_key:
                raise PanelProvisioningError("recovery_failed")
            await shell.connect()
            if record.phase is ProvisioningPhase.STAGED:
                await self._operations.cleanup_staged(shell, staged)
                await self._journal.async_transition(
                    record.transaction_id,
                    ProvisioningPhase.ROLLBACK_PENDING,
                    last_error=StoredJournalError(
                        stage="recovery",
                        code="recovery_interrupted",
                    ),
                )
                await self._journal.async_complete_rollback(
                    record.transaction_id,
                    verified=True,
                )
            elif record.phase in {
                ProvisioningPhase.ACTIVATION_PENDING,
                ProvisioningPhase.ROLLBACK_PENDING,
                ProvisioningPhase.PENDING_CONFIG_COMMIT,
            }:
                await self._async_recovery_rollback(
                    record,
                    shell,
                    snapshot,
                    staged,
                    original_code="recovery_interrupted",
                )
            elif record.phase in {
                ProvisioningPhase.ACTIVATED,
                ProvisioningPhase.VERIFYING,
            }:
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
                    await self._async_recovery_rollback(
                        record,
                        shell,
                        snapshot,
                        staged,
                        original_code="health_failed",
                    )
                else:
                    if record.phase is ProvisioningPhase.ACTIVATED:
                        record = await self._journal.async_transition(
                            record.transaction_id,
                            ProvisioningPhase.VERIFYING,
                        )
                    lookup = await self._transaction_lookup(record.transaction_id)
                    if lookup.state is TransactionLookupState.FLOW_ABORTED_OR_ABSENT:
                        await self._async_recovery_rollback(
                            record,
                            shell,
                            snapshot,
                            staged,
                            original_code="flow_aborted",
                        )
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
        except asyncio.CancelledError:
            raise
        except PanelProvisioningError as error:
            primary = error
        except Exception:
            primary = PanelProvisioningError("recovery_failed")

        observer_outcome = await _settle_close(
            observer.async_close if observer is not None else None
        )
        shell_outcome = await _settle_close(shell.close)
        if primary is not None:
            raise primary from None
        if observer_outcome.cancellation is not None:
            raise observer_outcome.cancellation from None
        if shell_outcome.cancellation is not None:
            raise shell_outcome.cancellation from None
        if observer_outcome.error is not None or shell_outcome.error is not None:
            raise PanelProvisioningError("recovery_failed")

    async def _async_recovery_rollback(
        self,
        record: ProvisioningRecord,
        shell: PanelShell,
        snapshot: PanelSnapshot,
        staged: StagedRelease,
        *,
        original_code: str,
    ) -> None:
        current = record
        if current.phase is not ProvisioningPhase.ROLLBACK_PENDING:
            current = await self._journal.async_transition(
                current.transaction_id,
                ProvisioningPhase.ROLLBACK_PENDING,
                last_error=StoredJournalError(
                    stage="recovery",
                    code=original_code,
                ),
            )
        rollback_failed = False
        try:
            await self._operations.rollback_snapshot(
                shell,
                snapshot,
                staged,
            )
            await self._operations.cleanup_staged(
                shell,
                staged,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            rollback_failed = True
        if rollback_failed:
            await _settle(
                self._repair_reporter.async_report_rollback_failure(
                    current.transaction_id,
                    original_code=original_code,
                    rollback_code="rollback_failed",
                )
            )
            raise PanelProvisioningError("rollback_failed")
        await self._journal.async_complete_rollback(
            current.transaction_id,
            verified=True,
        )

    def _next_id(self) -> UUID:
        candidate = self._id_factory()
        if not _is_uuid4(candidate):
            raise PanelProvisioningError("invalid_provisioning_dependency")
        return candidate


async def _settle(operation: Awaitable[Any]) -> _Settlement:
    """Settle cleanup despite caller cancellation and retain no raw detail."""
    task = asyncio.ensure_future(operation)
    owner = asyncio.current_task()
    observed_cancellations = owner.cancelling() if owner is not None else 0
    cancellation: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            current_cancellations = owner.cancelling() if owner is not None else 0
            if current_cancellations > observed_cancellations:
                cancellation = error
                observed_cancellations = current_cancellations
            if task.done():
                break
        except BaseException:
            break
    failure: BaseException | None = None
    try:
        task.result()
    except BaseException as error:
        failure = error
    return _Settlement(error=failure, cancellation=cancellation)


async def _settle_close(
    operation: Callable[[], Awaitable[Any]] | None,
) -> _Settlement:
    if operation is None:
        return _Settlement(error=None, cancellation=None)
    try:
        return await _settle(operation())
    except asyncio.CancelledError as error:
        return _Settlement(error=None, cancellation=error)
    except BaseException as error:
        return _Settlement(error=error, cancellation=None)


def _required_primary_code(
    primary: PanelProvisioningError | None,
) -> str:
    if primary is None:
        raise RuntimeError("missing_primary_provisioning_error")
    return primary.code


def _is_uuid4(value: object) -> bool:
    return isinstance(value, UUID) and value.version == 4 and value.variant == RFC_4122


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
