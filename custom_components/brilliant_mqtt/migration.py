"""Pure planner that consolidates legacy per-panel entries into one fleet entry.

The planner performs no I/O and never mutates Home Assistant state. It receives
detached :class:`LegacySnapshot` values, compares every fleet-global field across
them (secrets in constant time), converts each panel into its exact fleet subentry
shape, and returns either one complete :class:`MigrationPlan` or a deterministic,
redacted :class:`MigrationConflictReport`. One conflict makes the whole plan
ineligible; nothing is ever silently "fixed".

Identity is preserved byte-for-byte: every migrated panel keeps its slug, its mesh
priority and ``management_id`` equal to its legacy ``entry_id`` (entity unique IDs
are ``f"{management_id}_..."``), so no entity or MQTT identity changes.
"""

from __future__ import annotations

import copy
import hashlib
import hmac
import json
import math
import re
from collections.abc import Iterable, Mapping, Sequence
from dataclasses import dataclass, field
from types import MappingProxyType
from typing import Any

import asyncssh
from homeassistant.config_entries import ConfigEntry, ConfigSubentryDataWithId
from homeassistant.const import CONF_NAME

from .broker import BrokerKind
from .certificates import is_public_certificate_chain
from .const import (
    COMPONENT_BRIDGE,
    COMPONENT_HA_MIRROR,
    CONF_BROKER_KIND,
    CONF_COMPONENTS,
    CONF_ENTRY_KIND,
    CONF_FEATURE_OVERRIDES,
    CONF_HA_CONTROL_DOMAINS,
    CONF_HA_CONTROL_ENABLED,
    CONF_HA_CONTROL_LABEL,
    CONF_HA_MIRROR_LABEL,
    CONF_HA_MIRROR_LEADER_PRIORITY,
    CONF_HA_MIRROR_TOKEN,
    CONF_HA_MIRROR_WS_URL,
    CONF_HOST,
    CONF_HOT_POLL_SECONDS,
    CONF_HUE_CA_CERT,
    CONF_IDENTITY_FINGERPRINT,
    CONF_MANAGEMENT_ID,
    CONF_MAX_MIRRORED_ENTITIES,
    CONF_MESH_PRIORITY,
    CONF_MQTT_HOST,
    CONF_MQTT_PASSWORD,
    CONF_MQTT_PORT,
    CONF_MQTT_TLS_CA,
    CONF_MQTT_TLS_ENABLED,
    CONF_MQTT_USERNAME,
    CONF_NEXT_MESH_PRIORITY,
    CONF_PANEL,
    CONF_PROVISIONING_TRANSACTION_ID,
    CONF_RESYNC_SECONDS,
    CONF_ROOM_OVERRIDES,
    CONF_ROOT_PASSWORD,
    CONF_SCENE_ACTIONS,
    CONF_SCENE_PANEL,
    CONF_SCHEMA_VERSION,
    CONF_SSH_HOST_KEY,
    CONF_SSH_USERNAME,
    CONF_VOICE_ENABLED,
    CONF_VOICE_HA_HOST,
    CONF_VOICE_WAKE_WORD,
    CONFIG_ENTRY_VERSION,
    DATA_HA_MIRROR_RETIRE_VERIFIED,
    DATA_LAST_FIRMWARE,
    DEFAULT_AUTO_REPAIR,
    DEFAULT_HA_CONTROL_DOMAINS,
    DEFAULT_HA_CONTROL_ENABLED,
    DEFAULT_HA_CONTROL_LABEL,
    DEFAULT_MAX_MIRRORED_ENTITIES,
    DEFAULT_OFFLINE_GRACE_MINUTES,
    DEFAULT_REPAIR_COOLDOWN_MINUTES,
    DOMAIN,
    ENTRY_KIND_FLEET,
    ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION,
    FLEET_UNIQUE_ID,
    MESH_PANEL,
    OPT_AUTO_REPAIR,
    OPT_OFFLINE_GRACE_MINUTES,
    OPT_REPAIR_COOLDOWN_MINUTES,
    OPT_TRUST_HOST_KEY_CHANGES,
    SUBENTRY_TYPE_PANEL,
    panel_device_name,
)
from .entry_data import EntryDataError, FleetConfig, PanelConfig
from .shell import HostIdentity, PanelIdentityError, known_hosts_line

FLEET_TITLE = "Brilliant MQTT"
SSH_USERNAME = "root"
PLAN_SCHEMA_VERSION = 1

# Stable, redacted conflict codes. Each one names a field and the affected entries,
# never a stored value (passwords, usernames, CA bodies, host keys).
CONFLICT_NO_LEGACY_ENTRIES = "no_legacy_entries"
CONFLICT_FLEET_ENTRY_PRESENT = "fleet_entry_present"
CONFLICT_UNSUPPORTED_ENTRY_VERSION = "unsupported_entry_version"
CONFLICT_UNSUPPORTED_ENTRY_KIND = "unsupported_entry_kind"
CONFLICT_ENTRY_DISABLED = "entry_disabled"
CONFLICT_INVALID_VALUE = "invalid_value"
CONFLICT_UNSUPPORTED_LEGACY_FIELD = "unsupported_legacy_field"
CONFLICT_GLOBAL_MISMATCH = "global_mismatch"
CONFLICT_MISSING_HOST_KEY = "missing_host_key"
CONFLICT_INVALID_HOST_KEY = "invalid_host_key"
CONFLICT_DUPLICATE_FINGERPRINT = "duplicate_fingerprint"
CONFLICT_INVALID_PANEL_SLUG = "invalid_panel_slug"
CONFLICT_DUPLICATE_SLUG = "duplicate_slug"
CONFLICT_DUPLICATE_MESH_PRIORITY = "duplicate_mesh_priority"
CONFLICT_TRUST_HOST_KEY_CHANGES = "trust_host_key_changes_enabled"
CONFLICT_UNKNOWN_SCENE_PANEL = "unknown_scene_panel"
CONFLICT_HA_MIRROR_RETIREMENT_PENDING = "ha_mirror_retirement_pending"
CONFLICT_PROVISIONING_MARKER = "provisioning_marker_present"
CONFLICT_UNSUPPORTED_SSH_USERNAME = "unsupported_ssh_username"
CONFLICT_INTERNAL_PLAN_INVALID = "internal_plan_invalid"

_NEW_PANEL_SLUG = re.compile(r"^[a-z0-9][a-z0-9_-]{0,63}$")

# Legacy keys the fleet deliberately does not persist (runtime caches and fields
# already folded into newer keys by async_migrate_entry).
_DROPPED_LEGACY_KEYS = frozenset(
    {
        CONF_ENTRY_KIND,
        DATA_LAST_FIRMWARE,
        DATA_HA_MIRROR_RETIRE_VERIFIED,
        CONF_VOICE_ENABLED,
        CONF_HA_MIRROR_LABEL,
    }
)
_BROKER_KEYS = frozenset(
    {
        CONF_BROKER_KIND,
        CONF_MQTT_HOST,
        CONF_MQTT_PORT,
        CONF_MQTT_USERNAME,
        CONF_MQTT_PASSWORD,
        CONF_MQTT_TLS_ENABLED,
        CONF_MQTT_TLS_CA,
    }
)
_GLOBAL_KEYS = frozenset(
    {
        CONF_HA_CONTROL_ENABLED,
        CONF_HA_CONTROL_LABEL,
        CONF_ROOM_OVERRIDES,
        CONF_HA_CONTROL_DOMAINS,
        CONF_MAX_MIRRORED_ENTITIES,
        CONF_SCENE_PANEL,
        CONF_SCENE_ACTIONS,
    }
)
_PANEL_SOURCE_KEYS = frozenset(
    {
        CONF_HOST,
        CONF_ROOT_PASSWORD,
        CONF_PANEL,
        CONF_MESH_PRIORITY,
        CONF_SSH_HOST_KEY,
        CONF_SSH_USERNAME,
        CONF_COMPONENTS,
        CONF_HOT_POLL_SECONDS,
        CONF_RESYNC_SECONDS,
    }
)
_FEATURE_OVERRIDE_KEYS = (CONF_VOICE_WAKE_WORD, CONF_VOICE_HA_HOST, CONF_HUE_CA_CERT)
_HA_MIRROR_CONFIG_KEYS = frozenset(
    {CONF_HA_MIRROR_WS_URL, CONF_HA_MIRROR_TOKEN, CONF_HA_MIRROR_LEADER_PRIORITY}
)
_KNOWN_LEGACY_KEYS = (
    _DROPPED_LEGACY_KEYS
    | _BROKER_KEYS
    | _GLOBAL_KEYS
    | _PANEL_SOURCE_KEYS
    | frozenset(_FEATURE_OVERRIDE_KEYS)
    | _HA_MIRROR_CONFIG_KEYS
    | frozenset({CONF_PROVISIONING_TRANSACTION_ID})
)
_RESILIENCE_OPTIONS: tuple[tuple[str, object, int | None, int | None], ...] = (
    (OPT_AUTO_REPAIR, DEFAULT_AUTO_REPAIR, None, None),
    (OPT_OFFLINE_GRACE_MINUTES, DEFAULT_OFFLINE_GRACE_MINUTES, 2, 120),
    (OPT_REPAIR_COOLDOWN_MINUTES, DEFAULT_REPAIR_COOLDOWN_MINUTES, 5, 1440),
)
_KNOWN_OPTION_KEYS = frozenset(
    {key for key, _default, _minimum, _maximum in _RESILIENCE_OPTIONS}
    | {OPT_TRUST_HOST_KEY_CHANGES}
)
# Fields whose values are credentials. They are compared in constant time, are
# never copied into a conflict/report, and are replaced by presence booleans in the
# public digest so it cannot serve as an offline password oracle.
_SECRET_FLEET_FIELDS = frozenset({CONF_MQTT_USERNAME, CONF_MQTT_PASSWORD, CONF_MQTT_TLS_CA})


def planned_subentry_id(entry_id: str) -> str:
    """Return the deterministic panel subentry ID planned for one legacy entry.

    Deterministic IDs make a dry run show exactly the IDs a real run will create,
    and make a restarted migration converge on the same journaled subentries.
    """
    digest = hashlib.sha256(f"{DOMAIN}:legacy-fleet-migration:v1:{entry_id}".encode())
    return digest.hexdigest()[:32]


@dataclass(frozen=True, slots=True)
class LegacySnapshot:
    """Detached, immutable copy of one Brilliant config entry."""

    entry_id: str
    title: str
    unique_id: str | None
    version: int
    disabled: bool
    data: Mapping[str, Any] = field(repr=False)
    options: Mapping[str, Any] = field(repr=False)
    minor_version: int = 1

    @classmethod
    def from_entry(cls, entry: ConfigEntry[Any]) -> LegacySnapshot:
        """Copy one entry so later mutation cannot change the plan inputs."""
        return cls(
            entry_id=entry.entry_id,
            title=entry.title,
            unique_id=entry.unique_id,
            version=entry.version,
            disabled=entry.disabled_by is not None,
            data=MappingProxyType(copy.deepcopy(dict(entry.data))),
            options=MappingProxyType(copy.deepcopy(dict(entry.options))),
            minor_version=entry.minor_version,
        )


@dataclass(frozen=True, slots=True, order=True)
class MigrationConflict:
    """One redacted reason the automatic plan is ineligible."""

    field: str
    code: str
    entry_ids: tuple[str, ...]
    detail: str | int | None = None


@dataclass(frozen=True, slots=True)
class PlannedPanel:
    """The exact panel subentry one legacy entry becomes."""

    entry_id: str
    subentry_id: str
    title: str
    panel: str
    fingerprint: str
    mesh_priority: int
    data: Mapping[str, Any] = field(repr=False)


@dataclass(frozen=True, slots=True)
class MigrationPlan:
    """One complete, verified-parsable fleet conversion."""

    anchor_entry_id: str
    candidate_ids: tuple[str, ...]
    titles: Mapping[str, str]
    fleet_data: Mapping[str, Any] = field(repr=False)
    fleet_options: Mapping[str, Any] = field(repr=False)
    panels: tuple[PlannedPanel, ...] = field(repr=False)
    plan_digest: str = ""

    @property
    def sibling_ids(self) -> tuple[str, ...]:
        """Return candidates removed after the fleet is verified, in order."""
        return tuple(
            entry_id for entry_id in self.candidate_ids if entry_id != self.anchor_entry_id
        )

    def summary(self) -> dict[str, Any]:
        """Return the public, secret-free description of this plan."""
        by_subentry = {panel.subentry_id: panel.panel for panel in self.panels}
        return {
            "status": "eligible",
            "plan_digest": self.plan_digest,
            "anchor_entry_id": self.anchor_entry_id,
            "anchor_title": self.titles[self.anchor_entry_id],
            "candidate_count": len(self.candidate_ids),
            "fleet": {
                "title": FLEET_TITLE,
                "mqtt_host": self.fleet_data[CONF_MQTT_HOST],
                "mqtt_port": self.fleet_data[CONF_MQTT_PORT],
                "mqtt_tls_enabled": self.fleet_data[CONF_MQTT_TLS_ENABLED],
                "has_custom_ca": CONF_MQTT_TLS_CA in self.fleet_data,
                "has_hue_ca_default": CONF_HUE_CA_CERT in self.fleet_data,
                "ha_control_enabled": self.fleet_data[CONF_HA_CONTROL_ENABLED],
                "scene_panel": by_subentry[str(self.fleet_data[CONF_SCENE_PANEL])],
                "scene_panel_subentry_id": self.fleet_data[CONF_SCENE_PANEL],
                "next_mesh_priority": self.fleet_data[CONF_NEXT_MESH_PRIORITY],
                "options": dict(self.fleet_options),
            },
            "panels": [
                {
                    "entry_id": panel.entry_id,
                    "title": panel.title,
                    "panel": panel.panel,
                    "subentry_id": panel.subentry_id,
                    "management_id": panel.entry_id,
                    "fingerprint": panel.fingerprint,
                    "mesh_priority": panel.mesh_priority,
                    "removed_after_commit": panel.entry_id != self.anchor_entry_id,
                    "feature_overrides": sorted(panel.data[CONF_FEATURE_OVERRIDES]),
                }
                for panel in self.panels
            ],
            "conflicts": [],
        }


@dataclass(frozen=True, slots=True)
class MigrationConflictReport:
    """A deterministic, redacted ineligibility report."""

    conflicts: tuple[MigrationConflict, ...]
    titles: Mapping[str, str]

    def summary(self) -> dict[str, Any]:
        """Return field names, codes, and entry titles only; never stored values."""
        return {
            "status": "conflicts",
            "candidate_count": len(self.titles),
            "conflicts": [
                {
                    "code": conflict.code,
                    "field": conflict.field,
                    "entries": [
                        {"entry_id": entry_id, "title": self.titles.get(entry_id, "")}
                        for entry_id in conflict.entry_ids
                    ],
                    **({"detail": conflict.detail} if conflict.detail is not None else {}),
                }
                for conflict in self.conflicts
            ],
        }


class _Collector:
    """Accumulate conflicts without exposing any persisted value."""

    def __init__(self) -> None:
        self._conflicts: set[MigrationConflict] = set()

    def add(
        self,
        code: str,
        field_name: str,
        entry_ids: Iterable[str],
        detail: str | int | None = None,
    ) -> None:
        self._conflicts.add(
            MigrationConflict(
                field=field_name,
                code=code,
                entry_ids=tuple(sorted(set(entry_ids))),
                detail=detail,
            )
        )

    def __bool__(self) -> bool:
        return bool(self._conflicts)

    def sorted(self) -> tuple[MigrationConflict, ...]:
        return tuple(sorted(self._conflicts, key=_conflict_sort_key))


def _conflict_sort_key(conflict: MigrationConflict) -> tuple[str, str, tuple[str, ...], str]:
    return (conflict.field, conflict.code, conflict.entry_ids, str(conflict.detail))


def _json_copy(value: Any, *, depth: int = 0) -> Any:
    """Detach one JSON-compatible value or raise ValueError."""
    if depth > 64:
        raise ValueError("too_deep")
    if value is None or isinstance(value, (str, bool)):
        return value
    if type(value) is int:
        return value
    if type(value) is float:
        if not math.isfinite(value):
            raise ValueError("not_finite")
        return value
    if isinstance(value, (list, tuple)):
        return [_json_copy(item, depth=depth + 1) for item in value]
    if isinstance(value, Mapping):
        copied: dict[str, Any] = {}
        for key, item in value.items():
            if not isinstance(key, str):
                raise ValueError("non_string_key")
            copied[key] = _json_copy(item, depth=depth + 1)
        return copied
    raise ValueError("not_json")


def _canonical_json(value: Any) -> str:
    return json.dumps(value, sort_keys=True, separators=(",", ":"), ensure_ascii=False)


def _normalize_pem(value: str) -> str:
    """Normalize CA line endings to LF with exactly one trailing newline."""
    return value.replace("\r\n", "\n").replace("\r", "\n").strip() + "\n"


def secret_equal(left: object, right: object) -> bool:
    """Compare two optional credential strings in constant time."""
    if left is None or right is None:
        return left is right
    if not isinstance(left, str) or not isinstance(right, str):
        return False
    return hmac.compare_digest(
        left.encode("utf-8", errors="surrogatepass"),
        right.encode("utf-8", errors="surrogatepass"),
    )


@dataclass(slots=True)
class _Global:
    """One normalized fleet-global value from one entry."""

    entry_id: str
    value: Any
    secret: bool


def _group(values: Sequence[_Global]) -> list[list[_Global]]:
    """Group equal values, comparing secrets in constant time."""
    groups: list[list[_Global]] = []
    for item in values:
        for group in groups:
            reference = group[0]
            equal = (
                secret_equal(reference.value, item.value)
                if item.secret
                else reference.value == item.value
            )
            if equal:
                group.append(item)
                break
        else:
            groups.append([item])
    return groups


def _consensus(values: Sequence[_Global]) -> tuple[Any, tuple[str, ...]]:
    """Return the majority value and the entries that deviate from it.

    Ties resolve to the value held by the lexicographically smallest entry ID, so
    the result is independent of input order.
    """
    groups = _group(sorted(values, key=lambda item: item.entry_id))
    # Groups are built in entry-ID order and max() keeps the first maximal group.
    best = max(groups, key=len)
    deviants = tuple(item.entry_id for group in groups if group is not best for item in group)
    return best[0].value, deviants


def _resilience_value(
    options: Mapping[str, Any],
    key: str,
    default: object,
    minimum: int | None,
    maximum: int | None,
) -> object | None:
    """Return one validated effective option value, or None when invalid."""
    value = options.get(key, default)
    if minimum is None:
        return value if type(value) is bool else None
    if type(value) is not int or maximum is None or not minimum <= value <= maximum:
        return None
    return value


def _bounded_cadence(data: Mapping[str, Any], key: str, minimum: int, maximum: int) -> bool:
    if key not in data:
        return True
    value = data[key]
    return type(value) is int and minimum <= value <= maximum


@dataclass(slots=True)
class _PanelDraft:
    entry_id: str
    title: str
    panel: str
    fingerprint: str
    data: dict[str, Any]
    mesh_priority: int
    effective_options: dict[str, object]


def _host_identity(
    collector: _Collector,
    snapshot: LegacySnapshot,
    host: object,
) -> HostIdentity | None:
    """Canonicalize and fingerprint the TOFU pin exactly like fleet onboarding."""
    raw_key = snapshot.data.get(CONF_SSH_HOST_KEY)
    if raw_key is None or (isinstance(raw_key, str) and not raw_key.strip()):
        collector.add(CONFLICT_MISSING_HOST_KEY, CONF_SSH_HOST_KEY, [snapshot.entry_id])
        return None
    if not isinstance(raw_key, str):
        collector.add(CONFLICT_INVALID_HOST_KEY, CONF_SSH_HOST_KEY, [snapshot.entry_id])
        return None
    if isinstance(host, str) and host:
        try:
            known_hosts_line(host, raw_key)
        except PanelIdentityError as error:
            if error.code == "invalid_host":
                collector.add(CONFLICT_INVALID_VALUE, CONF_HOST, [snapshot.entry_id])
            else:
                collector.add(CONFLICT_INVALID_HOST_KEY, CONF_SSH_HOST_KEY, [snapshot.entry_id])
                return None
    try:
        canonical = raw_key.strip()
        fingerprint = asyncssh.import_public_key(canonical).get_fingerprint("sha256")
        return HostIdentity(public_key=canonical, fingerprint=fingerprint)
    except (PanelIdentityError, asyncssh.KeyImportError, ValueError, TypeError):
        collector.add(CONFLICT_INVALID_HOST_KEY, CONF_SSH_HOST_KEY, [snapshot.entry_id])
        return None


def _draft_panel(collector: _Collector, snapshot: LegacySnapshot) -> _PanelDraft | None:
    """Convert one legacy entry's panel-owned fields, reporting every defect."""
    data = snapshot.data
    entry_id = snapshot.entry_id
    valid = True

    for key in sorted(set(data) - _KNOWN_LEGACY_KEYS):
        collector.add(CONFLICT_UNSUPPORTED_LEGACY_FIELD, key, [entry_id])
        valid = False
    for key in sorted(set(snapshot.options) - _KNOWN_OPTION_KEYS):
        collector.add(CONFLICT_UNSUPPORTED_LEGACY_FIELD, f"options.{key}", [entry_id])
        valid = False
    if CONF_PROVISIONING_TRANSACTION_ID in data:
        collector.add(CONFLICT_PROVISIONING_MARKER, CONF_PROVISIONING_TRANSACTION_ID, [entry_id])
        valid = False

    panel = data.get(CONF_PANEL)
    if not isinstance(panel, str) or not panel:
        collector.add(CONFLICT_INVALID_VALUE, CONF_PANEL, [entry_id])
        panel = None
        valid = False
    elif panel == MESH_PANEL or _NEW_PANEL_SLUG.fullmatch(panel) is None:
        collector.add(CONFLICT_INVALID_PANEL_SLUG, CONF_PANEL, [entry_id])
        valid = False

    host = data.get(CONF_HOST)
    if not isinstance(host, str) or not host:
        collector.add(CONFLICT_INVALID_VALUE, CONF_HOST, [entry_id])
        valid = False
    root_password = data.get(CONF_ROOT_PASSWORD)
    if not isinstance(root_password, str) or not root_password:
        collector.add(CONFLICT_INVALID_VALUE, CONF_ROOT_PASSWORD, [entry_id])
        valid = False
    if data.get(CONF_SSH_USERNAME, SSH_USERNAME) != SSH_USERNAME:
        collector.add(CONFLICT_UNSUPPORTED_SSH_USERNAME, CONF_SSH_USERNAME, [entry_id])
        valid = False

    mesh_priority = data.get(CONF_MESH_PRIORITY, 0)
    if type(mesh_priority) is not int or mesh_priority < 0:
        collector.add(CONFLICT_INVALID_VALUE, CONF_MESH_PRIORITY, [entry_id])
        mesh_priority = 0
        valid = False

    raw_components = data.get(CONF_COMPONENTS, {})
    components: dict[str, bool] = {}
    if not isinstance(raw_components, Mapping) or any(
        not isinstance(key, str) or not key or type(value) is not bool
        for key, value in raw_components.items()
    ):
        collector.add(CONFLICT_INVALID_VALUE, CONF_COMPONENTS, [entry_id])
        valid = False
    else:
        components = dict(sorted(raw_components.items()))
        if components.get(COMPONENT_HA_MIRROR) is True:
            collector.add(CONFLICT_HA_MIRROR_RETIREMENT_PENDING, CONF_COMPONENTS, [entry_id])
            valid = False
    for key in sorted(_HA_MIRROR_CONFIG_KEYS & set(data)):
        collector.add(CONFLICT_HA_MIRROR_RETIREMENT_PENDING, key, [entry_id])
        valid = False
    components[COMPONENT_BRIDGE] = True
    components[COMPONENT_HA_MIRROR] = False

    for key, minimum, maximum in (
        (CONF_HOT_POLL_SECONDS, 0, 60),
        (CONF_RESYNC_SECONDS, 60, 86400),
    ):
        if not _bounded_cadence(data, key, minimum, maximum):
            collector.add(CONFLICT_INVALID_VALUE, key, [entry_id])
            valid = False

    feature_overrides: dict[str, Any] = {}
    for key in _FEATURE_OVERRIDE_KEYS:
        if key not in data:
            continue
        value = data[key]
        if not isinstance(value, str):
            collector.add(CONFLICT_INVALID_VALUE, key, [entry_id])
            valid = False
            continue
        feature_overrides[key] = value

    effective_options: dict[str, object] = {}
    for option, default, lower, upper in _RESILIENCE_OPTIONS:
        effective = _resilience_value(snapshot.options, option, default, lower, upper)
        if effective is None:
            collector.add(CONFLICT_INVALID_VALUE, f"options.{option}", [entry_id])
            valid = False
        else:
            effective_options[option] = effective
    trust = snapshot.options.get(OPT_TRUST_HOST_KEY_CHANGES, False)
    if type(trust) is not bool:
        collector.add(CONFLICT_INVALID_VALUE, f"options.{OPT_TRUST_HOST_KEY_CHANGES}", [entry_id])
        valid = False
    elif trust:
        collector.add(
            CONFLICT_TRUST_HOST_KEY_CHANGES,
            f"options.{OPT_TRUST_HOST_KEY_CHANGES}",
            [entry_id],
        )
        valid = False

    identity = _host_identity(collector, snapshot, host)
    if identity is None or not valid or panel is None:
        return None
    assert isinstance(host, str)
    assert isinstance(root_password, str)

    name = snapshot.title.strip() or panel_device_name(panel)
    panel_data: dict[str, Any] = {
        CONF_IDENTITY_FINGERPRINT: identity.fingerprint,
        CONF_SSH_HOST_KEY: identity.public_key,
        CONF_HOST: host,
        CONF_SSH_USERNAME: SSH_USERNAME,
        CONF_ROOT_PASSWORD: root_password,
        CONF_NAME: name,
        CONF_PANEL: panel,
        CONF_MANAGEMENT_ID: entry_id,
        CONF_COMPONENTS: components,
        CONF_FEATURE_OVERRIDES: feature_overrides,
        CONF_MESH_PRIORITY: mesh_priority,
    }
    for key in (CONF_HOT_POLL_SECONDS, CONF_RESYNC_SECONDS):
        if key in data:
            panel_data[key] = data[key]
    return _PanelDraft(
        entry_id=entry_id,
        title=name,
        panel=panel,
        fingerprint=identity.fingerprint,
        data=panel_data,
        mesh_priority=mesh_priority,
        effective_options=effective_options,
    )


def _broker_globals(
    collector: _Collector,
    snapshots: Sequence[LegacySnapshot],
) -> dict[str, list[_Global]]:
    """Normalize every entry's broker profile into comparable per-field values."""
    fields: dict[str, list[_Global]] = {key: [] for key in sorted(_BROKER_KEYS)}
    for snapshot in snapshots:
        data = snapshot.data
        entry_id = snapshot.entry_id
        kind = data.get(CONF_BROKER_KIND, BrokerKind.EXISTING_BROKER.value)
        if kind not in {member.value for member in BrokerKind}:
            collector.add(CONFLICT_INVALID_VALUE, CONF_BROKER_KIND, [entry_id])
            continue
        host = data.get(CONF_MQTT_HOST)
        tls_enabled = data.get(CONF_MQTT_TLS_ENABLED, False)
        username = data.get(CONF_MQTT_USERNAME)
        password = data.get(CONF_MQTT_PASSWORD)
        raw_ca = data.get(CONF_MQTT_TLS_CA)
        invalid: list[str] = []
        if not isinstance(host, str) or not host.strip():
            invalid.append(CONF_MQTT_HOST)
        if type(tls_enabled) is not bool:
            invalid.append(CONF_MQTT_TLS_ENABLED)
            tls_enabled = False
        port = data.get(CONF_MQTT_PORT, 8883 if tls_enabled else 1883)
        if type(port) is not int or not 1 <= port <= 65535:
            invalid.append(CONF_MQTT_PORT)
        if not isinstance(username, str) or not username:
            invalid.append(CONF_MQTT_USERNAME)
        if not isinstance(password, str) or not password:
            invalid.append(CONF_MQTT_PASSWORD)
        ca: str | None = None
        if raw_ca is not None:
            if (
                not isinstance(raw_ca, str)
                or not raw_ca.strip()
                or not tls_enabled
                or not is_public_certificate_chain(_normalize_pem(raw_ca))
            ):
                invalid.append(CONF_MQTT_TLS_CA)
            else:
                ca = _normalize_pem(raw_ca)
        for key in invalid:
            collector.add(CONFLICT_INVALID_VALUE, key, [entry_id])
        if invalid:
            continue
        assert isinstance(host, str)
        normalized: dict[str, Any] = {
            CONF_BROKER_KIND: kind,
            CONF_MQTT_HOST: host.strip().lower(),
            CONF_MQTT_PORT: port,
            CONF_MQTT_USERNAME: username,
            CONF_MQTT_PASSWORD: password,
            CONF_MQTT_TLS_ENABLED: tls_enabled,
            CONF_MQTT_TLS_CA: ca,
        }
        for key, value in normalized.items():
            fields[key].append(_Global(entry_id, value, key in _SECRET_FLEET_FIELDS))
    return fields


def _control_globals(
    collector: _Collector,
    snapshots: Sequence[LegacySnapshot],
) -> dict[str, list[_Global]]:
    """Normalize the HA-control/scene globals with the legacy runtime's defaults."""
    fields: dict[str, list[_Global]] = {key: [] for key in sorted(_GLOBAL_KEYS)}
    for snapshot in snapshots:
        data = snapshot.data
        entry_id = snapshot.entry_id
        values: dict[str, Any] = {}

        enabled = data.get(CONF_HA_CONTROL_ENABLED, DEFAULT_HA_CONTROL_ENABLED)
        if type(enabled) is bool:
            values[CONF_HA_CONTROL_ENABLED] = enabled
        label = data.get(CONF_HA_CONTROL_LABEL, DEFAULT_HA_CONTROL_LABEL)
        if isinstance(label, str) and label:
            values[CONF_HA_CONTROL_LABEL] = label
        domains = data.get(CONF_HA_CONTROL_DOMAINS, DEFAULT_HA_CONTROL_DOMAINS)
        if isinstance(domains, (list, tuple)) and all(
            isinstance(domain, str) and domain for domain in domains
        ):
            values[CONF_HA_CONTROL_DOMAINS] = tuple(sorted(set(domains)))
        maximum = data.get(CONF_MAX_MIRRORED_ENTITIES, DEFAULT_MAX_MIRRORED_ENTITIES)
        if type(maximum) is int and maximum >= 1:
            values[CONF_MAX_MIRRORED_ENTITIES] = maximum
        scene_panel = data.get(CONF_SCENE_PANEL, data.get(CONF_PANEL))
        if isinstance(scene_panel, str) and scene_panel:
            values[CONF_SCENE_PANEL] = scene_panel
        for key in (CONF_ROOM_OVERRIDES, CONF_SCENE_ACTIONS):
            raw = data.get(key, {})
            try:
                copied = _json_copy(raw)
            except ValueError:
                continue
            if isinstance(copied, dict):
                values[key] = _canonical_json(copied)

        for key in sorted(_GLOBAL_KEYS):
            if key not in values:
                collector.add(CONFLICT_INVALID_VALUE, key, [entry_id])
                continue
            fields[key].append(_Global(entry_id, values[key], secret=False))
    return fields


def _choose(
    collector: _Collector,
    fields: Mapping[str, Sequence[_Global]],
) -> dict[str, Any]:
    """Require one value per global field; report deviants without values."""
    chosen: dict[str, Any] = {}
    for key in sorted(fields):
        values = fields[key]
        if not values:
            continue
        value, deviants = _consensus(values)
        if deviants:
            collector.add(CONFLICT_GLOBAL_MISMATCH, key, deviants)
        chosen[key] = value
    return chosen


def _unanimous_hue_ca(drafts: Sequence[_PanelDraft]) -> str | None:
    """Return the diyHue CA every panel shares when it is one valid public chain.

    The fleet default is what panels added later inherit, so it is promoted only
    when unanimous: a panel with no CA or a different one means there is no single
    fleet CA, and picking among differing values would be arbitrary. Per-panel
    overrides are kept either way, so no migrated panel's effective CA changes.
    """
    values = {draft.data[CONF_FEATURE_OVERRIDES].get(CONF_HUE_CA_CERT) for draft in drafts}
    if len(values) != 1:
        return None
    (value,) = values
    # Validate the exact stored text: the strict fleet entry parser checks the same.
    return value if isinstance(value, str) and is_public_certificate_chain(value) else None


def _next_mesh_priority(priorities: Iterable[int]) -> int:
    """Match FleetManager's reconciled allocator: the smallest unused positive."""
    used = {priority for priority in priorities if priority > 0}
    candidate = 1
    while candidate in used:
        candidate += 1
    return candidate


def _redacted(value: Mapping[str, Any], secrets: frozenset[str]) -> dict[str, Any]:
    return {
        key: (item is not None and item != "") if key in secrets else item
        for key, item in sorted(value.items())
    }


def _plan_digest(
    anchor: str,
    candidates: Sequence[str],
    fleet_data: Mapping[str, Any],
    fleet_options: Mapping[str, Any],
    panels: Sequence[PlannedPanel],
) -> str:
    """Hash only the canonical non-secret structure of a plan."""
    structure = {
        "schema": PLAN_SCHEMA_VERSION,
        "anchor": anchor,
        "candidates": list(candidates),
        "fleet_data": _redacted(fleet_data, _SECRET_FLEET_FIELDS),
        "fleet_options": dict(sorted(fleet_options.items())),
        "panels": [
            {
                "entry_id": panel.entry_id,
                "subentry_id": panel.subentry_id,
                "title": panel.title,
                "data": _redacted(
                    {key: item for key, item in panel.data.items() if key != CONF_SSH_HOST_KEY},
                    frozenset({CONF_ROOT_PASSWORD}),
                ),
            }
            for panel in panels
        ],
    }
    return hashlib.sha256(_canonical_json(structure).encode("utf-8")).hexdigest()


def _self_check(
    fleet_data: Mapping[str, Any],
    fleet_options: Mapping[str, Any],
    panels: Sequence[PlannedPanel],
) -> bool:
    """Prove the produced shape parses through the fleet runtime's own contracts."""
    subentries: list[ConfigSubentryDataWithId] = [
        {
            "data": dict(panel.data),
            "subentry_id": panel.subentry_id,
            "subentry_type": SUBENTRY_TYPE_PANEL,
            "title": panel.title,
            "unique_id": panel.fingerprint,
        }
        for panel in panels
    ]
    try:
        entry: ConfigEntry[Any] = ConfigEntry(
            data=dict(fleet_data),
            discovery_keys=MappingProxyType({}),
            domain=DOMAIN,
            minor_version=1,
            options=dict(fleet_options),
            source="user",
            subentries_data=subentries,
            title=FLEET_TITLE,
            unique_id=FLEET_UNIQUE_ID,
            version=CONFIG_ENTRY_VERSION,
        )
        fleet = FleetConfig.from_entry(entry)
        parsed = [PanelConfig.from_subentry(subentry) for subentry in entry.subentries.values()]
    except (EntryDataError, ValueError):
        return False
    return fleet.scene_panel in entry.subentries and len(parsed) == len(panels)


def plan_migration(
    snapshots: Sequence[LegacySnapshot],
) -> MigrationPlan | MigrationConflictReport:
    """Plan one lossless legacy-to-fleet conversion, or explain why it cannot run."""
    ordered = tuple(sorted(snapshots, key=lambda snapshot: snapshot.entry_id))
    titles = MappingProxyType({snapshot.entry_id: snapshot.title for snapshot in ordered})
    collector = _Collector()
    if not ordered:
        collector.add(CONFLICT_NO_LEGACY_ENTRIES, CONF_ENTRY_KIND, [])
        return MigrationConflictReport(collector.sorted(), titles)

    legacy: list[LegacySnapshot] = []
    for snapshot in ordered:
        kind = snapshot.data.get(CONF_ENTRY_KIND)
        if kind == ENTRY_KIND_FLEET:
            collector.add(CONFLICT_FLEET_ENTRY_PRESENT, CONF_ENTRY_KIND, [snapshot.entry_id])
            continue
        if snapshot.version != CONFIG_ENTRY_VERSION:
            collector.add(CONFLICT_UNSUPPORTED_ENTRY_VERSION, "version", [snapshot.entry_id])
            continue
        if kind != ENTRY_KIND_LEGACY_PENDING_CONSOLIDATION:
            collector.add(CONFLICT_UNSUPPORTED_ENTRY_KIND, CONF_ENTRY_KIND, [snapshot.entry_id])
            continue
        if snapshot.disabled:
            collector.add(CONFLICT_ENTRY_DISABLED, "disabled_by", [snapshot.entry_id])
        legacy.append(snapshot)

    drafts = [draft for snapshot in legacy if (draft := _draft_panel(collector, snapshot))]
    broker = _choose(collector, _broker_globals(collector, legacy))
    control = _choose(collector, _control_globals(collector, legacy))

    for attribute, code in (
        ("panel", CONFLICT_DUPLICATE_SLUG),
        ("fingerprint", CONFLICT_DUPLICATE_FINGERPRINT),
    ):
        owners: dict[str, list[str]] = {}
        for draft in drafts:
            owners.setdefault(str(getattr(draft, attribute)), []).append(draft.entry_id)
        for value, entry_ids in sorted(owners.items()):
            if len(entry_ids) > 1:
                field_name = CONF_PANEL if attribute == "panel" else CONF_SSH_HOST_KEY
                detail = value if attribute == "panel" else None
                collector.add(code, field_name, entry_ids, detail)
    mesh_owners: dict[int, list[str]] = {}
    for draft in drafts:
        if draft.mesh_priority > 0:
            mesh_owners.setdefault(draft.mesh_priority, []).append(draft.entry_id)
    for priority, entry_ids in sorted(mesh_owners.items()):
        if len(entry_ids) > 1:
            collector.add(CONFLICT_DUPLICATE_MESH_PRIORITY, CONF_MESH_PRIORITY, entry_ids, priority)

    subentry_by_slug = {draft.panel: planned_subentry_id(draft.entry_id) for draft in drafts}
    scene_slug = control.get(CONF_SCENE_PANEL)
    if (
        isinstance(scene_slug, str)
        and len(drafts) == len(legacy)
        and scene_slug not in subentry_by_slug
    ):
        collector.add(
            CONFLICT_UNKNOWN_SCENE_PANEL,
            CONF_SCENE_PANEL,
            [snapshot.entry_id for snapshot in legacy],
        )

    if collector or not legacy or len(drafts) != len(legacy):
        return MigrationConflictReport(collector.sorted(), titles)

    fleet_options: dict[str, Any] = {}
    for key, _default, _minimum, _maximum in _RESILIENCE_OPTIONS:
        value, _deviants = _consensus(
            [_Global(draft.entry_id, draft.effective_options[key], False) for draft in drafts]
        )
        fleet_options[key] = value

    panels: list[PlannedPanel] = []
    for draft in drafts:
        overrides = dict(draft.data[CONF_FEATURE_OVERRIDES])
        for option, effective in draft.effective_options.items():
            if effective != fleet_options[option]:
                overrides[option] = effective
        data = {**draft.data, CONF_FEATURE_OVERRIDES: dict(sorted(overrides.items()))}
        panels.append(
            PlannedPanel(
                entry_id=draft.entry_id,
                subentry_id=planned_subentry_id(draft.entry_id),
                title=draft.title,
                panel=draft.panel,
                fingerprint=draft.fingerprint,
                mesh_priority=draft.mesh_priority,
                data=MappingProxyType(data),
            )
        )

    fleet_data: dict[str, Any] = {
        CONF_ENTRY_KIND: ENTRY_KIND_FLEET,
        CONF_BROKER_KIND: broker[CONF_BROKER_KIND],
        CONF_MQTT_HOST: broker[CONF_MQTT_HOST],
        CONF_MQTT_PORT: broker[CONF_MQTT_PORT],
        CONF_MQTT_USERNAME: broker[CONF_MQTT_USERNAME],
        CONF_MQTT_PASSWORD: broker[CONF_MQTT_PASSWORD],
        CONF_MQTT_TLS_ENABLED: broker[CONF_MQTT_TLS_ENABLED],
        CONF_NEXT_MESH_PRIORITY: _next_mesh_priority(draft.mesh_priority for draft in drafts),
        CONF_HA_CONTROL_ENABLED: control[CONF_HA_CONTROL_ENABLED],
        CONF_HA_CONTROL_LABEL: control[CONF_HA_CONTROL_LABEL],
        CONF_ROOM_OVERRIDES: json.loads(control[CONF_ROOM_OVERRIDES]),
        CONF_HA_CONTROL_DOMAINS: list(control[CONF_HA_CONTROL_DOMAINS]),
        CONF_MAX_MIRRORED_ENTITIES: control[CONF_MAX_MIRRORED_ENTITIES],
        CONF_SCENE_PANEL: subentry_by_slug[str(scene_slug)],
        CONF_SCENE_ACTIONS: json.loads(control[CONF_SCENE_ACTIONS]),
        CONF_SCHEMA_VERSION: CONFIG_ENTRY_VERSION,
    }
    if broker[CONF_MQTT_TLS_CA] is not None:
        fleet_data[CONF_MQTT_TLS_CA] = broker[CONF_MQTT_TLS_CA]
    if (hue_ca := _unanimous_hue_ca(drafts)) is not None:
        fleet_data[CONF_HUE_CA_CERT] = hue_ca

    candidate_ids = tuple(snapshot.entry_id for snapshot in legacy)
    anchor = candidate_ids[0]
    if not _self_check(fleet_data, fleet_options, panels):
        collector.add(CONFLICT_INTERNAL_PLAN_INVALID, CONF_ENTRY_KIND, candidate_ids)
        return MigrationConflictReport(collector.sorted(), titles)
    return MigrationPlan(
        anchor_entry_id=anchor,
        candidate_ids=candidate_ids,
        titles=titles,
        fleet_data=MappingProxyType(fleet_data),
        fleet_options=MappingProxyType(fleet_options),
        panels=tuple(panels),
        plan_digest=_plan_digest(anchor, candidate_ids, fleet_data, fleet_options, panels),
    )
