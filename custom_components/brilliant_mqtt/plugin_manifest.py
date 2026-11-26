"""Strict, pure plugin metadata validation. This module cannot activate a plugin."""

from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from pathlib import PurePosixPath
from typing import NoReturn
from urllib.parse import urlsplit

MAX_DOCUMENT_BYTES = 64 * 1024
MAX_ARTIFACT_BYTES = 64 * 1024 * 1024
MAX_UNPACKED_BYTES = 256 * 1024 * 1024
MAX_FILES = 4096
_ID = re.compile(r"[a-z][a-z0-9_]{0,63}")
_VERSION = re.compile(r"(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)")
_DIGEST = re.compile(r"[0-9a-f]{64}")
_UNIT = re.compile(r"[a-zA-Z0-9_@.-]+\.(?:service|timer|socket|target|slice)")
_PROTECTED_STEMS = frozenset(
    {
        "message_bus",
        "emperor",
        "zygote",
        "fork-server",
        "bootstrap",
        "monitor",
        "wifi",
        "hardware",
        "control_notification",
        "config_peripherals",
        "object_store_peripheral",
        "discovery_peripheral",
        "execution",
        "brilliant-mqtt",
        "brilliant-bus-watchdog",
        "brilliant-wifi-watchdog",
        "brilliant-plugin-helper",
        "brilliant-plugin-rescue",
        "bridge",
        "bus_watchdog",
        "wifi_watchdog",
    }
)
_PROTECTED_PATHS = (
    "/data/switch-embedded",
    "/var/run/brilliant",
    "/var/run/brilliant-mqtt",
    "/run/brilliant",
    "/run/brilliant-mqtt",
    "/var/brilliant-mqtt",
    "/var/lib/brilliant-plugin-recovery",
    "/etc/brilliant-mqtt.env",
)
_UNIT_SEARCH_DIRECTORIES = (
    "/etc/systemd/system.control",
    "/run/systemd/system.control",
    "/run/systemd/transient",
    "/run/systemd/generator.early",
    "/etc/systemd/system",
    "/etc/systemd/system.attached",
    "/run/systemd/system",
    "/run/systemd/system.attached",
    "/run/systemd/generator",
    "/usr/local/lib/systemd/system",
    "/usr/lib/systemd/system",
    "/lib/systemd/system",
    "/run/systemd/generator.late",
)
_SHARED_DROPIN_DIRECTORIES = frozenset(
    f"{unit_type}.d"
    for unit_type in (
        "service",
        "socket",
        "device",
        "mount",
        "automount",
        "swap",
        "target",
        "path",
        "timer",
        "slice",
        "scope",
    )
)


class PluginValidationError(ValueError):
    """Metadata is unsupported or disagrees with reviewed integration policy."""


@dataclass(frozen=True)
class Replacement:
    target: str
    kind: str
    identity: str
    firmware_adapter: str


@dataclass(frozen=True)
class UnitTopology:
    """Trusted firmware-profile evidence; never supplied by a manifest/catalog."""

    identity: str
    aliases: tuple[str, ...] = ()
    dependencies: tuple[str, ...] = ()
    paths: tuple[str, ...] = ()


@dataclass(frozen=True)
class ValidationPolicy:
    """Compiled-in reviewed policy; all qualification allowlists start empty."""

    adapters: frozenset[str] = frozenset()
    resources_profiles: frozenset[str] = frozenset()
    firmware_profiles: frozenset[str] = frozenset()
    health_profiles: frozenset[str] = frozenset()
    origins: frozenset[str] = frozenset()
    replacement_allowlist: frozenset[Replacement] = frozenset()
    topology: tuple[UnitTopology, ...] = ()
    qualified_firmware_profile: str | None = None


@dataclass(frozen=True)
class Artifact:
    url: str
    sha256: str
    size_bytes: int
    unpacked_bytes_max: int
    files_max: int


DEFAULT_POLICY = ValidationPolicy()


@dataclass(frozen=True)
class PluginManifest:
    id: str
    version: str
    publisher: str
    artifact: Artifact
    replaces: tuple[Replacement, ...]
    manifest_sha256: str
    raw: bytes
    firmware_profiles: tuple[str, ...] = ()


def _fail() -> NoReturn:
    # Errors contain no submitted metadata or provider URLs.
    raise PluginValidationError("invalid_plugin_metadata")


def _pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
    result: dict[str, object] = {}
    for key, value in pairs:
        if key in result:
            _fail()
        result[key] = value
    return result


def _constant(_value: str) -> object:
    _fail()
    return None


def parse_document(raw: bytes) -> dict[str, object]:
    """Bounded strict UTF-8 JSON, including nested duplicate-key refusal."""
    if type(raw) is not bytes or not raw or len(raw) > MAX_DOCUMENT_BYTES:
        _fail()
    try:
        document: object = json.loads(
            raw.decode("utf-8"), object_pairs_hook=_pairs, parse_constant=_constant
        )
    except (UnicodeError, ValueError, RecursionError):
        raise PluginValidationError("invalid_plugin_metadata") from None
    _check_depth(document, 0)
    return object_fields(document, None)


def _check_depth(value: object, depth: int) -> None:
    if depth > 16:
        _fail()
    if isinstance(value, dict):
        for child in value.values():
            _check_depth(child, depth + 1)
    elif isinstance(value, list):
        for child in value:
            _check_depth(child, depth + 1)


def object_fields(value: object, keys: set[str] | None) -> dict[str, object]:
    if not isinstance(value, dict) or any(type(key) is not str for key in value):
        raise PluginValidationError("invalid_plugin_metadata")
    result = {str(key): item for key, item in value.items()}
    if keys is not None and result.keys() != keys:
        _fail()
    return result


def text(value: object) -> str:
    if not isinstance(value, str) or not value or len(value) > 2048:
        raise PluginValidationError("invalid_plugin_metadata")
    if any(ord(char) < 32 or ord(char) > 126 for char in value):
        _fail()
    return value


def identifier(value: object) -> str:
    result = text(value)
    if _ID.fullmatch(result) is None:
        _fail()
    return result


def version(value: object) -> str:
    result = text(value)
    if _VERSION.fullmatch(result) is None:
        _fail()
    return result


def digest(value: object) -> str:
    result = text(value)
    if _DIGEST.fullmatch(result) is None:
        _fail()
    return result


def integer(value: object, low: int, high: int) -> int:
    if type(value) is not int or not low <= value <= high:
        raise PluginValidationError("invalid_plugin_metadata")
    return value


def sequence(value: object) -> list[object]:
    if not isinstance(value, list) or len(value) > 128:
        raise PluginValidationError("invalid_plugin_metadata")
    return value


def strings(value: object, allowed: frozenset[str] | None = None) -> tuple[str, ...]:
    result = tuple(text(item) for item in sequence(value))
    if len(set(result)) != len(result) or (allowed is not None and not set(result) <= allowed):
        _fail()
    return result


def _https_origin(value: str, *, origin_only: bool = False) -> tuple[str, str, int]:
    try:
        parsed = urlsplit(value)
        port = parsed.port if parsed.port is not None else 443
        if (
            parsed.scheme != "https"
            or not parsed.hostname
            or port == 0
            or parsed.username is not None
            or parsed.password is not None
            or parsed.fragment
            or "\\" in value
            or any(char.isspace() for char in value)
            or (origin_only and (parsed.path not in ("", "/") or parsed.query))
        ):
            _fail()
    except ValueError:
        raise PluginValidationError("invalid_plugin_metadata") from None
    return parsed.scheme, parsed.hostname, port


def https_url(value: object, origins: frozenset[str] | None = None) -> str:
    result = text(value)
    origin = _https_origin(result)
    if origins is not None and origin not in {
        _https_origin(text(allowed), origin_only=True) for allowed in origins
    }:
        _fail()
    return result


def _qualified(value: object, allowed: frozenset[str]) -> str:
    result = identifier(value)
    if result not in allowed:
        _fail()
    return result


def _protected_unit(value: str, known_names: frozenset[str] = frozenset()) -> bool:
    stem = value.rsplit(".", 1)[0].split("@", 1)[0]
    known_stems = {name.rsplit(".", 1)[0].split("@", 1)[0] for name in known_names}
    protected_stems = _PROTECTED_STEMS | known_stems
    return (
        stem in protected_stems
        or "watchdog" in stem
        or (stem.endswith("-") and any(name.startswith(stem) for name in protected_stems))
    )


def _unit(value: str, known_names: frozenset[str] = frozenset()) -> None:
    if _UNIT.fullmatch(value) is None or ".." in value:
        _fail()
    if _protected_unit(value, known_names):
        _fail()


def _canonical_path(value: str) -> PurePosixPath:
    path = PurePosixPath(value)
    if (
        not value.startswith("/")
        or value.startswith("//")
        or str(path) != value
        or ".." in path.parts
        or "\\" in value
    ):
        _fail()
    return path


def _paths_overlap(first: str, second: str) -> bool:
    return (
        first == second
        or first.startswith(second.rstrip("/") + "/")
        or second.startswith(first.rstrip("/") + "/")
    )


def _path(value: str, known_names: frozenset[str] = frozenset()) -> None:
    path = _canonical_path(value)
    for directory in _UNIT_SEARCH_DIRECTORIES:
        if value == directory or directory.startswith(value.rstrip("/") + "/"):
            _fail()
        if value.startswith(directory + "/"):
            first_child = value[len(directory) + 1 :].split("/", 1)[0]
            if first_child in _SHARED_DROPIN_DIRECTORIES:
                _fail()
    for protected in _PROTECTED_PATHS:
        if (
            value == protected
            or value.startswith(protected + "/")
            or protected.startswith(value.rstrip("/") + "/")
        ):
            _fail()
    for part in path.parts:
        unit = part
        for suffix in (".d", ".wants", ".requires", ".upholds"):
            unit = unit.removesuffix(suffix)
        if _UNIT.fullmatch(unit):
            _unit(unit, known_names)


def guard_protected_core(manifest: PluginManifest, policy: ValidationPolicy) -> None:
    """Resolve every trusted alias/dependency; unknown or protected edges fail closed.

    No filesystem probes occur here. A profile must already have resolved real
    path links and external supervisors into this topology before qualification.
    """
    if manifest.replaces and (
        policy.qualified_firmware_profile is None
        or manifest.firmware_profiles != (policy.qualified_firmware_profile,)
        or policy.qualified_firmware_profile not in policy.firmware_profiles
    ):
        _fail()
    if len(policy.topology) > 128:
        _fail()
    nodes: dict[str, UnitTopology] = {}
    for profile_node in policy.topology:
        for name in (profile_node.identity, *profile_node.aliases):
            if name in nodes:
                _fail()
            nodes[name] = profile_node
    # A prerequisite of core is itself core, even if its name looks optional.
    protected: set[str] = set()
    pending = [name for name in nodes if _protected_unit(name)]
    for profile_node in policy.topology:
        for profile_path in profile_node.paths:
            _canonical_path(profile_path)
            try:
                _path(profile_path)
            except PluginValidationError:
                pending.append(profile_node.identity)
    # Path ownership can reveal another core owner whose prerequisites and paths
    # must then be protected too. Each iteration adds at least one of 128 nodes.
    while True:
        while pending:
            name = pending.pop()
            if name in protected:
                continue
            node = nodes.get(name)
            if node is None:
                _fail()
            protected.update((node.identity, *node.aliases))
            pending.extend(node.dependencies)
        protected_profile_paths = tuple(
            path
            for profile_node in policy.topology
            if profile_node.identity in protected
            for path in profile_node.paths
        )
        for profile_node in policy.topology:
            if profile_node.identity in protected:
                continue
            touches_core = any(
                _paths_overlap(path, protected_path)
                for path in profile_node.paths
                for protected_path in protected_profile_paths
            )
            for path in profile_node.paths:
                try:
                    _path(path, frozenset(protected))
                except PluginValidationError:
                    touches_core = True
            if touches_core:
                pending.append(profile_node.identity)
        if not pending:
            break
    checked: set[str] = set()

    def check_cycle(name: str, visiting: frozenset[str]) -> None:
        node = nodes.get(name)
        if node is None or node.identity in visiting:
            _fail()
            return
        if node.identity in checked:
            return
        for dependency in node.dependencies:
            check_cycle(dependency, visiting | {node.identity})
        checked.add(node.identity)

    for name in nodes:
        check_cycle(name, frozenset())
    for replacement in manifest.replaces:
        if _protected_unit(replacement.target, frozenset(protected)):
            _fail()
        _unit(replacement.identity)
        if replacement not in policy.replacement_allowlist:
            _fail()
        pending = [replacement.identity]
        visited: set[str] = set()
        while pending:
            name = pending.pop()
            _unit(name)
            if name in protected:
                _fail()
            if name in visited:
                continue
            visited.add(name)
            node = nodes.get(name)
            if node is None:
                _fail()
                continue
            for identity in (node.identity, *node.aliases):
                _unit(identity)
            for path in node.paths:
                _path(path, frozenset(protected))
                for protected_path in protected_profile_paths:
                    if _paths_overlap(path, protected_path):
                        _fail()
            pending.extend(node.dependencies)


def _capabilities(value: object, plugin_id: str, policy: ValidationPolicy) -> None:
    capabilities = object_fields(
        value,
        {
            "network_egress",
            "network_listeners",
            "microphone",
            "camera",
            "display_takeover",
            "personal_media",
            "provider_accounts",
            "ha",
            "bus",
            "hardware",
            "secrets",
            "retention",
        },
    )
    for key in ("microphone", "camera", "display_takeover", "personal_media"):
        if type(capabilities[key]) is not bool:
            _fail()
    # These grants need additional capture/listener/provider contracts in a later slice.
    for key in ("microphone", "camera", "personal_media"):
        if capabilities[key] is not False:
            _fail()
    for key in ("network_listeners", "provider_accounts", "secrets"):
        if sequence(capabilities[key]):
            _fail()
    for origin in strings(capabilities["network_egress"]):
        _https_origin(origin, origin_only=True)
        https_url(origin, policy.origins)
    strings(capabilities["hardware"], frozenset({"display", "backlight", "touchscreen"}))
    ha = object_fields(capabilities["ha"], {"layout", "reads", "actions"})
    if ha["layout"] not in ("none", "configured_dashboard"):
        _fail()
    strings(ha["reads"], frozenset({"configured_entities"}))
    strings(ha["actions"], frozenset())
    bus = object_fields(capabilities["bus"], {"reads", "writes"})
    strings(bus["reads"], frozenset({"own_panel_summary"}))
    strings(bus["writes"], frozenset({"own_wired_on_intensity"}))
    retention = object_fields(
        capabilities["retention"], {"media_disk_bytes", "media_lifetime_seconds"}
    )
    integer(retention["media_disk_bytes"], 0, 0)
    integer(retention["media_lifetime_seconds"], 0, 0)


def parse_manifest(raw: bytes, policy: ValidationPolicy = DEFAULT_POLICY) -> PluginManifest:
    document = object_fields(
        parse_document(raw),
        {
            "schema_version",
            "id",
            "version",
            "publisher",
            "artifact",
            "compatibility",
            "runtime",
            "capabilities",
            "replaces",
            "conflicts",
            "ownership",
            "health_profile",
        },
    )
    integer(document["schema_version"], 1, 1)
    plugin_id = identifier(document["id"])
    artifact = object_fields(
        document["artifact"],
        {
            "url",
            "sha256",
            "size_bytes",
            "unpacked_bytes_max",
            "files_max",
        },
    )
    parsed_artifact = Artifact(
        https_url(artifact["url"], policy.origins),
        digest(artifact["sha256"]),
        integer(artifact["size_bytes"], 1, MAX_ARTIFACT_BYTES),
        integer(artifact["unpacked_bytes_max"], 1, MAX_UNPACKED_BYTES),
        integer(artifact["files_max"], 1, MAX_FILES),
    )
    compatibility = object_fields(
        document["compatibility"],
        {
            "architectures",
            "python",
            "firmware_profiles",
            "manager_api",
            "bridge_plugin_api",
        },
    )
    if not strings(compatibility["architectures"], frozenset({"armv7l", "aarch64"})):
        _fail()
    if compatibility["python"] != "3.10":
        _fail()
    firmware_profiles = strings(compatibility["firmware_profiles"], policy.firmware_profiles)
    if not firmware_profiles:
        _fail()
    integer(compatibility["manager_api"], 1, 1)
    integer(compatibility["bridge_plugin_api"], 1, 1)
    runtime = object_fields(document["runtime"], {"adapter", "entrypoint", "resources_profile"})
    _qualified(runtime["adapter"], policy.adapters)
    _qualified(runtime["resources_profile"], policy.resources_profiles)
    entrypoint = text(runtime["entrypoint"])
    path = PurePosixPath(entrypoint)
    if (
        not path.parts
        or path.is_absolute()
        or str(path) != entrypoint
        or ".." in path.parts
        or "\\" in entrypoint
        or any(re.fullmatch(r"[a-zA-Z0-9_.-]+", part) is None for part in path.parts)
    ):
        _fail()
    _qualified(document["health_profile"], policy.health_profiles)
    _capabilities(document["capabilities"], plugin_id, policy)
    strings(document["conflicts"])
    ownership = object_fields(document["ownership"], {"surfaces", "topics"})
    strings(
        ownership["surfaces"],
        frozenset(
            {
                "screen_power",
                "screen_brightness",
                "screen_idle",
                "screensaver",
                "gestures",
            }
        ),
    )
    strings(ownership["topics"], frozenset({f"plugins/{plugin_id}"}))
    replacements: list[Replacement] = []
    for item in sequence(document["replaces"]):
        replacement = object_fields(item, {"target", "kind", "identity", "firmware_adapter"})
        if replacement["kind"] != "systemd":
            _fail()
        replacements.append(
            Replacement(
                identifier(replacement["target"]),
                "systemd",
                text(replacement["identity"]),
                identifier(replacement["firmware_adapter"]),
            )
        )
    if len(set(replacements)) != len(replacements):
        _fail()
    result = PluginManifest(
        plugin_id,
        version(document["version"]),
        identifier(document["publisher"]),
        parsed_artifact,
        tuple(replacements),
        hashlib.sha256(raw).hexdigest(),
        raw,
        firmware_profiles,
    )
    guard_protected_core(result, policy)
    return result
