"""On-panel operation recipes — the ONLY module that composes panel shell commands.

Every function takes a connected PanelShell. No HA imports: unit-testable in
isolation, mirrors the agent repo's pure-module philosophy. The integration only
ever writes the paths it owns: /var/brilliant-mqtt/**, /etc/brilliant-mqtt.env,
and /etc/systemd/system/brilliant-mqtt.service (see the design spec §7).
"""

from __future__ import annotations

import asyncio
import base64
import binascii
import hashlib
import json
import os
import re
import secrets
from collections.abc import Awaitable, Callable
from dataclasses import dataclass
from enum import StrEnum
from uuid import RFC_4122, UUID

import asyncssh

from .const import (
    BUS_WATCHDOG_SERVICE_NAME,
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    DEFAULT_REBOOT_JOURNAL_LINES,
    HA_MIRROR_SERVICE_NAME,
    HUE_CA_TIMER_NAME,
    MAX_REBOOT_JOURNAL_LINES,
    PANEL_APP_DIR,
    PANEL_BUS_WATCHDOG_DIR,
    PANEL_BUS_WATCHDOG_UNIT_FILE,
    PANEL_CURRENT_LINK,
    PANEL_ENV_FILE,
    PANEL_HA_MIRROR_APP_DIR,
    PANEL_HA_MIRROR_ENV_FILE,
    PANEL_HA_MIRROR_STAGED_DIR,
    PANEL_HA_MIRROR_UNIT_FILE,
    PANEL_HA_MIRROR_VAR_DIR,
    PANEL_HUE_CA_CERT_FILE,
    PANEL_HUE_CA_DIR,
    PANEL_HUE_CA_SERVICE_UNIT_FILE,
    PANEL_HUE_CA_TIMER_UNIT_FILE,
    PANEL_MQTT_TLS_DIR,
    PANEL_RELEASES_DIR,
    PANEL_RETAINED_TOPICS_FILE,
    PANEL_STAGED_DIR,
    PANEL_UNIT_FILE,
    PANEL_VAR_DIR,
    PANEL_VENDOR_DIR,
    PANEL_VERSION_FILE,
    PANEL_VOICE_ENV_FILE,
    PANEL_VOICE_STAGED_DIR,
    PANEL_VOICE_UNIT_FILE,
    PANEL_VOICE_VAR_DIR,
    PANEL_VOICE_VERSION_FILE,
    PANEL_WIFI_WATCHDOG_DIR,
    PANEL_WIFI_WATCHDOG_UNIT_FILE,
    SERVICE_NAME,
    VOICE_SERVICE_NAME,
    WIFI_WATCHDOG_SERVICE_NAME,
)
from .setup_protocol import PreflightRequest
from .shell import PanelProcess, PanelShell, RunResult

_STAGING_DIR = f"{PANEL_VAR_DIR}.staging"
_STAGED_UNIT = f"{PANEL_STAGED_DIR}/{SERVICE_NAME}.service"
_STAGED_ENV = f"{PANEL_STAGED_DIR}/{SERVICE_NAME}.env"

_VOICE_STAGING_DIR = f"{PANEL_VOICE_VAR_DIR}.staging"
_VOICE_STAGING_TARBALL = f"{PANEL_VOICE_VAR_DIR}.staging.tar.gz"
_VOICE_STAGED_ENV = f"{PANEL_VOICE_STAGED_DIR}/{VOICE_SERVICE_NAME}.env"

_HA_MIRROR_STAGING_DIR = f"{PANEL_HA_MIRROR_APP_DIR}.staging"
_HA_MIRROR_STAGED_ENV = f"{PANEL_HA_MIRROR_STAGED_DIR}/{HA_MIRROR_SERVICE_NAME}.env"


class PanelOpError(RuntimeError):
    """A panel shell command exited non-zero."""


_CORE_COMPONENT_ORDER = (
    COMPONENT_BRIDGE,
    COMPONENT_WIFI_WATCHDOG,
    COMPONENT_BUS_WATCHDOG,
)
_CORE_SERVICES = (
    (COMPONENT_BRIDGE, SERVICE_NAME, PANEL_UNIT_FILE),
    (
        COMPONENT_WIFI_WATCHDOG,
        WIFI_WATCHDOG_SERVICE_NAME,
        PANEL_WIFI_WATCHDOG_UNIT_FILE,
    ),
    (
        COMPONENT_BUS_WATCHDOG,
        BUS_WATCHDOG_SERVICE_NAME,
        PANEL_BUS_WATCHDOG_UNIT_FILE,
    ),
)
_VERSION_PATTERN = r"[0-9A-Za-z][0-9A-Za-z._+-]{0,127}"
_VERSION = re.compile(rf"^{_VERSION_PATTERN}$")
_RELEASE_TARGET = re.compile(
    rf"^{re.escape(PANEL_RELEASES_DIR)}/{_VERSION_PATTERN}--[0-9a-f]{{32}}$"
)
_MANAGED_MQTT_CA_PATH = re.compile(
    rf"(?:"
    rf"{re.escape(PANEL_MQTT_TLS_DIR)}/mqtt-ca-[0-9a-f]{{16}}\.pem"
    rf"|{re.escape(PANEL_RELEASES_DIR)}/{_VERSION_PATTERN}"
    rf"--[0-9a-f]{{32}}/mqtt-ca\.pem"
    rf")"
)
MAX_SNAPSHOT_FILE_BYTES = 16 * 1024
MAX_SNAPSHOT_WIRE_BYTES = 24 * 1024
MAX_MQTT_CA_BYTES = 256 * 1024
_MAX_LAYOUT_WIRE_BYTES = 4096
_MAX_VERSION_BYTES = 128
_PANEL_PYTHON = "/data/switch-embedded/env/bin/python3"
_PANEL_ATOMIC_MOVER = "/usr/bin/mv.coreutils"
_PANEL_SHA256SUM = "/usr/bin/sha256sum"
_PanelPreflightLauncher = Callable[[str], Awaitable[PanelProcess]]


class PanelLayout(StrEnum):
    """Core installation layout captured before a fleet activation."""

    ABSENT = "absent"
    LEGACY_FIXED = "legacy_fixed"
    RELEASE_LINK = "release_link"


@dataclass(frozen=True, slots=True, repr=False)
class FileSnapshot:
    """Exact optional owned-file bytes and permission mode."""

    content: bytes | None
    mode: int | None

    def __post_init__(self) -> None:
        if self.content is None:
            if self.mode is not None:
                raise PanelOpError("invalid_panel_snapshot")
            return
        if (
            type(self.content) is not bytes
            or len(self.content) > MAX_SNAPSHOT_FILE_BYTES
            or type(self.mode) is not int
            or not 0 <= self.mode <= 0o777
        ):
            raise PanelOpError("invalid_panel_snapshot")
        object.__setattr__(self, "content", bytes(self.content))

    def __repr__(self) -> str:
        return "FileSnapshot(<redacted>)"


@dataclass(frozen=True, slots=True, repr=False)
class ServiceSnapshot:
    """Exact owned systemd unit and its independent enabled/active state."""

    unit_file: FileSnapshot
    enabled: bool
    active: bool

    def __post_init__(self) -> None:
        if (
            not isinstance(self.unit_file, FileSnapshot)
            or type(self.enabled) is not bool
            or type(self.active) is not bool
            or (self.unit_file.content is None and (self.enabled or self.active))
        ):
            raise PanelOpError("invalid_panel_snapshot")

    def __repr__(self) -> str:
        return "ServiceSnapshot(<redacted>)"


def _normalize_selected_components(
    selected_components: tuple[str, ...],
    *,
    allow_empty: bool = False,
    require_bridge: bool = True,
) -> tuple[str, ...]:
    if (
        not isinstance(selected_components, tuple)
        or (not selected_components and not allow_empty)
        or len(selected_components) > len(_CORE_COMPONENT_ORDER)
        or any(type(component_id) is not str for component_id in selected_components)
        or len(set(selected_components)) != len(selected_components)
        or any(component_id not in _CORE_COMPONENT_ORDER for component_id in selected_components)
        or (require_bridge and selected_components and COMPONENT_BRIDGE not in selected_components)
    ):
        raise PanelOpError("invalid_selected_components")
    chosen = set(selected_components)
    return tuple(component_id for component_id in _CORE_COMPONENT_ORDER if component_id in chosen)


@dataclass(frozen=True, slots=True, repr=False)
class PanelSnapshot:
    """Exact core-owned state needed for a verifiable rollback."""

    layout: PanelLayout
    active_release_target: str | None
    environment_file: FileSnapshot
    version_file: FileSnapshot
    bridge_service: ServiceSnapshot
    wifi_watchdog_service: ServiceSnapshot
    bus_watchdog_service: ServiceSnapshot
    selected_components: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.layout, PanelLayout)
            or not isinstance(self.environment_file, FileSnapshot)
            or not isinstance(self.version_file, FileSnapshot)
            or not isinstance(self.bridge_service, ServiceSnapshot)
            or not isinstance(self.wifi_watchdog_service, ServiceSnapshot)
            or not isinstance(self.bus_watchdog_service, ServiceSnapshot)
        ):
            raise PanelOpError("invalid_panel_snapshot")
        selected_invalid = False
        selected: tuple[str, ...] = ()
        try:
            selected = _normalize_selected_components(
                self.selected_components,
                allow_empty=True,
                require_bridge=self.layout is PanelLayout.RELEASE_LINK,
            )
        except PanelOpError:
            selected_invalid = True
        if selected_invalid:
            raise PanelOpError("invalid_panel_snapshot")
        if self.layout is PanelLayout.RELEASE_LINK:
            if (
                not isinstance(self.active_release_target, str)
                or _RELEASE_TARGET.fullmatch(self.active_release_target) is None
                or self.version_file.content is None
            ):
                raise PanelOpError("invalid_panel_snapshot")
        elif self.active_release_target is not None:
            raise PanelOpError("invalid_panel_snapshot")

        services = (
            (COMPONENT_BRIDGE, self.bridge_service),
            (COMPONENT_WIFI_WATCHDOG, self.wifi_watchdog_service),
            (COMPONENT_BUS_WATCHDOG, self.bus_watchdog_service),
        )
        selected_from_units = tuple(
            component_id
            for component_id, service in services
            if service.unit_file.content is not None
        )
        files_absent = (
            self.environment_file.content is None
            and self.version_file.content is None
            and not selected_from_units
        )
        if self.layout is PanelLayout.ABSENT:
            if not files_absent or selected:
                raise PanelOpError("invalid_panel_snapshot")
        elif selected != selected_from_units:
            raise PanelOpError("invalid_panel_snapshot")
        elif self.layout is PanelLayout.RELEASE_LINK and (
            self.environment_file.content is None or self.bridge_service.unit_file.content is None
        ):
            raise PanelOpError("invalid_panel_snapshot")
        object.__setattr__(self, "selected_components", selected)

    def __repr__(self) -> str:
        return "PanelSnapshot(<redacted>)"


def _valid_uuid4(value: object) -> bool:
    return (
        isinstance(value, UUID)
        and value.variant == RFC_4122
        and value.version == 4
        and UUID(value.hex) == value
    )


@dataclass(frozen=True, slots=True)
class StagedRelease:
    """One immutable, transaction-unique release prepared on a panel."""

    version: str
    transaction_id: UUID
    release_target: str
    selected_components: tuple[str, ...]

    def __post_init__(self) -> None:
        if (
            not isinstance(self.version, str)
            or _VERSION.fullmatch(self.version) is None
            or not _valid_uuid4(self.transaction_id)
        ):
            raise PanelOpError("invalid_staged_release")
        expected_target = f"{PANEL_RELEASES_DIR}/{self.version}--{self.transaction_id.hex}"
        if self.release_target != expected_target:
            raise PanelOpError("invalid_staged_release")
        object.__setattr__(
            self,
            "selected_components",
            _normalize_selected_components(self.selected_components),
        )


async def _provisioning_run(
    shell: PanelShell,
    command: str,
    error_code: str,
) -> RunResult:
    """Run a fixed provisioning command without retaining transport details."""
    failed = False
    result: RunResult | None = None
    try:
        result = await shell.run(command)
    except asyncio.CancelledError:
        raise
    except Exception:
        failed = True
    if failed or result is None or result.exit_status != 0:
        command = "<redacted>"
        result = None
        raise PanelOpError(error_code)
    return result


async def _provisioning_put_dir(
    shell: PanelShell,
    local_dir: str,
    remote_dir: str,
    error_code: str,
) -> None:
    failed = False
    try:
        await shell.put_dir(local_dir, remote_dir)
    except asyncio.CancelledError:
        raise
    except Exception:
        failed = True
    if failed:
        local_dir = "<redacted>"
        remote_dir = "<redacted>"
        raise PanelOpError(error_code)


async def _provisioning_put_bytes(
    shell: PanelShell,
    data: bytes,
    remote_path: str,
    mode: int,
    error_code: str,
) -> None:
    failed = False
    try:
        await shell.put_bytes(data, remote_path, mode)
    except asyncio.CancelledError:
        raise
    except Exception:
        failed = True
    if failed:
        data = b""
        remote_path = "<redacted>"
        raise PanelOpError(error_code)


def _layout_probe_command() -> str:
    legacy_paths = (
        PANEL_ENV_FILE,
        PANEL_VERSION_FILE,
        PANEL_UNIT_FILE,
        PANEL_WIFI_WATCHDOG_UNIT_FILE,
        PANEL_BUS_WATCHDOG_UNIT_FILE,
        PANEL_APP_DIR,
        PANEL_VENDOR_DIR,
        PANEL_WIFI_WATCHDOG_DIR,
        PANEL_BUS_WATCHDOG_DIR,
    )
    services = tuple(
        (component_id, service_name) for component_id, service_name, _ in _CORE_SERVICES
    )
    return (
        f"{_PANEL_PYTHON} - <<'BRILLIANT_MQTT_SNAPSHOT'\n"
        "import json, os, subprocess\n"
        f"current = {PANEL_CURRENT_LINK!r}\n"
        f"legacy_paths = {legacy_paths!r}\n"
        f"services = {services!r}\n"
        "if os.path.lexists(current):\n"
        "    if not os.path.islink(current):\n"
        "        raise SystemExit(41)\n"
        "    layout = 'release_link'\n"
        "    target = os.readlink(current)\n"
        "else:\n"
        "    legacy = any(os.path.lexists(path) for path in legacy_paths)\n"
        "    layout = 'legacy_fixed' if legacy else 'absent'\n"
        "    target = None\n"
        "def state(service_name, operation, truthy, falsey):\n"
        "    result = subprocess.run(\n"
        "        ['systemctl', operation, service_name],\n"
        "        stdout=subprocess.PIPE, stderr=subprocess.DEVNULL,\n"
        "        check=False, text=True,\n"
        "    )\n"
        "    value = result.stdout.strip()\n"
        # Older systemd can leave stdout empty for is-enabled on an absent unit,
        # so default that case to "not-found". is-active always prints a usable
        # state; leaving it empty makes SystemExit(45) fail the probe closed.
        "    if not value and operation == 'is-enabled':\n"
        "        value = 'not-found'\n"
        "    if value in truthy:\n"
        "        return True\n"
        "    if value in falsey:\n"
        "        return False\n"
        "    raise SystemExit(45)\n"
        "states = {}\n"
        "for component_id, service_name in services:\n"
        "    enabled = state(\n"
        "        service_name, 'is-enabled',\n"
        "        {'alias', 'enabled', 'enabled-runtime', 'linked', 'linked-runtime'},\n"
        "        {'disabled', 'generated', 'indirect', 'masked', 'not-found',\n"
        "         'static', 'transient'},\n"
        "    )\n"
        "    active = state(\n"
        "        service_name, 'is-active', {'active'},\n"
        "        {'failed', 'inactive', 'unknown'},\n"
        "    )\n"
        "    states[component_id] = {'active': active, 'enabled': enabled}\n"
        "print(json.dumps(\n"
        "    {'active_release_target': target, 'layout': layout, 'services': states},\n"
        "    sort_keys=True, separators=(',', ':'),\n"
        "))\n"
        "BRILLIANT_MQTT_SNAPSHOT"
    )


def _file_probe_command(path: str, maximum_bytes: int) -> str:
    return (
        f"{_PANEL_PYTHON} - <<'BRILLIANT_MQTT_FILE_SNAPSHOT'\n"
        "import base64, errno, json, os, stat\n"
        f"path = {path!r}\n"
        f"maximum = {maximum_bytes}\n"
        "try:\n"
        "    descriptor = os.open(path, os.O_RDONLY | os.O_CLOEXEC | os.O_NOFOLLOW)\n"
        "except FileNotFoundError:\n"
        '    print(\'{"content":null,"mode":null,"present":false}\')\n'
        "    raise SystemExit(0)\n"
        "except OSError as error:\n"
        "    if error.errno == errno.ENOENT:\n"
        '        print(\'{"content":null,"mode":null,"present":false}\')\n'
        "        raise SystemExit(0)\n"
        "    raise\n"
        "try:\n"
        "    metadata = os.fstat(descriptor)\n"
        "    if not stat.S_ISREG(metadata.st_mode) or metadata.st_size > maximum:\n"
        "        raise SystemExit(42)\n"
        "    chunks = []\n"
        "    remaining = metadata.st_size\n"
        "    while remaining:\n"
        "        chunk = os.read(descriptor, min(remaining, 65536))\n"
        "        if not chunk:\n"
        "            raise SystemExit(43)\n"
        "        chunks.append(chunk)\n"
        "        remaining -= len(chunk)\n"
        "    if os.read(descriptor, 1):\n"
        "        raise SystemExit(44)\n"
        "finally:\n"
        "    os.close(descriptor)\n"
        "content = b''.join(chunks)\n"
        "print(json.dumps(\n"
        "    {'content': base64.b64encode(content).decode('ascii'),\n"
        "     'mode': stat.S_IMODE(metadata.st_mode), 'present': True},\n"
        "    sort_keys=True, separators=(',', ':'),\n"
        "))\n"
        "BRILLIANT_MQTT_FILE_SNAPSHOT"
    )


SNAPSHOT_LAYOUT_COMMAND = _layout_probe_command()
SNAPSHOT_FILE_COMMANDS = {
    "environment": _file_probe_command(PANEL_ENV_FILE, MAX_SNAPSHOT_FILE_BYTES),
    "version": _file_probe_command(PANEL_VERSION_FILE, _MAX_VERSION_BYTES),
    "bridge_unit": _file_probe_command(PANEL_UNIT_FILE, MAX_SNAPSHOT_FILE_BYTES),
    "wifi_unit": _file_probe_command(
        PANEL_WIFI_WATCHDOG_UNIT_FILE,
        MAX_SNAPSHOT_FILE_BYTES,
    ),
    "bus_unit": _file_probe_command(
        PANEL_BUS_WATCHDOG_UNIT_FILE,
        MAX_SNAPSHOT_FILE_BYTES,
    ),
}


def _json_object(
    raw: str, expected_keys: frozenset[str], maximum_bytes: int
) -> dict[str, object] | None:
    if not isinstance(raw, str):
        return None
    try:
        wire_size = len(raw.encode("utf-8"))
    except UnicodeError:
        return None
    if wire_size > maximum_bytes:
        return None

    def object_pairs(pairs: list[tuple[str, object]]) -> dict[str, object]:
        value: dict[str, object] = {}
        for key, item in pairs:
            if type(key) is not str or key in value:
                raise ValueError
            value[key] = item
        return value

    try:
        value = json.loads(
            raw,
            object_pairs_hook=object_pairs,
            parse_constant=lambda _value: (_ for _ in ()).throw(ValueError()),
        )
    except (
        UnicodeError,
        ValueError,
        TypeError,
        RecursionError,
        json.JSONDecodeError,
    ):
        return None
    if type(value) is not dict or frozenset(value) != expected_keys:
        return None
    return value


def _parse_service_states(raw: object) -> dict[str, tuple[bool, bool]] | None:
    if type(raw) is not dict or frozenset(raw) != frozenset(_CORE_COMPONENT_ORDER):
        return None
    states: dict[str, tuple[bool, bool]] = {}
    for component_id in _CORE_COMPONENT_ORDER:
        value = raw.get(component_id)
        if (
            type(value) is not dict
            or frozenset(value) != frozenset({"active", "enabled"})
            or type(value.get("enabled")) is not bool
            or type(value.get("active")) is not bool
        ):
            return None
        states[component_id] = (value["enabled"], value["active"])
    return states


def _parse_file_snapshot(raw: str, maximum_bytes: int) -> FileSnapshot | None:
    value = _json_object(
        raw,
        frozenset({"content", "mode", "present"}),
        MAX_SNAPSHOT_WIRE_BYTES,
    )
    if value is None or type(value["present"]) is not bool:
        return None
    if value["present"] is False:
        if value["content"] is not None or value["mode"] is not None:
            return None
        return FileSnapshot(content=None, mode=None)
    encoded = value["content"]
    mode = value["mode"]
    if (
        not isinstance(encoded, str)
        or not encoded.isascii()
        or type(mode) is not int
        or not 0 <= mode <= 0o777
    ):
        return None
    try:
        content = base64.b64decode(encoded, validate=True)
    except (binascii.Error, ValueError):
        return None
    if len(content) > maximum_bytes or base64.b64encode(content).decode("ascii") != encoded:
        return None
    return FileSnapshot(content=content, mode=mode)


async def snapshot_panel(shell: PanelShell) -> PanelSnapshot:
    """Capture bounded, exact rollback state for the three fleet-owned services."""
    layout_result = await _provisioning_run(
        shell,
        SNAPSHOT_LAYOUT_COMMAND,
        "snapshot_probe_failed",
    )
    layout_value = _json_object(
        layout_result.stdout,
        frozenset({"active_release_target", "layout", "services"}),
        _MAX_LAYOUT_WIRE_BYTES,
    )
    if layout_value is None:
        raise PanelOpError("snapshot_payload_invalid")
    states = _parse_service_states(layout_value["services"])
    raw_layout = layout_value["layout"]
    if not isinstance(raw_layout, str):
        layout = None
    else:
        try:
            layout = PanelLayout(raw_layout)
        except ValueError:
            layout = None
    target = layout_value["active_release_target"]
    if layout is None or states is None or (target is not None and not isinstance(target, str)):
        raise PanelOpError("snapshot_payload_invalid")

    parsed_files: dict[str, FileSnapshot] = {}
    for key, command in SNAPSHOT_FILE_COMMANDS.items():
        result = await _provisioning_run(shell, command, "snapshot_probe_failed")
        maximum = _MAX_VERSION_BYTES if key == "version" else MAX_SNAPSHOT_FILE_BYTES
        parsed = _parse_file_snapshot(result.stdout, maximum)
        if parsed is None:
            raise PanelOpError("snapshot_payload_invalid")
        parsed_files[key] = parsed

    snapshot_values_invalid = False
    bridge: ServiceSnapshot | None = None
    wifi: ServiceSnapshot | None = None
    bus: ServiceSnapshot | None = None
    try:
        bridge = ServiceSnapshot(
            parsed_files["bridge_unit"],
            enabled=states[COMPONENT_BRIDGE][0],
            active=states[COMPONENT_BRIDGE][1],
        )
        wifi = ServiceSnapshot(
            parsed_files["wifi_unit"],
            enabled=states[COMPONENT_WIFI_WATCHDOG][0],
            active=states[COMPONENT_WIFI_WATCHDOG][1],
        )
        bus = ServiceSnapshot(
            parsed_files["bus_unit"],
            enabled=states[COMPONENT_BUS_WATCHDOG][0],
            active=states[COMPONENT_BUS_WATCHDOG][1],
        )
    except Exception:
        snapshot_values_invalid = True
    if snapshot_values_invalid or bridge is None or wifi is None or bus is None:
        raise PanelOpError("snapshot_payload_invalid")
    selected = tuple(
        component_id
        for component_id, service in (
            (COMPONENT_BRIDGE, bridge),
            (COMPONENT_WIFI_WATCHDOG, wifi),
            (COMPONENT_BUS_WATCHDOG, bus),
        )
        if service.unit_file.content is not None
    )
    invalid = False
    snapshot: PanelSnapshot | None = None
    try:
        snapshot = PanelSnapshot(
            layout=layout,
            active_release_target=target,
            environment_file=parsed_files["environment"],
            version_file=parsed_files["version"],
            bridge_service=bridge,
            wifi_watchdog_service=wifi,
            bus_watchdog_service=bus,
            selected_components=selected,
        )
    except Exception:
        invalid = True
    if invalid or snapshot is None:
        raise PanelOpError("snapshot_payload_invalid")
    return snapshot


def _staged_temp_path(staged: StagedRelease) -> str:
    return f"{PANEL_RELEASES_DIR}/.{staged.version}--{staged.transaction_id.hex}.tmp"


def _release_mqtt_ca_path(staged: StagedRelease) -> str:
    return f"{staged.release_target}/mqtt-ca.pem"


def _release_mqtt_ca_digest_command(staged: StagedRelease, digest: str) -> str:
    if re.fullmatch(r"[0-9a-f]{64}", digest) is None:
        raise PanelOpError("invalid_staged_release")
    path = f"{_staged_temp_path(staged)}/mqtt-ca.pem"
    return f"test \"$({_PANEL_SHA256SUM} -- {path})\" = '{digest}  {path}'"


def _mqtt_ca_environment_assignments(environment: str) -> tuple[str, ...]:
    assignments: list[str] = []
    for line in environment.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, _value = stripped.partition("=")
        if separator and key.strip() == ENV_MQTT_TLS_CA_FILE:
            assignments.append(stripped)
    return tuple(assignments)


def _no_symlinks_command(root: str) -> str:
    """Reject symlinks while preserving a failing ``find`` exit status."""
    return f'links="$(find {root} -type l -print -quit)" && test -z "$links"'


def _release_validation_command(
    staged: StagedRelease,
    *,
    promoted: bool = False,
) -> str:
    """Validate the uploaded release without interpolating caller-controlled paths."""
    root = staged.release_target if promoted else _staged_temp_path(staged)
    required_files = (
        f"{root}/app/brilliant_mqtt/__main__.py",
        f"{root}/wifi_watchdog/brilliant_wifi_watchdog/run.py",
        f"{root}/bus_watchdog/brilliant_bus_watchdog/run.py",
        f"{root}/brilliant-mqtt-release.service",
        f"{root}/brilliant-wifi-watchdog-release.service",
        f"{root}/brilliant-bus-watchdog-release.service",
        f"{root}/brilliant-mqtt.env",
        f"{root}/VERSION",
    )
    checks = [
        f"test -d {root}",
        f"test ! -L {root}",
        f"test -d {root}/vendor",
        f"test ! -L {root}/vendor",
    ]
    for path in required_files:
        checks.extend((f"test -f {path}", f"test ! -L {path}"))
    mqtt_ca_file = f"{root}/mqtt-ca.pem"
    mqtt_ca_binding = f"{ENV_MQTT_TLS_CA_FILE}={_release_mqtt_ca_path(staged)}"
    mqtt_ca_assignment = rf"^[[:space:]]*{ENV_MQTT_TLS_CA_FILE}[[:space:]]*="
    checks.extend(
        (
            f'test "$(stat -c %a -- {root}/brilliant-mqtt.env)" = 600',
            f'test "$(cat -- {root}/VERSION)" = {staged.version}',
            (
                f"(if grep -Eq -- '{mqtt_ca_assignment}' {root}/brilliant-mqtt.env; then "
                f"test \"$(grep -Ec -- '{mqtt_ca_assignment}' "
                f'{root}/brilliant-mqtt.env)" = 1 && '
                f"grep -Fxq -- {mqtt_ca_binding} {root}/brilliant-mqtt.env && "
                f"test -f {mqtt_ca_file} && test ! -L {mqtt_ca_file} && "
                f'test "$(stat -c %a -- {mqtt_ca_file})" = 644; '
                f"else test ! -e {mqtt_ca_file} && test ! -L {mqtt_ca_file}; fi)"
            ),
            _no_symlinks_command(root),
        )
    )
    return " && ".join(checks)


def _shell_arg(value: str) -> str:
    """Quote one bounded value as an inert POSIX-shell argument."""
    if not isinstance(value, str) or "\0" in value:
        raise PanelOpError("invalid_preflight_request")
    return "'" + value.replace("'", "'\"'\"'") + "'"


def panel_preflight_launcher(
    shell: PanelShell,
    staged: StagedRelease,
) -> _PanelPreflightLauncher:
    """Build a launcher pinned to one validated immutable release."""
    if not isinstance(staged, StagedRelease):
        raise PanelOpError("invalid_staged_release")

    async def launch(raw_request: str) -> PanelProcess:
        request: PreflightRequest | None = None
        invalid = False
        try:
            request = PreflightRequest.from_json(raw_request)
        except Exception:
            invalid = True
        raw_request = "<redacted>"
        if invalid or request is None:
            raise PanelOpError("invalid_preflight_request")

        canonical_request = request.to_json()
        await _provisioning_run(
            shell,
            _release_validation_command(staged, promoted=True),
            "preflight_release_invalid",
        )
        release = staged.release_target
        command = (
            f"export PYTHONPATH={_shell_arg(f'{release}/app:{release}/vendor')} && "
            f"exec {_PANEL_PYTHON} -m brilliant_mqtt.preflight "
            f"--environment-file {_shell_arg(f'{release}/brilliant-mqtt.env')} "
            f"--request-json {_shell_arg(canonical_request)}"
        )
        process: PanelProcess | None = None
        failed = False
        try:
            process = await shell.start(command)
        except asyncio.CancelledError:
            raise
        except Exception:
            failed = True
        command = "<redacted>"
        canonical_request = "<redacted>"
        request = None
        if failed or process is None:
            raise PanelOpError("preflight_start_failed")
        return process

    return launch


def _stage_prepare_command(staged: StagedRelease) -> str:
    temporary = _staged_temp_path(staged)
    return (
        f"mkdir -p -- {PANEL_RELEASES_DIR} && "
        f"test -d {PANEL_RELEASES_DIR} && test ! -L {PANEL_RELEASES_DIR} && "
        f"rm -rf -- {temporary} && "
        f"test ! -e {staged.release_target} && test ! -L {staged.release_target}"
    )


def _stage_promote_command(staged: StagedRelease) -> str:
    temporary = _staged_temp_path(staged)
    return (
        f"{_PANEL_ATOMIC_MOVER} --no-clobber --no-target-directory -- "
        f"{temporary} {staged.release_target} && "
        f"test ! -e {temporary} && test ! -L {temporary} && "
        f"test -d {staged.release_target} && test ! -L {staged.release_target}"
    )


async def _settled_cleanup_run(shell: PanelShell, command: str) -> bool:
    """Run cleanup to settlement and report failure without losing cancellation."""
    cleanup_task = asyncio.create_task(shell.run(command))
    owner = asyncio.current_task()
    observed_cancellations = owner.cancelling() if owner is not None else 0
    cancellation: asyncio.CancelledError | None = None
    while not cleanup_task.done():
        try:
            await asyncio.shield(cleanup_task)
        except asyncio.CancelledError as error:
            current_cancellations = owner.cancelling() if owner is not None else 0
            if current_cancellations > observed_cancellations:
                cancellation = error
                observed_cancellations = current_cancellations
            if cleanup_task.done():
                break
        except Exception:
            break
    succeeded = False
    try:
        succeeded = cleanup_task.result().exit_status == 0
    except BaseException:
        pass
    command = "<redacted>"
    if cancellation is not None:
        raise cancellation from None
    return succeeded


async def stage_release(
    shell: PanelShell,
    local_payload_dir: str,
    version: str,
    environment: str,
    selected_components: tuple[str, ...],
    transaction_id: UUID,
    mqtt_ca: bytes | None = None,
) -> StagedRelease:
    """Upload and remotely validate one unique release without changing live state."""
    normalized = _normalize_selected_components(selected_components)
    if (
        not isinstance(version, str)
        or _VERSION.fullmatch(version) is None
        or not _valid_uuid4(transaction_id)
        or not isinstance(local_payload_dir, str)
        or not local_payload_dir
        or not isinstance(environment, str)
    ):
        raise PanelOpError("invalid_staged_release")
    staged = StagedRelease(
        version=version,
        transaction_id=transaction_id,
        release_target=f"{PANEL_RELEASES_DIR}/{version}--{transaction_id.hex}",
        selected_components=normalized,
    )
    encoding_failed = False
    environment_bytes = b""
    try:
        environment_bytes = environment.encode("utf-8")
    except UnicodeError:
        encoding_failed = True
    if encoding_failed or len(environment_bytes) > MAX_SNAPSHOT_FILE_BYTES:
        environment = "<redacted>"
        environment_bytes = b""
        raise PanelOpError("invalid_staged_release")
    mqtt_ca_path = _release_mqtt_ca_path(staged)
    expected_ca_assignment = f"{ENV_MQTT_TLS_CA_FILE}={mqtt_ca_path}"
    ca_assignments = _mqtt_ca_environment_assignments(environment)
    if mqtt_ca is None:
        valid_ca = not ca_assignments
    else:
        valid_ca = (
            type(mqtt_ca) is bytes
            and bool(mqtt_ca)
            and len(mqtt_ca) <= MAX_MQTT_CA_BYTES
            and b"PRIVATE KEY" not in mqtt_ca.upper()
            and ca_assignments == (expected_ca_assignment,)
        )
    if not valid_ca:
        environment = "<redacted>"
        environment_bytes = b""
        mqtt_ca = None
        raise PanelOpError("invalid_staged_release")
    mqtt_ca_digest = hashlib.sha256(mqtt_ca).hexdigest() if mqtt_ca is not None else None

    failure: BaseException | None = None
    try:
        await _provisioning_run(
            shell,
            _stage_prepare_command(staged),
            "stage_prepare_failed",
        )
        await _provisioning_put_dir(
            shell,
            local_payload_dir,
            _staged_temp_path(staged),
            "stage_upload_failed",
        )
        await _provisioning_run(
            shell,
            f"rm -f -- {_staged_temp_path(staged)}/mqtt-ca.pem",
            "stage_upload_failed",
        )
        await _provisioning_put_bytes(
            shell,
            environment_bytes,
            f"{_staged_temp_path(staged)}/brilliant-mqtt.env",
            0o600,
            "stage_upload_failed",
        )
        environment_bytes = b""
        environment = "<redacted>"
        if mqtt_ca is not None:
            await _provisioning_put_bytes(
                shell,
                mqtt_ca,
                f"{_staged_temp_path(staged)}/mqtt-ca.pem",
                0o644,
                "stage_upload_failed",
            )
            mqtt_ca = None
            await _provisioning_run(
                shell,
                _release_mqtt_ca_digest_command(staged, mqtt_ca_digest or ""),
                "stage_validation_failed",
            )
            mqtt_ca_digest = None
        await _provisioning_put_bytes(
            shell,
            version.encode("ascii"),
            f"{_staged_temp_path(staged)}/VERSION",
            0o644,
            "stage_upload_failed",
        )
        await _provisioning_run(
            shell,
            _release_validation_command(staged),
            "stage_validation_failed",
        )
        await _provisioning_run(
            shell,
            _stage_promote_command(staged),
            "stage_promotion_failed",
        )
    except BaseException as error:
        failure = error

    local_payload_dir = "<redacted>"
    environment = "<redacted>"
    environment_bytes = b""
    mqtt_ca = None
    mqtt_ca_digest = None
    if failure is not None:
        cleanup_succeeded = await _settled_cleanup_run(
            shell,
            _cleanup_staged_command(staged),
        )
        if not cleanup_succeeded:
            if not isinstance(failure, Exception):
                raise failure from None
            raise PanelOpError("staged_cleanup_failed")
        raise failure from None
    return staged


def _activation_paths(staged: StagedRelease) -> dict[str, tuple[str, str]]:
    suffix = f".fleet-{staged.transaction_id.hex}.tmp"
    return {
        "environment": (
            f"{staged.release_target}/brilliant-mqtt.env",
            f"{PANEL_ENV_FILE}{suffix}",
        ),
        "version": (
            f"{staged.release_target}/VERSION",
            f"{PANEL_VERSION_FILE}{suffix}",
        ),
        "bridge_unit": (
            f"{staged.release_target}/brilliant-mqtt-release.service",
            f"{PANEL_UNIT_FILE}{suffix}",
        ),
        "wifi_unit": (
            f"{staged.release_target}/brilliant-wifi-watchdog-release.service",
            f"{PANEL_WIFI_WATCHDOG_UNIT_FILE}{suffix}",
        ),
        "bus_unit": (
            f"{staged.release_target}/brilliant-bus-watchdog-release.service",
            f"{PANEL_BUS_WATCHDOG_UNIT_FILE}{suffix}",
        ),
    }


def _activation_prepare_command(staged: StagedRelease) -> str:
    paths = _activation_paths(staged)
    temporary_paths = " ".join(temporary for _, temporary in paths.values())
    operations = [f"rm -f -- {temporary_paths}"]
    for source, temporary in paths.values():
        operations.append(f"cp -- {source} {temporary}")
    operations.extend(
        (
            f"chmod 600 {paths['environment'][1]}",
            f"chmod 644 {paths['version'][1]}",
            f"chmod 644 {paths['bridge_unit'][1]}",
            f"chmod 644 {paths['wifi_unit'][1]}",
            f"chmod 644 {paths['bus_unit'][1]}",
        )
    )
    return " && ".join(operations)


def _stop_service_command(service_name: str) -> str:
    return (
        f"if systemctl is-active --quiet {service_name}; then "
        f"systemctl stop {service_name}; fi; "
        f'state="$(systemctl is-active {service_name} 2>/dev/null || true)"; '
        'case "$state" in inactive|failed|unknown) ;; *) exit 47 ;; esac'
    )


def _is_enabled_state_capture(service_name: str) -> str:
    return (
        f'state="$(systemctl is-enabled {service_name} 2>/dev/null || true)"; '
        '[ -z "$state" ] && state=not-found;'
    )


def _disabled_state_command(service_name: str) -> str:
    return (
        f"{_is_enabled_state_capture(service_name)} "
        'case "$state" in '
        "disabled|generated|indirect|masked|not-found|static|transient) ;; "
        "*) exit 48 ;; esac"
    )


def _disable_service_command(service_name: str) -> str:
    return (
        f"systemctl disable {service_name} >/dev/null 2>&1 || true; "
        f"{_disabled_state_command(service_name)}"
    )


def _activation_commit_command(staged: StagedRelease) -> str:
    paths = _activation_paths(staged)
    selected = set(staged.selected_components)
    current_temporary = f"{PANEL_CURRENT_LINK}.fleet-{staged.transaction_id.hex}.tmp"
    operations = [
        (
            f"if [ -e {PANEL_CURRENT_LINK} ] || [ -L {PANEL_CURRENT_LINK} ]; "
            f"then [ -L {PANEL_CURRENT_LINK} ]; fi"
        ),
        f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
        f"{paths['environment'][1]} {PANEL_ENV_FILE}",
        f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
        f"{paths['version'][1]} {PANEL_VERSION_FILE}",
        f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
        f"{paths['bridge_unit'][1]} {PANEL_UNIT_FILE}",
    ]
    for component_id, key, destination in (
        (
            COMPONENT_WIFI_WATCHDOG,
            "wifi_unit",
            PANEL_WIFI_WATCHDOG_UNIT_FILE,
        ),
        (
            COMPONENT_BUS_WATCHDOG,
            "bus_unit",
            PANEL_BUS_WATCHDOG_UNIT_FILE,
        ),
    ):
        if component_id in selected:
            operations.append(
                f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
                f"{paths[key][1]} {destination}"
            )
        else:
            operations.append(f"rm -f -- {paths[key][1]} {destination}")
    operations.extend(
        (
            f"rm -f -- {current_temporary}",
            f"ln -s {staged.release_target} {current_temporary}",
            f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
            f"{current_temporary} {PANEL_CURRENT_LINK}",
        )
    )
    return " && ".join(operations)


async def _converge_service(
    shell: PanelShell,
    service_name: str,
    *,
    selected: bool,
    error_code: str,
) -> None:
    if selected:
        await _provisioning_run(
            shell,
            f"systemctl enable {service_name}",
            error_code,
        )
        await _provisioning_run(
            shell,
            f"systemctl start {service_name}",
            error_code,
        )
        await _provisioning_run(
            shell,
            (
                f"systemctl is-enabled --quiet {service_name} && "
                f"systemctl is-active --quiet {service_name}"
            ),
            error_code,
        )
        return
    await _provisioning_run(
        shell,
        _disabled_state_command(service_name),
        error_code,
    )
    await _provisioning_run(
        shell,
        _stop_service_command(service_name),
        error_code,
    )


async def activate_staged(
    shell: PanelShell,
    staged: StagedRelease,
    *,
    on_services_stopped: Callable[[], None],
) -> None:
    """Atomically select a validated release and converge only fleet core units."""
    if not isinstance(staged, StagedRelease) or not callable(on_services_stopped):
        raise PanelOpError("invalid_staged_release")
    await _provisioning_run(
        shell,
        _release_validation_command(staged, promoted=True),
        "activation_prepare_failed",
    )
    await _provisioning_run(
        shell,
        _activation_prepare_command(staged),
        "activation_prepare_failed",
    )
    for _, service_name, _ in _CORE_SERVICES:
        await _provisioning_run(
            shell,
            _stop_service_command(service_name),
            "activation_stop_failed",
        )
    selected = set(staged.selected_components)
    for component_id, service_name, _ in _CORE_SERVICES:
        if component_id not in selected:
            await _provisioning_run(
                shell,
                _disable_service_command(service_name),
                "activation_service_failed",
            )
    try:
        on_services_stopped()
    except asyncio.CancelledError:
        raise
    except Exception:
        raise PanelOpError("activation_boundary_failed") from None
    await _provisioning_run(
        shell,
        _activation_commit_command(staged),
        "activation_commit_failed",
    )
    await _provisioning_run(
        shell,
        "systemctl daemon-reload",
        "activation_reload_failed",
    )
    for component_id, service_name, _ in _CORE_SERVICES:
        await _converge_service(
            shell,
            service_name,
            selected=component_id in selected,
            error_code="activation_service_failed",
        )


def _rollback_temp_path(path: str, staged: StagedRelease) -> str:
    return f"{path}.rollback-{staged.transaction_id.hex}.tmp"


async def _restore_file(
    shell: PanelShell,
    path: str,
    snapshot: FileSnapshot,
    staged: StagedRelease,
) -> None:
    temporary = _rollback_temp_path(path, staged)
    await _provisioning_run(
        shell,
        f"rm -f -- {temporary}",
        "rollback_restore_failed",
    )
    if snapshot.content is None:
        await _provisioning_run(
            shell,
            f"rm -f -- {path}",
            "rollback_restore_failed",
        )
        return
    assert snapshot.mode is not None
    await _provisioning_put_bytes(
        shell,
        snapshot.content,
        temporary,
        snapshot.mode,
        "rollback_restore_failed",
    )
    await _provisioning_run(
        shell,
        (
            f"test -f {temporary} && test ! -L {temporary} && "
            f'test "$(stat -c %a -- {temporary})" = {snapshot.mode:o} && '
            f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
            f"{temporary} {path}"
        ),
        "rollback_restore_failed",
    )


def _rollback_link_command(
    snapshot: PanelSnapshot,
    staged: StagedRelease,
) -> str:
    temporary = f"{PANEL_CURRENT_LINK}.rollback-{staged.transaction_id.hex}.tmp"
    guard = (
        f"if [ -e {PANEL_CURRENT_LINK} ] || [ -L {PANEL_CURRENT_LINK} ]; "
        f"then [ -L {PANEL_CURRENT_LINK} ] || exit 46; fi"
    )
    if snapshot.layout is PanelLayout.RELEASE_LINK:
        assert snapshot.active_release_target is not None
        return (
            f"{guard}; rm -f -- {temporary}; "
            f"ln -s {snapshot.active_release_target} {temporary} && "
            f"{_PANEL_ATOMIC_MOVER} --force --no-target-directory -- "
            f"{temporary} {PANEL_CURRENT_LINK}"
        )
    return f"{guard}; rm -f -- {temporary} {PANEL_CURRENT_LINK}"


async def _restore_service_state(
    shell: PanelShell,
    service_name: str,
    snapshot: ServiceSnapshot,
) -> None:
    if snapshot.enabled:
        await _provisioning_run(
            shell,
            f"systemctl enable {service_name}",
            "rollback_service_failed",
        )
    else:
        await _provisioning_run(
            shell,
            _disabled_state_command(service_name),
            "rollback_service_failed",
        )
    if snapshot.active:
        await _provisioning_run(
            shell,
            f"systemctl start {service_name}",
            "rollback_service_failed",
        )
    else:
        await _provisioning_run(
            shell,
            _stop_service_command(service_name),
            "rollback_service_failed",
        )


def _rollback_cleanup_command(staged: StagedRelease) -> str:
    paths = (
        PANEL_ENV_FILE,
        PANEL_VERSION_FILE,
        PANEL_UNIT_FILE,
        PANEL_WIFI_WATCHDOG_UNIT_FILE,
        PANEL_BUS_WATCHDOG_UNIT_FILE,
    )
    temporary_paths = [_rollback_temp_path(path, staged) for path in paths]
    temporary_paths.extend(temporary for _, temporary in _activation_paths(staged).values())
    temporary_paths.append(f"{PANEL_CURRENT_LINK}.rollback-{staged.transaction_id.hex}.tmp")
    temporary_paths.append(f"{PANEL_CURRENT_LINK}.fleet-{staged.transaction_id.hex}.tmp")
    return f"rm -f -- {' '.join(temporary_paths)}"


async def rollback_snapshot(
    shell: PanelShell,
    snapshot: PanelSnapshot,
    staged: StagedRelease,
) -> None:
    """Restore exact files, link, and independent core service states."""
    if not isinstance(snapshot, PanelSnapshot):
        raise PanelOpError("invalid_panel_snapshot")
    if not isinstance(staged, StagedRelease):
        raise PanelOpError("invalid_staged_release")
    failure: BaseException | None = None
    try:
        for _, service_name, _ in _CORE_SERVICES:
            await _provisioning_run(
                shell,
                _stop_service_command(service_name),
                "rollback_service_failed",
            )
        for service_name, service_snapshot in (
            (SERVICE_NAME, snapshot.bridge_service),
            (
                WIFI_WATCHDOG_SERVICE_NAME,
                snapshot.wifi_watchdog_service,
            ),
            (
                BUS_WATCHDOG_SERVICE_NAME,
                snapshot.bus_watchdog_service,
            ),
        ):
            if not service_snapshot.enabled:
                await _provisioning_run(
                    shell,
                    _disable_service_command(service_name),
                    "rollback_service_failed",
                )
        for path, file_snapshot in (
            (PANEL_ENV_FILE, snapshot.environment_file),
            (PANEL_VERSION_FILE, snapshot.version_file),
            (PANEL_UNIT_FILE, snapshot.bridge_service.unit_file),
            (
                PANEL_WIFI_WATCHDOG_UNIT_FILE,
                snapshot.wifi_watchdog_service.unit_file,
            ),
            (
                PANEL_BUS_WATCHDOG_UNIT_FILE,
                snapshot.bus_watchdog_service.unit_file,
            ),
        ):
            await _restore_file(shell, path, file_snapshot, staged)
        await _provisioning_run(
            shell,
            _rollback_link_command(snapshot, staged),
            "rollback_restore_failed",
        )
        await _provisioning_run(
            shell,
            "systemctl daemon-reload",
            "rollback_reload_failed",
        )
        for service_name, service_snapshot in (
            (SERVICE_NAME, snapshot.bridge_service),
            (
                WIFI_WATCHDOG_SERVICE_NAME,
                snapshot.wifi_watchdog_service,
            ),
            (
                BUS_WATCHDOG_SERVICE_NAME,
                snapshot.bus_watchdog_service,
            ),
        ):
            await _restore_service_state(shell, service_name, service_snapshot)

        verification_failed = False
        observed: PanelSnapshot | None = None
        try:
            observed = await snapshot_panel(shell)
        except asyncio.CancelledError:
            raise
        except Exception:
            verification_failed = True
        if verification_failed or observed != snapshot:
            observed = None
            raise PanelOpError("rollback_verification_failed")
    except BaseException as error:
        failure = error

    cleanup_succeeded = await _settled_cleanup_run(
        shell,
        _rollback_cleanup_command(staged),
    )
    if not cleanup_succeeded:
        if failure is not None and not isinstance(failure, Exception):
            raise failure from None
        raise PanelOpError("rollback_cleanup_failed")
    if failure is not None:
        raise failure from None


def _cleanup_staged_command(staged: StagedRelease) -> str:
    temporary = _staged_temp_path(staged)
    return (
        f"rm -rf -- {temporary}; "
        f"if [ -e {PANEL_CURRENT_LINK} ] || [ -L {PANEL_CURRENT_LINK} ]; then "
        f"test -L {PANEL_CURRENT_LINK} || exit 46; "
        f'test "$(readlink {PANEL_CURRENT_LINK})" != {staged.release_target} '
        "|| exit 47; "
        f"fi; rm -rf -- {staged.release_target}"
    )


async def cleanup_staged(shell: PanelShell, staged: StagedRelease) -> None:
    """Delete only a transaction-owned candidate after proving it is inactive."""
    if not isinstance(staged, StagedRelease):
        raise PanelOpError("invalid_staged_release")
    await _provisioning_run(
        shell,
        _cleanup_staged_command(staged),
        "staged_cleanup_failed",
    )


async def restart_candidate(
    shell: PanelShell,
    staged: StagedRelease,
    *,
    on_service_stopped: Callable[[], None],
) -> None:
    """Restart the selected bridge around a stop-confirmed health boundary."""
    if not isinstance(staged, StagedRelease) or not callable(on_service_stopped):
        raise PanelOpError("invalid_staged_release")
    await _provisioning_run(
        shell,
        (
            f"test -L {PANEL_CURRENT_LINK} && "
            f'test "$(readlink {PANEL_CURRENT_LINK})" = {staged.release_target}'
        ),
        "candidate_not_active",
    )
    await _provisioning_run(
        shell,
        _stop_service_command(SERVICE_NAME),
        "candidate_restart_failed",
    )
    try:
        on_service_stopped()
    except asyncio.CancelledError:
        raise
    except Exception:
        raise PanelOpError("candidate_boundary_failed") from None
    await _provisioning_run(
        shell,
        f"systemctl start {SERVICE_NAME}",
        "candidate_restart_failed",
    )
    await _provisioning_run(
        shell,
        f"systemctl is-active --quiet {SERVICE_NAME}",
        "candidate_restart_failed",
    )


async def _checked(shell: PanelShell, command: str) -> RunResult:
    """Run a STATE-MUTATING command, raising if it fails.

    Without this a failed `systemctl`/`mv`/`rm` would vanish silently and the
    only symptom would be a 10-minute LWT availability timeout. Read-only probes
    and best-effort teardown steps deliberately stay on the plain `shell.run`.
    """
    result = await shell.run(command)
    if result.exit_status != 0:
        raise PanelOpError(f"`{command}` exited {result.exit_status}: {result.stderr.strip()}")
    return result


def _env_quote(value: str) -> str:
    """Double-quote a string value for a systemd EnvironmentFile.

    systemd parses the env file line-by-line and exports each into the
    root-running unit's environment. Unquoted, a newline injects extra vars
    (`LD_PRELOAD=…` → root RCE), a trailing backslash splices lines, and a
    leading `#` comments the var out. We fail closed on control chars and quote
    everything else so the value round-trips byte-for-byte.
    """
    if any(
        character in "\n\r" or _invalid_systemd_environment_character(character)
        for character in value
    ):
        raise ValueError(
            "control characters or unsupported Unicode are not allowed "
            "in systemd environment values"
        )
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


def _invalid_systemd_environment_character(character: str) -> bool:
    """Match systemd EnvironmentFile's excluded Unicode scalar values."""
    codepoint = ord(character)
    return (
        codepoint == 0
        or codepoint == 0xFEFF
        or 0xD800 <= codepoint <= 0xDFFF
        or 0xFDD0 <= codepoint <= 0xFDEF
        or codepoint & 0xFFFF in {0xFFFE, 0xFFFF}
    )


# One composite probe so inspection is a single round-trip. Key=0/1 lines, then
# (optionally) the deployed payload version as the last line.
INSPECT_COMMAND = (
    f"test -f {PANEL_UNIT_FILE} && echo unit=1 || echo unit=0; "
    f"test -f {PANEL_ENV_FILE} && echo env=1 || echo env=0; "
    f"systemctl is-enabled {SERVICE_NAME} >/dev/null 2>&1 && echo enabled=1 || echo enabled=0; "
    f"systemctl is-active {SERVICE_NAME} >/dev/null 2>&1 && echo active=1 || echo active=0; "
    f"test -f {_STAGED_UNIT} && echo sunit=1 || echo sunit=0; "
    f"test -f {_STAGED_ENV} && echo senv=1 || echo senv=0; "
    f"test -f {PANEL_VAR_DIR}/app/brilliant_mqtt/__main__.py "
    f"&& test -d {PANEL_VAR_DIR}/vendor && echo payload=1 || echo payload=0; "
    f"cat {PANEL_VERSION_FILE} 2>/dev/null || true"
)


@dataclass(frozen=True)
class PanelState:
    """Parsed result of one inspect_panel() probe."""

    unit_present: bool
    env_present: bool
    enabled: bool
    active: bool
    staged_unit_present: bool
    staged_env_present: bool
    payload_present: bool  # agent entrypoint (app/<pkg>/__main__.py) + vendor/ present
    payload_version: str | None  # None on pre-integration installs (no VERSION file)


async def inspect_panel(shell: PanelShell) -> PanelState:
    """Probe install/health state in one shell round-trip."""
    result = await shell.run(INSPECT_COMMAND)
    flags: dict[str, bool] = {}
    version: str | None = None
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and key in ("unit", "env", "enabled", "active", "sunit", "senv", "payload"):
            flags[key] = value == "1"
        elif line.strip():
            version = line.strip()
    return PanelState(
        unit_present=flags.get("unit", False),
        env_present=flags.get("env", False),
        enabled=flags.get("enabled", False),
        active=flags.get("active", False),
        staged_unit_present=flags.get("sunit", False),
        staged_env_present=flags.get("senv", False),
        payload_present=flags.get("payload", False),
        payload_version=version,
    )


# The on-panel agent's env-var contract (agent_payload/.../config.py Settings.from_env).
# render_env writes these; parse_env/read_env recover them when adopting an
# already-installed panel during onboarding.
ENV_PANEL = "BRILLIANT_PANEL"
ENV_DEPLOYMENT_ID = "BRILLIANT_DEPLOYMENT_ID"
ENV_MQTT_HOST = "MQTT_HOST"
ENV_MQTT_PORT = "MQTT_PORT"
ENV_MQTT_USERNAME = "MQTT_USERNAME"
ENV_MQTT_PASSWORD = "MQTT_PASSWORD"
ENV_MQTT_TLS_ENABLED = "MQTT_TLS_ENABLED"
ENV_MQTT_TLS_CA_FILE = "MQTT_TLS_CA_FILE"
ENV_RETAINED_TOPICS_FILE = "RETAINED_TOPICS_FILE"
ENV_MESH_PRIORITY = "MESH_PRIORITY"
ENV_SCENE_BRIDGE_ENABLED = "SCENE_BRIDGE_ENABLED"

_MQTT_TLS_TRUE_VALUES = frozenset({"1", "true", "on", "yes"})
_MQTT_TLS_FALSE_VALUES = frozenset({"0", "false", "off", "no"})
MQTT_TLS_GUARD_COMMAND = (
    f"for file in {PANEL_ENV_FILE} {_STAGED_ENV}; do "
    'if test -f "$file"; then '
    "awk -F= '$1 ~ /^[[:space:]]*MQTT_TLS_ENABLED[[:space:]]*$/ "
    '{ print }\' "$file" || exit $?; '
    "fi; done"
)


def render_env(
    panel: str,
    mesh_priority: int,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str,
    mqtt_password: str,
    scene_bridge_enabled: bool = False,
    mqtt_tls_enabled: bool = False,
    mqtt_tls_ca_file: str | None = None,
    deployment_id: str | None = None,
) -> str:
    """Render /etc/brilliant-mqtt.env — exactly what the agent's config.py reads.

    String values are quoted via _env_quote (user-typed broker passwords routinely
    contain `#`, quotes, `$`, backslash); the int fields are safe and stay bare.
    """
    if type(mqtt_tls_enabled) is not bool:
        raise ValueError("invalid_mqtt_tls_enabled")
    if mqtt_tls_ca_file and not mqtt_tls_enabled:
        raise ValueError("mqtt_tls_ca_file_requires_tls")
    if mqtt_tls_ca_file is not None and (
        not isinstance(mqtt_tls_ca_file, str)
        or _MANAGED_MQTT_CA_PATH.fullmatch(mqtt_tls_ca_file) is None
    ):
        raise ValueError("invalid_mqtt_tls_ca_file")
    if deployment_id is not None and re.fullmatch(r"[0-9a-f]{32}", deployment_id) is None:
        raise ValueError("invalid_deployment_id")

    broker_env = (
        f"{ENV_PANEL}={_env_quote(panel)}\n"
        f"{ENV_MQTT_HOST}={_env_quote(mqtt_host)}\n"
        f"{ENV_MQTT_PORT}={mqtt_port}\n"
        f"{ENV_MQTT_USERNAME}={_env_quote(mqtt_username)}\n"
        f"{ENV_MQTT_PASSWORD}={_env_quote(mqtt_password)}\n"
        f"{ENV_MQTT_TLS_ENABLED}={1 if mqtt_tls_enabled else 0}\n"
    )
    if deployment_id is not None:
        broker_env += f"{ENV_DEPLOYMENT_ID}={deployment_id}\n"
    if mqtt_tls_ca_file:
        broker_env += f"{ENV_MQTT_TLS_CA_FILE}={mqtt_tls_ca_file}\n"
    return (
        broker_env + f"{ENV_RETAINED_TOPICS_FILE}={PANEL_RETAINED_TOPICS_FILE}\n"
        f"{ENV_MESH_PRIORITY}={mesh_priority}\n"
        f"{ENV_SCENE_BRIDGE_ENABLED}={1 if scene_bridge_enabled else 0}\n"
        f"LOG_LEVEL=INFO\n"
    )


def _env_unquote(raw: str) -> str:
    r"""Reverse _env_quote: strip surrounding double-quotes, unescape \\ and \".

    _env_quote only ever escapes ``\`` and ``"``, so this unescapes ONLY those two
    sequences — any other backslash run (e.g. a hand-deployed ``\n`` or ``\$``) is
    left byte-for-byte intact rather than silently collapsed (``\n`` → ``n``), which
    would corrupt the value when it is later re-rendered and pushed to the panel.
    """
    if len(raw) >= 2 and raw[0] == '"' and raw[-1] == '"':
        inner = raw[1:-1]
        out: list[str] = []
        i = 0
        while i < len(inner):
            ch = inner[i]
            if ch == "\\" and i + 1 < len(inner) and inner[i + 1] in ('"', "\\"):
                out.append(inner[i + 1])
                i += 2
            else:
                out.append(ch)
                i += 1
        return "".join(out)
    return raw


def parse_env(text: str) -> dict[str, str]:
    """Parse a rendered env file back into ``{KEY: value}`` — reverse of render_env.

    Quoted strings are unquoted; bare ints keep their string form; blank/comment
    lines are skipped. Used ONLY by the config flow's adopt-installed path — repair
    still always regenerates the env from entry data, never reads it back.
    """
    parsed: dict[str, str] = {}
    for line in text.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#") or "=" not in stripped:
            continue
        key, _, raw = stripped.partition("=")
        parsed[key.strip()] = _env_unquote(raw.strip())
    return parsed


async def read_env(shell: PanelShell) -> dict[str, str]:
    """Read + parse the panel's live env file (adopt-installed onboarding path)."""
    result = await shell.run(f"cat {PANEL_ENV_FILE}")
    return parse_env(result.stdout)


def _mqtt_tls_assignments(env_content: str) -> tuple[bool | None, ...]:
    """Classify MQTT TLS assignments without under-parsing systemd quotes.

    ``None`` means an assignment was present but was not provably true or
    false. Existing ambiguous values are treated conservatively by the guard.
    """
    values: list[bool | None] = []
    for line in env_content.split("\n"):
        stripped = line.strip()
        if not stripped or stripped.startswith("#"):
            continue
        key, separator, raw = stripped.partition("=")
        if not separator or key.strip() != ENV_MQTT_TLS_ENABLED:
            continue
        value = raw.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in {'"', "'"}:
            value = value[1:-1].strip()
        normalized = value.lower()
        if normalized in _MQTT_TLS_TRUE_VALUES:
            values.append(True)
        elif normalized in _MQTT_TLS_FALSE_VALUES:
            values.append(False)
        else:
            values.append(None)
    return tuple(values)


async def async_assert_no_mqtt_tls_downgrade(
    shell: PanelShell,
    desired_env_content: str,
) -> None:
    """Refuse an implicit TLS-to-plaintext lifecycle operation before mutation."""
    desired_assignments = _mqtt_tls_assignments(desired_env_content)
    desired_tls = desired_assignments[-1] if desired_assignments else False
    if desired_tls is None:
        raise PanelOpError("invalid desired MQTT_TLS_ENABLED value")
    if desired_tls:
        return

    existing = await _checked(shell, MQTT_TLS_GUARD_COMMAND)
    existing_assignments = _mqtt_tls_assignments(existing.stdout)
    if any(value is not False for value in existing_assignments):
        raise PanelOpError(
            "mqtt_tls_downgrade_refused: existing panel TLS state is enabled or "
            "ambiguous; this Home Assistant release cannot round-trip TLS settings; "
            "keep this panel under manual lifecycle management"
        )


async def write_env(shell: PanelShell, env_content: str) -> None:
    """(Re)write ONLY the env file to /etc + the /var staged copy (0600); no unit.

    Reconfigure changes broker/mesh values on an already-installed panel where the
    unit is unchanged, so this rewrites just the env in both locations; the caller
    restarts the agent to pick it up.
    """
    await async_assert_no_mqtt_tls_downgrade(shell, env_content)
    await _checked(shell, f"mkdir -p {PANEL_STAGED_DIR}")
    env_bytes = env_content.encode()
    await shell.put_bytes(env_bytes, PANEL_ENV_FILE, 0o600)
    await shell.put_bytes(env_bytes, _STAGED_ENV, 0o600)


async def stage_mqtt_ca(shell: PanelShell, ca_bytes: bytes) -> str:
    """Verify and atomically install an immutable, content-addressed MQTT CA."""
    if type(ca_bytes) is not bytes or not ca_bytes or b"PRIVATE KEY" in ca_bytes.upper():
        raise ValueError("invalid_mqtt_tls_ca")

    digest = hashlib.sha256(ca_bytes).hexdigest()
    remote_path = f"{PANEL_MQTT_TLS_DIR}/mqtt-ca-{digest[:16]}.pem"
    temp_path = f"{remote_path}.tmp-{secrets.token_hex(16)}"
    await _checked(shell, f"mkdir -p {PANEL_MQTT_TLS_DIR}")
    try:
        # put_bytes may truncate its target, so it is restricted to a unique
        # temporary path. The content-addressed final is never opened for write.
        await shell.put_bytes(ca_bytes, temp_path, 0o644)
        remote_digest = await _checked(shell, f"{_PANEL_SHA256SUM} -- {temp_path}")
        remote_fields = remote_digest.stdout.split(maxsplit=1)
        if not remote_fields or remote_fields[0] != digest:
            raise PanelOpError("mqtt_ca_verification_failed")

        # A hard link is an atomic no-replace promotion on the persistent
        # filesystem. If another run already installed this short-hash path,
        # accept it only when the complete bytes match and it is not a symlink.
        promoted = await shell.run(f"ln {temp_path} {remote_path}")
        if promoted.exit_status != 0:
            existing = await shell.run(
                f"test -f {remote_path} && test ! -L {remote_path} "
                f"&& cmp -s {temp_path} {remote_path}"
            )
            if existing.exit_status != 0:
                raise PanelOpError("mqtt_ca_promotion_failed")
            existing_mode = await shell.run(f"stat -c %a -- {remote_path}")
            if existing_mode.exit_status != 0 or existing_mode.stdout.strip() != "644":
                raise PanelOpError("mqtt_ca_promotion_failed")
    except BaseException:
        # Cleanup is best-effort after a primary failure. Never replace a
        # verification/transport/process-control outcome with a later rm error.
        try:
            await _checked(shell, f"rm -f {temp_path}")
        except Exception:
            pass
        raise

    # A successful install is not reported while its staging file remains.
    await _checked(shell, f"rm -f {temp_path}")
    return remote_path


async def ensure_configs(shell: PanelShell, unit_content: str, env_content: str) -> None:
    """Idempotently (re)write unit + env to /etc AND the /var staged copies.

    The staged copies are the OTA-proof restore source: /var survives firmware
    updates, /etc may not. Env files carry the broker password → 0600 both places.
    """
    await async_assert_no_mqtt_tls_downgrade(shell, env_content)
    await _checked(shell, f"mkdir -p {PANEL_STAGED_DIR}")
    await shell.put_bytes(unit_content.encode(), PANEL_UNIT_FILE, 0o644)
    await shell.put_bytes(env_content.encode(), PANEL_ENV_FILE, 0o600)
    await shell.put_bytes(unit_content.encode(), _STAGED_UNIT, 0o644)
    await shell.put_bytes(env_content.encode(), _STAGED_ENV, 0o600)
    await _checked(shell, "systemctl daemon-reload")


async def enable_now(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl enable --now {SERVICE_NAME}")


async def restart(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl restart {SERVICE_NAME}")


def journal_command(lines: int) -> str:
    return f"journalctl -u {SERVICE_NAME} -n {lines} --no-pager"


async def collect_journal(shell: PanelShell, lines: int = 50) -> str:
    return (await shell.run(journal_command(lines))).stdout


REBOOT_COMMAND = "reboot"
# Bound the reboot issue: the box goes down mid-command, so run() should return (via
# the disconnect) quickly. If sshd never returns an exit status before the panel is
# gone the timeout fires and — like the disconnect — counts as success.
_REBOOT_ISSUE_TIMEOUT_SECONDS = 20.0
DIAGNOSTICS_COUNT_LIMIT = 64 * 1024
_DIAGNOSTICS_PARSE_LIMIT = 64 * 1024


async def reboot(shell: PanelShell) -> None:
    """Reboot the panel, treating the inevitable mid-command disconnect as success.

    A tolerant sibling of `_checked`: `reboot` tears down sshd as the panel goes down,
    so `run()` routinely ends in a dropped connection / EOF (and, defensively, could
    hang with no exit status). Every one of those means the command took, so we bound
    the wait and swallow the transport error / timeout rather than raise. A clean return
    (systemd accepted the reboot and answered before dropping) is success too. Only the
    caller's connect step can meaningfully fail — once `reboot` is in flight, the box
    going down IS the expected outcome.
    """
    try:
        async with asyncio.timeout(_REBOOT_ISSUE_TIMEOUT_SECONDS):
            await shell.run(REBOOT_COMMAND)
    except (OSError, asyncssh.Error):
        # Expected: sshd went down with the panel (disconnect/EOF), or the channel
        # never returned before the box did (TimeoutError, itself an OSError subclass).
        return


def _diagnostics_probes(lines: int) -> tuple[tuple[str, str], ...]:
    """Return fixed probe IDs and read-only commands.

    Probe output is never persisted directly. ``collect_diagnostics`` reduces every
    result to a fixed-schema allowlist of numeric metrics, enum states, and event counts.
    """
    unit_lines = max(1, lines // 2)
    return (
        ("uptime", "cat /proc/uptime"),
        ("memory", "free -k"),
        ("var_filesystem", "df -Pk /var"),
        ("boot_events", f"journalctl -b -n {lines} --no-pager"),
        (
            "bridge_events",
            f"journalctl -u {SERVICE_NAME} -n {unit_lines} --no-pager",
        ),
        (
            "kernel_events",
            "journalctl -k | grep -iE 'wlan|brcm|dhd|deauth|disassoc|carrier|oom' | tail -60",
        ),
        ("wifi_link", "iw dev wlan0 link"),
        ("wifi_power_save", "iw dev wlan0 get power_save"),
        ("connman_services", "connmanctl services | head -n 15"),
        (
            "bridge_status",
            f"systemctl status {SERVICE_NAME} --no-pager | head -n 15",
        ),
    )


def _bounded_count(value: int) -> int:
    return min(max(value, 0), DIAGNOSTICS_COUNT_LIMIT)


def _text_counts(prefix: str, value: str) -> dict[str, int]:
    return {
        f"{prefix}_bytes": _bounded_count(len(value.encode("utf-8", errors="replace"))),
        f"{prefix}_lines": _bounded_count(len(value.splitlines())),
    }


def _safe_nonnegative_int(value: str) -> int | None:
    if not re.fullmatch(r"\d{1,18}", value):
        return None
    return int(value)


def _parse_uptime(stdout: str) -> dict[str, object]:
    for line in stdout[:_DIAGNOSTICS_PARSE_LIMIT].splitlines():
        match = re.fullmatch(
            r"\s*(\d{1,12})(?:\.\d{1,6})?\s+\d{1,12}(?:\.\d{1,6})?\s*",
            line,
        )
        if match is not None:
            return {"uptime_seconds": int(match.group(1))}
    return {}


def _parse_memory(stdout: str) -> dict[str, object]:
    for line in stdout[:_DIAGNOSTICS_PARSE_LIMIT].splitlines():
        fields = line.split()
        if len(fields) < 4 or fields[0] != "Mem:":
            continue
        total, used, free = (_safe_nonnegative_int(field) for field in fields[1:4])
        if total is not None and used is not None and free is not None:
            return {
                "total_kib": total,
                "used_kib": used,
                "free_kib": free,
            }
    return {}


def _parse_var_filesystem(stdout: str) -> dict[str, object]:
    for line in reversed(stdout[:_DIAGNOSTICS_PARSE_LIMIT].splitlines()):
        fields = line.split()
        if len(fields) < 6 or fields[-1] != "/var":
            continue
        total, used, available = (_safe_nonnegative_int(field) for field in fields[1:4])
        used_match = re.fullmatch(r"(\d{1,3})%", fields[4])
        if (
            total is not None
            and used is not None
            and available is not None
            and used_match is not None
            and int(used_match.group(1)) <= 100
        ):
            return {
                "total_kib": total,
                "used_kib": used,
                "available_kib": available,
                "used_percent": int(used_match.group(1)),
            }
    return {}


def _event_categories(stdout: str) -> dict[str, object]:
    counts: dict[str, int] = {}
    bounded = stdout[-_DIAGNOSTICS_PARSE_LIMIT:]
    if len(stdout) > _DIAGNOSTICS_PARSE_LIMIT:
        _partial, separator, bounded = bounded.partition("\n")
        if not separator:
            return {}
    failure_pattern = re.compile(
        r"\b(?:fail(?:ed|ure)?|error|denied|reject(?:ed)?|invalid|expired|"
        r"unable|timeout|timed out)\b"
    )
    for line in bounded.splitlines():
        normalized = line.casefold()
        categories: set[str] = set()
        failure = failure_pattern.search(normalized) is not None
        if (
            failure
            and "mqtt" in normalized
            and ("auth" in normalized or "credential" in normalized)
        ):
            categories.add("mqtt_authentication_failure")
        if failure and ("tls" in normalized or "certificate" in normalized):
            categories.add("tls_failure")
        if "deauth" in normalized or "disassoc" in normalized:
            categories.add("wifi_deauthentication")
        if "carrier" in normalized:
            categories.add("carrier_change")
        if "out of memory" in normalized or re.search(r"\boom\b", normalized):
            categories.add("out_of_memory")
        if "timeout" in normalized or "timed out" in normalized:
            categories.add("timeout")
        if failure:
            categories.add("bridge_failure")
        if "connman" in normalized and categories & {"timeout", "bridge_failure"}:
            categories.add("connman_failure")
        for category in categories:
            counts[category] = min(counts.get(category, 0) + 1, DIAGNOSTICS_COUNT_LIMIT)
    return {"categories": dict(sorted(counts.items()))} if counts else {}


def _parse_wifi_link(stdout: str) -> dict[str, object]:
    bounded = stdout[:_DIAGNOSTICS_PARSE_LIMIT]
    normalized = bounded.casefold()
    result: dict[str, object] = {}
    if "not connected" in normalized:
        result["wifi_state"] = "disconnected"
    elif re.search(r"(?m)^\s*connected to\s", normalized):
        result["wifi_state"] = "connected"
    signal = re.search(r"(?im)^\s*signal:\s*(-?\d{1,3})\s*dBm\s*$", bounded)
    if signal is not None:
        rssi = int(signal.group(1))
        if -200 <= rssi <= 0:
            result["rssi_dbm"] = rssi
    return result


def _parse_power_save(stdout: str) -> dict[str, object]:
    match = re.search(
        r"(?im)^\s*power save:\s*(on|off)\s*$",
        stdout[:_DIAGNOSTICS_PARSE_LIMIT],
    )
    return {"power_save": match.group(1).lower()} if match is not None else {}


def _parse_connman_services(stdout: str) -> dict[str, object]:
    count = sum(bool(line.strip()) for line in stdout[:_DIAGNOSTICS_PARSE_LIMIT].splitlines())
    return {"service_count": _bounded_count(count)}


def _parse_bridge_status(stdout: str) -> dict[str, object]:
    bounded = stdout[:_DIAGNOSTICS_PARSE_LIMIT]
    result: dict[str, object] = {}
    active = re.search(
        r"(?im)^\s*Active:\s*(active|inactive|failed|activating|deactivating)\b",
        bounded,
    )
    if active is not None:
        result["active_state"] = active.group(1).lower()
    enabled = re.search(
        r"(?im)^\s*Loaded:.*;\s*(enabled|disabled|static|masked)\s*[;)]",
        bounded,
    )
    if enabled is not None:
        result["enabled_state"] = enabled.group(1).lower()
    return result


def _parse_diagnostics_probe(probe_id: str, stdout: str) -> dict[str, object]:
    if probe_id == "uptime":
        return _parse_uptime(stdout)
    if probe_id == "memory":
        return _parse_memory(stdout)
    if probe_id == "var_filesystem":
        return _parse_var_filesystem(stdout)
    if probe_id in {"boot_events", "bridge_events", "kernel_events"}:
        return _event_categories(stdout)
    if probe_id == "wifi_link":
        return _parse_wifi_link(stdout)
    if probe_id == "wifi_power_save":
        return _parse_power_save(stdout)
    if probe_id == "connman_services":
        return _parse_connman_services(stdout)
    if probe_id == "bridge_status":
        return _parse_bridge_status(stdout)
    return {}


async def _diagnostics_probe(
    shell: PanelShell,
    probe_id: str,
    command: str,
) -> dict[str, object]:
    """Reduce one probe to an allowlisted summary without retaining raw content."""
    try:
        result = await shell.run(command)
    except (OSError, asyncssh.Error):
        return {"id": probe_id, "outcome": "transport_error"}
    stdout = result.stdout if isinstance(result.stdout, str) else ""
    stderr = result.stderr if isinstance(result.stderr, str) else ""
    summary: dict[str, object] = {
        "id": probe_id,
        "outcome": "ok" if result.exit_status == 0 else "nonzero",
        **_text_counts("stdout", stdout),
        **_text_counts("stderr", stderr),
    }
    summary.update(_parse_diagnostics_probe(probe_id, stdout))
    return summary


async def collect_diagnostics(shell: PanelShell, lines: int = DEFAULT_REBOOT_JOURNAL_LINES) -> str:
    """Assemble a secret-safe, typed pre-reboot diagnostics summary.

    The panel's journald is volatile (/run tmpfs — only the current boot survives a
    reboot), so known wedge categories are counted before reboot. Original stdout,
    stderr, journal lines, command strings, SSIDs, MACs, and exception text are never
    serialized. Each probe is best-effort; a transport failure becomes a stable outcome.
    """
    bounded_lines = min(max(lines, 1), MAX_REBOOT_JOURNAL_LINES)
    probes = [
        await _diagnostics_probe(shell, probe_id, command)
        for probe_id, command in _diagnostics_probes(bounded_lines)
    ]
    return (
        json.dumps(
            {
                "journal_line_limit": bounded_lines,
                "probes": probes,
                "schema_version": 1,
            },
            separators=(",", ":"),
            sort_keys=True,
        )
        + "\n"
    )


async def deploy_payload(shell: PanelShell, local_payload_dir: str, version: str) -> None:
    """Upload the bundled agent payload and swap it into place.

    *local_payload_dir* must contain app/ and vendor/ (built by
    scripts/build_payload.sh). Full upload into a staging dir first so a failed
    transfer never half-replaces a working install; the in-place swap moves the
    current app/vendor aside (not rm) so a mid-swap mv failure stays recoverable.
    """
    await shell.run(f"rm -rf {_STAGING_DIR}")
    await shell.put_dir(local_payload_dir, _STAGING_DIR)
    await _checked(shell, _swap_command())
    await shell.put_bytes(version.encode(), PANEL_VERSION_FILE, 0o644)


def _swap_command() -> str:
    """The move-aside swap. Old dirs go to *.bak before the new ones land; the
    backups are removed only once both new moves succeed, so any single failed
    mv leaves a restorable .bak rather than an app-less panel.
    """
    return " && ".join(
        [
            f"mkdir -p {PANEL_VAR_DIR}",
            f"rm -rf {PANEL_VAR_DIR}/app.bak {PANEL_VAR_DIR}/vendor.bak",
            f"{{ [ -e {PANEL_VAR_DIR}/app ] && "
            f"mv {PANEL_VAR_DIR}/app {PANEL_VAR_DIR}/app.bak; true; }}",
            f"{{ [ -e {PANEL_VAR_DIR}/vendor ] && "
            f"mv {PANEL_VAR_DIR}/vendor {PANEL_VAR_DIR}/vendor.bak; true; }}",
            f"mv {_STAGING_DIR}/app {PANEL_VAR_DIR}/app",
            f"mv {_STAGING_DIR}/vendor {PANEL_VAR_DIR}/vendor",
            f"rm -rf {PANEL_VAR_DIR}/app.bak {PANEL_VAR_DIR}/vendor.bak {_STAGING_DIR}",
        ]
    )


async def uninstall(shell: PanelShell) -> None:
    """Stop + disable + remove everything the integration owns on the panel.

    The disable step is best-effort (`|| true`) — the unit may already be gone —
    but the removals and the reload must actually succeed or we'd leave orphaned
    files behind a "removed" config entry.
    """
    await shell.run(f"systemctl disable --now {SERVICE_NAME} 2>/dev/null || true")
    await _checked(shell, f"rm -f {PANEL_UNIT_FILE} {PANEL_ENV_FILE}")
    await _checked(shell, f"rm -rf {PANEL_VAR_DIR} {_STAGING_DIR}")
    await _checked(shell, "systemctl daemon-reload")


# ---------------------------------------------------------------------------
# Voice satellite recipes
# ---------------------------------------------------------------------------

VOICE_INSPECT_COMMAND = (
    f"test -f {PANEL_VOICE_UNIT_FILE} && echo unit=1 || echo unit=0; "
    f"test -f {PANEL_VOICE_ENV_FILE} && echo env=1 || echo env=0; "
    f"systemctl is-enabled {VOICE_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo enabled=1 || echo enabled=0; "
    f"systemctl is-active {VOICE_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo active=1 || echo active=0; "
    f"test -f {PANEL_VOICE_VAR_DIR}/app/brilliant_voice/__main__.py "
    f"&& test -d {PANEL_VOICE_VAR_DIR}/lva && test -d {PANEL_VOICE_VAR_DIR}/python "
    f"&& echo payload=1 || echo payload=0; "
    f"cat {PANEL_VOICE_VERSION_FILE} 2>/dev/null || true"
)


@dataclass(frozen=True)
class VoicePanelState:
    """Parsed result of one inspect_voice() probe."""

    unit_present: bool
    env_present: bool
    enabled: bool
    active: bool
    payload_present: bool  # supervisor entry + lva/ + python/ all present
    payload_version: str | None


async def inspect_voice(shell: PanelShell) -> VoicePanelState:
    """Probe voice install/health state in one shell round-trip."""
    result = await shell.run(VOICE_INSPECT_COMMAND)
    flags: dict[str, bool] = {}
    version: str | None = None
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and key in ("unit", "env", "enabled", "active", "payload"):
            flags[key] = value == "1"
        elif line.strip():
            version = line.strip()
    return VoicePanelState(
        unit_present=flags.get("unit", False),
        env_present=flags.get("env", False),
        enabled=flags.get("enabled", False),
        active=flags.get("active", False),
        payload_present=flags.get("payload", False),
        payload_version=version,
    )


def render_voice_env(
    panel: str,
    name: str,
    api_port: int,
    wake_word: str,
    ha_host: str,
    enable_aec: bool,
) -> str:
    """Render /etc/brilliant-voice.env — exactly what brilliant_voice/config.py reads."""
    return (
        f"BRILLIANT_PANEL={_env_quote(panel)}\n"
        f"VOICE_NAME={_env_quote(name)}\n"
        f"VOICE_API_PORT={api_port}\n"
        f"VOICE_WAKE_WORD={_env_quote(wake_word)}\n"
        f"VOICE_HA_HOST={_env_quote(ha_host)}\n"
        f"VOICE_ENABLE_AEC={1 if enable_aec else 0}\n"
        f"LOG_LEVEL=INFO\n"
    )


async def deploy_voice_payload(shell: PanelShell, local_tarball_path: str, version: str) -> None:
    """Upload the voice payload tarball and swap the extracted tree into place.

    *local_tarball_path* is the downloaded brilliant-voice-payload-*.tar.gz (its top
    dir is brilliant-voice/, stripped on extract). Stage+swap so a failed transfer
    or extract never half-replaces a working install; the current tree is moved aside
    (not rm) so a mid-swap mv failure stays recoverable.
    """
    await shell.run(f"rm -rf {_VOICE_STAGING_DIR} {_VOICE_STAGING_TARBALL}")
    await shell.put_file(local_tarball_path, _VOICE_STAGING_TARBALL, 0o644)
    await _checked(shell, _voice_swap_command())
    await shell.put_bytes(version.encode(), PANEL_VOICE_VERSION_FILE, 0o644)


def _voice_swap_command() -> str:
    return " && ".join(
        [
            f"mkdir -p {_VOICE_STAGING_DIR}",
            f"tar xzf {_VOICE_STAGING_TARBALL} -C {_VOICE_STAGING_DIR} --strip-components=1",
            f"rm -f {_VOICE_STAGING_TARBALL}",
            f"rm -rf {PANEL_VOICE_VAR_DIR}.bak",
            f"{{ [ -e {PANEL_VOICE_VAR_DIR} ] && "
            f"mv {PANEL_VOICE_VAR_DIR} {PANEL_VOICE_VAR_DIR}.bak; true; }}",
            f"mv {_VOICE_STAGING_DIR} {PANEL_VOICE_VAR_DIR}",
            f"rm -rf {PANEL_VOICE_VAR_DIR}.bak",
        ]
    )


async def ensure_voice_config(shell: PanelShell, env_content: str) -> None:
    """Install the voice unit (from the deployed payload) + write env to /etc + staged.

    The systemd unit lives in /var (inside the payload, OTA-persistent); /etc is
    OTA-replaced, so copy the unit into /etc and write the env to both /etc and the
    /var staged copy. The env carries no secret but stays 0600 to match the bridge.
    """
    await _checked(shell, f"mkdir -p {PANEL_VOICE_STAGED_DIR}")
    await _checked(
        shell, f"cp {PANEL_VOICE_VAR_DIR}/{VOICE_SERVICE_NAME}.service {PANEL_VOICE_UNIT_FILE}"
    )
    env_bytes = env_content.encode()
    await shell.put_bytes(env_bytes, PANEL_VOICE_ENV_FILE, 0o600)
    await shell.put_bytes(env_bytes, _VOICE_STAGED_ENV, 0o600)
    await _checked(shell, "systemctl daemon-reload")


async def enable_voice(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl enable --now {VOICE_SERVICE_NAME}")


async def restart_voice(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl restart {VOICE_SERVICE_NAME}")


async def uninstall_voice(shell: PanelShell) -> None:
    """Stop + disable + remove everything the voice feature owns on the panel."""
    await shell.run(f"systemctl disable --now {VOICE_SERVICE_NAME} 2>/dev/null || true")
    await _checked(shell, f"rm -f {PANEL_VOICE_UNIT_FILE} {PANEL_VOICE_ENV_FILE}")
    await _checked(
        shell, f"rm -rf {PANEL_VOICE_VAR_DIR} {_VOICE_STAGING_DIR} {_VOICE_STAGING_TARBALL}"
    )
    await _checked(shell, "systemctl daemon-reload")


# ---------------------------------------------------------------------------
# HA mirror recipes
# ---------------------------------------------------------------------------

HA_MIRROR_INSPECT_COMMAND = (
    f"test -f {PANEL_HA_MIRROR_UNIT_FILE} && echo unit=1 || echo unit=0; "
    f"test -f {PANEL_HA_MIRROR_ENV_FILE} && echo env=1 || echo env=0; "
    f"systemctl is-enabled {HA_MIRROR_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo enabled=1 || echo enabled=0; "
    f"systemctl is-active {HA_MIRROR_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo active=1 || echo active=0; "
    f"test -f {_HA_MIRROR_STAGED_ENV} && echo senv=1 || echo senv=0; "
    f"test -f {PANEL_HA_MIRROR_APP_DIR}/brilliant_ha_mirror/__main__.py "
    f"&& echo payload=1 || echo payload=0"
)


@dataclass(frozen=True)
class HaMirrorState:
    """Parsed result of one inspect_ha_mirror() probe."""

    unit_present: bool
    env_present: bool
    enabled: bool
    active: bool
    staged_env_present: bool
    payload_present: bool


async def inspect_ha_mirror(shell: PanelShell) -> HaMirrorState:
    """Probe HA mirror state, accepting only a complete unambiguous proof."""
    result = await shell.run(HA_MIRROR_INSPECT_COMMAND)
    expected = {"unit", "env", "enabled", "active", "senv", "payload"}
    if result.exit_status != 0:
        raise PanelOpError("HA mirror inspection failed")
    flags: dict[str, bool] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if not sep or key not in expected or key in flags or value not in {"0", "1"}:
            raise PanelOpError("HA mirror inspection returned ambiguous state")
        flags[key] = value == "1"
    if set(flags) != expected:
        raise PanelOpError("HA mirror inspection returned incomplete state")
    return HaMirrorState(
        unit_present=flags["unit"],
        env_present=flags["env"],
        enabled=flags["enabled"],
        active=flags["active"],
        staged_env_present=flags["senv"],
        payload_present=flags["payload"],
    )


def render_ha_mirror_env(
    *,
    panel: str,
    ha_ws_url: str,
    ha_token: str,
    mirror_label: str,
    leader_priority: int,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str,
    mqtt_password: str,
) -> str:
    """Render the HA mirror env file, including secret and leader-election settings."""
    return (
        f"PANEL={_env_quote(panel)}\n"
        f"HA_WS_URL={_env_quote(ha_ws_url)}\n"
        f"HA_TOKEN={_env_quote(ha_token)}\n"
        f"MIRROR_LABEL={_env_quote(mirror_label)}\n"
        f"LEADER_PRIORITY={_env_quote(str(leader_priority))}\n"
        f"MQTT_HOST={_env_quote(mqtt_host)}\n"
        f"MQTT_PORT={_env_quote(str(mqtt_port))}\n"
        f"MQTT_USERNAME={_env_quote(mqtt_username)}\n"
        f"MQTT_PASSWORD={_env_quote(mqtt_password)}\n"
        "LOG_LEVEL=INFO\n"
    )


async def deploy_ha_mirror(shell: PanelShell, local_dir: str) -> None:
    """Upload local_dir (contains brilliant_ha_mirror/) and swap its app into place."""
    await shell.run(f"rm -rf {_HA_MIRROR_STAGING_DIR}")
    await shell.put_dir(local_dir, _HA_MIRROR_STAGING_DIR)
    await _checked(shell, _ha_mirror_swap_command())


def _ha_mirror_swap_command() -> str:
    """Move-aside swap for the HA mirror app directory."""
    return " && ".join(
        [
            f"mkdir -p {PANEL_HA_MIRROR_VAR_DIR}",
            f"rm -rf {PANEL_HA_MIRROR_APP_DIR}.bak",
            f"{{ [ -e {PANEL_HA_MIRROR_APP_DIR} ] && "
            f"mv {PANEL_HA_MIRROR_APP_DIR} {PANEL_HA_MIRROR_APP_DIR}.bak; true; }}",
            f"mv {_HA_MIRROR_STAGING_DIR} {PANEL_HA_MIRROR_APP_DIR}",
            f"rm -rf {PANEL_HA_MIRROR_APP_DIR}.bak",
        ]
    )


async def ensure_ha_mirror_config(shell: PanelShell, unit_content: str, env_content: str) -> None:
    """Write the HA mirror unit and live/staged secret env, then reload systemd."""
    await _checked(shell, f"mkdir -p {PANEL_HA_MIRROR_STAGED_DIR}")
    await shell.put_bytes(unit_content.encode(), PANEL_HA_MIRROR_UNIT_FILE, 0o644)
    env_bytes = env_content.encode()
    await shell.put_bytes(env_bytes, PANEL_HA_MIRROR_ENV_FILE, 0o600)
    await shell.put_bytes(env_bytes, _HA_MIRROR_STAGED_ENV, 0o600)
    await _checked(shell, "systemctl daemon-reload")


async def enable_ha_mirror(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl enable --now {HA_MIRROR_SERVICE_NAME}")


async def uninstall_ha_mirror(shell: PanelShell) -> None:
    """Stop + disable + remove everything the HA mirror feature owns on the panel."""
    await shell.run(f"systemctl disable --now {HA_MIRROR_SERVICE_NAME} 2>/dev/null || true")
    await _checked(shell, f"rm -f {PANEL_HA_MIRROR_UNIT_FILE} {PANEL_HA_MIRROR_ENV_FILE}")
    await _checked(shell, f"rm -rf {PANEL_HA_MIRROR_VAR_DIR}")
    await _checked(shell, "systemctl daemon-reload")


# ---------------------------------------------------------------------------
# Wi-Fi watchdog recipes
# ---------------------------------------------------------------------------

_WATCHDOG_STAGING_DIR = f"{PANEL_WIFI_WATCHDOG_DIR}.staging"
_WATCHDOG_STAGED_UNIT = f"{PANEL_WIFI_WATCHDOG_DIR}/{WIFI_WATCHDOG_SERVICE_NAME}.service"
_WATCHDOG_LOG_FILE = f"{PANEL_VAR_DIR}/wifi-watchdog.log"
_WATCHDOG_STATE_FILE = f"{PANEL_VAR_DIR}/wifi-watchdog.state"

WIFI_WATCHDOG_INSPECT_COMMAND = (
    f"test -f {PANEL_WIFI_WATCHDOG_UNIT_FILE} && echo unit=1 || echo unit=0; "
    f"systemctl is-enabled {WIFI_WATCHDOG_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo enabled=1 || echo enabled=0; "
    f"systemctl is-active {WIFI_WATCHDOG_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo active=1 || echo active=0; "
    f"test -f {PANEL_WIFI_WATCHDOG_DIR}/brilliant_wifi_watchdog/run.py "
    f"&& echo payload=1 || echo payload=0"
)


@dataclass(frozen=True)
class WifiWatchdogState:
    """Parsed result of one inspect_wifi_watchdog() probe."""

    unit_present: bool
    enabled: bool
    active: bool
    payload_present: bool  # run.py entrypoint present inside PANEL_WIFI_WATCHDOG_DIR


async def inspect_wifi_watchdog(shell: PanelShell) -> WifiWatchdogState:
    """Probe Wi-Fi watchdog install/health state in one shell round-trip."""
    result = await shell.run(WIFI_WATCHDOG_INSPECT_COMMAND)
    flags: dict[str, bool] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and key in ("unit", "enabled", "active", "payload"):
            flags[key] = value == "1"
    return WifiWatchdogState(
        unit_present=flags.get("unit", False),
        enabled=flags.get("enabled", False),
        active=flags.get("active", False),
        payload_present=flags.get("payload", False),
    )


async def deploy_wifi_watchdog(shell: PanelShell, local_dir: str) -> None:
    """Upload local_dir (contains brilliant_wifi_watchdog/) and swap into place.

    *local_dir* is the integration payload's agent_payload/wifi_watchdog/, which
    contains the brilliant_wifi_watchdog/ package. Stage+swap via put_dir so a
    failed transfer never half-replaces a working install; the current dir is moved
    aside (.bak) so a mid-swap failure stays recoverable.

    Result on panel: {PANEL_WIFI_WATCHDOG_DIR}/brilliant_wifi_watchdog/run.py.
    """
    await shell.run(f"rm -rf {_WATCHDOG_STAGING_DIR}")
    await shell.put_dir(local_dir, _WATCHDOG_STAGING_DIR)
    await _checked(shell, _watchdog_swap_command())


def _watchdog_swap_command() -> str:
    """Move-aside swap for the watchdog directory.

    Ensures the parent /var/brilliant-mqtt exists (bridge already creates it, but
    the watchdog may be deployed standalone), moves the current install aside to
    .bak, puts the staged dir in place, then drops the backup.  A single failed
    mv leaves a restorable .bak rather than a missing install.
    """
    return " && ".join(
        [
            f"mkdir -p {PANEL_VAR_DIR}",
            f"rm -rf {PANEL_WIFI_WATCHDOG_DIR}.bak",
            f"{{ [ -e {PANEL_WIFI_WATCHDOG_DIR} ] && "
            f"mv {PANEL_WIFI_WATCHDOG_DIR} {PANEL_WIFI_WATCHDOG_DIR}.bak; true; }}",
            f"mv {_WATCHDOG_STAGING_DIR} {PANEL_WIFI_WATCHDOG_DIR}",
            f"rm -rf {PANEL_WIFI_WATCHDOG_DIR}.bak",
        ]
    )


async def ensure_wifi_watchdog_unit(shell: PanelShell, unit_content: str) -> None:
    """Write the watchdog unit to /etc and a staged OTA-proof copy, then reload.

    The watchdog has no env file of its own; the systemd unit uses
    EnvironmentFile=-/etc/brilliant-mqtt.env (optional, shared with the bridge).
    """
    await _checked(shell, f"mkdir -p {PANEL_WIFI_WATCHDOG_DIR}")
    unit_bytes = unit_content.encode()
    await shell.put_bytes(unit_bytes, PANEL_WIFI_WATCHDOG_UNIT_FILE, 0o644)
    await shell.put_bytes(unit_bytes, _WATCHDOG_STAGED_UNIT, 0o644)
    await _checked(shell, "systemctl daemon-reload")


async def enable_wifi_watchdog(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl enable --now {WIFI_WATCHDOG_SERVICE_NAME}")


async def uninstall_wifi_watchdog(shell: PanelShell) -> None:
    """Stop + disable + remove everything the Wi-Fi watchdog owns on the panel.

    Removes the unit file, the code directory (+ staging sibling), and the
    watchdog's persistent log/state files that live as siblings to the bridge's
    PANEL_VAR_DIR.  PANEL_VAR_DIR itself is NOT removed — it belongs to the bridge.
    """
    await shell.run(f"systemctl disable --now {WIFI_WATCHDOG_SERVICE_NAME} 2>/dev/null || true")
    await _checked(shell, f"rm -f {PANEL_WIFI_WATCHDOG_UNIT_FILE}")
    await _checked(shell, f"rm -rf {PANEL_WIFI_WATCHDOG_DIR} {_WATCHDOG_STAGING_DIR}")
    await _checked(shell, f"rm -f {_WATCHDOG_LOG_FILE} {_WATCHDOG_STATE_FILE}")
    await _checked(shell, "systemctl daemon-reload")


# ---------------------------------------------------------------------------
# Bus-health watchdog recipes
# ---------------------------------------------------------------------------

_BUS_WATCHDOG_STAGING_DIR = f"{PANEL_BUS_WATCHDOG_DIR}.staging"
_BUS_WATCHDOG_STAGED_UNIT = f"{PANEL_BUS_WATCHDOG_DIR}/{BUS_WATCHDOG_SERVICE_NAME}.service"
_BUS_WATCHDOG_LOG_FILE = f"{PANEL_VAR_DIR}/bus-watchdog.log"
_BUS_WATCHDOG_STATE_FILE = f"{PANEL_VAR_DIR}/bus-watchdog.state"

BUS_WATCHDOG_INSPECT_COMMAND = (
    f"test -f {PANEL_BUS_WATCHDOG_UNIT_FILE} && echo unit=1 || echo unit=0; "
    f"systemctl is-enabled {BUS_WATCHDOG_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo enabled=1 || echo enabled=0; "
    f"systemctl is-active {BUS_WATCHDOG_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo active=1 || echo active=0; "
    f"test -f {PANEL_BUS_WATCHDOG_DIR}/brilliant_bus_watchdog/run.py "
    f"&& echo payload=1 || echo payload=0"
)


@dataclass(frozen=True)
class BusWatchdogState:
    """Parsed result of one inspect_bus_watchdog() probe."""

    unit_present: bool
    enabled: bool
    active: bool
    payload_present: bool  # run.py entrypoint present inside PANEL_BUS_WATCHDOG_DIR


async def inspect_bus_watchdog(shell: PanelShell) -> BusWatchdogState:
    """Probe bus-health watchdog install/health state in one shell round-trip."""
    result = await shell.run(BUS_WATCHDOG_INSPECT_COMMAND)
    flags: dict[str, bool] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and key in ("unit", "enabled", "active", "payload"):
            flags[key] = value == "1"
    return BusWatchdogState(
        unit_present=flags.get("unit", False),
        enabled=flags.get("enabled", False),
        active=flags.get("active", False),
        payload_present=flags.get("payload", False),
    )


async def deploy_bus_watchdog(shell: PanelShell, local_dir: str) -> None:
    """Upload local_dir (contains brilliant_bus_watchdog/) and swap into place.

    *local_dir* is the integration payload's agent_payload/bus_watchdog/, which
    contains the brilliant_bus_watchdog/ package. Stage+swap via put_dir so a
    failed transfer never half-replaces a working install; the current dir is moved
    aside (.bak) so a mid-swap failure stays recoverable.

    Result on panel: {PANEL_BUS_WATCHDOG_DIR}/brilliant_bus_watchdog/run.py.
    """
    await shell.run(f"rm -rf {_BUS_WATCHDOG_STAGING_DIR}")
    await shell.put_dir(local_dir, _BUS_WATCHDOG_STAGING_DIR)
    await _checked(shell, _bus_watchdog_swap_command())


def _bus_watchdog_swap_command() -> str:
    """Move-aside swap for the bus watchdog directory.

    Ensures the parent /var/brilliant-mqtt exists (bridge already creates it, but
    the watchdog may be deployed standalone), moves the current install aside to
    .bak, puts the staged dir in place, then drops the backup.  A single failed
    mv leaves a restorable .bak rather than a missing install.
    """
    return " && ".join(
        [
            f"mkdir -p {PANEL_VAR_DIR}",
            f"rm -rf {PANEL_BUS_WATCHDOG_DIR}.bak",
            f"{{ [ -e {PANEL_BUS_WATCHDOG_DIR} ] && "
            f"mv {PANEL_BUS_WATCHDOG_DIR} {PANEL_BUS_WATCHDOG_DIR}.bak; true; }}",
            f"mv {_BUS_WATCHDOG_STAGING_DIR} {PANEL_BUS_WATCHDOG_DIR}",
            f"rm -rf {PANEL_BUS_WATCHDOG_DIR}.bak",
        ]
    )


async def ensure_bus_watchdog_unit(shell: PanelShell, unit_content: str) -> None:
    """Write the bus watchdog unit to /etc and a staged OTA-proof copy, then reload.

    The watchdog has no env file of its own; the systemd unit uses
    EnvironmentFile=-/etc/brilliant-mqtt.env (optional, shared with the bridge).
    """
    await _checked(shell, f"mkdir -p {PANEL_BUS_WATCHDOG_DIR}")
    unit_bytes = unit_content.encode()
    await shell.put_bytes(unit_bytes, PANEL_BUS_WATCHDOG_UNIT_FILE, 0o644)
    await shell.put_bytes(unit_bytes, _BUS_WATCHDOG_STAGED_UNIT, 0o644)
    await _checked(shell, "systemctl daemon-reload")


async def enable_bus_watchdog(shell: PanelShell) -> None:
    await _checked(shell, f"systemctl enable --now {BUS_WATCHDOG_SERVICE_NAME}")


async def uninstall_bus_watchdog(shell: PanelShell) -> None:
    """Stop + disable + remove everything the bus watchdog owns on the panel.

    Removes the unit file, the code directory (+ staging sibling), and the
    watchdog's persistent log/state files that live as siblings to the bridge's
    PANEL_VAR_DIR.  PANEL_VAR_DIR itself is NOT removed — it belongs to the bridge.
    """
    await shell.run(f"systemctl disable --now {BUS_WATCHDOG_SERVICE_NAME} 2>/dev/null || true")
    await _checked(shell, f"rm -f {PANEL_BUS_WATCHDOG_UNIT_FILE}")
    await _checked(shell, f"rm -rf {PANEL_BUS_WATCHDOG_DIR} {_BUS_WATCHDOG_STAGING_DIR}")
    await _checked(shell, f"rm -f {_BUS_WATCHDOG_LOG_FILE} {_BUS_WATCHDOG_STATE_FILE}")
    await _checked(shell, "systemctl daemon-reload")


# ---------------------------------------------------------------------------
# diyHue CA-recovery hook recipes
# ---------------------------------------------------------------------------

_HUE_CA_STAGING_DIR = f"{PANEL_HUE_CA_DIR}.staging"
# Staged OTA-proof unit copies, mirroring _BUS_WATCHDOG_STAGED_UNIT. Two units (the
# hook is a periodic oneshot: a .service the timer activates + the .timer itself).
_HUE_CA_STAGED_SERVICE = f"{PANEL_HUE_CA_DIR}/{os.path.basename(PANEL_HUE_CA_SERVICE_UNIT_FILE)}"
_HUE_CA_STAGED_TIMER = f"{PANEL_HUE_CA_DIR}/{os.path.basename(PANEL_HUE_CA_TIMER_UNIT_FILE)}"

HUE_CA_INSPECT_COMMAND = (
    f"test -f {PANEL_HUE_CA_DIR}/brilliant_hue_ca/run.py && echo payload=1 || echo payload=0"
)


@dataclass(frozen=True)
class HueCaState:
    """Parsed result of one inspect_hue_ca() probe."""

    payload_present: bool  # run.py entrypoint present inside PANEL_HUE_CA_DIR


async def inspect_hue_ca(shell: PanelShell) -> HueCaState:
    """Probe diyHue CA-recovery hook install state in one shell round-trip."""
    result = await shell.run(HUE_CA_INSPECT_COMMAND)
    flags: dict[str, bool] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if sep and key == "payload":
            flags[key] = value == "1"
    return HueCaState(payload_present=flags.get("payload", False))


async def deploy_hue_ca(shell: PanelShell, local_dir: str, ca_pem: str) -> None:
    """Upload local_dir (contains brilliant_hue_ca/), swap into place, write the CA.

    *local_dir* is the integration payload's agent_payload/hue_ca/, which contains
    the brilliant_hue_ca/ package. Stage+swap via put_dir so a failed transfer never
    half-replaces a working install; the current dir is moved aside (.bak) so a
    mid-swap failure stays recoverable. The operator's CA PEM is written LAST, after
    the swap succeeds, to its own top-level path (PANEL_HUE_CA_CERT_FILE, outside
    PANEL_HUE_CA_DIR) so a failed code swap never leaves a stale/half-written CA.

    Result on panel: {PANEL_HUE_CA_DIR}/brilliant_hue_ca/run.py + PANEL_HUE_CA_CERT_FILE.
    """
    await shell.run(f"rm -rf {_HUE_CA_STAGING_DIR}")
    await shell.put_dir(local_dir, _HUE_CA_STAGING_DIR)
    await _checked(shell, _hue_ca_swap_command())
    await _checked(shell, f"mkdir -p {os.path.dirname(PANEL_HUE_CA_CERT_FILE)}")
    await shell.put_bytes(ca_pem.encode(), PANEL_HUE_CA_CERT_FILE, 0o644)


def _hue_ca_swap_command() -> str:
    """Move-aside swap for the hue-ca recovery hook directory.

    Ensures the parent /var/brilliant-mqtt exists (bridge already creates it, but
    the hook may be deployed standalone), moves the current install aside to .bak,
    puts the staged dir in place, then drops the backup. A single failed mv leaves
    a restorable .bak rather than a missing install.
    """
    return " && ".join(
        [
            f"mkdir -p {PANEL_VAR_DIR}",
            f"rm -rf {PANEL_HUE_CA_DIR}.bak",
            f"{{ [ -e {PANEL_HUE_CA_DIR} ] && "
            f"mv {PANEL_HUE_CA_DIR} {PANEL_HUE_CA_DIR}.bak; true; }}",
            f"mv {_HUE_CA_STAGING_DIR} {PANEL_HUE_CA_DIR}",
            f"rm -rf {PANEL_HUE_CA_DIR}.bak",
        ]
    )


async def ensure_hue_ca_units(shell: PanelShell, service_content: str, timer_content: str) -> None:
    """Write the hue-ca service+timer to /etc and staged OTA-proof copies, then reload.

    The hook has no env file of its own — the oneshot service execs the deployed
    run.py directly, which reads PANEL_HUE_CA_CERT_FILE itself. Both units are
    staged under PANEL_HUE_CA_DIR (like _BUS_WATCHDOG_STAGED_UNIT) so a firmware
    OTA that wipes /etc leaves a restorable copy in the OTA-proof /var tree.
    """
    await _checked(shell, f"mkdir -p {PANEL_HUE_CA_DIR}")
    service_bytes = service_content.encode()
    timer_bytes = timer_content.encode()
    await shell.put_bytes(service_bytes, PANEL_HUE_CA_SERVICE_UNIT_FILE, 0o644)
    await shell.put_bytes(timer_bytes, PANEL_HUE_CA_TIMER_UNIT_FILE, 0o644)
    await shell.put_bytes(service_bytes, _HUE_CA_STAGED_SERVICE, 0o644)
    await shell.put_bytes(timer_bytes, _HUE_CA_STAGED_TIMER, 0o644)
    await _checked(shell, "systemctl daemon-reload")


async def enable_hue_ca(shell: PanelShell) -> None:
    """Enable + start the TIMER (not the oneshot service — the timer activates it)."""
    await _checked(shell, f"systemctl enable --now {HUE_CA_TIMER_NAME}")


async def uninstall_hue_ca(shell: PanelShell) -> None:
    """Stop + disable + remove everything the hue-ca recovery hook owns on the panel.

    Removes both unit files, the code directory (+ staging sibling), and the
    operator's injected CA cert. PANEL_VAR_DIR itself is NOT removed — it belongs
    to the bridge.
    """
    await shell.run(f"systemctl disable --now {HUE_CA_TIMER_NAME} 2>/dev/null || true")
    await _checked(shell, f"rm -f {PANEL_HUE_CA_SERVICE_UNIT_FILE} {PANEL_HUE_CA_TIMER_UNIT_FILE}")
    await _checked(shell, f"rm -rf {PANEL_HUE_CA_DIR} {_HUE_CA_STAGING_DIR}")
    await _checked(shell, f"rm -f {PANEL_HUE_CA_CERT_FILE}")
    await _checked(shell, "systemctl daemon-reload")
