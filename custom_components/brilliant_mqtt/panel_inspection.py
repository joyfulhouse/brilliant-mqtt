"""Bounded, read-only compatibility inspection for a pinned Brilliant panel."""

from __future__ import annotations

import asyncio
import re
from dataclasses import dataclass

from .const import (
    BUS_WATCHDOG_SERVICE_NAME,
    HA_MIRROR_SERVICE_NAME,
    HUE_CA_TIMER_NAME,
    PANEL_VERSION_FILE,
    SERVICE_NAME,
    VOICE_SERVICE_NAME,
    WIFI_WATCHDOG_SERVICE_NAME,
)
from .shell import HostIdentity, PanelIdentityError, PanelShell, RunResult

_INSPECTION_TIMEOUT = 15.0
_MAX_OUTPUT_BYTES = 16_384
_MIN_AVAILABLE_BYTES = 64 * 1024 * 1024
_MIN_AVAILABLE_MEMORY_BYTES = 32 * 1024 * 1024
_MAX_KIB_VALUE = (2**63 - 1) // 1024

_PANEL_PYTHON = "/data/switch-embedded/env/bin/python3"
_MODEL_PATH = "/sys/firmware/devicetree/base/model"
_RELEASE_INFO_PATH = "/etc/release_info.json"
_SUPPORTED_ARCHITECTURES = frozenset({"armv7l"})
_SERVICE_STATES = frozenset(
    {
        "active",
        "activating",
        "deactivating",
        "failed",
        "inactive",
        "reloading",
        "unknown",
    }
)
_RUNNING_STATES = frozenset({"active", "activating", "reloading"})
_ENABLED_SERVICE_STATES = frozenset(
    {
        "alias",
        "bad",
        "disabled",
        "enabled",
        "enabled-runtime",
        "generated",
        "indirect",
        "linked",
        "linked-runtime",
        "masked",
        "masked-runtime",
        "not-found",
        "static",
        "transient",
    }
)
_SYSTEMD_MANAGER_STATES = frozenset(
    {
        "degraded",
        "initializing",
        "maintenance",
        "offline",
        "running",
        "starting",
        "stopping",
        "unknown",
    }
)
_PYTHON_VERSION = re.compile(r"^Python ([0-9]{1,3})\.([0-9]{1,3})\.([0-9]{1,3})$")
_SYSTEMD_VERSION = re.compile(r"^systemd [0-9]+$")
_FIRMWARE_VERSION = re.compile(r"^v[0-9]+(?:\.[0-9]+){3}$")
_HOSTNAME = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,252}$")
_AGENT_VERSION = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+(?:[A-Za-z0-9.+-]*)?$")

_OWNED_SERVICES = (
    ("service_brilliant_mqtt", SERVICE_NAME),
    ("service_brilliant_voice", VOICE_SERVICE_NAME),
    ("service_brilliant_wifi_watchdog", WIFI_WATCHDOG_SERVICE_NAME),
    ("service_brilliant_bus_watchdog", BUS_WATCHDOG_SERVICE_NAME),
    ("service_brilliant_hue_ca_timer", HUE_CA_TIMER_NAME),
)
_RETIRED_SERVICES = (("service_brilliant_ha_mirror", HA_MIRROR_SERVICE_NAME),)
_EMPTY_SHA256 = "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855"


def _no_symlinks_command(root: str) -> str:
    """Preserve ``find`` failure instead of treating empty stderr as success."""
    return f'links="$(find {root} -type l -print -quit)" && test -z "$links"'


_TOOLCHAIN_PROBES = (
    (
        "toolchain_mv_no_clobber",
        "mv_no_clobber",
        (
            'help=$(/usr/bin/mv.coreutils --help 2>/dev/null) && case "$help" in '
            "*--no-clobber*) true ;; *) false ;; esac"
        ),
    ),
    (
        "toolchain_mv_no_target_directory",
        "mv_no_target_directory",
        (
            'help=$(/usr/bin/mv.coreutils --help 2>/dev/null) && case "$help" in '
            "*--no-target-directory*) true ;; *) false ;; esac"
        ),
    ),
    (
        "toolchain_stat_mode",
        "stat_mode",
        (
            'mode="$(stat -c %a -- /dev/null 2>/dev/null)" && case "$mode" in '
            "[0-7][0-7][0-7]|[0-7][0-7][0-7][0-7]) true ;; *) false ;; esac"
        ),
    ),
    (
        "toolchain_grep_fixed_exact_quiet",
        "grep_fixed_exact_quiet",
        ("printf '%s\\n' brilliant-mqtt-toolchain | grep -Fxq -- brilliant-mqtt-toolchain"),
    ),
    (
        "toolchain_grep_extended_count",
        "grep_extended_count",
        (
            "test \"$(printf '%s\\n' brilliant-mqtt-toolchain | "
            "grep -Ec -- '^brilliant-mqtt-toolchain$')\" = 1"
        ),
    ),
    (
        "toolchain_find_print_quit",
        "find_print_quit",
        _no_symlinks_command("/dev/null"),
    ),
    (
        "toolchain_sha256sum",
        "sha256sum",
        (f"test \"$(/usr/bin/sha256sum -- /dev/null 2>/dev/null)\" = '{_EMPTY_SHA256}  /dev/null'"),
    ),
    (
        "toolchain_python_3_10",
        "python_3_10",
        (
            f"{_PANEL_PYTHON} -B -c 'import sys;"
            "raise SystemExit(0 if sys.version_info >= (3,10) else 1)'"
        ),
    ),
    (
        "toolchain_systemd_manager",
        "systemd_manager",
        (
            'state="$(systemctl is-system-running 2>/dev/null || true)"; '
            f'case "$state" in {"|".join(sorted(_SYSTEMD_MANAGER_STATES))}) '
            "true ;; *) false ;; esac"
        ),
    ),
    (
        "toolchain_systemd_is_active",
        "systemd_is_active",
        (
            f'state="$(systemctl is-active {SERVICE_NAME} 2>/dev/null || true)"; '
            f'case "$state" in {"|".join(sorted(_SERVICE_STATES))}) '
            "true ;; *) false ;; esac"
        ),
    ),
    (
        "toolchain_systemd_is_enabled",
        "systemd_is_enabled",
        (
            f'state="$(systemctl is-enabled {SERVICE_NAME} 2>/dev/null || true)"; '
            f'case "$state" in {"|".join(sorted(_ENABLED_SERVICE_STATES))}) '
            "true ;; *) false ;; esac"
        ),
    ),
)
PANEL_TOOLCHAIN_CAPABILITIES = frozenset(
    capability for _key, capability, _command in _TOOLCHAIN_PROBES
)
_BASE_KEYS = frozenset(
    {
        "hostname",
        "model",
        "architecture",
        "firmware",
        "python_version",
        "init_system",
        "available_kib",
        "available_memory_kib",
        "installed_agent_version",
    }
)
_EXPECTED_KEYS = (
    _BASE_KEYS
    | {key for key, _service in (*_OWNED_SERVICES, *_RETIRED_SERVICES)}
    | {key for key, _capability, _command in _TOOLCHAIN_PROBES}
)
_ERROR_CODES = frozenset(
    {
        "inspection_timeout",
        "inspection_failed",
        "inspection_output_invalid",
        "unsupported_architecture",
        "firmware_unknown",
        "unsupported_python",
        "insufficient_storage",
        "insufficient_memory",
        "unsupported_panel_toolchain",
    }
)


def _line_probe(key: str, command: str, *, merge_stderr: bool = False) -> str:
    redirection = "2>&1" if merge_stderr else "2>/dev/null"
    return (
        f"{command} {redirection} | "
        f'awk \'NR == 1 {{ print "{key}=" $0; found=1 }} '
        f'END {{ if (!found) print "{key}=" }}\''
    )


def _service_probe(key: str, service: str) -> str:
    return (
        f"state=$(systemctl is-active {service} 2>/dev/null || true); "
        'case "$state" in '
        "active|activating|deactivating|failed|inactive|reloading|unknown) ;; "
        "*) state=unknown ;; esac; "
        f"printf '{key}=%s\\n' \"$state\""
    )


def _capability_probe(key: str, command: str) -> str:
    return (
        f"if {{ {command}; }} >/dev/null 2>&1; "
        f"then printf '{key}=1\\n'; else printf '{key}=0\\n'; fi"
    )


_MODEL_PROBE = (
    f"{_PANEL_PYTHON} -B -c 'import pathlib; "
    f'raw=pathlib.Path("{_MODEL_PATH}").open("rb").read(129); '
    'value=raw[:-1] if raw.endswith(b"\\x00") else raw; '
    'text=value.decode("ascii","replace"); '
    "ok=len(raw)<=128 and bool(value) and text.isascii() and "
    "all(32<=ord(char)<127 for char in text); "
    'print("model="+(text if ok else ""))\' '
    "2>/dev/null || printf 'model=\\n'"
)
_FIRMWARE_PROBE = (
    f"{_PANEL_PYTHON} -B -c 'import json,pathlib; "
    f'raw=pathlib.Path("{_RELEASE_INFO_PATH}").open("rb").read(4097); '
    'data=json.loads(raw.decode("utf-8")) if len(raw)<=4096 else {}; '
    'value=data.get("release_tag") if isinstance(data,dict) else None; '
    "ok=isinstance(value,str) and len(value)<=64 and value.isascii() and "
    "all(32<=ord(char)<127 for char in value); "
    'print("firmware="+(value if ok else ""))\' '
    "2>/dev/null || printf 'firmware=\\n'"
)
_AGENT_VERSION_PROBE = (
    f"{_PANEL_PYTHON} -B -c 'import pathlib,re; "
    f'path=pathlib.Path("{PANEL_VERSION_FILE}"); exists=path.is_file(); '
    'raw=path.open("rb").read(129) if exists else b""; '
    'value=raw.rstrip(b"\\r\\n"); text=value.decode("ascii","replace"); '
    "ok=len(raw)<=128 and bool(value) and text.isascii() and "
    "all(32<=ord(char)<127 for char in text) and "
    f're.fullmatch(r"{_AGENT_VERSION.pattern}",text) is not None; '
    'output=text if exists and ok else ""; '
    'print("installed_agent_version="+output)\' '
    "2>/dev/null || printf 'installed_agent_version=\\n'"
)

_PANEL_INSPECTION_PROBES = (
    _line_probe("hostname", "hostname"),
    _MODEL_PROBE,
    _line_probe("architecture", "uname -m"),
    _FIRMWARE_PROBE,
    _line_probe(
        "python_version",
        f"{_PANEL_PYTHON} --version",
        merge_stderr=True,
    ),
    (
        "systemctl --version 2>/dev/null | "
        'awk \'NR == 1 { print "init_system=" $1 " " $2; found=1 } '
        'END { if (!found) print "init_system=" }\''
    ),
    (
        "df -Pk /var 2>/dev/null | "
        'awk \'NR == 2 { print "available_kib=" $4; found=1 } '
        'END { if (!found) print "available_kib=" }\''
    ),
    (
        "awk '/^MemAvailable:[[:space:]]/ "
        '{ print "available_memory_kib=" $2; found=1 } '
        'END { if (!found) print "available_memory_kib=" }\' /proc/meminfo'
    ),
    _AGENT_VERSION_PROBE,
    *(_service_probe(key, service) for key, service in _OWNED_SERVICES),
    *(_service_probe(key, service) for key, service in _RETIRED_SERVICES),
    *(_capability_probe(key, command) for key, _capability, command in _TOOLCHAIN_PROBES),
)

PANEL_INSPECTION_COMMAND = "{ " + "; ".join(_PANEL_INSPECTION_PROBES) + "; } 2>&1 | head -c 16385"


class PanelCompatibilityError(ValueError):
    """Redacted inspection or compatibility result with a stable code."""

    __slots__ = ("capability", "code")

    def __init__(self, code: str, *, capability: str | None = None) -> None:
        valid_toolchain_capability = (
            isinstance(capability, str) and capability in PANEL_TOOLCHAIN_CAPABILITIES
        )
        if (
            code not in _ERROR_CODES
            or (code == "unsupported_panel_toolchain" and not valid_toolchain_capability)
            or code != "unsupported_panel_toolchain"
            and capability is not None
        ):
            raise ValueError("invalid_panel_compatibility_error_code")
        self.code = code
        self.capability = capability
        super().__init__(code)
        self.__suppress_context__ = True


@dataclass(frozen=True, slots=True)
class PanelFacts:
    """Allowlisted facts required before staging software on a panel."""

    fingerprint: str
    hostname: str
    model: str
    architecture: str
    firmware: str
    python_version: str
    init_system: str
    available_bytes: int
    available_memory_bytes: int
    installed_agent_version: str | None
    active_services: tuple[str, ...]
    conflicting_services: tuple[str, ...]


def _parse_output(stdout: str) -> dict[str, str]:
    if (
        not isinstance(stdout, str)
        or not stdout.isascii()
        or len(stdout) > _MAX_OUTPUT_BYTES
        or "\r" in stdout
        or any(ord(char) < 32 and char != "\n" for char in stdout)
    ):
        raise PanelCompatibilityError("inspection_output_invalid")
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        key, separator, value = line.partition("=")
        if not separator or key not in _EXPECTED_KEYS or key in values:
            raise PanelCompatibilityError("inspection_output_invalid")
        values[key] = value
    if values.keys() != _EXPECTED_KEYS:
        raise PanelCompatibilityError("inspection_output_invalid")
    return values


def _parse_kib(value: str) -> int:
    if not value.isascii() or not value.isdigit() or len(value) > 19:
        raise PanelCompatibilityError("inspection_output_invalid")
    parsed = int(value)
    if parsed > _MAX_KIB_VALUE:
        raise PanelCompatibilityError("inspection_output_invalid")
    return parsed * 1024


def _validate_service_states(values: dict[str, str]) -> None:
    if any(
        values[key] not in _SERVICE_STATES
        for key, _service in (*_OWNED_SERVICES, *_RETIRED_SERVICES)
    ):
        raise PanelCompatibilityError("inspection_output_invalid")


def _validate_toolchain(values: dict[str, str]) -> None:
    for key, capability, _command in _TOOLCHAIN_PROBES:
        result = values[key]
        if result not in {"0", "1"}:
            raise PanelCompatibilityError("inspection_output_invalid")
        if result == "0":
            raise PanelCompatibilityError(
                "unsupported_panel_toolchain",
                capability=capability,
            )


async def async_inspect_panel(shell: PanelShell, identity: HostIdentity) -> PanelFacts:
    """Run one bounded command and return strictly parsed compatibility facts."""
    if not isinstance(identity, HostIdentity):
        raise TypeError("identity must be a HostIdentity")
    if shell.pinned_host_key() != identity.public_key:
        raise PanelIdentityError("host_key_changed")

    result: RunResult | None = None
    failure_code: str | None = None
    try:
        async with asyncio.timeout(_INSPECTION_TIMEOUT):
            result = await shell.run(PANEL_INSPECTION_COMMAND)
    except TimeoutError:
        failure_code = "inspection_timeout"
    except Exception:
        failure_code = "inspection_failed"
    if failure_code is not None:
        raise PanelCompatibilityError(failure_code)

    if not isinstance(result, RunResult) or result.exit_status != 0 or result.stderr:
        raise PanelCompatibilityError("inspection_failed")
    values = _parse_output(result.stdout)
    _validate_service_states(values)
    _validate_toolchain(values)

    architecture = values["architecture"]
    if architecture not in _SUPPORTED_ARCHITECTURES:
        raise PanelCompatibilityError("unsupported_architecture")
    python_match = _PYTHON_VERSION.fullmatch(values["python_version"])
    if python_match is None:
        raise PanelCompatibilityError("unsupported_python")
    python_version = tuple(int(part) for part in python_match.groups())
    if python_version < (3, 10, 0):
        raise PanelCompatibilityError("unsupported_python")
    model = values["model"]
    if (
        not 1 <= len(model) <= 128
        or not model.isascii()
        or not model.isprintable()
        or not model.strip()
    ):
        raise PanelCompatibilityError("inspection_output_invalid")
    firmware = values["firmware"]
    if _FIRMWARE_VERSION.fullmatch(firmware) is None:
        raise PanelCompatibilityError("firmware_unknown")
    if _SYSTEMD_VERSION.fullmatch(values["init_system"]) is None:
        raise PanelCompatibilityError("inspection_output_invalid")
    if _HOSTNAME.fullmatch(values["hostname"]) is None:
        raise PanelCompatibilityError("inspection_output_invalid")

    available_bytes = _parse_kib(values["available_kib"])
    if available_bytes < _MIN_AVAILABLE_BYTES:
        raise PanelCompatibilityError("insufficient_storage")
    available_memory_bytes = _parse_kib(values["available_memory_kib"])
    if available_memory_bytes < _MIN_AVAILABLE_MEMORY_BYTES:
        raise PanelCompatibilityError("insufficient_memory")

    raw_agent_version = values["installed_agent_version"]
    installed_agent_version = raw_agent_version or None
    if (
        installed_agent_version is not None
        and _AGENT_VERSION.fullmatch(installed_agent_version) is None
    ):
        raise PanelCompatibilityError("inspection_output_invalid")

    active_services = tuple(service for key, service in _OWNED_SERVICES if values[key] == "active")
    conflicting_services = tuple(
        service for key, service in _RETIRED_SERVICES if values[key] in _RUNNING_STATES
    )
    return PanelFacts(
        fingerprint=identity.fingerprint,
        hostname=values["hostname"],
        model=model,
        architecture=architecture,
        firmware=firmware,
        python_version=".".join(python_match.groups()),
        init_system=values["init_system"],
        available_bytes=available_bytes,
        available_memory_bytes=available_memory_bytes,
        installed_agent_version=installed_agent_version,
        active_services=active_services,
        conflicting_services=conflicting_services,
    )
