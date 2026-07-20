"""On-panel operation recipes — the ONLY module that composes panel shell commands.

Every function takes a connected PanelShell. No HA imports: unit-testable in
isolation, mirrors the agent repo's pure-module philosophy. The integration only
ever writes the paths it owns: /var/brilliant-mqtt/**, /etc/brilliant-mqtt.env,
and /etc/systemd/system/brilliant-mqtt.service (see the design spec §7).
"""

from __future__ import annotations

import asyncio
import os
from dataclasses import dataclass

import asyncssh

from .async_cleanup import shielded_cleanup_after_failure
from .const import (
    BLE_OBSERVER_SERVICE_NAME,
    BUS_WATCHDOG_SERVICE_NAME,
    DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON,
    DEFAULT_REBOOT_JOURNAL_LINES,
    HA_MIRROR_SERVICE_NAME,
    HUE_CA_TIMER_NAME,
    PANEL_BLE_OBSERVER_DIR,
    PANEL_BLE_OBSERVER_UNIT_FILE,
    PANEL_BUS_WATCHDOG_DIR,
    PANEL_BUS_WATCHDOG_UNIT_FILE,
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
    PANEL_STAGED_DIR,
    PANEL_UNIT_FILE,
    PANEL_VAR_DIR,
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
from .shell import PanelShell, RunResult

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
    if "\n" in value or "\r" in value or "\x00" in value:
        raise ValueError("control characters are not allowed in env values")
    escaped = value.replace("\\", "\\\\").replace('"', '\\"')
    return f'"{escaped}"'


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
ENV_MQTT_HOST = "MQTT_HOST"
ENV_MQTT_PORT = "MQTT_PORT"
ENV_MQTT_USERNAME = "MQTT_USERNAME"
ENV_MQTT_PASSWORD = "MQTT_PASSWORD"
ENV_MESH_PRIORITY = "MESH_PRIORITY"
ENV_SCENE_BRIDGE_ENABLED = "SCENE_BRIDGE_ENABLED"
ENV_BLE_OBSERVER_ENABLED = "BLE_OBSERVER_ENABLED"
ENV_BLE_OBSERVER_ALLOWLIST_JSON = "BLE_OBSERVER_ALLOWLIST_JSON"
ENV_BLE_OBSERVER_ADAPTER = "BLE_OBSERVER_ADAPTER"
ENV_BLE_OBSERVER_MAX_EVENTS_PER_SECOND = "BLE_OBSERVER_MAX_EVENTS_PER_SECOND"
ENV_BLE_OBSERVER_LOG_LEVEL = "BLE_OBSERVER_LOG_LEVEL"

_BLE_OBSERVER_ENV_KEYS = frozenset(
    {
        ENV_BLE_OBSERVER_ENABLED,
        ENV_BLE_OBSERVER_ALLOWLIST_JSON,
        ENV_BLE_OBSERVER_ADAPTER,
        ENV_BLE_OBSERVER_MAX_EVENTS_PER_SECOND,
        ENV_BLE_OBSERVER_LOG_LEVEL,
    }
)


def render_ble_observer_env(*, enabled: bool, allowlist_json: str) -> str:
    """Render the observer-only block for the bridge's shared 0600 env file."""
    return (
        f"{ENV_BLE_OBSERVER_ENABLED}={1 if enabled else 0}\n"
        f"{ENV_BLE_OBSERVER_ALLOWLIST_JSON}={_env_quote(allowlist_json)}\n"
        f"{ENV_BLE_OBSERVER_ADAPTER}={_env_quote('hci0')}\n"
        f"{ENV_BLE_OBSERVER_MAX_EVENTS_PER_SECOND}=10\n"
        f"{ENV_BLE_OBSERVER_LOG_LEVEL}={_env_quote('INFO')}\n"
    )


def render_env(
    panel: str,
    mesh_priority: int,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str,
    mqtt_password: str,
    scene_bridge_enabled: bool = False,
    ble_observer_enabled: bool = False,
    ble_observer_allowlist_json: str = DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON,
) -> str:
    """Render /etc/brilliant-mqtt.env — exactly what the agent's config.py reads.

    String values are quoted via _env_quote (user-typed broker passwords routinely
    contain `#`, quotes, `$`, backslash); the int fields are safe and stay bare.
    """
    observer_env = render_ble_observer_env(
        enabled=ble_observer_enabled,
        allowlist_json=ble_observer_allowlist_json,
    )
    return (
        f"{ENV_PANEL}={_env_quote(panel)}\n"
        f"{ENV_MQTT_HOST}={_env_quote(mqtt_host)}\n"
        f"{ENV_MQTT_PORT}={mqtt_port}\n"
        f"{ENV_MQTT_USERNAME}={_env_quote(mqtt_username)}\n"
        f"{ENV_MQTT_PASSWORD}={_env_quote(mqtt_password)}\n"
        f"{ENV_MESH_PRIORITY}={mesh_priority}\n"
        f"{ENV_SCENE_BRIDGE_ENABLED}={1 if scene_bridge_enabled else 0}\n"
        f"{observer_env}"
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
    for line in text.splitlines():
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


async def write_env(shell: PanelShell, env_content: str) -> None:
    """(Re)write ONLY the env file to /etc + the /var staged copy (0600); no unit.

    Reconfigure changes broker/mesh values on an already-installed panel where the
    unit is unchanged, so this rewrites just the env in both locations; the caller
    restarts the agent to pick it up.
    """
    await _checked(shell, f"mkdir -p {PANEL_STAGED_DIR}")
    env_bytes = env_content.encode()
    await shell.put_bytes(env_bytes, PANEL_ENV_FILE, 0o600)
    await shell.put_bytes(env_bytes, _STAGED_ENV, 0o600)


def _with_ble_observer_env(
    existing: str,
    *,
    enabled: bool,
    allowlist_json: str,
) -> str:
    """Replace only observer-owned variables in an existing shared env file."""
    kept: list[str] = []
    for line in existing.splitlines():
        stripped = line.strip()
        key = stripped.partition("=")[0].strip() if "=" in stripped else ""
        if key not in _BLE_OBSERVER_ENV_KEYS:
            kept.append(line)
    prefix = "\n".join(kept).rstrip("\n")
    observer = render_ble_observer_env(
        enabled=enabled,
        allowlist_json=allowlist_json,
    ).rstrip("\n")
    return f"{prefix}\n{observer}\n" if prefix else f"{observer}\n"


async def configure_ble_observer_env(
    shell: PanelShell,
    *,
    enabled: bool,
    allowlist_json: str,
) -> None:
    """Patch observer values into the shared 0600 env without restarting anything."""
    result = await shell.run(f"cat {PANEL_ENV_FILE}")
    if result.exit_status != 0 or not result.stdout.strip():
        raise PanelOpError("BLE observer needs an existing Brilliant MQTT environment")
    await write_env(
        shell,
        _with_ble_observer_env(
            result.stdout,
            enabled=enabled,
            allowlist_json=allowlist_json,
        ),
    )


async def ensure_configs(shell: PanelShell, unit_content: str, env_content: str) -> None:
    """Idempotently (re)write unit + env to /etc AND the /var staged copies.

    The staged copies are the OTA-proof restore source: /var survives firmware
    updates, /etc may not. Env files carry the broker password → 0600 both places.
    """
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
    """(title, command) pairs for collect_diagnostics — POSIX-simple, no bashisms.

    Two probes scale with *lines*: the whole current boot (the wedge context) and the
    bridge unit's own tail at half that depth. The rest are fixed one-shots covering the
    two observed wedge classes — the uptime-decay wedge and Wi-Fi power-save starvation
    (connman timeouts, kernel wlan/deauth/carrier noise, the radio's power_save state).
    """
    unit_lines = max(1, lines // 2)
    return (
        ("uptime", "uptime"),
        ("free", "free"),
        ("df /var", "df /var"),
        (f"journalctl -b (last {lines})", f"journalctl -b -n {lines} --no-pager"),
        (
            f"journalctl -u {SERVICE_NAME} (last {unit_lines})",
            f"journalctl -u {SERVICE_NAME} -n {unit_lines} --no-pager",
        ),
        (
            "kernel wifi/oom",
            "journalctl -k | grep -iE 'wlan|brcm|dhd|deauth|disassoc|carrier|oom' | tail -60",
        ),
        ("iw dev wlan0 link", "iw dev wlan0 link"),
        ("iw dev wlan0 power_save", "iw dev wlan0 get power_save"),
        ("connmanctl services", "connmanctl services 2>&1 | head -n 15"),
        (
            f"systemctl status {SERVICE_NAME}",
            f"systemctl status {SERVICE_NAME} --no-pager 2>&1 | head -n 15",
        ),
    )


async def _diagnostics_probe(shell: PanelShell, command: str) -> str:
    """Run one read-only diagnostics probe, returning its output or an error note.

    Failure-tolerant by contract so one dead probe never aborts the surrounding bundle:
    a transport error becomes a text note. Exit status is deliberately NOT checked — a
    non-zero probe (e.g. `iw ... get power_save` on a downed link, or `grep` finding no
    match) still yields output worth capturing.
    """
    try:
        result = await shell.run(command)
    except (OSError, asyncssh.Error) as err:
        return f"<probe failed: {type(err).__name__}: {err}>"
    body = result.stdout.rstrip("\n")
    stderr = result.stderr.rstrip("\n")
    if stderr:
        body = f"{body}\n[stderr] {stderr}" if body else f"[stderr] {stderr}"
    return body or "<no output>"


async def collect_diagnostics(shell: PanelShell, lines: int = DEFAULT_REBOOT_JOURNAL_LINES) -> str:
    """Assemble a pre-reboot diagnostics bundle from independent, failure-tolerant probes.

    The panel's journald is volatile (/run tmpfs — only the current boot survives a
    reboot), so the wedge evidence MUST be pulled over SSH BEFORE the reboot that would
    erase it. Sections are clearly delimited; each probe is best-effort (a raising probe
    yields an error note in its section, never an abort — a partial bundle beats none).
    """
    sections: list[str] = []
    for title, command in _diagnostics_probes(lines):
        sections.append(f"===== {title} =====")
        sections.append(f"$ {command}")
        sections.append(await _diagnostics_probe(shell, command))
        sections.append("")
    return "\n".join(sections)


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


# ---------------------------------------------------------------------------
# Passive BLE observer recipes
# ---------------------------------------------------------------------------

_BLE_OBSERVER_STAGING_DIR = f"{PANEL_BLE_OBSERVER_DIR}.staging"
_BLE_OBSERVER_STAGED_UNIT = f"{PANEL_BLE_OBSERVER_DIR}/{BLE_OBSERVER_SERVICE_NAME}.service"
_BLE_OBSERVER_DISABLE_COMMAND = f"systemctl disable --now {BLE_OBSERVER_SERVICE_NAME}"
_BLE_OBSERVER_STOP_COMMAND = f"systemctl stop {BLE_OBSERVER_SERVICE_NAME}"
BLE_OBSERVER_ACTIVE_COMMAND = f"systemctl is-active {BLE_OBSERVER_SERVICE_NAME} 2>/dev/null"

BLE_OBSERVER_INSPECT_COMMAND = (
    f"test -f {PANEL_BLE_OBSERVER_UNIT_FILE} && echo unit=1 || echo unit=0; "
    f"systemctl is-enabled {BLE_OBSERVER_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo enabled=1 || echo enabled=0; "
    f"systemctl is-active {BLE_OBSERVER_SERVICE_NAME} >/dev/null 2>&1"
    f" && echo active=1 || echo active=0; "
    f"test -f {_BLE_OBSERVER_STAGED_UNIT} && echo sunit=1 || echo sunit=0; "
    f"test -f {PANEL_BLE_OBSERVER_DIR}/brilliant_ble_observer/__main__.py "
    f"&& test -f {PANEL_BLE_OBSERVER_DIR}/vendor/dbus_next/__init__.py "
    f"&& echo payload=1 || echo payload=0"
)


@dataclass(frozen=True)
class BleObserverState:
    """Strict install/health proof for the isolated BLE observer."""

    unit_present: bool
    enabled: bool
    active: bool
    staged_unit_present: bool
    payload_present: bool


async def inspect_ble_observer(shell: PanelShell) -> BleObserverState:
    """Probe observer state, rejecting incomplete or ambiguous shell output."""
    result = await shell.run(BLE_OBSERVER_INSPECT_COMMAND)
    expected = {"unit", "enabled", "active", "sunit", "payload"}
    if result.exit_status != 0:
        raise PanelOpError("BLE observer inspection failed")
    flags: dict[str, bool] = {}
    for line in result.stdout.splitlines():
        key, sep, value = line.partition("=")
        if not sep or key not in expected or key in flags or value not in {"0", "1"}:
            raise PanelOpError("BLE observer inspection returned ambiguous state")
        flags[key] = value == "1"
    if set(flags) != expected:
        raise PanelOpError("BLE observer inspection returned incomplete state")
    return BleObserverState(
        unit_present=flags["unit"],
        enabled=flags["enabled"],
        active=flags["active"],
        staged_unit_present=flags["sunit"],
        payload_present=flags["payload"],
    )


def _ble_observer_swap_command() -> str:
    """Return the recoverable move-aside swap for observer code + vendor."""
    return " && ".join(
        [
            f"mkdir -p {PANEL_VAR_DIR}",
            f"rm -rf {PANEL_BLE_OBSERVER_DIR}.bak",
            f"{{ [ -e {PANEL_BLE_OBSERVER_DIR} ] && "
            f"mv {PANEL_BLE_OBSERVER_DIR} {PANEL_BLE_OBSERVER_DIR}.bak; true; }}",
            f"mv {_BLE_OBSERVER_STAGING_DIR} {PANEL_BLE_OBSERVER_DIR}",
            f"rm -rf {PANEL_BLE_OBSERVER_DIR}.bak",
        ]
    )


async def deploy_ble_observer(shell: PanelShell, local_dir: str) -> None:
    """Upload the observer package + dedicated vendor tree, then atomically swap."""
    await shell.run(f"rm -rf {_BLE_OBSERVER_STAGING_DIR}")
    await shell.put_dir(local_dir, _BLE_OBSERVER_STAGING_DIR)
    await _checked(shell, _ble_observer_swap_command())


async def ensure_ble_observer_unit(shell: PanelShell, unit_content: str) -> None:
    """Write live + OTA-proof observer unit copies and reload systemd only."""
    await _checked(shell, f"mkdir -p {PANEL_BLE_OBSERVER_DIR}")
    unit_bytes = unit_content.encode()
    await shell.put_bytes(unit_bytes, PANEL_BLE_OBSERVER_UNIT_FILE, 0o644)
    await shell.put_bytes(unit_bytes, _BLE_OBSERVER_STAGED_UNIT, 0o644)
    await _checked(shell, "systemctl daemon-reload")


async def _activate_and_prove_ble_observer(
    shell: PanelShell,
    commands: tuple[str, ...],
) -> None:
    """Run activation commands, prove full health, and quarantine every failed proof."""
    try:
        for command in commands:
            await _checked(shell, command)
        state = await inspect_ble_observer(shell)
        if state != BleObserverState(
            unit_present=True,
            enabled=True,
            active=True,
            staged_unit_present=True,
            payload_present=True,
        ):
            raise PanelOpError("BLE observer activation health proof failed")
    except BaseException as error:
        await shielded_cleanup_after_failure(error, quarantine_ble_observer(shell))
        raise


async def enable_ble_observer(shell: PanelShell) -> None:
    """Enable/start only the observer, then prove its complete isolated install."""
    await _activate_and_prove_ble_observer(
        shell,
        (f"systemctl enable --now {BLE_OBSERVER_SERVICE_NAME}",),
    )


async def activate_updated_ble_observer(shell: PanelShell) -> None:
    """Restart replaced code, then prove the selected observer completely healthy."""
    await _activate_and_prove_ble_observer(
        shell,
        (
            f"systemctl enable {BLE_OBSERVER_SERVICE_NAME}",
            f"systemctl restart {BLE_OBSERVER_SERVICE_NAME}",
        ),
    )


async def _ble_observer_is_active(shell: PanelShell) -> bool:
    """Return exact systemd activity, rejecting output that cannot prove either state."""
    result = await shell.run(BLE_OBSERVER_ACTIVE_COMMAND)
    state = result.stdout.strip()
    if result.stderr.strip():
        raise PanelOpError("BLE observer activity probe returned ambiguous state")
    if result.exit_status == 0 and state == "active":
        return True
    if result.exit_status == 3 and state in {"failed", "inactive"}:
        return False
    if result.exit_status == 4 and state == "unknown":
        return False
    raise PanelOpError("BLE observer activity probe returned ambiguous state")


async def _remove_inactive_ble_observer(shell: PanelShell) -> None:
    """Remove observer-owned state after a caller has proven the unit inactive."""
    env_error: OSError | asyncssh.Error | PanelOpError | None = None

    # Keep the independent on-panel kill switch false if the shared env survives.
    # A missing/corrupt bridge env must not prevent removal of the observer service.
    result = await shell.run(f"cat {PANEL_ENV_FILE}")
    if result.exit_status == 0 and result.stdout.strip():
        try:
            await write_env(
                shell,
                _with_ble_observer_env(
                    result.stdout,
                    enabled=False,
                    allowlist_json=DEFAULT_BLE_OBSERVER_ALLOWLIST_JSON,
                ),
            )
        except (OSError, asyncssh.Error, PanelOpError) as error:
            # Still remove the unit and payload: either the false env OR no runnable
            # unit is enough to keep a stopped observer fail-closed after this call.
            env_error = error

    await _checked(shell, f"rm -f {PANEL_BLE_OBSERVER_UNIT_FILE}")
    await _checked(
        shell,
        f"rm -rf {PANEL_BLE_OBSERVER_DIR} {_BLE_OBSERVER_STAGING_DIR} {PANEL_BLE_OBSERVER_DIR}.bak",
    )
    await _checked(shell, "systemctl daemon-reload")
    if env_error is not None:
        raise PanelOpError("BLE observer env disable failed after safe removal") from env_error


async def quarantine_ble_observer(shell: PanelShell) -> None:
    """Bounded failure cleanup: force-stop, prove inactive, then remove the observer."""
    await shell.run(_BLE_OBSERVER_DISABLE_COMMAND)
    if await _ble_observer_is_active(shell):
        # An enable/restart/disable command can mutate systemd and still report a
        # failure. One explicit stop + re-probe closes that partial-success window.
        await shell.run(_BLE_OBSERVER_STOP_COMMAND)
        if await _ble_observer_is_active(shell):
            raise PanelOpError("BLE observer is still active after quarantine stop")
    await _remove_inactive_ble_observer(shell)


async def uninstall_ble_observer(shell: PanelShell) -> None:
    """Remove only observer-owned state after proving its service is inactive."""
    await shell.run(_BLE_OBSERVER_DISABLE_COMMAND)
    if await _ble_observer_is_active(shell):
        # Do not delete the unit, payload, or kill-switch env around a process that
        # systemd still reports active. The caller keeps the selected entry state.
        raise PanelOpError("BLE observer is still active after disable")
    await _remove_inactive_ble_observer(shell)
