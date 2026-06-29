"""On-panel operation recipes — the ONLY module that composes panel shell commands.

Every function takes a connected PanelShell. No HA imports: unit-testable in
isolation, mirrors the agent repo's pure-module philosophy. The integration only
ever writes the paths it owns: /var/brilliant-mqtt/**, /etc/brilliant-mqtt.env,
and /etc/systemd/system/brilliant-mqtt.service (see the design spec §7).
"""

from __future__ import annotations

from dataclasses import dataclass

from .const import (
    PANEL_ENV_FILE,
    PANEL_STAGED_DIR,
    PANEL_UNIT_FILE,
    PANEL_VAR_DIR,
    PANEL_VERSION_FILE,
    PANEL_VOICE_ENV_FILE,
    PANEL_VOICE_STAGED_DIR,
    PANEL_VOICE_UNIT_FILE,
    PANEL_VOICE_VAR_DIR,
    PANEL_VOICE_VERSION_FILE,
    SERVICE_NAME,
    VOICE_SERVICE_NAME,
)
from .shell import PanelShell, RunResult

_STAGING_DIR = f"{PANEL_VAR_DIR}.staging"
_STAGED_UNIT = f"{PANEL_STAGED_DIR}/{SERVICE_NAME}.service"
_STAGED_ENV = f"{PANEL_STAGED_DIR}/{SERVICE_NAME}.env"

_VOICE_STAGING_DIR = f"{PANEL_VOICE_VAR_DIR}.staging"
_VOICE_STAGING_TARBALL = f"{PANEL_VOICE_VAR_DIR}.staging.tar.gz"
_VOICE_STAGED_ENV = f"{PANEL_VOICE_STAGED_DIR}/{VOICE_SERVICE_NAME}.env"


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


def render_env(
    panel: str,
    mesh_priority: int,
    mqtt_host: str,
    mqtt_port: int,
    mqtt_username: str,
    mqtt_password: str,
) -> str:
    """Render /etc/brilliant-mqtt.env — exactly what the agent's config.py reads.

    String values are quoted via _env_quote (user-typed broker passwords routinely
    contain `#`, quotes, `$`, backslash); the int fields are safe and stay bare.
    """
    return (
        f"{ENV_PANEL}={_env_quote(panel)}\n"
        f"{ENV_MQTT_HOST}={_env_quote(mqtt_host)}\n"
        f"{ENV_MQTT_PORT}={mqtt_port}\n"
        f"{ENV_MQTT_USERNAME}={_env_quote(mqtt_username)}\n"
        f"{ENV_MQTT_PASSWORD}={_env_quote(mqtt_password)}\n"
        f"{ENV_MESH_PRIORITY}={mesh_priority}\n"
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
