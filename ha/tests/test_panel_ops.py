"""panel_ops recipes against FakeShell — command sequences and parsing."""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    BUS_WATCHDOG_SERVICE_NAME,
    HA_MIRROR_SERVICE_NAME,
    PANEL_BUS_WATCHDOG_DIR,
    PANEL_BUS_WATCHDOG_UNIT_FILE,
    PANEL_ENV_FILE,
    PANEL_HA_MIRROR_APP_DIR,
    PANEL_HA_MIRROR_ENV_FILE,
    PANEL_HA_MIRROR_STAGED_DIR,
    PANEL_HA_MIRROR_UNIT_FILE,
    PANEL_HA_MIRROR_VAR_DIR,
    PANEL_UNIT_FILE,
    PANEL_VAR_DIR,
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
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell

_FULL_INSPECT = RunResult(
    0,
    "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\npayload=1\n9.9.9\n",
    "",
)


async def _connected(shell: FakeShell) -> FakeShell:
    await shell.connect()
    return shell


async def test_inspect_parses_healthy_panel() -> None:
    shell = await _connected(FakeShell(responses={panel_ops.INSPECT_COMMAND: _FULL_INSPECT}))
    state = await panel_ops.inspect_panel(shell)
    assert state == panel_ops.PanelState(
        unit_present=True,
        env_present=True,
        enabled=True,
        active=True,
        staged_unit_present=True,
        staged_env_present=True,
        payload_present=True,
        payload_version="9.9.9",
    )


async def test_inspect_parses_wiped_etc() -> None:
    wiped = RunResult(0, "unit=0\nenv=0\nenabled=0\nactive=0\nsunit=1\nsenv=1\n9.9.9\n", "")
    shell = await _connected(FakeShell(responses={panel_ops.INSPECT_COMMAND: wiped}))
    state = await panel_ops.inspect_panel(shell)
    assert not state.unit_present and not state.env_present
    assert state.staged_unit_present and state.staged_env_present
    assert state.payload_version == "9.9.9"


async def test_inspect_handles_pre_integration_install() -> None:
    legacy = RunResult(0, "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=0\nsenv=0\n", "")
    shell = await _connected(FakeShell(responses={panel_ops.INSPECT_COMMAND: legacy}))
    state = await panel_ops.inspect_panel(shell)
    assert state.payload_version is None
    assert not state.staged_unit_present


async def test_inspect_detects_absent_payload() -> None:
    """A never-installed (or code-wiped) panel: no app/+vendor/ → payload_present False.

    This is the signal async_repair uses to deploy the agent code before enabling the
    unit, so the Repair button can bootstrap a code-less panel instead of enabling a
    unit whose ExecStart points at nothing.
    """
    fresh = RunResult(0, "unit=0\nenv=0\nenabled=0\nactive=0\nsunit=0\nsenv=0\npayload=0\n", "")
    shell = await _connected(FakeShell(responses={panel_ops.INSPECT_COMMAND: fresh}))
    state = await panel_ops.inspect_panel(shell)
    assert state.payload_present is False
    # The probe checks the actual entrypoint the unit runs (not just an app/ dir that
    # could be empty) plus the vendored deps — not inferred.
    assert f"{PANEL_VAR_DIR}/app/brilliant_mqtt/__main__.py" in panel_ops.INSPECT_COMMAND
    assert f"test -d {PANEL_VAR_DIR}/vendor" in panel_ops.INSPECT_COMMAND


def test_render_env_matches_agent_config_contract() -> None:
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=1,
        mqtt_host="192.168.1.250",
        mqtt_port=1883,
        mqtt_username="brilliant",
        mqtt_password="secret",
        scene_bridge_enabled=True,
    )
    # String values are systemd-double-quoted; the int fields stay bare.
    assert env.splitlines() == [
        'BRILLIANT_PANEL="office"',
        'MQTT_HOST="192.168.1.250"',
        "MQTT_PORT=1883",
        'MQTT_USERNAME="brilliant"',
        'MQTT_PASSWORD="secret"',
        "MESH_PRIORITY=1",
        "SCENE_BRIDGE_ENABLED=1",
        "LOG_LEVEL=INFO",
    ]


def _password_line(password: str) -> str:
    """Render with *password* and return just the MQTT_PASSWORD env line."""
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="h",
        mqtt_port=1883,
        mqtt_username="u",
        mqtt_password=password,
    )
    lines = [line for line in env.splitlines() if line.startswith("MQTT_PASSWORD=")]
    assert len(lines) == 1
    return lines[0]


@pytest.mark.parametrize("ctrl", ["pass\nword", "pass\rword", "pass\x00word"])
def test_render_env_rejects_control_characters(ctrl: str) -> None:
    with pytest.raises(ValueError, match="control characters"):
        panel_ops.render_env(
            panel="office",
            mesh_priority=0,
            mqtt_host="h",
            mqtt_port=1883,
            mqtt_username="u",
            mqtt_password=ctrl,
        )


@pytest.mark.parametrize(
    "password",
    [
        "trailing\\",  # trailing backslash must not splice the next line
        'has"quote',  # embedded double-quote
        "#comment-like",  # leading # must not comment the var out
        "space sep arated",  # spaces must survive (no word-splitting)
        "dollar$VAR-ish",  # literal $ should round-trip through our quoting
    ],
)
def test_render_env_password_round_trips_through_systemd_quoting(password: str) -> None:
    line = _password_line(password)
    key, _, value = line.partition("=")
    assert key == "MQTT_PASSWORD"
    # The value is double-quoted and parse_env (production) recovers the original input.
    assert value.startswith('"') and value.endswith('"')
    assert panel_ops.parse_env(line)["MQTT_PASSWORD"] == password


def test_parse_env_round_trips_render_env() -> None:
    """parse_env recovers every value render_env wrote (the adopt-installed path)."""
    env = panel_ops.render_env(
        panel="office-bath",
        mesh_priority=7,
        mqtt_host="192.168.1.250",
        mqtt_port=8883,
        mqtt_username="brilliant",
        mqtt_password='p#a"s\\s',  # the hostile chars _env_quote escapes
        scene_bridge_enabled=False,
    )
    assert panel_ops.parse_env(env) == {
        "BRILLIANT_PANEL": "office-bath",
        "MQTT_HOST": "192.168.1.250",
        "MQTT_PORT": "8883",
        "MQTT_USERNAME": "brilliant",
        "MQTT_PASSWORD": 'p#a"s\\s',
        "MESH_PRIORITY": "7",
        "SCENE_BRIDGE_ENABLED": "0",
        "LOG_LEVEL": "INFO",
    }


@pytest.mark.parametrize(
    "password",
    ["trailing\\", 'has"quote', "#comment-like", "space sep arated", "dollar$VAR-ish"],
)
def test_parse_env_recovers_quoted_password(password: str) -> None:
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="h",
        mqtt_port=1883,
        mqtt_username="u",
        mqtt_password=password,
    )
    assert panel_ops.parse_env(env)["MQTT_PASSWORD"] == password


def test_parse_env_skips_blank_and_comment_lines() -> None:
    parsed = panel_ops.parse_env('# a comment\n\nBRILLIANT_PANEL="office"\n   \nMQTT_PORT=1883\n')
    assert parsed == {"BRILLIANT_PANEL": "office", "MQTT_PORT": "1883"}


def test_parse_env_leaves_foreign_escapes_literal() -> None:
    r"""_env_quote only emits \\ and \"; any other backslash run in a hand-deployed
    file must round-trip byte-for-byte, not collapse (\n must stay \n, not become n)."""
    assert panel_ops.parse_env(r'MQTT_PASSWORD="a\nb"') == {"MQTT_PASSWORD": r"a\nb"}
    assert panel_ops.parse_env(r'X="v\$z"') == {"X": r"v\$z"}
    # The two sequences we DO unescape still work.
    assert panel_ops.parse_env(r'Y="a\\b\"c"') == {"Y": r'a\b"c'}


async def test_read_env_cats_and_parses_the_live_env_file() -> None:
    env_text = panel_ops.render_env(
        panel="office",
        mesh_priority=2,
        mqtt_host="h",
        mqtt_port=1883,
        mqtt_username="u",
        mqtt_password="pw",
    )
    shell = await _connected(
        FakeShell(responses={f"cat {PANEL_ENV_FILE}": RunResult(0, env_text, "")})
    )
    parsed = await panel_ops.read_env(shell)
    assert parsed["BRILLIANT_PANEL"] == "office"
    assert parsed["MESH_PRIORITY"] == "2"
    assert shell.commands == [f"cat {PANEL_ENV_FILE}"]


async def test_write_env_writes_only_env_to_etc_and_staged() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.write_env(shell, "ENVDATA")
    # Only the env file (no unit), both locations, 0600.
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        ("/etc/brilliant-mqtt.env", 0o600),
        ("/var/brilliant-mqtt/system/brilliant-mqtt.env", 0o600),
    ]
    assert shell.commands[0] == "mkdir -p /var/brilliant-mqtt/system"


async def test_write_env_raises_when_mkdir_fails() -> None:
    shell = await _connected(
        FakeShell(responses={"mkdir -p /var/brilliant-mqtt/system": RunResult(1, "", "denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.write_env(shell, "ENVDATA")
    assert shell.uploads == []


async def test_ensure_configs_writes_etc_and_staged_copies_then_reloads() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.ensure_configs(shell, unit_content="UNIT", env_content="ENV")
    # /etc unit (0644) + /etc env (0600) + staged copies of both (same modes).
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        ("/etc/systemd/system/brilliant-mqtt.service", 0o644),
        ("/etc/brilliant-mqtt.env", 0o600),
        ("/var/brilliant-mqtt/system/brilliant-mqtt.service", 0o644),
        ("/var/brilliant-mqtt/system/brilliant-mqtt.env", 0o600),
    ]
    assert shell.commands[0] == "mkdir -p /var/brilliant-mqtt/system"
    assert shell.commands[-1] == "systemctl daemon-reload"


async def test_ensure_configs_raises_when_mkdir_fails() -> None:
    failing_mkdir = "mkdir -p /var/brilliant-mqtt/system"
    shell = await _connected(
        FakeShell(responses={failing_mkdir: RunResult(1, "", "permission denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.ensure_configs(shell, unit_content="UNIT", env_content="ENV")
    # Failed precondition → no files written.
    assert shell.uploads == []


async def test_enable_now_and_journal() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.journal_command(50): RunResult(0, "log line\n", "")})
    )
    await panel_ops.enable_now(shell)
    assert "systemctl enable --now brilliant-mqtt" in shell.commands
    assert (await panel_ops.collect_journal(shell, 50)) == "log line\n"


async def test_enable_now_raises_on_nonzero_exit() -> None:
    shell = await _connected(
        FakeShell(
            responses={
                "systemctl enable --now brilliant-mqtt": RunResult(1, "", "Job failed\n"),
            }
        )
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.enable_now(shell)


# Move-aside swap: stage fully, set old app/vendor aside, move new in, drop the
# backups. Recoverable if any single mv fails mid-way. Spelled out literally
# (not derived from the impl's path constants) so the assertion is independent;
# the long segments use adjacent-string concatenation to stay under 100 cols.
_EXPECTED_SWAP = " && ".join(
    [
        "mkdir -p /var/brilliant-mqtt",
        "rm -rf /var/brilliant-mqtt/app.bak /var/brilliant-mqtt/vendor.bak",
        "{ [ -e /var/brilliant-mqtt/app ] && "
        "mv /var/brilliant-mqtt/app /var/brilliant-mqtt/app.bak; true; }",
        "{ [ -e /var/brilliant-mqtt/vendor ] && "
        "mv /var/brilliant-mqtt/vendor /var/brilliant-mqtt/vendor.bak; true; }",
        "mv /var/brilliant-mqtt.staging/app /var/brilliant-mqtt/app",
        "mv /var/brilliant-mqtt.staging/vendor /var/brilliant-mqtt/vendor",
        "rm -rf /var/brilliant-mqtt/app.bak /var/brilliant-mqtt/vendor.bak "
        "/var/brilliant-mqtt.staging",
    ]
)


async def test_deploy_payload_uploads_tree_then_swaps() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_payload(shell, "/local/payload", version="9.9.9")
    assert shell.commands[0] == "rm -rf /var/brilliant-mqtt.staging"
    assert shell.dir_uploads == [("/local/payload", "/var/brilliant-mqtt.staging")]
    assert _EXPECTED_SWAP in shell.commands
    assert ("/var/brilliant-mqtt/VERSION", b"9.9.9", 0o644) in shell.uploads


async def test_deploy_payload_failed_upload_records_no_destructive_swap() -> None:
    # A failed transfer must never half-replace a working install: only the
    # pre-stage `rm -rf <staging>` may run before put_dir; nothing after.
    shell = await _connected(FakeShell(put_dir_error=OSError("transfer aborted")))
    with pytest.raises(OSError, match="transfer aborted"):
        await panel_ops.deploy_payload(shell, "/local/payload", version="9.9.9")
    assert shell.commands == ["rm -rf /var/brilliant-mqtt.staging"]
    assert shell.uploads == []  # VERSION not written


async def test_deploy_payload_raises_and_skips_version_when_swap_fails() -> None:
    # The swap goes through _checked: a non-zero swap aborts before VERSION lands,
    # so a panel that failed to swap is never stamped with the new version.
    shell = await _connected(FakeShell(responses={_EXPECTED_SWAP: RunResult(1, "", "mv failed\n")}))
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.deploy_payload(shell, "/local/payload", version="9.9.9")
    assert shell.uploads == []  # VERSION not written


async def test_uninstall_sequence_and_paths() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.uninstall(shell)
    assert shell.commands == [
        "systemctl disable --now brilliant-mqtt 2>/dev/null || true",
        "rm -f /etc/systemd/system/brilliant-mqtt.service /etc/brilliant-mqtt.env",
        "rm -rf /var/brilliant-mqtt /var/brilliant-mqtt.staging",
        "systemctl daemon-reload",
    ]
    # Every absolute-path token references only the four owned path constants
    # (the staging sibling is derived from PANEL_VAR_DIR, so prefix-matches it).
    owned = (PANEL_VAR_DIR, PANEL_ENV_FILE, PANEL_UNIT_FILE, SERVICE_NAME)
    for command in shell.commands:
        for token in command.split():
            if token.startswith("/"):
                assert any(token.startswith(prefix) for prefix in owned), token


# ---------------------------------------------------------------------------
# Voice recipes
# ---------------------------------------------------------------------------

_FULL_VOICE_INSPECT = RunResult(
    0,
    "unit=1\nenv=1\nenabled=1\nactive=1\npayload=1\n0.1.0\n",
    "",
)

_ABSENT_VOICE_INSPECT = RunResult(
    0,
    "unit=0\nenv=0\nenabled=0\nactive=0\npayload=0\n",
    "",
)


async def test_inspect_voice_parses_fully_installed() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.VOICE_INSPECT_COMMAND: _FULL_VOICE_INSPECT})
    )
    state = await panel_ops.inspect_voice(shell)
    assert state == panel_ops.VoicePanelState(
        unit_present=True,
        env_present=True,
        enabled=True,
        active=True,
        payload_present=True,
        payload_version="0.1.0",
    )


async def test_inspect_voice_parses_all_absent() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.VOICE_INSPECT_COMMAND: _ABSENT_VOICE_INSPECT})
    )
    state = await panel_ops.inspect_voice(shell)
    assert state == panel_ops.VoicePanelState(
        unit_present=False,
        env_present=False,
        enabled=False,
        active=False,
        payload_present=False,
        payload_version=None,
    )


def test_render_voice_env_default_values() -> None:
    env = panel_ops.render_voice_env(
        panel="office",
        name="Brilliant office",
        api_port=6053,
        wake_word="okay_nabu",
        ha_host="192.168.1.10",
        enable_aec=False,
    )
    assert env.splitlines() == [
        'BRILLIANT_PANEL="office"',
        'VOICE_NAME="Brilliant office"',
        "VOICE_API_PORT=6053",
        'VOICE_WAKE_WORD="okay_nabu"',
        'VOICE_HA_HOST="192.168.1.10"',
        "VOICE_ENABLE_AEC=0",
        "LOG_LEVEL=INFO",
    ]


def test_render_voice_env_enable_aec_true() -> None:
    env = panel_ops.render_voice_env(
        panel="office",
        name="Brilliant office",
        api_port=6053,
        wake_word="okay_nabu",
        ha_host="",
        enable_aec=True,
    )
    lines = {line.split("=", 1)[0]: line.split("=", 1)[1] for line in env.splitlines()}
    assert lines["VOICE_ENABLE_AEC"] == "1"


def test_render_voice_env_empty_ha_host() -> None:
    env = panel_ops.render_voice_env(
        panel="office",
        name="Brilliant office",
        api_port=6053,
        wake_word="okay_nabu",
        ha_host="",
        enable_aec=False,
    )
    lines = {line.split("=", 1)[0]: line.split("=", 1)[1] for line in env.splitlines()}
    assert lines["VOICE_HA_HOST"] == '""'


def test_render_voice_env_quotes_special_chars() -> None:
    env = panel_ops.render_voice_env(
        panel='office"main',
        name='My "Panel"',
        api_port=6053,
        wake_word="okay_nabu",
        ha_host="",
        enable_aec=False,
    )
    # The double-quote in panel/name must be escaped, not break the quoting.
    assert 'BRILLIANT_PANEL="office\\"main"' in env
    assert 'VOICE_NAME="My \\"Panel\\""' in env


# The expected voice swap command spelled out independently of the impl's path
# constants (so the assertion is independent of any accidental const change).
_EXPECTED_VOICE_SWAP = " && ".join(
    [
        "mkdir -p /var/brilliant-voice.staging",
        "tar xzf /var/brilliant-voice.staging.tar.gz"
        " -C /var/brilliant-voice.staging --strip-components=1",
        "rm -f /var/brilliant-voice.staging.tar.gz",
        "rm -rf /var/brilliant-voice.bak",
        "{ [ -e /var/brilliant-voice ] && "
        "mv /var/brilliant-voice /var/brilliant-voice.bak; true; }",
        "mv /var/brilliant-voice.staging /var/brilliant-voice",
        "rm -rf /var/brilliant-voice.bak",
    ]
)


async def test_deploy_voice_payload_uploads_tarball_then_swaps() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_voice_payload(shell, "/local/voice.tar.gz", version="0.1.0")
    # 1. Staging clear runs first.
    assert shell.commands[0] == (
        "rm -rf /var/brilliant-voice.staging /var/brilliant-voice.staging.tar.gz"
    )
    # 2. Tarball uploaded via put_file (not put_dir).
    assert shell.file_uploads == [
        ("/local/voice.tar.gz", "/var/brilliant-voice.staging.tar.gz", 0o644)
    ]
    # 3. Swap command ran.
    assert _EXPECTED_VOICE_SWAP in shell.commands
    # 4. VERSION file written.
    assert (PANEL_VOICE_VERSION_FILE, b"0.1.0", 0o644) in shell.uploads


async def test_deploy_voice_payload_skips_version_when_swap_fails() -> None:
    shell = await _connected(
        FakeShell(responses={_EXPECTED_VOICE_SWAP: RunResult(1, "", "mv failed\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.deploy_voice_payload(shell, "/local/voice.tar.gz", version="0.1.0")
    # VERSION must not be stamped after a failed swap.
    assert shell.uploads == []


async def test_ensure_voice_config_writes_unit_env_and_reloads() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.ensure_voice_config(shell, env_content="ENVDATA")
    # mkdir and cp unit commands precede the daemon-reload.
    assert shell.commands[0] == f"mkdir -p {PANEL_VOICE_STAGED_DIR}"
    assert shell.commands[1] == (
        f"cp {PANEL_VOICE_VAR_DIR}/{VOICE_SERVICE_NAME}.service {PANEL_VOICE_UNIT_FILE}"
    )
    assert shell.commands[-1] == "systemctl daemon-reload"
    # env written to /etc (0600) and staged copy (0600).
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        (PANEL_VOICE_ENV_FILE, 0o600),
        (f"{PANEL_VOICE_STAGED_DIR}/{VOICE_SERVICE_NAME}.env", 0o600),
    ]
    # Both copies carry the same content.
    for _path, data, _mode in shell.uploads:
        assert data == b"ENVDATA"


async def test_ensure_voice_config_raises_when_mkdir_fails() -> None:
    shell = await _connected(
        FakeShell(responses={f"mkdir -p {PANEL_VOICE_STAGED_DIR}": RunResult(1, "", "denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.ensure_voice_config(shell, env_content="ENVDATA")
    assert shell.uploads == []


async def test_enable_voice_issues_systemctl_enable() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.enable_voice(shell)
    assert f"systemctl enable --now {VOICE_SERVICE_NAME}" in shell.commands


async def test_restart_voice_issues_systemctl_restart() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.restart_voice(shell)
    assert f"systemctl restart {VOICE_SERVICE_NAME}" in shell.commands


async def test_uninstall_voice_sequence_and_paths() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.uninstall_voice(shell)
    assert shell.commands == [
        f"systemctl disable --now {VOICE_SERVICE_NAME} 2>/dev/null || true",
        f"rm -f {PANEL_VOICE_UNIT_FILE} {PANEL_VOICE_ENV_FILE}",
        f"rm -rf {PANEL_VOICE_VAR_DIR}"
        " /var/brilliant-voice.staging /var/brilliant-voice.staging.tar.gz",
        "systemctl daemon-reload",
    ]


# ---------------------------------------------------------------------------
# HA mirror recipes
# ---------------------------------------------------------------------------

_FULL_HA_MIRROR_INSPECT = RunResult(
    0,
    "unit=1\nenv=1\nenabled=1\nactive=1\nsenv=1\npayload=1\n",
    "",
)

_ABSENT_HA_MIRROR_INSPECT = RunResult(
    0,
    "unit=0\nenv=0\nenabled=0\nactive=0\nsenv=0\npayload=0\n",
    "",
)


async def test_inspect_ha_mirror_parses_fully_installed() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.HA_MIRROR_INSPECT_COMMAND: _FULL_HA_MIRROR_INSPECT})
    )
    state = await panel_ops.inspect_ha_mirror(shell)
    assert state == panel_ops.HaMirrorState(
        unit_present=True,
        env_present=True,
        enabled=True,
        active=True,
        staged_env_present=True,
        payload_present=True,
    )


async def test_inspect_ha_mirror_parses_all_absent() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.HA_MIRROR_INSPECT_COMMAND: _ABSENT_HA_MIRROR_INSPECT})
    )
    state = await panel_ops.inspect_ha_mirror(shell)
    assert state == panel_ops.HaMirrorState(
        unit_present=False,
        env_present=False,
        enabled=False,
        active=False,
        staged_env_present=False,
        payload_present=False,
    )


def test_ha_mirror_inspect_command_checks_main_entrypoint() -> None:
    assert (
        f"{PANEL_HA_MIRROR_APP_DIR}/brilliant_ha_mirror/__main__.py"
        in panel_ops.HA_MIRROR_INSPECT_COMMAND
    )
    assert f"test -f {PANEL_HA_MIRROR_ENV_FILE}" in panel_ops.HA_MIRROR_INSPECT_COMMAND
    assert (
        f"{PANEL_HA_MIRROR_STAGED_DIR}/{HA_MIRROR_SERVICE_NAME}.env"
        in panel_ops.HA_MIRROR_INSPECT_COMMAND
    )


@pytest.mark.parametrize(("enabled", "expected"), [(False, "0"), (True, "1")])
def test_render_env_exposes_only_scene_bridge_toggle_to_panel(enabled: bool, expected: str) -> None:
    secret_action = "scene.private_action"
    secret_label = "private-label"
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=1,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="user",
        mqtt_password="password",
        scene_bridge_enabled=enabled,
    )
    assert panel_ops.parse_env(env)["SCENE_BRIDGE_ENABLED"] == expected
    for forbidden in (
        "HA_CONTROL_LABEL",
        "ROOM_OVERRIDES",
        "SCENE_ACTIONS",
        "HA_TOKEN",
        "HA_WS_URL",
        secret_action,
        secret_label,
    ):
        assert forbidden not in env


def test_render_ha_mirror_env_quotes_complete_contract() -> None:
    env = panel_ops.render_ha_mirror_env(
        panel='office"main',
        ha_ws_url="ws://homeassistant.local:8123/api/websocket",
        ha_token='secret"token',
        mirror_label="brilliant",
        leader_priority=7,
        mqtt_host="192.168.1.250",
        mqtt_port=1883,
        mqtt_username="brilliant",
        mqtt_password='p#a"ss',
    )
    assert env.splitlines() == [
        'PANEL="office\\"main"',
        'HA_WS_URL="ws://homeassistant.local:8123/api/websocket"',
        'HA_TOKEN="secret\\"token"',
        'MIRROR_LABEL="brilliant"',
        'LEADER_PRIORITY="7"',
        'MQTT_HOST="192.168.1.250"',
        'MQTT_PORT="1883"',
        'MQTT_USERNAME="brilliant"',
        'MQTT_PASSWORD="p#a\\"ss"',
        "LOG_LEVEL=INFO",
    ]


_EXPECTED_HA_MIRROR_SWAP = " && ".join(
    [
        "mkdir -p /var/brilliant-ha-mirror",
        "rm -rf /var/brilliant-ha-mirror/app.bak",
        "{ [ -e /var/brilliant-ha-mirror/app ] && "
        "mv /var/brilliant-ha-mirror/app /var/brilliant-ha-mirror/app.bak; true; }",
        "mv /var/brilliant-ha-mirror/app.staging /var/brilliant-ha-mirror/app",
        "rm -rf /var/brilliant-ha-mirror/app.bak",
    ]
)


async def test_deploy_ha_mirror_uploads_tree_then_swaps() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_ha_mirror(shell, "/local/ha_mirror")
    assert shell.commands[0] == "rm -rf /var/brilliant-ha-mirror/app.staging"
    assert shell.dir_uploads == [("/local/ha_mirror", "/var/brilliant-ha-mirror/app.staging")]
    assert _EXPECTED_HA_MIRROR_SWAP in shell.commands


async def test_deploy_ha_mirror_failed_upload_does_not_swap() -> None:
    shell = await _connected(FakeShell(put_dir_error=OSError("transfer aborted")))
    with pytest.raises(OSError, match="transfer aborted"):
        await panel_ops.deploy_ha_mirror(shell, "/local/ha_mirror")
    assert shell.commands == ["rm -rf /var/brilliant-ha-mirror/app.staging"]
    assert shell.dir_uploads == []


async def test_ensure_ha_mirror_config_writes_secret_env_0600() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.ensure_ha_mirror_config(shell, "UNIT_CONTENT", "ENV_CONTENT")
    assert shell.commands[0] == f"mkdir -p {PANEL_HA_MIRROR_STAGED_DIR}"
    assert shell.commands[-1] == "systemctl daemon-reload"
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        (PANEL_HA_MIRROR_UNIT_FILE, 0o644),
        (PANEL_HA_MIRROR_ENV_FILE, 0o600),
        (f"{PANEL_HA_MIRROR_STAGED_DIR}/{HA_MIRROR_SERVICE_NAME}.env", 0o600),
    ]
    assert shell.uploads[0][1] == b"UNIT_CONTENT"
    assert shell.uploads[1][1] == b"ENV_CONTENT"
    assert shell.uploads[2][1] == b"ENV_CONTENT"


async def test_enable_ha_mirror_issues_systemctl_enable() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.enable_ha_mirror(shell)
    assert f"systemctl enable --now {HA_MIRROR_SERVICE_NAME}" in shell.commands


async def test_uninstall_ha_mirror_sequence_and_paths() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.uninstall_ha_mirror(shell)
    assert shell.commands == [
        f"systemctl disable --now {HA_MIRROR_SERVICE_NAME} 2>/dev/null || true",
        f"rm -f {PANEL_HA_MIRROR_UNIT_FILE} {PANEL_HA_MIRROR_ENV_FILE}",
        f"rm -rf {PANEL_HA_MIRROR_VAR_DIR}",
        "systemctl daemon-reload",
    ]


# ---------------------------------------------------------------------------
# Wi-Fi watchdog recipes
# ---------------------------------------------------------------------------

_FULL_WATCHDOG_INSPECT = RunResult(
    0,
    "unit=1\nenabled=1\nactive=1\npayload=1\n",
    "",
)

_ABSENT_WATCHDOG_INSPECT = RunResult(
    0,
    "unit=0\nenabled=0\nactive=0\npayload=0\n",
    "",
)


async def test_inspect_wifi_watchdog_parses_fully_installed() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.WIFI_WATCHDOG_INSPECT_COMMAND: _FULL_WATCHDOG_INSPECT})
    )
    state = await panel_ops.inspect_wifi_watchdog(shell)
    assert state == panel_ops.WifiWatchdogState(
        unit_present=True,
        enabled=True,
        active=True,
        payload_present=True,
    )


async def test_inspect_wifi_watchdog_parses_all_absent() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.WIFI_WATCHDOG_INSPECT_COMMAND: _ABSENT_WATCHDOG_INSPECT})
    )
    state = await panel_ops.inspect_wifi_watchdog(shell)
    assert state == panel_ops.WifiWatchdogState(
        unit_present=False,
        enabled=False,
        active=False,
        payload_present=False,
    )


def test_wifi_watchdog_inspect_command_checks_run_py_entrypoint() -> None:
    """Probe checks the actual run.py entrypoint — not just the directory."""
    assert (
        f"{PANEL_WIFI_WATCHDOG_DIR}/brilliant_wifi_watchdog/run.py"
        in panel_ops.WIFI_WATCHDOG_INSPECT_COMMAND
    )


# The expected watchdog swap command spelled out independently of the impl's path
# constants (so the assertion is independent of any accidental const change).
_EXPECTED_WATCHDOG_SWAP = " && ".join(
    [
        "mkdir -p /var/brilliant-mqtt",
        "rm -rf /var/brilliant-mqtt/wifi_watchdog.bak",
        "{ [ -e /var/brilliant-mqtt/wifi_watchdog ] && "
        "mv /var/brilliant-mqtt/wifi_watchdog /var/brilliant-mqtt/wifi_watchdog.bak; true; }",
        "mv /var/brilliant-mqtt/wifi_watchdog.staging /var/brilliant-mqtt/wifi_watchdog",
        "rm -rf /var/brilliant-mqtt/wifi_watchdog.bak",
    ]
)


async def test_deploy_wifi_watchdog_uploads_tree_then_swaps() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_wifi_watchdog(shell, "/local/wifi_watchdog")
    assert shell.commands[0] == "rm -rf /var/brilliant-mqtt/wifi_watchdog.staging"
    assert shell.dir_uploads == [
        ("/local/wifi_watchdog", "/var/brilliant-mqtt/wifi_watchdog.staging")
    ]
    assert _EXPECTED_WATCHDOG_SWAP in shell.commands


async def test_deploy_wifi_watchdog_failed_upload_records_no_destructive_swap() -> None:
    """A failed put_dir must not trigger the swap — no partial-replace of working install."""
    shell = await _connected(FakeShell(put_dir_error=OSError("transfer aborted")))
    with pytest.raises(OSError, match="transfer aborted"):
        await panel_ops.deploy_wifi_watchdog(shell, "/local/wifi_watchdog")
    assert shell.commands == ["rm -rf /var/brilliant-mqtt/wifi_watchdog.staging"]
    assert shell.dir_uploads == []  # the put_dir failed → staging dir was never uploaded


async def test_deploy_wifi_watchdog_raises_when_swap_fails() -> None:
    shell = await _connected(
        FakeShell(responses={_EXPECTED_WATCHDOG_SWAP: RunResult(1, "", "mv failed\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.deploy_wifi_watchdog(shell, "/local/wifi_watchdog")


async def test_ensure_wifi_watchdog_unit_writes_etc_and_staged_then_reloads() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.ensure_wifi_watchdog_unit(shell, "UNIT_CONTENT")
    # mkdir runs first, daemon-reload last.
    assert shell.commands[0] == f"mkdir -p {PANEL_WIFI_WATCHDOG_DIR}"
    assert shell.commands[-1] == "systemctl daemon-reload"
    # Unit written to /etc (0644) and a staged copy under PANEL_WIFI_WATCHDOG_DIR (0644).
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        (PANEL_WIFI_WATCHDOG_UNIT_FILE, 0o644),
        (f"{PANEL_WIFI_WATCHDOG_DIR}/{WIFI_WATCHDOG_SERVICE_NAME}.service", 0o644),
    ]
    for _path, data, _mode in shell.uploads:
        assert data == b"UNIT_CONTENT"


async def test_ensure_wifi_watchdog_unit_raises_when_mkdir_fails() -> None:
    shell = await _connected(
        FakeShell(responses={f"mkdir -p {PANEL_WIFI_WATCHDOG_DIR}": RunResult(1, "", "denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.ensure_wifi_watchdog_unit(shell, "UNIT_CONTENT")
    assert shell.uploads == []


async def test_enable_wifi_watchdog_issues_systemctl_enable() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.enable_wifi_watchdog(shell)
    assert f"systemctl enable --now {WIFI_WATCHDOG_SERVICE_NAME}" in shell.commands


async def test_uninstall_wifi_watchdog_sequence_and_paths() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.uninstall_wifi_watchdog(shell)
    assert shell.commands == [
        f"systemctl disable --now {WIFI_WATCHDOG_SERVICE_NAME} 2>/dev/null || true",
        f"rm -f {PANEL_WIFI_WATCHDOG_UNIT_FILE}",
        "rm -rf /var/brilliant-mqtt/wifi_watchdog /var/brilliant-mqtt/wifi_watchdog.staging",
        "rm -f /var/brilliant-mqtt/wifi-watchdog.log /var/brilliant-mqtt/wifi-watchdog.state",
        "systemctl daemon-reload",
    ]
    # Uninstall must never rm the bridge's PANEL_VAR_DIR itself.
    for cmd in shell.commands:
        tokens = cmd.split()
        assert PANEL_VAR_DIR not in tokens, f"Command removes PANEL_VAR_DIR itself: {cmd!r}"


async def test_fake_shell_put_file_records_call() -> None:
    """put_file records (local_path, remote_path, mode) and requires connect."""
    shell = FakeShell()
    with pytest.raises(RuntimeError, match="not connected"):
        await shell.put_file("/local/x.tar.gz", "/remote/x.tar.gz", 0o644)
    await shell.connect()
    await shell.put_file("/local/x.tar.gz", "/remote/x.tar.gz", 0o644)
    assert shell.file_uploads == [("/local/x.tar.gz", "/remote/x.tar.gz", 0o644)]


# ---------------------------------------------------------------------------
# Bus-health watchdog recipes
# ---------------------------------------------------------------------------

_FULL_BUS_WATCHDOG_INSPECT = RunResult(
    0,
    "unit=1\nenabled=1\nactive=1\npayload=1\n",
    "",
)

_ABSENT_BUS_WATCHDOG_INSPECT = RunResult(
    0,
    "unit=0\nenabled=0\nactive=0\npayload=0\n",
    "",
)


async def test_inspect_bus_watchdog_parses_fully_installed() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.BUS_WATCHDOG_INSPECT_COMMAND: _FULL_BUS_WATCHDOG_INSPECT})
    )
    state = await panel_ops.inspect_bus_watchdog(shell)
    assert state == panel_ops.BusWatchdogState(
        unit_present=True,
        enabled=True,
        active=True,
        payload_present=True,
    )


async def test_inspect_bus_watchdog_parses_all_absent() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.BUS_WATCHDOG_INSPECT_COMMAND: _ABSENT_BUS_WATCHDOG_INSPECT})
    )
    state = await panel_ops.inspect_bus_watchdog(shell)
    assert state == panel_ops.BusWatchdogState(
        unit_present=False,
        enabled=False,
        active=False,
        payload_present=False,
    )


def test_bus_watchdog_inspect_command_checks_run_py_entrypoint() -> None:
    """Probe checks the actual run.py entrypoint — not just the directory."""
    assert (
        f"{PANEL_BUS_WATCHDOG_DIR}/brilliant_bus_watchdog/run.py"
        in panel_ops.BUS_WATCHDOG_INSPECT_COMMAND
    )


# The expected bus watchdog swap command spelled out independently of the impl's path
# constants (so the assertion is independent of any accidental const change).
_EXPECTED_BUS_WATCHDOG_SWAP = " && ".join(
    [
        "mkdir -p /var/brilliant-mqtt",
        "rm -rf /var/brilliant-mqtt/bus_watchdog.bak",
        "{ [ -e /var/brilliant-mqtt/bus_watchdog ] && "
        "mv /var/brilliant-mqtt/bus_watchdog /var/brilliant-mqtt/bus_watchdog.bak; true; }",
        "mv /var/brilliant-mqtt/bus_watchdog.staging /var/brilliant-mqtt/bus_watchdog",
        "rm -rf /var/brilliant-mqtt/bus_watchdog.bak",
    ]
)


async def test_deploy_bus_watchdog_uploads_tree_then_swaps() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_bus_watchdog(shell, "/local/bus_watchdog")
    assert shell.commands[0] == "rm -rf /var/brilliant-mqtt/bus_watchdog.staging"
    assert shell.dir_uploads == [
        ("/local/bus_watchdog", "/var/brilliant-mqtt/bus_watchdog.staging")
    ]
    assert _EXPECTED_BUS_WATCHDOG_SWAP in shell.commands


async def test_deploy_bus_watchdog_failed_upload_records_no_destructive_swap() -> None:
    """A failed put_dir must not trigger the swap — no partial-replace of working install."""
    shell = await _connected(FakeShell(put_dir_error=OSError("transfer aborted")))
    with pytest.raises(OSError, match="transfer aborted"):
        await panel_ops.deploy_bus_watchdog(shell, "/local/bus_watchdog")
    assert shell.commands == ["rm -rf /var/brilliant-mqtt/bus_watchdog.staging"]
    assert shell.dir_uploads == []  # the put_dir failed → staging dir was never uploaded


async def test_deploy_bus_watchdog_raises_when_swap_fails() -> None:
    shell = await _connected(
        FakeShell(responses={_EXPECTED_BUS_WATCHDOG_SWAP: RunResult(1, "", "mv failed\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.deploy_bus_watchdog(shell, "/local/bus_watchdog")


async def test_ensure_bus_watchdog_unit_writes_etc_and_staged_then_reloads() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.ensure_bus_watchdog_unit(shell, "UNIT_CONTENT")
    # mkdir runs first, daemon-reload last.
    assert shell.commands[0] == f"mkdir -p {PANEL_BUS_WATCHDOG_DIR}"
    assert shell.commands[-1] == "systemctl daemon-reload"
    # Unit written to /etc (0644) and a staged copy under PANEL_BUS_WATCHDOG_DIR (0644).
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        (PANEL_BUS_WATCHDOG_UNIT_FILE, 0o644),
        (f"{PANEL_BUS_WATCHDOG_DIR}/{BUS_WATCHDOG_SERVICE_NAME}.service", 0o644),
    ]
    for _path, data, _mode in shell.uploads:
        assert data == b"UNIT_CONTENT"


async def test_ensure_bus_watchdog_unit_raises_when_mkdir_fails() -> None:
    shell = await _connected(
        FakeShell(responses={f"mkdir -p {PANEL_BUS_WATCHDOG_DIR}": RunResult(1, "", "denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.ensure_bus_watchdog_unit(shell, "UNIT_CONTENT")
    assert shell.uploads == []


async def test_enable_bus_watchdog_issues_systemctl_enable() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.enable_bus_watchdog(shell)
    assert f"systemctl enable --now {BUS_WATCHDOG_SERVICE_NAME}" in shell.commands


async def test_uninstall_bus_watchdog_sequence_and_paths() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.uninstall_bus_watchdog(shell)
    assert shell.commands == [
        f"systemctl disable --now {BUS_WATCHDOG_SERVICE_NAME} 2>/dev/null || true",
        f"rm -f {PANEL_BUS_WATCHDOG_UNIT_FILE}",
        "rm -rf /var/brilliant-mqtt/bus_watchdog /var/brilliant-mqtt/bus_watchdog.staging",
        "rm -f /var/brilliant-mqtt/bus-watchdog.log /var/brilliant-mqtt/bus-watchdog.state",
        "systemctl daemon-reload",
    ]
    # Uninstall must never rm the bridge's PANEL_VAR_DIR itself.
    for cmd in shell.commands:
        tokens = cmd.split()
        assert PANEL_VAR_DIR not in tokens, f"Command removes PANEL_VAR_DIR itself: {cmd!r}"
