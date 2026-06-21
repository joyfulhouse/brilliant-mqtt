"""panel_ops recipes against FakeShell — command sequences and parsing."""

from __future__ import annotations

import pytest

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.const import (
    PANEL_ENV_FILE,
    PANEL_UNIT_FILE,
    PANEL_VAR_DIR,
    SERVICE_NAME,
)
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell

_FULL_INSPECT = RunResult(
    0,
    "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\n9.9.9\n",
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


def test_render_env_matches_agent_config_contract() -> None:
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=1,
        mqtt_host="172.16.1.205",
        mqtt_port=1883,
        mqtt_username="brilliant",
        mqtt_password="secret",
    )
    # String values are systemd-double-quoted; the int fields stay bare.
    assert env.splitlines() == [
        'BRILLIANT_PANEL="office"',
        'MQTT_HOST="172.16.1.205"',
        "MQTT_PORT=1883",
        'MQTT_USERNAME="brilliant"',
        'MQTT_PASSWORD="secret"',
        "MESH_PRIORITY=1",
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


def _systemd_unquote(rendered_value: str) -> str:
    r"""Reverse systemd's double-quote rules: strip the quotes, unescape \\ and \"."""
    assert rendered_value.startswith('"') and rendered_value.endswith('"')
    inner = rendered_value[1:-1]
    out: list[str] = []
    i = 0
    while i < len(inner):
        ch = inner[i]
        if ch == "\\" and i + 1 < len(inner):
            out.append(inner[i + 1])
            i += 2
        else:
            out.append(ch)
            i += 1
    return "".join(out)


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
    # The value is double-quoted and what systemd would parse back equals the input.
    assert value.startswith('"') and value.endswith('"')
    assert _systemd_unquote(value) == password


def test_parse_env_round_trips_render_env() -> None:
    """parse_env recovers every value render_env wrote (the adopt-installed path)."""
    env = panel_ops.render_env(
        panel="office-bath",
        mesh_priority=7,
        mqtt_host="172.16.1.205",
        mqtt_port=8883,
        mqtt_username="brilliant",
        mqtt_password='p#a"s\\s',  # the hostile chars _env_quote escapes
    )
    assert panel_ops.parse_env(env) == {
        "BRILLIANT_PANEL": "office-bath",
        "MQTT_HOST": "172.16.1.205",
        "MQTT_PORT": "8883",
        "MQTT_USERNAME": "brilliant",
        "MQTT_PASSWORD": 'p#a"s\\s',
        "MESH_PRIORITY": "7",
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
