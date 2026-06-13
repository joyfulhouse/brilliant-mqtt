"""panel_ops recipes against FakeShell — command sequences and parsing."""

from __future__ import annotations

from custom_components.brilliant_mqtt import panel_ops
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakeShell

_FULL_INSPECT = RunResult(
    0,
    "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\n0.2.0\n",
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
        payload_version="0.2.0",
    )


async def test_inspect_parses_wiped_etc() -> None:
    wiped = RunResult(0, "unit=0\nenv=0\nenabled=0\nactive=0\nsunit=1\nsenv=1\n0.2.0\n", "")
    shell = await _connected(FakeShell(responses={panel_ops.INSPECT_COMMAND: wiped}))
    state = await panel_ops.inspect_panel(shell)
    assert not state.unit_present and not state.env_present
    assert state.staged_unit_present and state.staged_env_present
    assert state.payload_version == "0.2.0"


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
    # Exactly the variables src/brilliant_mqtt/config.py reads.
    assert env.splitlines() == [
        "BRILLIANT_PANEL=office",
        "MQTT_HOST=172.16.1.205",
        "MQTT_PORT=1883",
        "MQTT_USERNAME=brilliant",
        "MQTT_PASSWORD=secret",
        "MESH_PRIORITY=1",
        "LOG_LEVEL=INFO",
    ]


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


async def test_enable_now_and_journal() -> None:
    shell = await _connected(
        FakeShell(responses={panel_ops.journal_command(50): RunResult(0, "log line\n", "")})
    )
    await panel_ops.enable_now(shell)
    assert "systemctl enable --now brilliant-mqtt" in shell.commands
    assert (await panel_ops.collect_journal(shell, 50)) == "log line\n"


async def test_deploy_payload_uploads_tree_then_swaps() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.deploy_payload(shell, "/local/payload", version="0.2.0")
    assert shell.commands[0] == "rm -rf /var/brilliant-mqtt.staging"
    assert shell.dir_uploads == [("/local/payload", "/var/brilliant-mqtt.staging")]
    # Atomic-ish swap: stage fully, then move app+vendor into place.
    swap = (
        "mkdir -p /var/brilliant-mqtt && "
        "rm -rf /var/brilliant-mqtt/app /var/brilliant-mqtt/vendor && "
        "mv /var/brilliant-mqtt.staging/app /var/brilliant-mqtt/app && "
        "mv /var/brilliant-mqtt.staging/vendor /var/brilliant-mqtt/vendor && "
        "rm -rf /var/brilliant-mqtt.staging"
    )
    assert swap in shell.commands
    assert ("/var/brilliant-mqtt/VERSION", b"0.2.0", 0o644) in shell.uploads
