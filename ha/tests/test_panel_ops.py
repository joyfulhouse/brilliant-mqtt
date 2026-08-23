"""panel_ops recipes against FakeShell — command sequences and parsing."""

from __future__ import annotations

import asyncio
import base64
import hashlib
import json
import os
import re
import secrets
import shlex
import subprocess
import sys
from dataclasses import replace
from pathlib import Path
from uuid import UUID

import asyncssh
import pytest

from custom_components.brilliant_mqtt import panel_inspection, panel_ops
from custom_components.brilliant_mqtt.const import (
    BUS_WATCHDOG_SERVICE_NAME,
    COMPONENT_BRIDGE,
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_WIFI_WATCHDOG,
    HA_MIRROR_SERVICE_NAME,
    HUE_CA_TIMER_NAME,
    PANEL_BUS_WATCHDOG_DIR,
    PANEL_BUS_WATCHDOG_UNIT_FILE,
    PANEL_CURRENT_LINK,
    PANEL_ENV_FILE,
    PANEL_HA_MIRROR_APP_DIR,
    PANEL_HA_MIRROR_ENV_FILE,
    PANEL_HA_MIRROR_STAGED_DIR,
    PANEL_HA_MIRROR_UNIT_FILE,
    PANEL_HA_MIRROR_VAR_DIR,
    PANEL_MQTT_TLS_DIR,
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
from custom_components.brilliant_mqtt.setup_protocol import PreflightRequest
from custom_components.brilliant_mqtt.shell import RunResult
from tests.fakes import FakePanelProcess, FakeShell

_FULL_INSPECT = RunResult(
    0,
    "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\npayload=1\n9.9.9\n",
    "",
)
_EXPECTED_ENV_MQTT_CA_PATH = "/var/brilliant-mqtt/tls/mqtt-ca-0fa6a631898df0f5.pem"
_TEST_MQTT_CA = b"test-ca\n"
_TEST_MQTT_CA_DIGEST = hashlib.sha256(_TEST_MQTT_CA).hexdigest()
_TEST_MQTT_CA_PATH = f"/var/brilliant-mqtt/tls/mqtt-ca-{_TEST_MQTT_CA_DIGEST[:16]}.pem"
_TEMP_TOKEN = "0" * 32
_TEST_MQTT_CA_TEMP_PATH = f"{_TEST_MQTT_CA_PATH}.tmp-{_TEMP_TOKEN}"
_VERIFY_MQTT_CA_COMMAND = f"/usr/bin/sha256sum -- {_TEST_MQTT_CA_TEMP_PATH}"
_PROMOTE_MQTT_CA_COMMAND = f"ln {_TEST_MQTT_CA_TEMP_PATH} {_TEST_MQTT_CA_PATH}"
_COMPARE_MQTT_CA_COMMAND = (
    f"test -f {_TEST_MQTT_CA_PATH} && test ! -L {_TEST_MQTT_CA_PATH} "
    f"&& cmp -s {_TEST_MQTT_CA_TEMP_PATH} {_TEST_MQTT_CA_PATH}"
)
_STAT_MQTT_CA_MODE_COMMAND = f"stat -c %a -- {_TEST_MQTT_CA_PATH}"
_CLEAN_MQTT_CA_TEMP_COMMAND = f"rm -f {_TEST_MQTT_CA_TEMP_PATH}"


def _private_key_pem(secret: str) -> bytes:
    """Build rejected private-key material without embedding a scanner signature."""
    label = "PRIVATE KEY"
    return f"-----BEGIN {label}-----\n{secret}\n-----END {label}-----\n".encode()


async def _connected[ShellT: FakeShell](shell: ShellT) -> ShellT:
    await shell.connect()
    return shell


def _assert_valid_posix_shell(command: str) -> None:
    parsed = subprocess.run(
        ["sh", "-n"],
        input=command,
        text=True,
        capture_output=True,
        check=False,
    )
    assert parsed.returncode == 0, parsed.stderr


def _noop_boundary() -> None:
    pass


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
        "MQTT_TLS_ENABLED=0",
        "RETAINED_TOPICS_FILE=/var/brilliant-mqtt/state/owned-topics.json",
        "MESH_PRIORITY=1",
        "SCENE_BRIDGE_ENABLED=1",
        "LOG_LEVEL=INFO",
    ]


def test_render_env_custom_tls_uses_only_content_addressed_ca_path() -> None:
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=1,
        mqtt_host='broker "quoted".example',
        mqtt_port=8883,
        mqtt_username='fleet # "user"',
        mqtt_password='p#a"s\\word',
        mqtt_tls_enabled=True,
        mqtt_tls_ca_file=_EXPECTED_ENV_MQTT_CA_PATH,
    )

    assert "MQTT_TLS_ENABLED=1\n" in env
    assert f"MQTT_TLS_CA_FILE={_EXPECTED_ENV_MQTT_CA_PATH}\n" in env
    assert "RETAINED_TOPICS_FILE=/var/brilliant-mqtt/state/owned-topics.json\n" in env
    assert "test-ca" not in env
    assert "test-ca" not in repr(env)
    parsed = panel_ops.parse_env(env)
    assert parsed["MQTT_HOST"] == 'broker "quoted".example'
    assert parsed["MQTT_USERNAME"] == 'fleet # "user"'
    assert parsed["MQTT_PASSWORD"] == 'p#a"s\\word'


@pytest.mark.parametrize(
    ("tls_enabled", "expected_tls_line"),
    [(False, "MQTT_TLS_ENABLED=0"), (True, "MQTT_TLS_ENABLED=1")],
)
def test_render_env_without_custom_ca_uses_plaintext_or_system_trust(
    tls_enabled: bool,
    expected_tls_line: str,
) -> None:
    env = panel_ops.render_env(
        panel="office",
        mesh_priority=1,
        mqtt_host="broker.example",
        mqtt_port=8883 if tls_enabled else 1883,
        mqtt_username="fleet",
        mqtt_password="password",
        mqtt_tls_enabled=tls_enabled,
    )

    assert expected_tls_line in env.splitlines()
    assert "MQTT_TLS_CA_FILE=" not in env
    assert "RETAINED_TOPICS_FILE=/var/brilliant-mqtt/state/owned-topics.json" in env.splitlines()


def test_render_env_rejects_custom_ca_path_without_tls() -> None:
    with pytest.raises(ValueError, match="mqtt_tls_ca_file_requires_tls"):
        panel_ops.render_env(
            panel="office",
            mesh_priority=1,
            mqtt_host="broker.example",
            mqtt_port=1883,
            mqtt_username="fleet",
            mqtt_password="password",
            mqtt_tls_enabled=False,
            mqtt_tls_ca_file=_EXPECTED_ENV_MQTT_CA_PATH,
        )


@pytest.mark.parametrize("tls_enabled", ["true", "false", 0, 1, None])
def test_render_env_rejects_non_boolean_tls_flag(tls_enabled: object) -> None:
    with pytest.raises(ValueError, match="invalid_mqtt_tls_enabled"):
        panel_ops.render_env(
            panel="office",
            mesh_priority=1,
            mqtt_host="broker.example",
            mqtt_port=8883,
            mqtt_username="fleet",
            mqtt_password="password",
            mqtt_tls_enabled=tls_enabled,  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "ca_path",
    [
        "/var/brilliant-mqtt/tls/mqtt-ca-0fa6a631898df0f5.pem\nLD_PRELOAD=/tmp/evil.so",
        "/tmp/mqtt-ca-0fa6a631898df0f5.pem",
        "/var/brilliant-mqtt/tls/../mqtt-ca-0fa6a631898df0f5.pem",
        "/var/brilliant-mqtt/tls/mqtt-ca-0FA6A631898DF0F5.pem",
        "/var/brilliant-mqtt/tls/mqtt-ca-0fa6a631898df0f5.pem ",
    ],
)
def test_render_env_rejects_unmanaged_or_injectable_ca_path(ca_path: str) -> None:
    with pytest.raises(ValueError, match="invalid_mqtt_tls_ca_file"):
        panel_ops.render_env(
            panel="office",
            mesh_priority=1,
            mqtt_host="broker.example",
            mqtt_port=8883,
            mqtt_username="fleet",
            mqtt_password="password",
            mqtt_tls_enabled=True,
            mqtt_tls_ca_file=ca_path,
        )


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
    lines = [line for line in env.split("\n") if line.startswith("MQTT_PASSWORD=")]
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
    "invalid_character",
    [
        "\ufeff",
        "\ufdd0",
        "\ufdef",
        "\ufffe",
        "\uffff",
        "\U0001fffe",
        "\U0010ffff",
        "\ud800",
    ],
)
def test_render_env_rejects_systemd_invalid_unicode(
    invalid_character: str,
) -> None:
    with pytest.raises(ValueError, match="environment values"):
        panel_ops.render_env(
            panel="office",
            mesh_priority=0,
            mqtt_host="h",
            mqtt_port=1883,
            mqtt_username="u",
            mqtt_password=f"before{invalid_character}after",
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
        "MQTT_TLS_ENABLED": "0",
        "RETAINED_TOPICS_FILE": "/var/brilliant-mqtt/state/owned-topics.json",
        "MESH_PRIORITY": "7",
        "SCENE_BRIDGE_ENABLED": "0",
        "LOG_LEVEL": "INFO",
    }


@pytest.mark.parametrize(
    "password",
    [
        "trailing\\",
        'has"quote',
        "#comment-like",
        "space sep arated",
        "dollar$VAR-ish",
        "vertical\vtab",
        "form\ffeed",
        "next\x85line",
        "unicode\u2028line",
        "unicode\u2029paragraph",
    ],
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


def test_render_env_round_trips_through_deployed_preflight_parser(
    tmp_path: Path,
) -> None:
    command_substitution = tmp_path / "must-not-exist"
    password = f'before"\\$HOME$(touch {command_substitution})`id`\v\f\x85\u2028\u2029after'
    deployment_id = "1234567812344abc8def1234567890ab"
    environment_file = tmp_path / "brilliant-mqtt.env"
    environment_file.write_text(
        panel_ops.render_env(
            panel="office",
            mesh_priority=0,
            mqtt_host="broker.internal.example",
            mqtt_port=1883,
            mqtt_username="u",
            mqtt_password=password,
            deployment_id=deployment_id,
        ),
        encoding="utf-8",
    )
    payload_app = Path(panel_ops.__file__).parent / "agent_payload" / "app"
    process_environment = dict(os.environ)
    process_environment.update(
        {
            "PYTHONPATH": str(payload_app),
            "EXPECTED_PASSWORD": password,
            "EXPECTED_DEPLOYMENT_ID": deployment_id,
        }
    )
    completed = subprocess.run(
        [
            sys.executable,
            "-c",
            (
                "import os,sys;"
                "from brilliant_mqtt.preflight import _settings_from_environment_file;"
                "s=_settings_from_environment_file(sys.argv[1]);"
                "raise SystemExit(0 if "
                "s.mqtt_password==os.environ['EXPECTED_PASSWORD'] and "
                "s.deployment_id==os.environ['EXPECTED_DEPLOYMENT_ID'] else 2)"
            ),
            str(environment_file),
        ],
        check=False,
        capture_output=True,
        env=process_environment,
        text=True,
        timeout=5.0,
    )

    assert completed.returncode == 0
    assert completed.stdout == ""
    assert completed.stderr == ""
    assert not command_substitution.exists()


def test_release_ca_detection_uses_only_literal_lf_records() -> None:
    ca_path = f"{_CANDIDATE_RELEASE}/mqtt-ca.pem"
    environment = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="h",
        mqtt_port=8883,
        mqtt_username="u",
        mqtt_password=f'before\u2028{panel_ops.ENV_MQTT_TLS_CA_FILE}="not-an-assignment"',
        mqtt_tls_enabled=True,
        mqtt_tls_ca_file=ca_path,
    )

    assert panel_ops._mqtt_ca_environment_assignments(environment) == (
        f"{panel_ops.ENV_MQTT_TLS_CA_FILE}={ca_path}",
    )


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


def test_mqtt_tls_guard_command_is_valid_posix_shell() -> None:
    _assert_valid_posix_shell(panel_ops.MQTT_TLS_GUARD_COMMAND)


async def test_write_env_writes_only_env_to_etc_and_staged() -> None:
    shell = await _connected(FakeShell())
    await panel_ops.write_env(shell, "ENVDATA")
    # Only the env file (no unit), both locations, 0600.
    assert [(path, mode) for (path, _data, mode) in shell.uploads] == [
        ("/etc/brilliant-mqtt.env", 0o600),
        ("/var/brilliant-mqtt/system/brilliant-mqtt.env", 0o600),
    ]
    assert shell.commands == [
        panel_ops.MQTT_TLS_GUARD_COMMAND,
        "mkdir -p /var/brilliant-mqtt/system",
    ]


async def test_write_env_refuses_existing_tls_to_plaintext_before_mutation() -> None:
    desired = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="fleet",
        mqtt_password="password",
    )
    shell = await _connected(
        FakeShell(
            responses={
                panel_ops.MQTT_TLS_GUARD_COMMAND: RunResult(
                    0,
                    # The live file may be plaintext while the OTA-proof staged
                    # copy still carries the operator's TLS configuration.
                    "MQTT_TLS_ENABLED=0\nMQTT_TLS_ENABLED='true'\n",
                    "",
                )
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_tls_downgrade_refused") as exc_info:
        await panel_ops.write_env(shell, desired)

    message = str(exc_info.value)
    assert "cannot round-trip TLS settings" in message
    assert "manual lifecycle management" in message
    assert "configure MQTT TLS in Home Assistant" not in message
    assert shell.commands == [panel_ops.MQTT_TLS_GUARD_COMMAND]
    assert PANEL_ENV_FILE in panel_ops.MQTT_TLS_GUARD_COMMAND
    assert "/var/brilliant-mqtt/system/brilliant-mqtt.env" in (panel_ops.MQTT_TLS_GUARD_COMMAND)
    assert shell.uploads == []


async def test_write_env_refuses_ambiguous_existing_tls_assignment() -> None:
    desired = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="fleet",
        mqtt_password="password",
    )
    shell = await _connected(
        FakeShell(
            responses={
                panel_ops.MQTT_TLS_GUARD_COMMAND: RunResult(
                    0,
                    r"MQTT_TLS_ENABLED=t\rue" + "\n",
                    "",
                )
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_tls_downgrade_refused"):
        await panel_ops.write_env(shell, desired)

    assert shell.commands == [panel_ops.MQTT_TLS_GUARD_COMMAND]
    assert shell.uploads == []


@pytest.mark.parametrize(
    "existing",
    [
        "MQTT_TLS_ENABLED=0\n",
        "MQTT_TLS_ENABLED='false'\n",
        'MQTT_TLS_ENABLED="off"\n',
        "MQTT_TLS_ENABLED=no\n",
    ],
)
async def test_write_env_accepts_provably_plaintext_existing_env(existing: str) -> None:
    desired = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="fleet",
        mqtt_password="password",
    )
    shell = await _connected(
        FakeShell(
            responses={
                panel_ops.MQTT_TLS_GUARD_COMMAND: RunResult(0, existing, ""),
            }
        )
    )

    await panel_ops.write_env(shell, desired)

    assert len(shell.uploads) == 2


async def test_write_env_tls_destination_needs_no_existing_env_probe() -> None:
    desired = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="broker",
        mqtt_port=8883,
        mqtt_username="fleet",
        mqtt_password="password",
        mqtt_tls_enabled=True,
    )
    shell = await _connected(
        FakeShell(
            run_errors={
                panel_ops.MQTT_TLS_GUARD_COMMAND: RuntimeError("must not probe"),
            }
        )
    )

    await panel_ops.write_env(shell, desired)

    assert panel_ops.MQTT_TLS_GUARD_COMMAND not in shell.commands


async def test_write_env_raises_when_mkdir_fails() -> None:
    shell = await _connected(
        FakeShell(responses={"mkdir -p /var/brilliant-mqtt/system": RunResult(1, "", "denied\n")})
    )
    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.write_env(shell, "ENVDATA")
    assert shell.uploads == []


async def test_stage_mqtt_ca_verifies_temp_then_promotes_without_writing_final(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{_TEST_MQTT_CA_DIGEST}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                )
            }
        )
    )

    path = await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert path == _EXPECTED_ENV_MQTT_CA_PATH == _TEST_MQTT_CA_PATH
    assert shell.commands == [
        "mkdir -p /var/brilliant-mqtt/tls",
        _VERIFY_MQTT_CA_COMMAND,
        _PROMOTE_MQTT_CA_COMMAND,
        _CLEAN_MQTT_CA_TEMP_COMMAND,
    ]
    assert shell.uploads == [(_TEST_MQTT_CA_TEMP_PATH, _TEST_MQTT_CA, 0o644)]
    assert all(path != _TEST_MQTT_CA_PATH for path, _data, _mode in shell.uploads)


async def test_stage_mqtt_ca_repeat_compares_existing_file_without_replacing_it(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{_TEST_MQTT_CA_DIGEST}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                ),
                _PROMOTE_MQTT_CA_COMMAND: RunResult(1, "", "File exists\n"),
                _COMPARE_MQTT_CA_COMMAND: RunResult(0, "", ""),
                _STAT_MQTT_CA_MODE_COMMAND: RunResult(0, "644\n", ""),
            }
        )
    )

    assert await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA) == _TEST_MQTT_CA_PATH
    assert shell.commands[-4:] == [
        _PROMOTE_MQTT_CA_COMMAND,
        _COMPARE_MQTT_CA_COMMAND,
        _STAT_MQTT_CA_MODE_COMMAND,
        _CLEAN_MQTT_CA_TEMP_COMMAND,
    ]
    assert shell.uploads == [(_TEST_MQTT_CA_TEMP_PATH, _TEST_MQTT_CA, 0o644)]


@pytest.mark.parametrize("mode", ["600", "666"])
async def test_stage_mqtt_ca_rejects_exact_existing_bytes_with_wrong_mode(
    monkeypatch: pytest.MonkeyPatch,
    mode: str,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{_TEST_MQTT_CA_DIGEST}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                ),
                _PROMOTE_MQTT_CA_COMMAND: RunResult(1, "", "File exists\n"),
                _COMPARE_MQTT_CA_COMMAND: RunResult(0, "", ""),
                _STAT_MQTT_CA_MODE_COMMAND: RunResult(0, f"{mode}\n", ""),
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_ca_promotion_failed"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert shell.commands[-1] == _CLEAN_MQTT_CA_TEMP_COMMAND
    assert all("chmod" not in command for command in shell.commands)
    assert all(path != _TEST_MQTT_CA_PATH for path, _data, _mode in shell.uploads)


async def test_stage_mqtt_ca_rejects_conflicting_existing_file_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{_TEST_MQTT_CA_DIGEST}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                ),
                _PROMOTE_MQTT_CA_COMMAND: RunResult(1, "", "File exists\n"),
                _COMPARE_MQTT_CA_COMMAND: RunResult(1, "", ""),
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_ca_promotion_failed"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert shell.commands[-1] == _CLEAN_MQTT_CA_TEMP_COMMAND
    assert all(path != _TEST_MQTT_CA_PATH for path, _data, _mode in shell.uploads)


async def test_stage_mqtt_ca_rejects_remote_digest_mismatch_before_promotion(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{'0' * 64}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                )
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_ca_verification_failed"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert _PROMOTE_MQTT_CA_COMMAND not in shell.commands
    assert shell.commands[-1] == _CLEAN_MQTT_CA_TEMP_COMMAND


async def test_stage_mqtt_ca_rejects_missing_remote_digest_and_cleans_temp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(0, "", ""),
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_ca_verification_failed"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert _PROMOTE_MQTT_CA_COMMAND not in shell.commands
    assert shell.commands[-1] == _CLEAN_MQTT_CA_TEMP_COMMAND


async def test_stage_mqtt_ca_interruption_cleans_only_its_unique_temp(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(run_errors={_VERIFY_MQTT_CA_COMMAND: ConnectionError("transport interrupted")})
    )

    with pytest.raises(ConnectionError, match="transport interrupted"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert shell.commands[-1] == _CLEAN_MQTT_CA_TEMP_COMMAND
    assert f"rm -f {_TEST_MQTT_CA_PATH}" not in shell.commands


async def test_stage_mqtt_ca_cleanup_failure_does_not_replace_verification_error(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{'0' * 64}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                ),
                _CLEAN_MQTT_CA_TEMP_COMMAND: RunResult(1, "", "cleanup failed\n"),
            }
        )
    )

    with pytest.raises(
        panel_ops.PanelOpError,
        match="^mqtt_ca_verification_failed$",
    ):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)


async def test_stage_mqtt_ca_new_cancellation_during_cleanup_wins_over_primary(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    cleanup_entered = asyncio.Event()
    cleanup_gate = asyncio.Event()

    class BlockingCleanupShell(FakeShell):
        async def run(self, command: str) -> RunResult:
            if command == _CLEAN_MQTT_CA_TEMP_COMMAND:
                self._require_connected()
                self.commands.append(command)
                cleanup_entered.set()
                await cleanup_gate.wait()
                return RunResult(0, "", "")
            return await super().run(command)

    shell = await _connected(
        BlockingCleanupShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{'0' * 64}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                ),
            }
        )
    )
    task = asyncio.create_task(panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA))
    await asyncio.wait_for(cleanup_entered.wait(), timeout=1)

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task


@pytest.mark.parametrize(
    "primary",
    [ConnectionError("transport interrupted"), asyncio.CancelledError()],
)
async def test_stage_mqtt_ca_cleanup_failure_preserves_transport_or_cancellation(
    monkeypatch: pytest.MonkeyPatch,
    primary: BaseException,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)

    class PrimaryFailureShell(FakeShell):
        async def run(self, command: str) -> RunResult:
            if command == _VERIFY_MQTT_CA_COMMAND:
                self._require_connected()
                self.commands.append(command)
                raise primary
            return await super().run(command)

    shell = await _connected(
        PrimaryFailureShell(
            responses={
                _CLEAN_MQTT_CA_TEMP_COMMAND: RunResult(1, "", "cleanup failed\n"),
            },
        )
    )

    with pytest.raises(type(primary)) as caught:
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert caught.value is primary


async def test_stage_mqtt_ca_success_surfaces_temp_cleanup_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(secrets, "token_hex", lambda length: _TEMP_TOKEN)
    shell = await _connected(
        FakeShell(
            responses={
                _VERIFY_MQTT_CA_COMMAND: RunResult(
                    0,
                    f"{_TEST_MQTT_CA_DIGEST}  {_TEST_MQTT_CA_TEMP_PATH}\n",
                    "",
                ),
                _CLEAN_MQTT_CA_TEMP_COMMAND: RunResult(1, "", "cleanup failed\n"),
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)


@pytest.mark.parametrize(
    "ca_bytes",
    [
        None,
        "",
        bytearray(_TEST_MQTT_CA),
        b"",
        _private_key_pem("secret"),
        _TEST_MQTT_CA + _private_key_pem("secret"),
    ],
)
async def test_stage_mqtt_ca_rejects_invalid_or_private_material_before_shell(
    ca_bytes: object,
) -> None:
    shell = await _connected(FakeShell())

    with pytest.raises(ValueError, match="invalid_mqtt_tls_ca"):
        await panel_ops.stage_mqtt_ca(shell, ca_bytes)  # type: ignore[arg-type]

    assert shell.commands == []
    assert shell.uploads == []


async def test_stage_mqtt_ca_mkdir_failure_prevents_upload() -> None:
    mkdir = "mkdir -p /var/brilliant-mqtt/tls"
    shell = await _connected(FakeShell(responses={mkdir: RunResult(1, "", "permission denied\n")}))

    with pytest.raises(panel_ops.PanelOpError, match="exited 1"):
        await panel_ops.stage_mqtt_ca(shell, _TEST_MQTT_CA)

    assert shell.commands == [mkdir]
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
    assert shell.commands[0] == panel_ops.MQTT_TLS_GUARD_COMMAND
    assert shell.commands[1] == "mkdir -p /var/brilliant-mqtt/system"
    assert shell.commands[-1] == "systemctl daemon-reload"


async def test_ensure_configs_refuses_existing_tls_to_plaintext_before_mutation() -> None:
    desired = panel_ops.render_env(
        panel="office",
        mesh_priority=0,
        mqtt_host="broker",
        mqtt_port=1883,
        mqtt_username="fleet",
        mqtt_password="password",
    )
    shell = await _connected(
        FakeShell(
            responses={
                panel_ops.MQTT_TLS_GUARD_COMMAND: RunResult(
                    0,
                    ' MQTT_TLS_ENABLED = "true"\n',
                    "",
                )
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="mqtt_tls_downgrade_refused"):
        await panel_ops.ensure_configs(shell, unit_content="UNIT", env_content=desired)

    assert shell.commands == [panel_ops.MQTT_TLS_GUARD_COMMAND]
    assert shell.uploads == []


async def test_tls_guard_probe_failure_is_fail_closed_before_mutation() -> None:
    shell = await _connected(
        FakeShell(
            responses={
                panel_ops.MQTT_TLS_GUARD_COMMAND: RunResult(
                    2,
                    "",
                    "permission denied\n",
                )
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError, match="exited 2"):
        await panel_ops.ensure_configs(shell, unit_content="UNIT", env_content="ENV")

    assert shell.commands == [panel_ops.MQTT_TLS_GUARD_COMMAND]
    assert shell.uploads == []


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


async def test_collect_diagnostics_assembles_sections_and_tolerates_a_failing_probe() -> None:
    secret = "MQTT_PASSWORD=diagnostics-secret-canary"
    shell = await _connected(
        FakeShell(
            responses={
                "cat /proc/uptime": RunResult(0, "259200.5 100.0\n", secret),
                "free -k": RunResult(
                    0,
                    f"Mem: 131072 65536 32768 0 0 32768\n{secret}\n",
                    "",
                ),
                "df -Pk /var": RunResult(
                    0,
                    "Filesystem 1024-blocks Used Available Capacity Mounted on\n"
                    f"/dev/root 1000000 250000 750000 25% /var\n{secret}\n",
                    "",
                ),
                "journalctl -b -n 400 --no-pager": RunResult(
                    0,
                    f"bridge failed after timeout; {secret}\n",
                    "",
                ),
                f"journalctl -u {SERVICE_NAME} -n 200 --no-pager": RunResult(
                    0,
                    f"MQTT authentication failed; {secret}\n",
                    "",
                ),
                "iw dev wlan0 link": RunResult(
                    0,
                    f"Connected to 00:11:22:33:44:55\nSSID: {secret}\nsignal: -55 dBm\n",
                    "",
                ),
                "iw dev wlan0 get power_save": RunResult(
                    0,
                    f"Power save: on\n{secret}\n",
                    "",
                ),
                f"systemctl status {SERVICE_NAME} --no-pager | head -n 15": RunResult(
                    0,
                    f"Loaded: loaded (/secret; enabled)\nActive: active (running)\n{secret}\n",
                    secret,
                ),
            },
            run_errors={"connmanctl services | head -n 15": asyncssh.ConnectionLost(secret)},
        )
    )
    bundle = await panel_ops.collect_diagnostics(shell, lines=400)
    summary = json.loads(bundle)

    assert summary["schema_version"] == 1
    assert summary["journal_line_limit"] == 400
    probes = {probe["id"]: probe for probe in summary["probes"]}
    assert len(probes) == 10
    assert probes["uptime"]["uptime_seconds"] == 259200
    assert probes["memory"] == {
        "id": "memory",
        "outcome": "ok",
        "stderr_bytes": 0,
        "stderr_lines": 0,
        "stdout_bytes": 74,
        "stdout_lines": 2,
        "free_kib": 32768,
        "total_kib": 131072,
        "used_kib": 65536,
    }
    assert probes["var_filesystem"]["used_percent"] == 25
    assert probes["boot_events"]["categories"] == {
        "bridge_failure": 1,
        "timeout": 1,
    }
    assert probes["bridge_events"]["categories"] == {
        "bridge_failure": 1,
        "mqtt_authentication_failure": 1,
    }
    assert probes["wifi_link"]["wifi_state"] == "connected"
    assert probes["wifi_link"]["rssi_dbm"] == -55
    assert probes["wifi_power_save"]["power_save"] == "on"
    assert probes["bridge_status"]["active_state"] == "active"
    assert probes["bridge_status"]["enabled_state"] == "enabled"
    assert probes["connman_services"] == {
        "id": "connman_services",
        "outcome": "transport_error",
    }
    assert secret not in bundle
    assert "00:11:22:33:44:55" not in bundle
    assert "/secret" not in bundle
    assert "journalctl" not in bundle
    assert set(probes) == {
        "uptime",
        "memory",
        "var_filesystem",
        "boot_events",
        "bridge_events",
        "kernel_events",
        "wifi_link",
        "wifi_power_save",
        "connman_services",
        "bridge_status",
    }


async def test_collect_diagnostics_unit_depth_never_floors_to_zero() -> None:
    # lines//2 must stay ≥ 1 (journalctl -n 0 prints nothing).
    shell = await _connected(FakeShell())
    summary = json.loads(await panel_ops.collect_diagnostics(shell, lines=1))

    assert summary["journal_line_limit"] == 1
    assert f"journalctl -u {SERVICE_NAME} -n 1 --no-pager" in shell.commands


async def test_collect_diagnostics_bounds_counts_and_omits_malformed_content() -> None:
    secret = "API_TOKEN=malformed-diagnostics-secret"
    shell = await _connected(
        FakeShell(
            responses={
                "cat /proc/uptime": RunResult(
                    0,
                    f"{secret}\x00" * 20_000,
                    f"{secret}\n" * 20_000,
                )
            }
        )
    )

    bundle = await panel_ops.collect_diagnostics(shell)
    summary = json.loads(bundle)
    uptime = next(probe for probe in summary["probes"] if probe["id"] == "uptime")

    assert uptime["stdout_bytes"] == panel_ops.DIAGNOSTICS_COUNT_LIMIT
    assert uptime["stderr_bytes"] == panel_ops.DIAGNOSTICS_COUNT_LIMIT
    assert "uptime_seconds" not in uptime
    assert secret not in bundle


def test_diagnostic_events_scan_the_newest_bounded_complete_lines() -> None:
    old_event = "out of memory\n"
    padding = "neutral status\n" * (panel_ops._DIAGNOSTICS_PARSE_LIMIT // 8)
    newest_event = "MQTT authentication failed\n"

    summary = panel_ops._event_categories(old_event + padding + newest_event)

    assert summary == {
        "categories": {
            "bridge_failure": 1,
            "mqtt_authentication_failure": 1,
        }
    }


def test_diagnostic_events_do_not_classify_success_as_failure() -> None:
    summary = panel_ops._event_categories(
        "MQTT authentication succeeded\nTLS certificate loaded successfully\n"
    )

    assert summary == {}


async def test_reboot_treats_ssh_disconnect_as_success() -> None:
    # `reboot` tears down sshd; the mid-command asyncssh disconnect IS the success signal.
    shell = await _connected(
        FakeShell(run_errors={panel_ops.REBOOT_COMMAND: asyncssh.ConnectionLost("bye")})
    )
    await panel_ops.reboot(shell)  # must NOT raise
    assert panel_ops.REBOOT_COMMAND in shell.commands


async def test_reboot_tolerates_oserror_and_a_clean_return() -> None:
    dropped = await _connected(FakeShell(run_errors={panel_ops.REBOOT_COMMAND: OSError("reset")}))
    await panel_ops.reboot(dropped)  # OSError disconnect → success
    clean = await _connected(FakeShell())
    await panel_ops.reboot(clean)  # systemd answered 0 before dropping → success
    assert panel_ops.REBOOT_COMMAND in clean.commands


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


@pytest.mark.parametrize(
    "result",
    [
        RunResult(1, _ABSENT_HA_MIRROR_INSPECT.stdout, "probe failed"),
        RunResult(0, "unit=0\nenv=0\nenabled=0\nactive=0\nsenv=0\n", ""),
        RunResult(
            0,
            "unit=0\nunit=0\nenv=0\nenabled=0\nactive=0\nsenv=0\npayload=0\n",
            "",
        ),
        RunResult(
            0,
            "unit=yes\nenv=0\nenabled=0\nactive=0\nsenv=0\npayload=0\n",
            "",
        ),
        RunResult(
            0,
            "unit=0\nenv=0\nenabled=0\nactive=0\nsenv=0\npayload=0\nunknown=0\n",
            "",
        ),
        RunResult(
            0,
            "unit=0\nenv=0\nenabled=0\nactive=0\nsenv=0\npay",
            "",
        ),
    ],
    ids=["nonzero", "missing", "duplicate", "malformed", "unknown", "truncated"],
)
async def test_inspect_ha_mirror_rejects_ambiguous_proof(result: RunResult) -> None:
    shell = await _connected(FakeShell(responses={panel_ops.HA_MIRROR_INSPECT_COMMAND: result}))
    with pytest.raises(panel_ops.PanelOpError):
        await panel_ops.inspect_ha_mirror(shell)


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


# ---------------------------------------------------------------------------
# Fleet staged-release operations
# ---------------------------------------------------------------------------

_TRANSACTION_ID = UUID("12345678-1234-4abc-8def-1234567890ab")
_PRIOR_RELEASE = "/var/brilliant-mqtt/releases/0.5.7--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
_CANDIDATE_RELEASE = "/var/brilliant-mqtt/releases/0.6.0--1234567812344abc8def1234567890ab"
_CORE_COMPONENTS = (
    COMPONENT_BRIDGE,
    COMPONENT_WIFI_WATCHDOG,
    COMPONENT_BUS_WATCHDOG,
)


def _encoded_file(content: bytes | None, mode: int | None) -> RunResult:
    value = {
        "content": (None if content is None else base64.b64encode(content).decode("ascii")),
        "mode": mode,
        "present": content is not None,
    }
    return RunResult(0, json.dumps(value, sort_keys=True, separators=(",", ":")), "")


def _snapshot_responses(
    *,
    layout: str = "release_link",
    target: str | None = _PRIOR_RELEASE,
    env: bytes | None = b"MQTT_PASSWORD=snapshot-secret\n",
    version: bytes | None = b"0.5.7",
    bridge_unit: bytes | None = b"bridge-unit",
    wifi_unit: bytes | None = b"wifi-unit",
    bus_unit: bytes | None = b"bus-unit",
    bridge_state: tuple[bool, bool] = (True, True),
    wifi_state: tuple[bool, bool] = (True, True),
    bus_state: tuple[bool, bool] = (True, True),
) -> dict[str, RunResult]:
    layout_value = {
        "active_release_target": target,
        "layout": layout,
        "services": {
            COMPONENT_BRIDGE: {
                "active": bridge_state[1],
                "enabled": bridge_state[0],
            },
            COMPONENT_WIFI_WATCHDOG: {
                "active": wifi_state[1],
                "enabled": wifi_state[0],
            },
            COMPONENT_BUS_WATCHDOG: {
                "active": bus_state[1],
                "enabled": bus_state[0],
            },
        },
    }
    return {
        panel_ops.SNAPSHOT_LAYOUT_COMMAND: RunResult(
            0,
            json.dumps(layout_value, sort_keys=True, separators=(",", ":")),
            "",
        ),
        panel_ops.SNAPSHOT_FILE_COMMANDS["environment"]: _encoded_file(
            env, None if env is None else 0o600
        ),
        panel_ops.SNAPSHOT_FILE_COMMANDS["version"]: _encoded_file(
            version, None if version is None else 0o644
        ),
        panel_ops.SNAPSHOT_FILE_COMMANDS["bridge_unit"]: _encoded_file(
            bridge_unit, None if bridge_unit is None else 0o644
        ),
        panel_ops.SNAPSHOT_FILE_COMMANDS["wifi_unit"]: _encoded_file(
            wifi_unit, None if wifi_unit is None else 0o644
        ),
        panel_ops.SNAPSHOT_FILE_COMMANDS["bus_unit"]: _encoded_file(
            bus_unit, None if bus_unit is None else 0o644
        ),
    }


def _exception_graph(root: BaseException) -> list[BaseException]:
    found: list[BaseException] = []
    pending = [root]
    seen: set[int] = set()
    while pending:
        error = pending.pop()
        if id(error) in seen:
            continue
        seen.add(id(error))
        found.append(error)
        if error.__context__ is not None:
            pending.append(error.__context__)
        if error.__cause__ is not None:
            pending.append(error.__cause__)
        pending.extend(argument for argument in error.args if isinstance(argument, BaseException))
    return found


def _staged(
    selected_components: tuple[str, ...] = _CORE_COMPONENTS,
) -> panel_ops.StagedRelease:
    return panel_ops.StagedRelease(
        version="0.6.0",
        transaction_id=_TRANSACTION_ID,
        release_target=_CANDIDATE_RELEASE,
        selected_components=selected_components,
    )


def _file(content: bytes | None, mode: int | None) -> panel_ops.FileSnapshot:
    return panel_ops.FileSnapshot(content=content, mode=mode)


def _service(
    content: bytes | None,
    *,
    enabled: bool,
    active: bool,
) -> panel_ops.ServiceSnapshot:
    return panel_ops.ServiceSnapshot(
        unit_file=_file(content, None if content is None else 0o644),
        enabled=enabled,
        active=active,
    )


def _release_snapshot(
    *,
    bridge_state: tuple[bool, bool] = (True, True),
    wifi_state: tuple[bool, bool] = (True, True),
    bus_state: tuple[bool, bool] = (True, True),
) -> panel_ops.PanelSnapshot:
    return panel_ops.PanelSnapshot(
        layout=panel_ops.PanelLayout.RELEASE_LINK,
        active_release_target=_PRIOR_RELEASE,
        environment_file=_file(b"MQTT_PASSWORD=snapshot-secret\n", 0o600),
        version_file=_file(b"0.5.7", 0o644),
        bridge_service=_service(
            b"bridge-unit",
            enabled=bridge_state[0],
            active=bridge_state[1],
        ),
        wifi_watchdog_service=_service(
            b"wifi-unit",
            enabled=wifi_state[0],
            active=wifi_state[1],
        ),
        bus_watchdog_service=_service(
            b"bus-unit",
            enabled=bus_state[0],
            active=bus_state[1],
        ),
        selected_components=_CORE_COMPONENTS,
    )


def _absent_snapshot() -> panel_ops.PanelSnapshot:
    missing = _file(None, None)
    missing_service = panel_ops.ServiceSnapshot(
        unit_file=missing,
        enabled=False,
        active=False,
    )
    return panel_ops.PanelSnapshot(
        layout=panel_ops.PanelLayout.ABSENT,
        active_release_target=None,
        environment_file=missing,
        version_file=missing,
        bridge_service=missing_service,
        wifi_watchdog_service=missing_service,
        bus_watchdog_service=missing_service,
        selected_components=(),
    )


def _watchdog_residue_snapshot() -> panel_ops.PanelSnapshot:
    missing = _file(None, None)
    missing_service = panel_ops.ServiceSnapshot(
        unit_file=missing,
        enabled=False,
        active=False,
    )
    return panel_ops.PanelSnapshot(
        layout=panel_ops.PanelLayout.LEGACY_FIXED,
        active_release_target=None,
        environment_file=missing,
        version_file=missing,
        bridge_service=missing_service,
        wifi_watchdog_service=_service(
            b"orphaned-wifi-unit",
            enabled=True,
            active=False,
        ),
        bus_watchdog_service=missing_service,
        selected_components=(COMPONENT_WIFI_WATCHDOG,),
    )


async def test_fleet_atomic_moves_use_only_the_verified_coreutils_mover() -> None:
    staged = _staged()
    restore_shell = await _connected(FakeShell())
    await panel_ops._restore_file(
        restore_shell,
        PANEL_ENV_FILE,
        _file(b"restored", 0o600),
        staged,
    )
    commands = (
        panel_ops._stage_promote_command(staged),
        panel_ops._activation_commit_command(staged),
        restore_shell.commands[-1],
        panel_ops._rollback_link_command(_release_snapshot(), staged),
    )
    joined = "\n".join(commands)

    assert (
        "/usr/bin/mv.coreutils --no-clobber --no-target-directory -- "
        f"{panel_ops._staged_temp_path(staged)} {_CANDIDATE_RELEASE}"
    ) in commands[0]
    assert joined.count("/usr/bin/mv.coreutils --force --no-target-directory -- ") == 8
    assert re.search(r"(?<![/A-Za-z0-9_.-])mv\s+-T", joined) is None
    assert "mv -Tf" not in joined
    assert "mv -T -n" not in joined


def test_release_ca_digest_uses_verified_sha256sum_without_a_cut_dependency() -> None:
    staged = _staged((COMPONENT_BRIDGE,))
    path = f"{panel_ops._staged_temp_path(staged)}/mqtt-ca.pem"

    command = panel_ops._release_mqtt_ca_digest_command(
        staged,
        _TEST_MQTT_CA_DIGEST,
    )

    assert command == (
        f"test \"$(/usr/bin/sha256sum -- {path})\" = '{_TEST_MQTT_CA_DIGEST}  {path}'"
    )
    assert " cut " not in command


@pytest.mark.parametrize("result", ("masked-runtime", "bad"))
def test_toolchain_accepts_documented_is_enabled_states(result: str) -> None:
    command = next(
        command
        for _key, capability, command in panel_inspection._TOOLCHAIN_PROBES
        if capability == "systemd_is_enabled"
    )
    script = f"systemctl() {{ printf '%s\\n' {result}; return 1; }}; {command}"

    assert subprocess.run(["sh", "-c", script], check=False).returncode == 0


def test_toolchain_accepts_is_enabled_without_stdout() -> None:
    """An unknown unit prints nothing on stdout on older systemd.

    systemd 250 (Brilliant firmware v26.07.15.1) reports an unknown unit on
    stderr and exits non-zero, leaving stdout empty, where newer systemd prints
    ``not-found``. Inspection must read that as ``not-found`` rather than
    rejecting the panel with ``unsupported_panel_toolchain`` -- otherwise a
    first install can never pass, because the probed unit is the one the
    integration has not installed yet.
    """
    command = next(
        command
        for _key, capability, command in panel_inspection._TOOLCHAIN_PROBES
        if capability == "systemd_is_enabled"
    )
    script = (
        "systemctl() { "
        "printf '%s\\n' 'Failed to get unit file state for brilliant-mqtt.service: "
        "No such file or directory' >&2; "
        "return 1; }; "
    ) + command

    assert subprocess.run(["sh", "-c", script], check=False).returncode == 0


def test_snapshot_layout_probe_accepts_is_enabled_without_stdout(tmp_path: Path) -> None:
    """The layout probe must not abort when a core unit is absent on systemd 250.

    ``state()`` raises SystemExit(45) for any value outside its known set, and
    an empty string is outside that set, so a panel with no bridge units yet
    fails the snapshot with ``snapshot_failed``.
    """
    stub = tmp_path / "systemctl"
    stub.write_text(
        "#!/bin/sh\n"
        'if [ "$1" = "is-enabled" ]; then\n'
        '  echo "Failed to get unit file state for $2: No such file or directory" >&2\n'
        "  exit 1\n"
        "fi\n"
        "printf 'inactive\\n'\n"
    )
    stub.chmod(0o755)
    command = panel_ops.SNAPSHOT_LAYOUT_COMMAND.replace(
        panel_ops._PANEL_PYTHON, shlex.quote(sys.executable)
    )

    result = subprocess.run(
        ["/bin/sh", "-c", command],
        check=False,
        capture_output=True,
        text=True,
        timeout=15,
        env={**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr
    services = json.loads(result.stdout)["services"]
    assert services
    assert all(state == {"active": False, "enabled": False} for state in services.values())


def test_disabled_state_command_accepts_is_enabled_without_stdout(tmp_path: Path) -> None:
    """An absent unit is a valid disabled state on systemd 250."""
    stub = tmp_path / "systemctl"
    stub.write_text(
        "#!/bin/sh\n"
        'echo "Failed to get unit file state for $2: No such file or directory" >&2\n'
        "exit 1\n"
    )
    stub.chmod(0o755)

    result = subprocess.run(
        ["/bin/sh", "-c", panel_ops._disabled_state_command(SERVICE_NAME)],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"},
    )

    assert result.returncode == 0, result.stderr


def test_disabled_state_command_rejects_enabled_unit(tmp_path: Path) -> None:
    """The empty-stdout default must not weaken the disabled assertion.

    ``_disabled_state_command`` exists to assert the unit is *not* enabled, so a
    unit that reports ``enabled`` must still fail with 48. This guards the
    systemd-250 empty->not-found default from being an accidentally permissive
    classifier that accepts any state.
    """
    stub = tmp_path / "systemctl"
    stub.write_text("#!/bin/sh\nprintf 'enabled\\n'\n")
    stub.chmod(0o755)

    result = subprocess.run(
        ["/bin/sh", "-c", panel_ops._disabled_state_command(SERVICE_NAME)],
        check=False,
        capture_output=True,
        text=True,
        env={**os.environ, "PATH": f"{tmp_path}{os.pathsep}{os.environ['PATH']}"},
    )

    assert result.returncode == 48, result.stdout


def test_release_symlink_validation_preserves_find_failure() -> None:
    command = panel_ops._no_symlinks_command("/dev/null")
    script = f"find() {{ return 23; }}; {command}"

    assert "find /dev/null -type l -print -quit" in command
    assert subprocess.run(["sh", "-c", script], check=False).returncode == 23


async def test_snapshot_panel_reads_exact_bounded_release_state() -> None:
    shell = await _connected(FakeShell(responses=_snapshot_responses()))

    snapshot = await panel_ops.snapshot_panel(shell)

    assert panel_ops.SNAPSHOT_LAYOUT_COMMAND.startswith(
        "/data/switch-embedded/env/bin/python3 - <<'BRILLIANT_MQTT_SNAPSHOT'"
    )
    assert all(
        command.startswith(
            "/data/switch-embedded/env/bin/python3 - <<'BRILLIANT_MQTT_FILE_SNAPSHOT'"
        )
        for command in panel_ops.SNAPSHOT_FILE_COMMANDS.values()
    )
    assert not panel_ops.SNAPSHOT_LAYOUT_COMMAND.startswith("python3 ")
    assert snapshot == _release_snapshot()
    assert shell.commands == [
        panel_ops.SNAPSHOT_LAYOUT_COMMAND,
        panel_ops.SNAPSHOT_FILE_COMMANDS["environment"],
        panel_ops.SNAPSHOT_FILE_COMMANDS["version"],
        panel_ops.SNAPSHOT_FILE_COMMANDS["bridge_unit"],
        panel_ops.SNAPSHOT_FILE_COMMANDS["wifi_unit"],
        panel_ops.SNAPSHOT_FILE_COMMANDS["bus_unit"],
    ]
    assert "snapshot-secret" not in repr(snapshot)
    assert "snapshot-secret" not in repr(snapshot.environment_file)


async def test_snapshot_panel_accepts_only_fully_absent_first_install() -> None:
    shell = await _connected(
        FakeShell(
            responses=_snapshot_responses(
                layout="absent",
                target=None,
                env=None,
                version=None,
                bridge_unit=None,
                wifi_unit=None,
                bus_unit=None,
                bridge_state=(False, False),
                wifi_state=(False, False),
                bus_state=(False, False),
            )
        )
    )

    assert await panel_ops.snapshot_panel(shell) == _absent_snapshot()


async def test_snapshot_panel_accepts_bridge_less_legacy_watchdog_residue() -> None:
    shell = await _connected(
        FakeShell(
            responses=_snapshot_responses(
                layout="legacy_fixed",
                target=None,
                env=None,
                version=None,
                bridge_unit=None,
                wifi_unit=b"orphaned-wifi-unit",
                bus_unit=None,
                bridge_state=(False, False),
                wifi_state=(True, False),
                bus_state=(False, False),
            )
        )
    )

    assert await panel_ops.snapshot_panel(shell) == _watchdog_residue_snapshot()


@pytest.mark.parametrize(
    "target",
    [
        "/var/brilliant-mqtt/releases/../../etc",
        "/var/brilliant-mqtt/releases/0.5.7",
        "releases/0.5.7--aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
        "/var/brilliant-mqtt/releases/0.5.7--AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA",
    ],
)
async def test_snapshot_panel_rejects_release_targets_outside_strict_grammar(
    target: str,
) -> None:
    shell = await _connected(FakeShell(responses=_snapshot_responses(target=target)))

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.snapshot_panel(shell)

    assert str(raised.value) == "snapshot_payload_invalid"


async def test_snapshot_panel_redacts_non_symlink_or_probe_failure() -> None:
    shell = await _connected(
        FakeShell(
            responses={
                panel_ops.SNAPSHOT_LAYOUT_COMMAND: RunResult(
                    41,
                    "",
                    "SECRET current is not a symlink",
                )
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.snapshot_panel(shell)

    assert str(raised.value) == "snapshot_probe_failed"
    graph = _exception_graph(raised.value)
    assert graph == [raised.value]
    assert all("SECRET" not in str(error) for error in graph)


async def test_snapshot_panel_rejects_oversized_or_malformed_file_payload() -> None:
    responses = _snapshot_responses()
    responses[panel_ops.SNAPSHOT_FILE_COMMANDS["environment"]] = RunResult(
        0,
        "x" * (panel_ops.MAX_SNAPSHOT_WIRE_BYTES + 1),
        "",
    )
    shell = await _connected(FakeShell(responses=responses))

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.snapshot_panel(shell)

    assert str(raised.value) == "snapshot_payload_invalid"


async def test_snapshot_panel_maps_json_recursion_without_raw_exception(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    def recursive_json(*_args: object, **_kwargs: object) -> object:
        raise RecursionError("SECRET recursive payload")

    monkeypatch.setattr(
        "custom_components.brilliant_mqtt.panel_ops.json.loads",
        recursive_json,
    )
    shell = await _connected(
        FakeShell(responses={panel_ops.SNAPSHOT_LAYOUT_COMMAND: RunResult(0, "{}", "")})
    )

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.snapshot_panel(shell)

    assert str(raised.value) == "snapshot_payload_invalid"
    assert _exception_graph(raised.value) == [raised.value]


async def test_stage_release_is_write_only_below_unique_release_temp() -> None:
    shell = await _connected(FakeShell())

    staged = await panel_ops.stage_release(
        shell,
        "/trusted/local/payload",
        "0.6.0",
        "MQTT_PASSWORD=staged-secret\n",
        (COMPONENT_BUS_WATCHDOG, COMPONENT_BRIDGE),
        _TRANSACTION_ID,
    )

    assert staged == _staged((COMPONENT_BRIDGE, COMPONENT_BUS_WATCHDOG))
    assert staged.release_target == _CANDIDATE_RELEASE
    assert shell.dir_uploads == [
        (
            "/trusted/local/payload",
            "/var/brilliant-mqtt/releases/.0.6.0--1234567812344abc8def1234567890ab.tmp",
        )
    ]
    assert any(
        path.endswith("/brilliant-mqtt.env") and mode == 0o600 for path, _, mode in shell.uploads
    )
    assert any(path.endswith("/VERSION") and mode == 0o644 for path, _, mode in shell.uploads)
    temporary_ca = (
        "/var/brilliant-mqtt/releases/.0.6.0--1234567812344abc8def1234567890ab.tmp/mqtt-ca.pem"
    )
    assert f"rm -f -- {temporary_ca}" in shell.commands
    assert all("/etc/" not in command for command in shell.commands)
    assert all(PANEL_CURRENT_LINK not in command for command in shell.commands)
    assert all("systemctl" not in command for command in shell.commands)
    validation = panel_ops._release_validation_command(staged)
    assert validation in shell.commands
    assert f"test ! -e {temporary_ca}" in validation
    for required in (
        "app/brilliant_mqtt/__main__.py",
        "vendor",
        "wifi_watchdog/brilliant_wifi_watchdog/run.py",
        "bus_watchdog/brilliant_bus_watchdog/run.py",
        "brilliant-mqtt-release.service",
        "brilliant-wifi-watchdog-release.service",
        "brilliant-bus-watchdog-release.service",
    ):
        assert required in validation


async def test_stage_release_owns_custom_ca_and_validates_exact_binding() -> None:
    shell = await _connected(FakeShell())
    ca_path = f"{_CANDIDATE_RELEASE}/mqtt-ca.pem"

    staged = await panel_ops.stage_release(
        shell,
        "/trusted/local/payload",
        "0.6.0",
        f"MQTT_TLS_CA_FILE={ca_path}\n",
        (COMPONENT_BRIDGE,),
        _TRANSACTION_ID,
        mqtt_ca=_TEST_MQTT_CA,
    )

    temporary_ca = (
        "/var/brilliant-mqtt/releases/.0.6.0--1234567812344abc8def1234567890ab.tmp/mqtt-ca.pem"
    )
    assert staged.release_target == _CANDIDATE_RELEASE
    assert (temporary_ca, _TEST_MQTT_CA, 0o644) in shell.uploads
    assert all(PANEL_MQTT_TLS_DIR not in path for path, _, _ in shell.uploads)
    assert all(PANEL_MQTT_TLS_DIR not in command for command in shell.commands)
    reserved_path_clear = f"rm -f -- {temporary_ca}"
    assert reserved_path_clear in shell.commands
    digest_validation = panel_ops._release_mqtt_ca_digest_command(
        staged,
        _TEST_MQTT_CA_DIGEST,
    )
    assert digest_validation in shell.commands
    assert shell.commands.index(reserved_path_clear) < shell.commands.index(digest_validation)
    validation = panel_ops._release_validation_command(staged)
    assert ca_path in validation
    assert temporary_ca in validation
    assert "MQTT_TLS_CA_FILE=" in validation
    assert "stat -c %a" in validation


async def test_stage_release_ca_digest_mismatch_cleans_transaction_release() -> None:
    staged = _staged((COMPONENT_BRIDGE,))
    digest_validation = panel_ops._release_mqtt_ca_digest_command(
        staged,
        _TEST_MQTT_CA_DIGEST,
    )
    shell = await _connected(
        FakeShell(responses={digest_validation: RunResult(1, "", "SECRET digest mismatch")})
    )
    ca_path = f"{_CANDIDATE_RELEASE}/mqtt-ca.pem"

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            f"MQTT_TLS_CA_FILE={ca_path}\n",
            (COMPONENT_BRIDGE,),
            _TRANSACTION_ID,
            mqtt_ca=_TEST_MQTT_CA,
        )

    assert str(raised.value) == "stage_validation_failed"
    assert shell.commands[-1] == panel_ops._cleanup_staged_command(staged)
    assert PANEL_MQTT_TLS_DIR not in "\n".join(shell.commands)


@pytest.mark.parametrize(
    ("environment", "mqtt_ca"),
    [
        ("ENV=1\n", _TEST_MQTT_CA),
        (f"MQTT_TLS_CA_FILE={_CANDIDATE_RELEASE}/mqtt-ca.pem\n", None),
        ("MQTT_TLS_CA_FILE=/var/brilliant-mqtt/tls/mqtt-ca-deadbeefdeadbeef.pem\n", _TEST_MQTT_CA),
    ],
)
async def test_stage_release_rejects_unbound_custom_ca_before_shell(
    environment: str,
    mqtt_ca: bytes | None,
) -> None:
    shell = await _connected(FakeShell())

    with pytest.raises(panel_ops.PanelOpError, match="invalid_staged_release"):
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            environment,
            (COMPONENT_BRIDGE,),
            _TRANSACTION_ID,
            mqtt_ca=mqtt_ca,
        )

    assert not shell.commands
    assert not shell.dir_uploads
    assert not shell.uploads


class _FailingReleaseCaUploadShell(FakeShell):
    async def put_bytes(self, data: bytes, remote_path: str, mode: int) -> None:
        if remote_path.endswith("/mqtt-ca.pem"):
            raise OSError("SECRET custom CA transfer failure")
        await super().put_bytes(data, remote_path, mode)


async def test_stage_release_ca_upload_failure_cleans_transaction_release_only() -> None:
    shell = await _connected(_FailingReleaseCaUploadShell())
    ca_path = f"{_CANDIDATE_RELEASE}/mqtt-ca.pem"

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            f"MQTT_TLS_CA_FILE={ca_path}\n",
            (COMPONENT_BRIDGE,),
            _TRANSACTION_ID,
            mqtt_ca=_TEST_MQTT_CA,
        )

    assert str(raised.value) == "stage_upload_failed"
    cleanup = shell.commands[-1]
    assert _CANDIDATE_RELEASE in cleanup
    assert ".0.6.0--1234567812344abc8def1234567890ab.tmp" in cleanup
    assert PANEL_MQTT_TLS_DIR not in cleanup
    assert all(PANEL_MQTT_TLS_DIR not in command for command in shell.commands)


@pytest.mark.parametrize(
    "selected",
    [
        (),
        (COMPONENT_WIFI_WATCHDOG,),
        (COMPONENT_BRIDGE, COMPONENT_BRIDGE),
        (COMPONENT_BRIDGE, "voice"),
        (COMPONENT_BRIDGE, "hue_ca"),
        (COMPONENT_BRIDGE, "ha_mirror"),
        (COMPONENT_BRIDGE, "unknown"),
    ],
)
async def test_stage_release_rejects_invalid_core_selection_before_shell(
    selected: tuple[str, ...],
) -> None:
    shell = await _connected(FakeShell())

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            "ENV=1\n",
            selected,
            _TRANSACTION_ID,
        )

    assert str(raised.value) == "invalid_selected_components"
    assert not shell.commands
    assert not shell.dir_uploads
    assert not shell.uploads


async def test_stage_release_never_interpolates_local_caller_path_into_commands() -> None:
    local_path = "/tmp/payload; touch /tmp/SHOULD_NOT_EXIST"
    shell = await _connected(FakeShell())

    await panel_ops.stage_release(
        shell,
        local_path,
        "0.6.0",
        "ENV=1\n",
        (COMPONENT_BRIDGE,),
        _TRANSACTION_ID,
    )

    assert shell.dir_uploads[0][0] == local_path
    assert all(local_path not in command for command in shell.commands)
    assert all("SHOULD_NOT_EXIST" not in command for command in shell.commands)


async def test_stage_release_maps_upload_failure_without_retaining_secret() -> None:
    shell = await _connected(FakeShell(put_dir_error=OSError("SECRET local transfer detail")))

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            "MQTT_PASSWORD=SECRET\n",
            (COMPONENT_BRIDGE,),
            _TRANSACTION_ID,
        )

    assert str(raised.value) == "stage_upload_failed"
    graph = _exception_graph(raised.value)
    assert graph == [raised.value]
    assert all("SECRET" not in str(error) for error in graph)
    assert any("; rm -rf -- " in command for command in shell.commands)


async def test_panel_preflight_launcher_uses_only_the_validated_release_and_request() -> None:
    request = PreflightRequest(
        setup_id=UUID("87654321-4321-4cba-8fed-ba0987654321"),
        panel_nonce="panel-'nonce",
        ha_nonce="ha-'; touch /tmp/SHOULD_NOT_EXIST; #",
        timeout_seconds=10.0,
    )
    shell = await _connected(FakeShell())

    launched = await panel_ops.panel_preflight_launcher(shell, _staged())(request.to_json())

    assert launched is shell.started_processes[0]
    assert isinstance(launched, FakePanelProcess)
    assert shell.commands[0] == panel_ops._release_validation_command(
        _staged(),
        promoted=True,
    )
    command = shell.commands[1]
    assert (f"export PYTHONPATH='{_CANDIDATE_RELEASE}/app:{_CANDIDATE_RELEASE}/vendor'") in command
    assert ("exec /data/switch-embedded/env/bin/python3 -m brilliant_mqtt.preflight ") in command
    assert f"--environment-file '{_CANDIDATE_RELEASE}/brilliant-mqtt.env' " in command
    assert "--request-json " in command
    assert "set -a" not in command
    assert f". '{_CANDIDATE_RELEASE}/brilliant-mqtt.env'" not in command
    assert "/etc/brilliant-mqtt.env" not in command
    assert "/var/brilliant-mqtt.staging" not in command
    assert "SHOULD_NOT_EXIST" in command
    assert "'\"'\"'" in command
    _assert_valid_posix_shell(command)


async def test_panel_preflight_launcher_rejects_noncanonical_request_before_shell() -> None:
    shell = await _connected(FakeShell())
    launcher = panel_ops.panel_preflight_launcher(shell, _staged())

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await launcher('{"raw-secret":"SECRET request"}')

    assert str(raised.value) == "invalid_preflight_request"
    assert _exception_graph(raised.value) == [raised.value]
    assert "SECRET" not in str(raised.value)
    assert not shell.commands
    assert not shell.started_processes


class _FailingPreflightStartShell(FakeShell):
    async def start(self, command: str) -> FakePanelProcess:
        self._require_connected()
        self.commands.append(command)
        raise RuntimeError("SECRET process start detail")


async def test_panel_preflight_launcher_redacts_process_start_failure() -> None:
    request = PreflightRequest(
        setup_id=UUID("87654321-4321-4cba-8fed-ba0987654321"),
        panel_nonce="panel-nonce",
        ha_nonce="ha-nonce",
        timeout_seconds=10.0,
    )
    shell = await _connected(_FailingPreflightStartShell())

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.panel_preflight_launcher(shell, _staged())(request.to_json())

    assert str(raised.value) == "preflight_start_failed"
    assert _exception_graph(raised.value) == [raised.value]
    assert "SECRET" not in str(raised.value)


class _BlockingPutDirShell(FakeShell):
    def __init__(self) -> None:
        super().__init__()
        self.put_started = asyncio.Event()

    async def put_dir(self, local_dir: str, remote_dir: str) -> None:
        self._require_connected()
        del local_dir, remote_dir
        self.put_started.set()
        await asyncio.Event().wait()


class _BlockingCancellationCleanupShell(_BlockingPutDirShell):
    def __init__(self) -> None:
        super().__init__()
        self.cleanup_started = asyncio.Event()
        self.cleanup_release = asyncio.Event()
        self.cleanup_finished = asyncio.Event()

    async def run(self, command: str) -> RunResult:
        result = await super().run(command)
        if "; rm -rf -- " in command:
            self.cleanup_started.set()
            await self.cleanup_release.wait()
            self.cleanup_finished.set()
        return result


async def test_stage_release_cancellation_cleans_unique_candidate_only() -> None:
    shell = await _connected(_BlockingPutDirShell())
    task = asyncio.create_task(
        panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            "ENV=1\n",
            (COMPONENT_BRIDGE,),
            _TRANSACTION_ID,
        )
    )
    await shell.put_started.wait()

    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task

    cleanup = shell.commands[-1]
    assert _CANDIDATE_RELEASE in cleanup
    assert _PRIOR_RELEASE not in cleanup
    assert PANEL_MQTT_TLS_DIR not in cleanup
    assert "/etc/" not in cleanup


async def test_stage_release_repeated_cancellation_settles_cleanup_task() -> None:
    shell = await _connected(_BlockingCancellationCleanupShell())
    task = asyncio.create_task(
        panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            "0.6.0",
            "ENV=1\n",
            (COMPONENT_BRIDGE,),
            _TRANSACTION_ID,
        )
    )
    await shell.put_started.wait()

    task.cancel("initial-cancellation")
    await shell.cleanup_started.wait()
    task.cancel("cleanup-cancellation")
    await asyncio.sleep(0)

    cleanup_was_awaited = not task.done()
    shell.cleanup_release.set()
    if task.done():
        await shell.cleanup_finished.wait()
    else:
        with pytest.raises(asyncio.CancelledError) as raised:
            await task
        assert raised.value.args == ("cleanup-cancellation",)
    assert cleanup_was_awaited
    assert shell.cleanup_finished.is_set()


async def test_settled_cleanup_preserves_same_turn_cancellation() -> None:
    loop = asyncio.get_running_loop()
    completion: asyncio.Future[RunResult] = loop.create_future()

    class SameTurnCleanupShell(FakeShell):
        async def run(self, command: str) -> RunResult:
            self._require_connected()
            self.commands.append(command)
            return await completion

    shell = await _connected(SameTurnCleanupShell())
    task = asyncio.create_task(panel_ops._settled_cleanup_run(shell, "cleanup"))
    await asyncio.sleep(0)
    await asyncio.sleep(0)

    loop.call_soon(completion.set_result, RunResult(0, "", ""))
    loop.call_soon(task.cancel, "same-turn-cancellation")

    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value.args == ("same-turn-cancellation",)


async def test_settled_cleanup_treats_child_cancellation_as_cleanup_failure() -> None:
    class InternallyCancelledCleanupShell(FakeShell):
        async def run(self, command: str) -> RunResult:
            self._require_connected()
            self.commands.append(command)
            raise asyncio.CancelledError("child-cancelled")

    shell = await _connected(InternallyCancelledCleanupShell())

    assert await panel_ops._settled_cleanup_run(shell, "cleanup") is False


async def test_settled_cleanup_preserves_latest_repeated_caller_cancellation() -> None:
    cleanup_started = asyncio.Event()
    cleanup_release = asyncio.Event()

    class BlockingCleanupShell(FakeShell):
        async def run(self, command: str) -> RunResult:
            self._require_connected()
            self.commands.append(command)
            cleanup_started.set()
            await cleanup_release.wait()
            return RunResult(0, "", "")

    shell = await _connected(BlockingCleanupShell())
    task = asyncio.create_task(panel_ops._settled_cleanup_run(shell, "cleanup"))
    await cleanup_started.wait()

    task.cancel("first-cancellation")
    await asyncio.sleep(0)
    task.cancel("latest-cancellation")
    await asyncio.sleep(0)
    cleanup_release.set()

    with pytest.raises(asyncio.CancelledError) as raised:
        await task
    assert raised.value.args == ("latest-cancellation",)


async def test_stage_release_surfaces_nonzero_cleanup_without_raw_details() -> None:
    staged = _staged((COMPONENT_BRIDGE,))
    cleanup_command = panel_ops._cleanup_staged_command(staged)
    shell = await _connected(
        FakeShell(
            responses={
                cleanup_command: RunResult(
                    1,
                    "",
                    "SECRET candidate cleanup failure",
                )
            },
            put_dir_error=OSError("SECRET primary transfer failure"),
        )
    )

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            staged.version,
            "MQTT_PASSWORD=SECRET\n",
            staged.selected_components,
            staged.transaction_id,
        )

    assert str(raised.value) == "staged_cleanup_failed"
    assert _exception_graph(raised.value) == [raised.value]


async def test_stage_release_preserves_cancellation_when_cleanup_fails() -> None:
    staged = _staged((COMPONENT_BRIDGE,))
    cleanup_command = panel_ops._cleanup_staged_command(staged)

    class CancelledTransferShell(FakeShell):
        async def put_dir(self, local_dir: str, remote_dir: str) -> None:
            del local_dir, remote_dir
            self._require_connected()
            raise asyncio.CancelledError("caller-cancelled")

    shell = await _connected(
        CancelledTransferShell(
            responses={
                cleanup_command: RunResult(
                    1,
                    "",
                    "SECRET candidate cleanup failure",
                )
            },
        )
    )

    with pytest.raises(asyncio.CancelledError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            staged.version,
            "MQTT_PASSWORD=SECRET\n",
            staged.selected_components,
            staged.transaction_id,
        )

    assert raised.value.args == ("caller-cancelled",)
    assert _exception_graph(raised.value) == [raised.value]


async def test_stage_prepare_failure_uses_active_release_guard_before_cleanup() -> None:
    staged = _staged((COMPONENT_BRIDGE,))
    prepare = panel_ops._stage_prepare_command(staged)
    guarded_cleanup = panel_ops._cleanup_staged_command(staged)
    shell = await _connected(
        FakeShell(
            responses={
                prepare: RunResult(1, "", "SECRET pre-existing release"),
                guarded_cleanup: RunResult(47, "", "SECRET active release guard"),
            }
        )
    )

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.stage_release(
            shell,
            "/trusted/local/payload",
            staged.version,
            "ENV=1\n",
            staged.selected_components,
            staged.transaction_id,
        )

    assert str(raised.value) == "staged_cleanup_failed"
    assert shell.commands == [prepare, guarded_cleanup]
    assert f'test "$(readlink {PANEL_CURRENT_LINK})" != {_CANDIDATE_RELEASE}' in guarded_cleanup
    temporary_cleanup = f"rm -rf -- {panel_ops._staged_temp_path(staged)}; "
    assert guarded_cleanup.startswith(temporary_cleanup)
    assert not guarded_cleanup.removeprefix(temporary_cleanup).startswith("rm -rf -- ")
    assert _exception_graph(raised.value) == [raised.value]


async def test_activate_staged_switches_current_and_converges_only_core_services() -> None:
    shell = await _connected(FakeShell())
    staged = _staged((COMPONENT_BRIDGE, COMPONENT_BUS_WATCHDOG))
    boundary = "<candidate-health-boundary>"

    await panel_ops.activate_staged(
        shell,
        staged,
        on_services_stopped=lambda: shell.commands.append(boundary),
    )

    joined = "\n".join(shell.commands)
    assert f"ln -s {_CANDIDATE_RELEASE}" in joined
    assert "/usr/bin/mv.coreutils --force --no-target-directory --" in joined
    assert PANEL_CURRENT_LINK in joined
    assert shell.commands.count("systemctl daemon-reload") == 1
    for service in (
        SERVICE_NAME,
        WIFI_WATCHDOG_SERVICE_NAME,
        BUS_WATCHDOG_SERVICE_NAME,
    ):
        assert any(
            f"systemctl is-active --quiet {service}" in command for command in shell.commands
        )
    assert f"systemctl enable {SERVICE_NAME}" in shell.commands
    assert f"systemctl start {SERVICE_NAME}" in shell.commands
    assert f"systemctl enable {BUS_WATCHDOG_SERVICE_NAME}" in shell.commands
    assert f"systemctl start {BUS_WATCHDOG_SERVICE_NAME}" in shell.commands
    assert any(
        f"systemctl disable {WIFI_WATCHDOG_SERVICE_NAME}" in command for command in shell.commands
    )
    assert f"systemctl start {WIFI_WATCHDOG_SERVICE_NAME}" not in shell.commands
    for forbidden in (VOICE_SERVICE_NAME, HUE_CA_TIMER_NAME, HA_MIRROR_SERVICE_NAME):
        assert forbidden not in joined
    boundary_index = shell.commands.index(boundary)
    initial_stop_indices = [
        next(
            index
            for index, command in enumerate(shell.commands)
            if f"systemctl stop {service}" in command
        )
        for service in (
            SERVICE_NAME,
            WIFI_WATCHDOG_SERVICE_NAME,
            BUS_WATCHDOG_SERVICE_NAME,
        )
    ]
    commit_index = next(
        index
        for index, command in enumerate(shell.commands)
        if command.startswith("if [ -e ") and PANEL_CURRENT_LINK in command
    )
    start_indices = [
        index
        for index, command in enumerate(shell.commands)
        if command.startswith("systemctl start ")
    ]
    assert max(initial_stop_indices) < boundary_index < commit_index
    assert start_indices and boundary_index < min(start_indices)


async def test_activate_staged_boundary_failure_prevents_commit_and_start() -> None:
    shell = await _connected(FakeShell())

    def fail_boundary() -> None:
        raise RuntimeError("SECRET boundary failure")

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.activate_staged(
            shell,
            _staged(),
            on_services_stopped=fail_boundary,
        )

    assert str(raised.value) == "activation_boundary_failed"
    assert not any(command.startswith("if [ -e ") for command in shell.commands)
    assert not any(command.startswith("systemctl start ") for command in shell.commands)
    assert "SECRET" not in repr(raised.value)


async def test_activate_disables_deselected_unit_before_removing_its_file() -> None:
    shell = await _connected(FakeShell())

    await panel_ops.activate_staged(
        shell,
        _staged((COMPONENT_BRIDGE, COMPONENT_BUS_WATCHDOG)),
        on_services_stopped=_noop_boundary,
    )

    disable_index = next(
        index
        for index, command in enumerate(shell.commands)
        if f"systemctl disable {WIFI_WATCHDOG_SERVICE_NAME}" in command
    )
    commit_index = next(
        index
        for index, command in enumerate(shell.commands)
        if command.startswith("if [ -e ") and PANEL_WIFI_WATCHDOG_UNIT_FILE in command
    )
    assert disable_index < commit_index


@pytest.mark.parametrize(
    ("selector", "expected_code"),
    [
        ("cp --", "activation_prepare_failed"),
        ("is-active --quiet", "activation_stop_failed"),
        (
            "/usr/bin/mv.coreutils --force --no-target-directory",
            "activation_commit_failed",
        ),
        ("daemon-reload", "activation_reload_failed"),
        ("systemctl enable brilliant-mqtt", "activation_service_failed"),
    ],
)
async def test_activate_staged_cut_points_are_fixed_and_redacted(
    selector: str,
    expected_code: str,
) -> None:
    probe = await _connected(FakeShell())
    await panel_ops.activate_staged(
        probe,
        _staged(),
        on_services_stopped=_noop_boundary,
    )
    failed_command = next(command for command in probe.commands if selector in command)
    shell = await _connected(
        FakeShell(responses={failed_command: RunResult(1, "", "SECRET remote command detail")})
    )

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.activate_staged(
            shell,
            _staged(),
            on_services_stopped=_noop_boundary,
        )

    assert str(raised.value) == expected_code
    assert _exception_graph(raised.value) == [raised.value]
    assert "SECRET" not in repr(raised.value)


async def test_rollback_restores_exact_files_link_and_every_service_state(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _release_snapshot(
        bridge_state=(True, False),
        wifi_state=(False, True),
        bus_state=(True, True),
    )
    shell = await _connected(FakeShell())
    observed = 0

    async def verify_snapshot(_shell: FakeShell) -> panel_ops.PanelSnapshot:
        nonlocal observed
        observed += 1
        return snapshot

    monkeypatch.setattr(panel_ops, "snapshot_panel", verify_snapshot)

    await panel_ops.rollback_snapshot(shell, snapshot, _staged())

    assert observed == 1
    uploaded = {(data, mode) for _, data, mode in shell.uploads}
    assert (b"MQTT_PASSWORD=snapshot-secret\n", 0o600) in uploaded
    assert (b"0.5.7", 0o644) in uploaded
    assert (b"bridge-unit", 0o644) in uploaded
    assert (b"wifi-unit", 0o644) in uploaded
    assert (b"bus-unit", 0o644) in uploaded
    joined = "\n".join(shell.commands)
    assert f"ln -s {_PRIOR_RELEASE}" in joined
    assert _CANDIDATE_RELEASE not in joined
    assert shell.commands.count("systemctl daemon-reload") == 1
    assert f"systemctl enable {SERVICE_NAME}" in shell.commands
    assert any(f"systemctl stop {SERVICE_NAME}" in command for command in shell.commands)
    assert any(
        f"systemctl disable {WIFI_WATCHDOG_SERVICE_NAME}" in command for command in shell.commands
    )
    assert f"systemctl start {WIFI_WATCHDOG_SERVICE_NAME}" in shell.commands
    assert f"systemctl enable {BUS_WATCHDOG_SERVICE_NAME}" in shell.commands
    assert f"systemctl start {BUS_WATCHDOG_SERVICE_NAME}" in shell.commands
    for forbidden in (VOICE_SERVICE_NAME, HUE_CA_TIMER_NAME, HA_MIRROR_SERVICE_NAME):
        assert forbidden not in joined


async def test_first_install_rollback_removes_candidate_files_and_verifies_inactive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _absent_snapshot()
    shell = await _connected(FakeShell())
    monkeypatch.setattr(panel_ops, "snapshot_panel", lambda _shell: _async_value(snapshot))

    await panel_ops.rollback_snapshot(shell, snapshot, _staged())

    joined = "\n".join(shell.commands)
    for path in (
        PANEL_ENV_FILE,
        PANEL_VERSION_FILE,
        PANEL_UNIT_FILE,
        PANEL_WIFI_WATCHDOG_UNIT_FILE,
        PANEL_BUS_WATCHDOG_UNIT_FILE,
        PANEL_CURRENT_LINK,
    ):
        assert path in joined
    for service in (
        SERVICE_NAME,
        WIFI_WATCHDOG_SERVICE_NAME,
        BUS_WATCHDOG_SERVICE_NAME,
    ):
        assert f"systemctl start {service}" not in shell.commands
        assert any(f"systemctl stop {service}" in command for command in shell.commands)
    for service, unit_file in (
        (SERVICE_NAME, PANEL_UNIT_FILE),
        (WIFI_WATCHDOG_SERVICE_NAME, PANEL_WIFI_WATCHDOG_UNIT_FILE),
        (BUS_WATCHDOG_SERVICE_NAME, PANEL_BUS_WATCHDOG_UNIT_FILE),
    ):
        disable_index = next(
            index
            for index, command in enumerate(shell.commands)
            if f"systemctl disable {service}" in command
        )
        remove_index = shell.commands.index(f"rm -f -- {unit_file}")
        assert disable_index < remove_index


async def test_rollback_restores_bridge_less_legacy_watchdog_residue_exactly(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _watchdog_residue_snapshot()
    shell = await _connected(FakeShell())
    observed: list[panel_ops.PanelSnapshot] = []

    async def verify_snapshot(_shell: FakeShell) -> panel_ops.PanelSnapshot:
        observed.append(snapshot)
        return snapshot

    monkeypatch.setattr(panel_ops, "snapshot_panel", verify_snapshot)

    await panel_ops.rollback_snapshot(shell, snapshot, _staged())

    assert observed == [snapshot]
    uploaded = {(path, data, mode) for path, data, mode in shell.uploads}
    assert any(
        path.startswith(f"{PANEL_WIFI_WATCHDOG_UNIT_FILE}.rollback-")
        and data == b"orphaned-wifi-unit"
        and mode == 0o644
        for path, data, mode in uploaded
    )
    assert any(
        command.endswith(f" {PANEL_WIFI_WATCHDOG_UNIT_FILE}")
        and "/usr/bin/mv.coreutils --force --no-target-directory --" in command
        for command in shell.commands
    )
    assert not any(path == PANEL_UNIT_FILE for path, _, _ in uploaded)
    assert not any(path == PANEL_ENV_FILE for path, _, _ in uploaded)
    assert f"systemctl enable {WIFI_WATCHDOG_SERVICE_NAME}" in shell.commands
    assert f"systemctl start {WIFI_WATCHDOG_SERVICE_NAME}" not in shell.commands
    assert f"systemctl start {SERVICE_NAME}" not in shell.commands


async def _async_value[T](value: T) -> T:
    return value


async def test_rollback_requires_exact_resnapshot_equality(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _release_snapshot()
    mismatched = replace(
        snapshot,
        active_release_target=(
            "/var/brilliant-mqtt/releases/0.5.8--bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
        ),
    )
    monkeypatch.setattr(panel_ops, "snapshot_panel", lambda _shell: _async_value(mismatched))
    shell = await _connected(FakeShell())

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.rollback_snapshot(shell, snapshot, _staged())

    assert str(raised.value) == "rollback_verification_failed"


async def test_rollback_surfaces_nonzero_temporary_cleanup(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _release_snapshot()
    staged = _staged()
    cleanup_command = panel_ops._rollback_cleanup_command(staged)
    shell = await _connected(
        FakeShell(
            responses={
                cleanup_command: RunResult(
                    1,
                    "",
                    "SECRET rollback cleanup failure",
                )
            }
        )
    )
    monkeypatch.setattr(
        panel_ops,
        "snapshot_panel",
        lambda _shell: _async_value(snapshot),
    )

    with pytest.raises(panel_ops.PanelOpError) as raised:
        await panel_ops.rollback_snapshot(shell, snapshot, staged)

    assert str(raised.value) == "rollback_cleanup_failed"
    assert _exception_graph(raised.value) == [raised.value]


async def test_rollback_preserves_cancellation_when_temporary_cleanup_fails(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    snapshot = _release_snapshot()
    staged = _staged()
    cleanup_command = panel_ops._rollback_cleanup_command(staged)
    shell = await _connected(
        FakeShell(
            responses={
                cleanup_command: RunResult(
                    1,
                    "",
                    "SECRET rollback cleanup failure",
                )
            }
        )
    )

    async def cancelled_snapshot(_shell: FakeShell) -> panel_ops.PanelSnapshot:
        raise asyncio.CancelledError("caller-cancelled")

    monkeypatch.setattr(panel_ops, "snapshot_panel", cancelled_snapshot)

    with pytest.raises(asyncio.CancelledError) as raised:
        await panel_ops.rollback_snapshot(shell, snapshot, staged)

    assert raised.value.args == ("caller-cancelled",)
    assert _exception_graph(raised.value) == [raised.value]


async def test_rollback_cleanup_covers_activation_and_restore_secret_temps() -> None:
    staged = _staged()
    command = panel_ops._rollback_cleanup_command(staged)

    assert f"{PANEL_ENV_FILE}.fleet-{staged.transaction_id.hex}.tmp" in command
    assert f"{PANEL_ENV_FILE}.rollback-{staged.transaction_id.hex}.tmp" in command
    assert _CANDIDATE_RELEASE not in command
    assert _PRIOR_RELEASE not in command
    assert PANEL_MQTT_TLS_DIR not in command


async def test_cleanup_staged_removes_only_inactive_candidate_release() -> None:
    shell = await _connected(FakeShell())

    await panel_ops.cleanup_staged(shell, _staged())

    assert len(shell.commands) == 1
    command = shell.commands[0]
    assert _CANDIDATE_RELEASE in command
    assert _PRIOR_RELEASE not in command
    assert PANEL_ENV_FILE not in command
    assert PANEL_MQTT_TLS_DIR not in command
    assert "systemctl" not in command


async def test_cleanup_staged_command_refuses_non_symlink_current() -> None:
    staged = _staged()
    command = panel_ops._cleanup_staged_command(staged)
    temporary_cleanup = f"rm -rf -- {panel_ops._staged_temp_path(staged)}; "
    assert command.startswith(temporary_cleanup)
    guard, separator, removal = command.removeprefix(temporary_cleanup).partition("; rm -rf -- ")

    assert separator
    assert f"test -L {PANEL_CURRENT_LINK} || exit" in guard
    assert _CANDIDATE_RELEASE in removal
    assert panel_ops._staged_temp_path(staged) not in removal


async def test_cleanup_staged_command_refuses_active_candidate_exactly() -> None:
    staged = _staged()
    command = panel_ops._cleanup_staged_command(staged)
    temporary_cleanup = f"rm -rf -- {panel_ops._staged_temp_path(staged)}; "
    assert command.startswith(temporary_cleanup)
    guard, separator, removal = command.removeprefix(temporary_cleanup).partition("; rm -rf -- ")

    assert separator
    assert f'test "$(readlink {PANEL_CURRENT_LINK})" != {_CANDIDATE_RELEASE} || exit' in guard
    assert _CANDIDATE_RELEASE in removal
    assert panel_ops._staged_temp_path(staged) not in removal


async def test_restart_candidate_verifies_link_and_restarts_only_bridge() -> None:
    shell = await _connected(FakeShell())
    boundary = "<candidate-health-boundary>"

    await panel_ops.restart_candidate(
        shell,
        _staged(),
        on_service_stopped=lambda: shell.commands.append(boundary),
    )

    assert any(f"readlink {PANEL_CURRENT_LINK}" in command for command in shell.commands)
    boundary_index = shell.commands.index(boundary)
    stop_index = next(
        index for index, command in enumerate(shell.commands) if "systemctl stop" in command
    )
    start_index = shell.commands.index(f"systemctl start {SERVICE_NAME}")
    assert stop_index < boundary_index < start_index
    assert shell.commands[-1] == f"systemctl is-active --quiet {SERVICE_NAME}"
    joined = "\n".join(shell.commands)
    assert WIFI_WATCHDOG_SERVICE_NAME not in joined
    assert BUS_WATCHDOG_SERVICE_NAME not in joined
