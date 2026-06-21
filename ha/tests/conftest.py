"""Shared fixtures for the brilliant_mqtt integration tests."""

from __future__ import annotations

from collections.abc import Iterator
from dataclasses import dataclass, field
from pathlib import Path
from unittest.mock import patch

import asyncssh
import pytest

from tests.fakes import FakeShell

# The key an unpinned re-pin connect captures (mirrors the rotated server key).
REPIN_NEW_KEY = "ssh-ed25519 NEWKEY"


@dataclass
class RepinShells:
    """Factory standing in for manager.AsyncsshShell during host-key-rotation tests.

    Keyed on the THIRD constructor arg (the pinned host key):

    - a non-``None`` pin → a FakeShell whose ``connect()`` raises
      ``asyncssh.HostKeyNotVerifiable`` (the rotated panel rejects the stored pin
      before auth).
    - a ``None`` pin → the UNPINNED re-pin connect; succeeds and reports
      ``REPIN_NEW_KEY``, UNLESS ``unpinned_connect_error`` is set (then it raises,
      modelling a panel that is also unreachable on the second attempt).

    Tests assert on ``pinned_shell`` / ``unpinned_shell``: if ``unpinned_shell`` is
    still ``None`` the OFF path never constructed an unpinned shell, i.e. the root
    password was never offered to the new-key host.
    """

    unpinned_connect_error: Exception | None = None
    pinned_shell: FakeShell | None = None
    unpinned_shell: FakeShell | None = None
    pins_seen: list[str | None] = field(default_factory=list)

    def __call__(self, host: str, password: str, pinned_host_key: str | None) -> FakeShell:
        self.pins_seen.append(pinned_host_key)
        if pinned_host_key is not None:
            self.pinned_shell = FakeShell(
                connect_error=asyncssh.HostKeyNotVerifiable("changed"), pinned=pinned_host_key
            )
            return self.pinned_shell
        self.unpinned_shell = FakeShell(
            connect_error=self.unpinned_connect_error, pinned=REPIN_NEW_KEY
        )
        return self.unpinned_shell


@pytest.fixture
def repin_shells() -> Iterator[RepinShells]:
    """Patch manager.AsyncsshShell with a pin-keyed factory (see RepinShells)."""
    factory = RepinShells()
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", side_effect=factory):
        yield factory


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Let the HA test harness load custom_components/ from this directory."""


@pytest.fixture
def payload_dir(tmp_path: Path) -> Iterator[Path]:
    """A minimal built agent payload, patched in as the bundled one."""
    (tmp_path / "app" / "brilliant_mqtt").mkdir(parents=True)
    (tmp_path / "vendor").mkdir()
    (tmp_path / "VERSION").write_text("0.2.0")
    (tmp_path / "brilliant-mqtt.service").write_text("[Unit]\nDescription=test unit\n")
    with patch("custom_components.brilliant_mqtt.manager._payload_dir", return_value=tmp_path):
        yield tmp_path


@pytest.fixture
def fake_shell() -> Iterator[FakeShell]:
    """Route every manager SSH op through one inspectable FakeShell.

    Its inspect probe reports a fully-installed panel (agent code + unit + env all
    present), so a repair takes the light path — rewrite config + enable, WITHOUT
    re-uploading the payload. Tests exercising the code-absent install path script
    their own shell with a ``payload=0`` inspect.
    """
    from custom_components.brilliant_mqtt import panel_ops
    from custom_components.brilliant_mqtt.shell import RunResult

    installed = RunResult(
        0, "unit=1\nenv=1\nenabled=1\nactive=1\nsunit=1\nsenv=1\npayload=1\n0.2.0\n", ""
    )
    shell = FakeShell(responses={panel_ops.INSPECT_COMMAND: installed})
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        yield shell


@pytest.fixture
def expected_lingering_timers(request: pytest.FixtureRequest) -> bool:
    """Tolerate lingering timers ONLY for tests marked `allow_lingering_timers`
    (the core mqtt integration starts its own recurring timer via mqtt_mock).
    Every other test keeps the harness's strict guard, so a leaked manager timer
    fails loudly instead of hiding here."""
    return request.node.get_closest_marker("allow_lingering_timers") is not None
