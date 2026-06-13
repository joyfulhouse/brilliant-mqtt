"""Shared fixtures for the brilliant_mqtt integration tests."""

from __future__ import annotations

from collections.abc import Iterator
from pathlib import Path
from unittest.mock import patch

import pytest

from tests.fakes import FakeShell


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
    """Route every manager SSH op through one inspectable FakeShell."""
    shell = FakeShell()
    with patch("custom_components.brilliant_mqtt.manager.AsyncsshShell", return_value=shell):
        yield shell


@pytest.fixture
def expected_lingering_timers(request: pytest.FixtureRequest) -> bool:
    """Tolerate lingering timers ONLY for tests marked `allow_lingering_timers`
    (the core mqtt integration starts its own recurring timer via mqtt_mock).
    Every other test keeps the harness's strict guard, so a leaked manager timer
    fails loudly instead of hiding here."""
    return request.node.get_closest_marker("allow_lingering_timers") is not None
