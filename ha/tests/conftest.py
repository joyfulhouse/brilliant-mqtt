"""Shared fixtures for the brilliant_mqtt integration tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Let the HA test harness load custom_components/ from this directory."""


@pytest.fixture
def expected_lingering_timers(request: pytest.FixtureRequest) -> bool:
    """Tolerate lingering timers ONLY for tests marked `allow_lingering_timers`
    (the core mqtt integration starts its own recurring timer via mqtt_mock).
    Every other test keeps the harness's strict guard, so a leaked manager timer
    fails loudly instead of hiding here."""
    return request.node.get_closest_marker("allow_lingering_timers") is not None
