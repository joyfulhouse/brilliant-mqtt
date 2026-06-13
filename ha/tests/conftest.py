"""Shared fixtures for the brilliant_mqtt integration tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Let the HA test harness load custom_components/ from this directory."""


@pytest.fixture
def expected_lingering_timers() -> bool:
    """Allow the MQTT integration's own misc periodic timer to linger.

    Tests that pull in the ``mqtt_mock`` fixture set up the real ``mqtt``
    integration, which schedules a recurring ``_async_misc`` timer that the
    fixture never tears down. That timer belongs to MQTT, not to this
    integration, so the harness's lingering-timer guard would otherwise fail
    an otherwise-clean test. HA core bypasses this for tests under
    ``tests/components/<component>/``; custom components opt in explicitly.
    """
    return True
