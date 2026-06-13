"""Shared fixtures for the brilliant_mqtt integration tests."""

from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def auto_enable_custom_integrations(enable_custom_integrations: None) -> None:
    """Let the HA test harness load custom_components/ from this directory."""
