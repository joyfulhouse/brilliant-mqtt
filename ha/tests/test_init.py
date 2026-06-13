"""The integration is discoverable and its manifest is coherent."""

from __future__ import annotations

from homeassistant.core import HomeAssistant
from homeassistant.loader import async_get_integration

from custom_components.brilliant_mqtt.const import DOMAIN


async def test_integration_discoverable(hass: HomeAssistant) -> None:
    """The HA loader resolves the integration and the manifest carries the contract."""
    integration = await async_get_integration(hass, DOMAIN)
    assert integration.domain == DOMAIN
    assert integration.integration_type == "device"
    assert "mqtt" in (integration.dependencies or [])
    assert any(r.startswith("asyncssh==") for r in integration.requirements or [])
