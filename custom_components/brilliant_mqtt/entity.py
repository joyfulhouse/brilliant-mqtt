"""Base entity: attaches to the panel's existing MQTT-discovery device."""

from __future__ import annotations

from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers import entity_registry as er
from homeassistant.helpers.device_registry import DeviceInfo
from homeassistant.helpers.dispatcher import async_dispatcher_connect
from homeassistant.helpers.entity import Entity

from .const import DOMAIN, panel_device_name
from .manager import PanelManager


def async_remove_gated_entity(hass: HomeAssistant, platform_domain: str, unique_id: str) -> None:
    """Drop a stale registry entry for an entity a feature flag has disabled.

    A feature-gated entity (e.g. scene controls when ha_control is off) must not
    linger as a permanently-unavailable registry entry once its flag flips off.
    """
    registry = er.async_get(hass)
    entity_id = registry.async_get_entity_id(platform_domain, DOMAIN, unique_id)
    if entity_id is not None:
        registry.async_remove(entity_id)


class BrilliantPanelEntity(Entity):
    """Pushes only; refreshed via the manager's dispatcher signal.

    device_info deliberately claims the SAME registry identifier the agent's MQTT
    discovery payload creates — ("mqtt", "brilliant_panel_<slug>") — so management
    entities land on the panel's existing device page. (If a future HA release
    rejects cross-domain identifier claims, fall back to identifiers
    {(DOMAIN, slug)} + via_device pointing at the mqtt identifier, and update
    test_entities_attach_to_the_mqtt_discovery_device accordingly.)
    """

    _attr_should_poll = False
    _attr_has_entity_name = True

    def __init__(self, manager: PanelManager) -> None:
        self._manager = manager
        self._attr_device_info = DeviceInfo(
            identifiers={("mqtt", f"brilliant_panel_{manager.panel}")},
            name=panel_device_name(manager.panel),
            manufacturer="Brilliant",
        )

    async def async_added_to_hass(self) -> None:
        self.async_on_remove(
            async_dispatcher_connect(self.hass, self._manager.signal, self._refresh)
        )

    @callback
    def _refresh(self) -> None:
        self.async_write_ha_state()
