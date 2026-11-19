"""Device-registry lookups that behave the same on both HA registry models.

HA <= 2026.9 shares one device across config entries: the MQTT-discovery device and
the management entities' device are ONE record owned by both entries. HA 2026.10+
gives every device a single owner, so the same ``("mqtt", "brilliant_panel_<slug>")``
identifier yields linked twins - one owned by the MQTT entry, one by ours.
``device_registry.async_get_device`` is deprecated (and raises from test code) on
2026.10+, so tests look devices up through ``async_entries_for_config_entry``, which
exists with the same meaning on both models.
"""

from __future__ import annotations

from homeassistant.core import HomeAssistant
from homeassistant.helpers import device_registry as dr

from custom_components.brilliant_mqtt.const import panel_device_identifier
from custom_components.brilliant_mqtt.migration_coordinator import device_links


def devices_with_identifier(
    hass: HomeAssistant, identifier: tuple[str, str]
) -> list[dr.DeviceEntry]:
    """Every device (on any config entry) carrying the identifier, ordered by id."""
    registry = dr.async_get(hass)
    found: dict[str, dr.DeviceEntry] = {}
    for entry in hass.config_entries.async_entries():
        for device in dr.async_entries_for_config_entry(registry, entry.entry_id):
            if identifier in device.identifiers:
                found[device.id] = device
    return [found[device_id] for device_id in sorted(found)]


def panel_devices(hass: HomeAssistant, slug: str) -> list[dr.DeviceEntry]:
    """The panel's device records: one shared device, or its single-owner twins."""
    return devices_with_identifier(hass, panel_device_identifier(slug))


def panel_device(hass: HomeAssistant, slug: str, owner_entry_id: str) -> dr.DeviceEntry:
    """The panel device record that ``owner_entry_id`` owns (the shared one pre-2026.10)."""
    owned = [
        device for device in panel_devices(hass, slug) if owner_entry_id in device_links(device)
    ]
    assert len(owned) == 1, owned
    return owned[0]
