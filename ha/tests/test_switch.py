"""Tests for the voice satellite, Wi-Fi watchdog, and bus watchdog switches."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from custom_components.brilliant_mqtt.const import (
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
)
from custom_components.brilliant_mqtt.manager import PanelManager, _HostKeyChanged
from custom_components.brilliant_mqtt.switch import (
    BusWatchdogSwitch,
    VoiceSatelliteSwitch,
    WifiWatchdogSwitch,
)


@pytest.mark.asyncio
async def test_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = VoiceSatelliteSwitch(entry)
    # default: not selected
    assert sw.is_on is False
    # Update entry data via hass config entry API
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_COMPONENTS: {COMPONENT_VOICE: True}}
    )
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = WifiWatchdogSwitch(entry)
    # default: not selected
    assert sw.is_on is False
    # Update entry data via hass config entry API
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_COMPONENTS: {COMPONENT_WIFI_WATCHDOG: True}}
    )
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_off_calls_remove_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = WifiWatchdogSwitch(entry)
    with patch.object(
        PanelManager, "async_remove_component", new_callable=AsyncMock
    ) as mock_remove:
        await sw.async_turn_off()
        mock_remove.assert_awaited_once_with(COMPONENT_WIFI_WATCHDOG)


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_on_calls_install_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = WifiWatchdogSwitch(entry)
    with patch.object(
        PanelManager, "async_install_component", new_callable=AsyncMock
    ) as mock_install:
        await sw.async_turn_on()
        mock_install.assert_awaited_once_with(COMPONENT_WIFI_WATCHDOG)


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_on_maps_ssh_error(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = WifiWatchdogSwitch(entry)
    with (
        patch.object(
            PanelManager,
            "async_install_component",
            new_callable=AsyncMock,
            side_effect=OSError("unreachable"),
        ),
        pytest.raises(HomeAssistantError) as err,
    ):
        await sw.async_turn_on()
    assert err.value.translation_key == "wifi_watchdog_failed"


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_on_maps_host_key_changed(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = WifiWatchdogSwitch(entry)
    with (
        patch.object(
            PanelManager,
            "async_install_component",
            new_callable=AsyncMock,
            side_effect=_HostKeyChanged(),
        ),
        pytest.raises(HomeAssistantError) as err,
    ):
        await sw.async_turn_on()
    assert err.value.translation_key == "host_key_changed"


@pytest.mark.asyncio
async def test_bus_watchdog_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = BusWatchdogSwitch(entry)
    # default: not selected
    assert sw.is_on is False
    # Update entry data via hass config entry API
    hass.config_entries.async_update_entry(
        entry, data={**entry.data, CONF_COMPONENTS: {COMPONENT_BUS_WATCHDOG: True}}
    )
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_off_calls_remove_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = BusWatchdogSwitch(entry)
    with patch.object(
        PanelManager, "async_remove_component", new_callable=AsyncMock
    ) as mock_remove:
        await sw.async_turn_off()
        mock_remove.assert_awaited_once_with(COMPONENT_BUS_WATCHDOG)


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_on_calls_install_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = BusWatchdogSwitch(entry)
    with patch.object(
        PanelManager, "async_install_component", new_callable=AsyncMock
    ) as mock_install:
        await sw.async_turn_on()
        mock_install.assert_awaited_once_with(COMPONENT_BUS_WATCHDOG)


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_on_maps_ssh_error(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = BusWatchdogSwitch(entry)
    with (
        patch.object(
            PanelManager,
            "async_install_component",
            new_callable=AsyncMock,
            side_effect=OSError("unreachable"),
        ),
        pytest.raises(HomeAssistantError) as err,
    ):
        await sw.async_turn_on()
    assert err.value.translation_key == "bus_watchdog_failed"


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_on_maps_host_key_changed(
    manager_with_fake_panel: PanelManager,
) -> None:
    entry = manager_with_fake_panel.entry
    entry.runtime_data = manager_with_fake_panel
    sw = BusWatchdogSwitch(entry)
    with (
        patch.object(
            PanelManager,
            "async_install_component",
            new_callable=AsyncMock,
            side_effect=_HostKeyChanged(),
        ),
        pytest.raises(HomeAssistantError) as err,
    ):
        await sw.async_turn_on()
    assert err.value.translation_key == "host_key_changed"
