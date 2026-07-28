"""Tests for the per-panel component switches."""

from __future__ import annotations

from unittest.mock import AsyncMock, patch

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError

from custom_components.brilliant_mqtt.const import (
    COMPONENT_BUS_WATCHDOG,
    COMPONENT_HA_MIRROR,
    COMPONENT_HUE_CA,
    COMPONENT_VOICE,
    COMPONENT_WIFI_WATCHDOG,
    CONF_COMPONENTS,
)
from custom_components.brilliant_mqtt.manager import PanelManager, _HostKeyChanged
from custom_components.brilliant_mqtt.switch import (
    BusWatchdogSwitch,
    HaMirrorSwitch,
    HueCaSwitch,
    VoiceSatelliteSwitch,
    WifiWatchdogSwitch,
)


def test_retired_ha_mirror_switch_preserves_management_unique_id(
    manager_with_fake_panel: PanelManager,
) -> None:
    """Registry migration can still identify the retired entity without re-registering it."""
    switch = HaMirrorSwitch(manager_with_fake_panel)

    assert switch.unique_id == f"{manager_with_fake_panel.store.management_id}_ha_mirror_enabled"


@pytest.mark.asyncio
async def test_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    store = manager_with_fake_panel.store
    sw = VoiceSatelliteSwitch(manager_with_fake_panel)
    # default: not selected
    assert sw.is_on is False
    store.update_data({**store.data, CONF_COMPONENTS: {COMPONENT_VOICE: True}})
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    store = manager_with_fake_panel.store
    sw = WifiWatchdogSwitch(manager_with_fake_panel)
    # default: not selected
    assert sw.is_on is False
    store.update_data({**store.data, CONF_COMPONENTS: {COMPONENT_WIFI_WATCHDOG: True}})
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_off_calls_remove_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = WifiWatchdogSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_remove_component", new_callable=AsyncMock
    ) as mock_remove:
        await sw.async_turn_off()
        mock_remove.assert_awaited_once_with(COMPONENT_WIFI_WATCHDOG)


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_on_calls_install_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = WifiWatchdogSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_install_component", new_callable=AsyncMock
    ) as mock_install:
        await sw.async_turn_on()
        mock_install.assert_awaited_once_with(COMPONENT_WIFI_WATCHDOG)


@pytest.mark.asyncio
async def test_wifi_watchdog_switch_turn_on_maps_ssh_error(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = WifiWatchdogSwitch(manager_with_fake_panel)
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
    sw = WifiWatchdogSwitch(manager_with_fake_panel)
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
    store = manager_with_fake_panel.store
    sw = BusWatchdogSwitch(manager_with_fake_panel)
    # default: not selected
    assert sw.is_on is False
    store.update_data({**store.data, CONF_COMPONENTS: {COMPONENT_BUS_WATCHDOG: True}})
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_off_calls_remove_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = BusWatchdogSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_remove_component", new_callable=AsyncMock
    ) as mock_remove:
        await sw.async_turn_off()
        mock_remove.assert_awaited_once_with(COMPONENT_BUS_WATCHDOG)


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_on_calls_install_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = BusWatchdogSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_install_component", new_callable=AsyncMock
    ) as mock_install:
        await sw.async_turn_on()
        mock_install.assert_awaited_once_with(COMPONENT_BUS_WATCHDOG)


@pytest.mark.asyncio
async def test_bus_watchdog_switch_turn_on_maps_ssh_error(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = BusWatchdogSwitch(manager_with_fake_panel)
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
    sw = BusWatchdogSwitch(manager_with_fake_panel)
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
async def test_hue_ca_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    store = manager_with_fake_panel.store
    sw = HueCaSwitch(manager_with_fake_panel)
    # default: not selected
    assert sw.is_on is False
    store.update_data({**store.data, CONF_COMPONENTS: {COMPONENT_HUE_CA: True}})
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_hue_ca_switch_turn_off_calls_remove_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HueCaSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_remove_component", new_callable=AsyncMock
    ) as mock_remove:
        await sw.async_turn_off()
        mock_remove.assert_awaited_once_with(COMPONENT_HUE_CA)


@pytest.mark.asyncio
async def test_hue_ca_switch_turn_on_calls_install_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HueCaSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_install_component", new_callable=AsyncMock
    ) as mock_install:
        await sw.async_turn_on()
        mock_install.assert_awaited_once_with(COMPONENT_HUE_CA)


@pytest.mark.asyncio
async def test_hue_ca_switch_turn_on_maps_ssh_error(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HueCaSwitch(manager_with_fake_panel)
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
    assert err.value.translation_key == "hue_ca_failed"


@pytest.mark.asyncio
async def test_hue_ca_switch_turn_on_maps_host_key_changed(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HueCaSwitch(manager_with_fake_panel)
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
async def test_ha_mirror_switch_reads_components_dict(
    manager_with_fake_panel: PanelManager, hass: HomeAssistant
) -> None:
    store = manager_with_fake_panel.store
    sw = HaMirrorSwitch(manager_with_fake_panel)
    assert sw.is_on is False
    store.update_data({**store.data, CONF_COMPONENTS: {COMPONENT_HA_MIRROR: True}})
    await hass.async_block_till_done()
    assert sw.is_on is True


@pytest.mark.asyncio
async def test_ha_mirror_switch_turn_off_calls_remove_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HaMirrorSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_remove_component", new_callable=AsyncMock
    ) as mock_remove:
        await sw.async_turn_off()
        mock_remove.assert_awaited_once_with(COMPONENT_HA_MIRROR)


@pytest.mark.asyncio
async def test_ha_mirror_switch_turn_on_calls_install_component(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HaMirrorSwitch(manager_with_fake_panel)
    with patch.object(
        PanelManager, "async_install_component", new_callable=AsyncMock
    ) as mock_install:
        await sw.async_turn_on()
        mock_install.assert_awaited_once_with(COMPONENT_HA_MIRROR)


@pytest.mark.asyncio
async def test_ha_mirror_switch_turn_on_maps_ssh_error(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HaMirrorSwitch(manager_with_fake_panel)
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
    assert err.value.translation_key == "ha_mirror_failed"


@pytest.mark.asyncio
async def test_ha_mirror_switch_turn_on_maps_host_key_changed(
    manager_with_fake_panel: PanelManager,
) -> None:
    sw = HaMirrorSwitch(manager_with_fake_panel)
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
