"""PanelManager — per-entry runtime: MQTT watchers and (Task 8) the OTA state machine."""

from __future__ import annotations

import asyncio
import json
import logging
from typing import Any

from homeassistant.components import mqtt
from homeassistant.components.mqtt.models import ReceiveMessage
from homeassistant.config_entries import ConfigEntry
from homeassistant.core import HomeAssistant, callback
from homeassistant.helpers.dispatcher import async_dispatcher_send

from .const import (
    CONF_HOST,
    CONF_PANEL,
    CONF_ROOT_PASSWORD,
    DATA_SSH_HOST_KEY,
    SIGNAL_PANEL_STATE,
    availability_topic,
    meta_topic,
)
from .shell import AsyncsshShell, PanelShell

_LOGGER = logging.getLogger(__name__)


class PanelManager:
    """Owns one panel's state. Entities read it; the state machine mutates it."""

    def __init__(self, hass: HomeAssistant, entry: ConfigEntry, ssh_lock: asyncio.Lock) -> None:
        self.hass = hass
        self.entry = entry
        self.panel: str = entry.data[CONF_PANEL]
        self._ssh_lock = ssh_lock  # fleet-wide: ONE panel SSH op at a time
        self.availability: str | None = None  # None until the retained LWT arrives
        self.meta: dict[str, Any] | None = None
        self.problem = False
        self.problem_reason: str | None = None
        self._unsubs: list[Any] = []

    @property
    def signal(self) -> str:
        """Dispatcher signal entities subscribe to for state refreshes."""
        return f"{SIGNAL_PANEL_STATE}_{self.entry.entry_id}"

    def _shell(self) -> PanelShell:
        return AsyncsshShell(
            self.entry.data[CONF_HOST],
            self.entry.data[CONF_ROOT_PASSWORD],
            self.entry.data.get(DATA_SSH_HOST_KEY),
        )

    async def async_setup(self) -> None:
        self._unsubs.append(
            await mqtt.async_subscribe(
                self.hass, availability_topic(self.panel), self._on_availability
            )
        )
        self._unsubs.append(
            await mqtt.async_subscribe(self.hass, meta_topic(self.panel), self._on_meta)
        )

    async def async_shutdown(self) -> None:
        for unsub in self._unsubs:
            unsub()
        self._unsubs.clear()

    @callback
    def _notify(self) -> None:
        async_dispatcher_send(self.hass, self.signal)

    async def _on_availability(self, msg: ReceiveMessage) -> None:
        self.availability = str(msg.payload)
        self._notify()

    async def _on_meta(self, msg: ReceiveMessage) -> None:
        try:
            meta = json.loads(str(msg.payload))
        except ValueError:
            _LOGGER.warning("%s: unparseable bridge meta payload: %r", self.panel, msg.payload)
            return
        self.meta = meta
        self._notify()
