"""Fetch + cache the on-panel voice payload (a GitHub release asset).

The ~57 MB voice tarball is too large for the HACS zip, so the integration
downloads the asset matching its own installed release version, caches it under
.storage, and reuses it across panels/restarts. The cached tarball is then
SFTP-pushed to the panel by panel_ops.deploy_voice_payload.
"""

from __future__ import annotations

import logging
import os
from pathlib import Path

import aiohttp
from homeassistant.core import HomeAssistant
from homeassistant.exceptions import HomeAssistantError
from homeassistant.helpers.aiohttp_client import async_get_clientsession
from homeassistant.helpers.storage import STORAGE_DIR
from homeassistant.loader import LoaderError, async_get_integration

from .const import DOMAIN, VOICE_PAYLOAD_VERSION, voice_asset_url

_LOGGER = logging.getLogger(__name__)

_DOWNLOAD_TIMEOUT = aiohttp.ClientTimeout(total=300)  # 57 MB over a home uplink


class VoicePayloadError(HomeAssistantError):
    """The voice payload asset could not be fetched."""


def _cache_path(hass: HomeAssistant) -> Path:
    """Where the downloaded tarball is cached (persists across restarts)."""
    return (
        Path(hass.config.path(STORAGE_DIR))
        / DOMAIN
        / f"brilliant-voice-payload-{VOICE_PAYLOAD_VERSION}.tar.gz"
    )


def _read_cached(target: Path) -> str | None:
    """Return the cached path as str if a non-empty file is already present."""
    if target.exists() and target.stat().st_size > 0:
        return str(target)
    return None


def _write_atomic(target: Path, data: bytes) -> str:
    """Write *data* to *target* atomically (tmp + os.replace); return the path."""
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(target.suffix + ".tmp")
    tmp.write_bytes(data)
    os.replace(tmp, target)
    return str(target)


async def async_fetch_voice_payload(hass: HomeAssistant) -> str:
    """Return a local path to the voice payload tarball, downloading if needed.

    Cached by version: a second call (another panel, a repair) reuses the file.
    Raises ONLY VoicePayloadError on any failure — the cache read/write executor
    jobs (which can raise OSError) and the integration lookup are wrapped so a repair
    caller can swallow exactly one exception type and never leak an OSError that would
    leave it stuck (see manager.async_repair).
    """
    target = _cache_path(hass)
    try:
        cached = await hass.async_add_executor_job(_read_cached, target)
    except OSError as err:
        raise VoicePayloadError(f"Could not read the cached voice payload: {err}") from err
    if cached is not None:
        return cached

    try:
        integration = await async_get_integration(hass, DOMAIN)
    except (LoaderError, OSError) as err:
        raise VoicePayloadError(f"Could not resolve the integration version: {err}") from err
    url = voice_asset_url(str(integration.version))
    _LOGGER.debug("Downloading voice payload from %s", url)
    session = async_get_clientsession(hass)
    try:
        async with session.get(url, timeout=_DOWNLOAD_TIMEOUT) as resp:
            resp.raise_for_status()
            data = await resp.read()
    except (aiohttp.ClientError, TimeoutError) as err:
        raise VoicePayloadError(f"Could not download the voice payload from {url}: {err}") from err
    if not data:
        raise VoicePayloadError(f"Voice payload at {url} was empty")
    try:
        return await hass.async_add_executor_job(_write_atomic, target, data)
    except OSError as err:
        raise VoicePayloadError(f"Could not write the voice payload cache: {err}") from err
