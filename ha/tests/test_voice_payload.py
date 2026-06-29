"""Tests for voice_payload.py — download/cache helper for the voice tarball."""

from __future__ import annotations

from pathlib import Path

import pytest
from homeassistant.core import HomeAssistant
from homeassistant.helpers.storage import STORAGE_DIR
from homeassistant.loader import async_get_integration
from pytest_homeassistant_custom_component.test_util.aiohttp import AiohttpClientMocker

from custom_components.brilliant_mqtt import voice_payload
from custom_components.brilliant_mqtt.const import DOMAIN, VOICE_PAYLOAD_VERSION, voice_asset_url


# Override the default shared testing_config dir with a per-test temp directory
# so that leftover cache files from previous runs do not interfere.
@pytest.fixture
def hass_config_dir(hass_tmp_config_dir: str) -> str:
    """Use a temporary config directory so each test gets an isolated storage."""
    return hass_tmp_config_dir


async def _expected_url(hass: HomeAssistant) -> str:
    integration = await async_get_integration(hass, DOMAIN)
    return voice_asset_url(str(integration.version))


def _cache_file(hass: HomeAssistant) -> Path:
    return (
        Path(hass.config.path(STORAGE_DIR))
        / DOMAIN
        / f"brilliant-voice-payload-{VOICE_PAYLOAD_VERSION}.tar.gz"
    )


async def test_download_success(hass: HomeAssistant, aioclient_mock: AiohttpClientMocker) -> None:
    """A fresh download writes the tarball and returns its path."""
    url = await _expected_url(hass)
    content = b"TARBALL-BYTES"
    aioclient_mock.get(url, content=content)

    result = await voice_payload.async_fetch_voice_payload(hass)

    assert aioclient_mock.call_count == 1
    # Use executor-wrapped calls (not direct pathlib methods) to satisfy ASYNC240.
    assert await hass.async_add_executor_job(lambda: Path(result).exists())
    assert await hass.async_add_executor_job(Path(result).read_bytes) == content


async def test_cache_hit(hass: HomeAssistant, aioclient_mock: AiohttpClientMocker) -> None:
    """A pre-existing non-empty cache file is returned without an HTTP call."""
    cached = _cache_file(hass)
    cached.parent.mkdir(parents=True, exist_ok=True)
    cached.write_bytes(b"CACHED-CONTENT")

    result = await voice_payload.async_fetch_voice_payload(hass)

    assert aioclient_mock.call_count == 0
    assert result == str(cached)


async def test_http_error_raises_voice_payload_error(
    hass: HomeAssistant, aioclient_mock: AiohttpClientMocker
) -> None:
    """A 404 response is turned into VoicePayloadError."""
    url = await _expected_url(hass)
    aioclient_mock.get(url, status=404)

    with pytest.raises(voice_payload.VoicePayloadError):
        await voice_payload.async_fetch_voice_payload(hass)


async def test_url_correctness(hass: HomeAssistant, aioclient_mock: AiohttpClientMocker) -> None:
    """The request URL uses v<integration_version> and the VOICE_PAYLOAD_VERSION filename."""
    url = await _expected_url(hass)
    aioclient_mock.get(url, content=b"PAYLOAD")

    await voice_payload.async_fetch_voice_payload(hass)

    assert aioclient_mock.call_count == 1
    called_url = str(aioclient_mock.mock_calls[0][1])
    assert called_url == url
    assert f"brilliant-voice-payload-{VOICE_PAYLOAD_VERSION}.tar.gz" in called_url
