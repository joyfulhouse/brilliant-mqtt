"""Environment-based configuration for the on-panel voice agent.

Read from environment variables at startup (the ``brilliant-voice.service``
``EnvironmentFile``). The single required variable raises ``KeyError`` when
absent; everything else falls back to the live-verified pilot defaults.

This agent runs linux-voice-assistant (LVA) as the voice satellite, exposing
an ESPHome-compatible native API so Home Assistant's ``esphome`` integration
discovers it as an ``assist_satellite`` entity. LVA coexists with the panel's
built-in Alexa vassal — no disable required in this phase.

Pure stdlib — no panel imports, no ML imports.
"""

from __future__ import annotations

import os
from collections.abc import Mapping
from dataclasses import dataclass

_TRUE = frozenset({"1", "true", "yes", "on"})


def _env_bool(env: Mapping[str, str], key: str, default: bool) -> bool:
    """Parse a boolean env var: 1/true/yes/on (any case) is True, all else False."""
    raw = env.get(key)
    if raw is None:
        return default
    return raw.strip().lower() in _TRUE


@dataclass(frozen=True)
class VoiceSettings:
    """Immutable voice-agent configuration sourced from environment variables."""

    panel: str
    # HA satellite display name. Derived from the panel slug when VOICE_NAME is
    # unset; field default must be "" because a default referencing `panel` is
    # not allowed in a dataclass (field ordering / forward-reference constraint).
    name: str = ""
    # LVA ESPHome native API port (HA connects IN to this port).
    api_port: int = 6053
    wake_word: str = "okay_nabu"
    # ALSA devices (live-verified on the pilot). Capture uses the panel's own
    # `default` device, which IS Brilliant's tuned wake-word chain:
    #   hw:2 mic -> dsnoop -> LADSPA dcRemove -> amp(x30) -> average-downmix mono
    # (see /etc/asound.conf; the panel's own comment: the low-pass is omitted
    # here because it "interferes with word audio recognition and the wakeword
    # engine"). The raw `plug:dsnoop_48000` tap is pre-DC-removal/pre-gain, so
    # far-field speech is too quiet to detect — verified: far-field "hey jarvis"
    # scores ~0.996 via `default` vs ~0.88 via the raw tap. `plug:dmix_48000`
    # mixes our playback with the panel's other audio.
    mic_device: str = "default"
    snd_device: str = "plug:dmix_48000"
    # AEC (Acoustic Echo Cancellation) via the panel's audio_dsp ctypes package.
    # Default OFF — only needed for barge-in (continued conversation). When
    # enabled, the AEC daemon reads the raw 2-mic tap and outputs clean audio.
    enable_aec: bool = False
    aec_mic_device: str = "plug:dsnoop_48000"
    aec_delay_ms: int = 0
    # AEC algorithm: 0=DSP_WIDGETS, 1=SPEEX, 2=WEBRTC
    aec_type: int = 1
    # Optional "hostname=ip" mapping added to /etc/hosts so the panel can
    # resolve HA's TTS-URL host on segmented networks. Empty = rely on the
    # panel's own DNS.
    ha_host: str = ""
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> VoiceSettings:
        """Construct VoiceSettings from environment variables.

        Required: ``BRILLIANT_PANEL``. Optional: ``VOICE_NAME``,
        ``VOICE_API_PORT``, ``VOICE_WAKE_WORD``, ``VOICE_MIC_DEVICE``,
        ``VOICE_SND_DEVICE``, ``VOICE_ENABLE_AEC``, ``VOICE_AEC_MIC_DEVICE``,
        ``VOICE_AEC_DELAY_MS``, ``VOICE_AEC_TYPE``, ``VOICE_HA_HOST``,
        ``LOG_LEVEL``.

        Raises ``KeyError`` when ``BRILLIANT_PANEL`` is absent.
        """
        env = os.environ

        # Required — intentionally use direct __getitem__ so KeyError propagates.
        panel = env["BRILLIANT_PANEL"]

        # Derive the HA satellite display name from the panel slug when unset.
        name = env.get("VOICE_NAME") or f"Brilliant {panel}"

        return cls(
            panel=panel,
            name=name,
            api_port=int(env.get("VOICE_API_PORT", "6053")),
            wake_word=env.get("VOICE_WAKE_WORD", "okay_nabu"),
            mic_device=env.get("VOICE_MIC_DEVICE", "default"),
            snd_device=env.get("VOICE_SND_DEVICE", "plug:dmix_48000"),
            enable_aec=_env_bool(env, "VOICE_ENABLE_AEC", False),
            aec_mic_device=env.get("VOICE_AEC_MIC_DEVICE", "plug:dsnoop_48000"),
            aec_delay_ms=int(env.get("VOICE_AEC_DELAY_MS", "0")),
            aec_type=int(env.get("VOICE_AEC_TYPE", "1")),
            ha_host=env.get("VOICE_HA_HOST", ""),
            log_level=env.get("LOG_LEVEL", "INFO"),
        )
