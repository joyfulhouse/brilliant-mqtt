"""Environment-based configuration for the on-panel voice agent.

Read from environment variables at startup (the ``brilliant-voice.service``
``EnvironmentFile``). The single required variable raises ``KeyError`` when
absent; everything else falls back to the live-verified pilot defaults.

Pure stdlib — no panel imports, no Wyoming/ML imports.
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
    # Wyoming satellite the HA Wyoming integration connects IN to. The panel's
    # nftables host firewall must accept this port (the agent ensures it).
    satellite_port: int = 10700
    # Local wyoming-openwakeword service the satellite calls for wake detection.
    wake_port: int = 10400
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
    # Wake word: a model bundled with wyoming-openwakeword (dev) or a custom
    # `.tflite` (production). `enable_wake=False` runs tap-to-talk only.
    wake_word: str = "hey_jarvis"
    wake_threshold: float = 0.5
    enable_wake: bool = True
    # Stop the built-in Alexa vassal so we own the mic (no double-trigger).
    disable_alexa: bool = True
    log_level: str = "INFO"

    @classmethod
    def from_env(cls) -> VoiceSettings:
        """Construct VoiceSettings from environment variables.

        Required: ``BRILLIANT_PANEL``. Optional: ``VOICE_SATELLITE_PORT``,
        ``VOICE_WAKE_PORT``, ``VOICE_MIC_DEVICE``, ``VOICE_SND_DEVICE``,
        ``VOICE_WAKE_WORD``, ``VOICE_WAKE_THRESHOLD``, ``VOICE_ENABLE_WAKE``,
        ``VOICE_DISABLE_ALEXA``, ``LOG_LEVEL``.

        Raises ``KeyError`` when ``BRILLIANT_PANEL`` is absent.
        """
        env = os.environ
        return cls(
            panel=env["BRILLIANT_PANEL"],
            satellite_port=int(env.get("VOICE_SATELLITE_PORT", "10700")),
            wake_port=int(env.get("VOICE_WAKE_PORT", "10400")),
            mic_device=env.get("VOICE_MIC_DEVICE", "default"),
            snd_device=env.get("VOICE_SND_DEVICE", "plug:dmix_48000"),
            wake_word=env.get("VOICE_WAKE_WORD", "hey_jarvis"),
            wake_threshold=float(env.get("VOICE_WAKE_THRESHOLD", "0.5")),
            enable_wake=_env_bool(env, "VOICE_ENABLE_WAKE", True),
            disable_alexa=_env_bool(env, "VOICE_DISABLE_ALEXA", True),
            log_level=env.get("LOG_LEVEL", "INFO"),
        )
