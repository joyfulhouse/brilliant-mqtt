"""Voice-agent configuration: env parsing, defaults, and required vars."""

from __future__ import annotations

import os

import pytest

from brilliant_voice.config import VoiceSettings


def test_defaults_with_only_required(monkeypatch: pytest.MonkeyPatch) -> None:
    for k in list(os.environ):
        if k.startswith("VOICE_") or k in ("BRILLIANT_PANEL", "LOG_LEVEL"):
            monkeypatch.delenv(k, raising=False)
    monkeypatch.setenv("BRILLIANT_PANEL", "office")

    s = VoiceSettings.from_env()

    assert s.panel == "office"
    assert s.name == "Brilliant office"
    assert s.api_port == 6053
    assert s.wake_word == "okay_nabu"
    assert s.mic_device == "default"
    assert s.snd_device == "plug:dmix_48000"
    assert s.enable_aec is False
    assert s.aec_mic_device == "plug:dsnoop_48000"
    assert s.aec_delay_ms == 0
    assert s.aec_type == 1
    assert s.ha_host == ""
    assert s.log_level == "INFO"


def test_required_panel_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRILLIANT_PANEL", raising=False)
    with pytest.raises(KeyError):
        VoiceSettings.from_env()


def test_overrides_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRILLIANT_PANEL", "kitchen")
    monkeypatch.setenv("VOICE_NAME", "Kitchen Panel")
    monkeypatch.setenv("VOICE_API_PORT", "6054")
    monkeypatch.setenv("VOICE_WAKE_WORD", "hey_jarvis")
    monkeypatch.setenv("VOICE_MIC_DEVICE", "plug:dsnoop_48000")
    monkeypatch.setenv("VOICE_SND_DEVICE", "plug:dmix")
    monkeypatch.setenv("VOICE_ENABLE_AEC", "1")
    monkeypatch.setenv("VOICE_AEC_MIC_DEVICE", "hw:2,0")
    monkeypatch.setenv("VOICE_AEC_DELAY_MS", "150")
    monkeypatch.setenv("VOICE_AEC_TYPE", "2")
    monkeypatch.setenv("VOICE_HA_HOST", "homeassistant=192.168.1.10")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    s = VoiceSettings.from_env()

    assert s.panel == "kitchen"
    assert s.name == "Kitchen Panel"
    assert s.api_port == 6054
    assert s.wake_word == "hey_jarvis"
    assert s.mic_device == "plug:dsnoop_48000"
    assert s.snd_device == "plug:dmix"
    assert s.enable_aec is True
    assert s.aec_mic_device == "hw:2,0"
    assert s.aec_delay_ms == 150
    assert s.aec_type == 2
    assert s.ha_host == "homeassistant=192.168.1.10"
    assert s.log_level == "DEBUG"


def test_voice_name_derived_when_unset(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("VOICE_NAME", raising=False)
    monkeypatch.setenv("BRILLIANT_PANEL", "sunroom")

    s = VoiceSettings.from_env()

    assert s.name == "Brilliant sunroom"


def test_voice_name_override_takes_precedence(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRILLIANT_PANEL", "sunroom")
    monkeypatch.setenv("VOICE_NAME", "Sun Room")

    s = VoiceSettings.from_env()

    assert s.name == "Sun Room"


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("1", True),
        ("true", True),
        ("YES", True),
        ("on", True),
        ("0", False),
        ("false", False),
        ("no", False),
        ("off", False),
        ("garbage", False),
    ],
)
def test_bool_parsing_enable_aec(
    monkeypatch: pytest.MonkeyPatch, value: str, expected: bool
) -> None:
    monkeypatch.setenv("BRILLIANT_PANEL", "office")
    monkeypatch.setenv("VOICE_ENABLE_AEC", value)
    assert VoiceSettings.from_env().enable_aec is expected


@pytest.mark.parametrize(
    ("env_key", "env_value", "field", "expected"),
    [
        ("VOICE_API_PORT", "7000", "api_port", 7000),
        ("VOICE_AEC_DELAY_MS", "200", "aec_delay_ms", 200),
        ("VOICE_AEC_TYPE", "0", "aec_type", 0),
    ],
)
def test_int_parsing(
    monkeypatch: pytest.MonkeyPatch,
    env_key: str,
    env_value: str,
    field: str,
    expected: int,
) -> None:
    monkeypatch.setenv("BRILLIANT_PANEL", "office")
    monkeypatch.setenv(env_key, env_value)
    assert getattr(VoiceSettings.from_env(), field) == expected
