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
    assert s.satellite_port == 10700
    assert s.wake_port == 10400
    assert s.mic_device == "default"
    assert s.snd_device == "plug:dmix_48000"
    assert s.wake_word == "hey_jarvis"
    assert s.wake_threshold == 0.5
    assert s.enable_wake is True
    assert s.disable_alexa is True
    assert s.log_level == "INFO"


def test_required_panel_missing_raises(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("BRILLIANT_PANEL", raising=False)
    with pytest.raises(KeyError):
        VoiceSettings.from_env()


def test_overrides_from_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("BRILLIANT_PANEL", "kitchen")
    monkeypatch.setenv("VOICE_SATELLITE_PORT", "42700")
    monkeypatch.setenv("VOICE_WAKE_PORT", "42400")
    monkeypatch.setenv("VOICE_MIC_DEVICE", "default")
    monkeypatch.setenv("VOICE_SND_DEVICE", "plug:dmix")
    monkeypatch.setenv("VOICE_WAKE_WORD", "ok_nabu")
    monkeypatch.setenv("VOICE_WAKE_THRESHOLD", "0.7")
    monkeypatch.setenv("VOICE_ENABLE_WAKE", "0")
    monkeypatch.setenv("VOICE_DISABLE_ALEXA", "false")
    monkeypatch.setenv("LOG_LEVEL", "DEBUG")

    s = VoiceSettings.from_env()

    assert s.panel == "kitchen"
    assert s.satellite_port == 42700
    assert s.wake_port == 42400
    assert s.mic_device == "default"
    assert s.snd_device == "plug:dmix"
    assert s.wake_word == "ok_nabu"
    assert s.wake_threshold == 0.7
    assert s.enable_wake is False
    assert s.disable_alexa is False
    assert s.log_level == "DEBUG"


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
        ("", False),
    ],
)
def test_bool_parsing(monkeypatch: pytest.MonkeyPatch, value: str, expected: bool) -> None:
    monkeypatch.setenv("BRILLIANT_PANEL", "office")
    monkeypatch.setenv("VOICE_DISABLE_ALEXA", value)
    assert VoiceSettings.from_env().disable_alexa is expected
