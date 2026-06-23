"""On-panel voice agent: a Home Assistant Assist satellite (mic + speaker) with
on-panel wake word.

Runs as the ``brilliant-voice`` systemd service alongside the message-bus
bridge. The supervisor launches two children: the bundled py3.11
linux-voice-assistant (LVA) satellite for mic capture, wake-word detection,
and TTS playback; and, when ``VOICE_ENABLE_AEC=1``, the panel-native py3.10
audio_dsp AEC daemon that provides echo cancellation and noise suppression via
the panel's own ``audio_dsp`` C extension. Like the bridge this package imports
no panel-only or heavy ML libraries at module scope — the wake-word runtime
lives behind the supervised subprocesses so the package stays unit-testable on
any machine.
"""

from __future__ import annotations

__version__ = "0.1.0"
