"""GStreamer audio player — drop-in for LibMpvPlayer (the panel has no libmpv).

Implements the interface MpvMediaPlayer drives:
  play(url, done_callback, stop_first), pause(), resume(),
  stop(for_replacement), state(), set_volume(), duck(), unduck().

Each play() spawns `gst-launch-1.0` with an explicit decode pipeline to an
ALSA sink. This handles local FLAC/WAV (the wake/timer sounds) and remote
http(s) MP3/WAV (Home Assistant TTS) — everything aplay cannot. EOF is the
subprocess exiting cleanly, which fires done_callback (drives LVA's
continued-conversation re-arm). A generation counter makes stop()/replace
race-free against the per-play waiter thread.

Limitation: gst-launch CLI volume is fixed at launch, so set_volume/duck apply
to the NEXT playback, not mid-stream. Adequate for wake->TTS; live ducking of
simultaneous music is a follow-up.
"""
from __future__ import annotations

import logging
import os
import signal
import subprocess
import threading
from pathlib import Path
from typing import Callable, Optional

from linux_voice_assistant.player.base import AudioPlayer
from linux_voice_assistant.player.state import PlayerState


class GstPlayer(AudioPlayer):
    def __init__(self, device: Optional[str] = None) -> None:
        self._log = logging.getLogger(self.__class__.__name__)
        self._device = device  # ALSA PCM for alsasink, or None -> default
        self._state: PlayerState = PlayerState.IDLE
        self._lock = threading.RLock()
        self._proc: Optional[subprocess.Popen] = None
        self._done: Optional[Callable[[], None]] = None
        self._gen = 0  # invalidates stale waiter threads on stop/replace
        self._user_volume = 100.0
        self._duck_factor = 1.0

    # -------- pipeline --------

    def _build_cmd(self, url: str) -> list:
        if "://" in url:
            src = ["souphttpsrc", f"location={url}"]
        else:
            src = ["filesrc", f"location={Path(url).absolute()}"]
        vol = max(0.0, min(10.0, self._effective() / 100.0))
        sink = ["alsasink"]
        if self._device:
            sink.append(f"device={self._device}")
        # If LVA_REF_FIFO is set, tee the post-volume audio (what the speaker
        # actually plays) as a 16k/mono/S16 stream into that FIFO — the AEC
        # daemon uses it as the echo-cancellation reference. The daemon paces
        # consumption to real time, so the tap need not be clock-synced here.
        ref = os.environ.get("LVA_REF_FIFO")
        if ref:
            return (
                ["gst-launch-1.0", "-q"]
                + src
                + ["!", "decodebin", "!", "audioconvert", "!", "audioresample",
                   "!", "volume", f"volume={vol:.3f}", "!", "tee", "name=t"]
                + ["t.", "!", "queue", "!", "audioconvert", "!", "audioresample", "!"] + sink
                + ["t.", "!", "queue", "!", "audioconvert", "!", "audioresample",
                   "!", "audio/x-raw,rate=16000,channels=1,format=S16LE",
                   "!", "filesink", f"location={ref}", "buffer-mode=2"]
            )
        return (
            ["gst-launch-1.0", "-q"]
            + src
            + ["!", "decodebin", "!", "audioconvert", "!", "audioresample",
               "!", "volume", f"volume={vol:.3f}", "!"]
            + sink
        )

    # -------- playback control --------

    def play(self, url: str, done_callback: Optional[Callable[[], None]] = None,
             stop_first: bool = True) -> None:
        with self._lock:
            self._stop_locked(for_replacement=True)
            self._done = done_callback
            self._gen += 1
            gen = self._gen
            cmd = self._build_cmd(url)
            self._log.debug("gst play: %s", " ".join(cmd))
            try:
                self._proc = subprocess.Popen(cmd, stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
            except Exception:
                self._log.exception("gst-launch failed to start")
                self._state = PlayerState.ERROR
                self._done = None
                return
            self._state = PlayerState.PLAYING
            threading.Thread(target=self._wait, args=(self._proc, gen), daemon=True).start()

    def _wait(self, proc: subprocess.Popen, gen: int) -> None:
        rc = proc.wait()
        cb: Optional[Callable[[], None]] = None
        with self._lock:
            if gen != self._gen:
                return  # superseded by a newer play()/stop()
            self._proc = None
            self._state = PlayerState.IDLE if rc == 0 else PlayerState.ERROR
            cb = self._done
            self._done = None
        if cb is not None:
            try:
                cb()
            except Exception:
                self._log.exception("done_callback raised")

    def pause(self) -> None:
        with self._lock:
            if self._proc is not None and self._state == PlayerState.PLAYING:
                try:
                    self._proc.send_signal(signal.SIGSTOP)
                    self._state = PlayerState.PAUSED
                except Exception:
                    pass

    def resume(self) -> None:
        with self._lock:
            if self._proc is not None and self._state == PlayerState.PAUSED:
                try:
                    self._proc.send_signal(signal.SIGCONT)
                    self._state = PlayerState.PLAYING
                except Exception:
                    pass

    def stop(self, for_replacement: bool = False) -> None:
        with self._lock:
            self._stop_locked(for_replacement)

    def _stop_locked(self, for_replacement: bool) -> None:
        if for_replacement:
            self._done = None
        self._gen += 1  # invalidate any waiter so it won't fire done_callback
        if self._proc is not None:
            try:
                self._proc.send_signal(signal.SIGCONT)  # in case paused
                self._proc.terminate()
                self._proc.wait(timeout=2)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None
        self._state = PlayerState.IDLE

    def state(self) -> PlayerState:
        with self._lock:
            return self._state

    # -------- volume / ducking (applied on next play) --------

    def set_volume(self, volume: float) -> None:
        with self._lock:
            self._user_volume = max(0.0, min(100.0, float(volume)))

    def duck(self, factor: float = 0.5) -> None:
        with self._lock:
            self._duck_factor = max(0.0, min(1.0, float(factor)))

    def unduck(self) -> None:
        with self._lock:
            self._duck_factor = 1.0

    def _effective(self) -> float:
        return self._user_volume * self._duck_factor
