"""ALSA/arecord drop-in replacement for the `soundcard` API surface LVA uses.

LVA's __main__ only needs: all_microphones()/get_microphone()/default_microphone()
returning an object with `.name` and `.recorder(samplerate, channels, blocksize)`,
whose context manager yields an object with `.record(n) -> float32 ndarray
(n, channels) in [-1, 1]`. We back that with an `arecord` subprocess so the panel
needs no PulseAudio (it has none) — just ALSA, which it already runs.

The device string is passed straight to `arecord -D` (e.g. an ALSA PCM name like
`default`, `dsnoop_filtered`, or the AEC-cleaned loopback `hw:0,1,0`).
"""
from __future__ import annotations

import logging
import subprocess
from typing import Optional

import numpy as np

_LOGGER = logging.getLogger(__name__)


class _Recorder:
    def __init__(self, device: str, samplerate: int, channels: int, blocksize: int) -> None:
        self._device = device
        self._sr = samplerate
        self._ch = channels
        # "fifo:/path" → read raw S16 PCM from a FIFO (the AEC daemon's clean
        # output) instead of spawning arecord. Used to insert echo cancellation
        # in front of LVA without LVA knowing.
        self._fifo = device[5:] if device.startswith("fifo:") else None
        self._proc: Optional[subprocess.Popen] = None
        self._f = None

    def __enter__(self) -> "_Recorder":
        if self._fifo is not None:
            _LOGGER.debug("mic FIFO: %s", self._fifo)
            self._f = open(self._fifo, "rb", buffering=0)
            return self
        cmd = [
            "arecord", "-q", "-D", self._device,
            "-f", "S16_LE", "-c", str(self._ch), "-r", str(self._sr),
            "-t", "raw",
        ]
        _LOGGER.debug("arecord: %s", " ".join(cmd))
        self._proc = subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)
        return self

    def record(self, numframes: int) -> np.ndarray:
        need = numframes * self._ch * 2
        stream = self._f if self._fifo is not None else (self._proc.stdout if self._proc else None)
        assert stream is not None
        buf = bytearray()
        while len(buf) < need:
            chunk = stream.read(need - len(buf))
            if not chunk:
                break
            buf += chunk
        a = np.frombuffer(bytes(buf), dtype="<i2").astype(np.float32) / 32768.0
        if self._ch > 1:
            return a.reshape(-1, self._ch)
        return a.reshape(-1, 1)

    def __exit__(self, *exc) -> None:
        if self._f is not None:
            try:
                self._f.close()
            except Exception:
                pass
            self._f = None
        if self._proc is not None:
            try:
                self._proc.terminate()
                self._proc.wait(timeout=2)
            except Exception:
                try:
                    self._proc.kill()
                except Exception:
                    pass
            self._proc = None


class _Mic:
    def __init__(self, device: str) -> None:
        self.device = device
        self.name = f"alsa:{device}"

    def recorder(self, samplerate: int = 16000, channels: int = 1, blocksize: int = 1024) -> _Recorder:
        return _Recorder(self.device, samplerate, channels, blocksize)


def get_microphone(device) -> _Mic:
    return _Mic(str(device))


def default_microphone() -> _Mic:
    return _Mic("default")


def all_microphones(include_loopback: bool = False):
    # arecord -L would enumerate PCMs; LVA only uses this for --list-input-devices.
    return [_Mic("default")]
