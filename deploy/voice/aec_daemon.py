#!/usr/bin/env python3
"""On-panel AEC sidecar (py3.10) — feeds LVA echo-cancelled mic audio via FIFO.

Runs the PROVEN AudioDSPProcessor (Brilliant's own AEC/beamform/NS, ~44 dB):

  2-mic capture (dsnoop) ─┐
                          ├─ apply_dsp_to_audio ─→ clean mono 16k ─→ CLEAN_FIFO ─→ LVA mic
  LVA TTS (REF_FIFO) ─────┘     (DC-removed, AEC)

The reference is whatever LVA is playing (TTS/chimes), tee'd to REF_FIFO by LVA's
GStreamer player. When nothing plays, the reference is silence and the mic passes
through (DC-remove + beamform + noise-suppress only). Runs under the panel's 3.10
venv (audio_dsp is a cpython-310 module); LVA runs under py3.11; FIFOs bridge them.
"""
from __future__ import annotations

import argparse
import asyncio
import collections
import errno
import logging
import os
import subprocess
import sys

import numpy as np
from peripherals.voice.alexa.audio_dsp_processor import AudioDSPProcessor

_LOG = logging.getLogger("aec_daemon")
FR = 640                    # frame_chunk per the processor (40 ms @ 16 kHz)
MIC_BYTES = FR * 2 * 2      # 640 frames * 2ch * 2 bytes
MONO_BYTES = FR * 2         # 640 frames * 1ch * 2 bytes


class _DummyRouter:
    def __getattr__(self, _):
        return lambda *a, **k: None


def _arecord(device: str, channels: int) -> subprocess.Popen:
    cmd = ["arecord", "-q", "-D", device, "-f", "S16_LE", "-c", str(channels),
           "-r", "16000", "-t", "raw"]
    return subprocess.Popen(cmd, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL, bufsize=0)


def _read_exact(stream, n: int) -> bytes:
    buf = bytearray()
    while len(buf) < n:
        chunk = stream.read(n - len(buf))
        if not chunk:
            break
        buf += chunk
    return bytes(buf)


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--mic-device", default="plug:dsnoop_48000")
    ap.add_argument("--ref-fifo", default="/var/lva/ref.fifo")
    ap.add_argument("--clean-fifo", default="/var/lva/clean.fifo")
    ap.add_argument("--aec-delay-ms", type=int, default=0)
    ap.add_argument("--aec-type", type=int, default=1)  # SPEEX
    ap.add_argument("--gain", type=float, default=1.0)
    ap.add_argument("--debug", action="store_true")
    args = ap.parse_args()
    logging.basicConfig(level=logging.DEBUG if args.debug else logging.INFO,
                        format="%(asctime)s.%(msecs)03d aec %(message)s", datefmt="%H:%M:%S")

    for p in (args.ref_fifo, args.clean_fifo):
        if not os.path.exists(p):
            os.mkfifo(p)

    proc = AudioDSPProcessor(asyncio.new_event_loop(), collections.deque(), _DummyRouter(),
                             FR, True, args.aec_delay_ms, args.aec_type, False)
    try:
        proc.set_gain(args.gain)
    except Exception:
        pass
    _LOG.info("AudioDSPProcessor ready (delay=%dms type=%d gain=%.1f)",
              args.aec_delay_ms, args.aec_type, args.gain)

    # clean: O_RDWR so opening never blocks and writes never EPIPE when LVA cycles.
    clean_fd = os.open(args.clean_fifo, os.O_RDWR)
    # ref: non-blocking reader — silence when LVA isn't playing.
    ref_fd = os.open(args.ref_fifo, os.O_RDONLY | os.O_NONBLOCK)

    mic = _arecord(args.mic_device, 2)
    _LOG.info("mic=%s  ref=%s  clean=%s", args.mic_device, args.ref_fifo, args.clean_fifo)

    silence = b"\x00" * MONO_BYTES
    ref_buf = bytearray()
    frames = 0
    active = 0
    try:
        while True:
            raw = _read_exact(mic.stdout, MIC_BYTES)
            if len(raw) < MIC_BYTES:
                _LOG.warning("mic stream ended"); break
            m = np.frombuffer(raw, dtype="<i2").reshape(-1, 2).astype(np.float64)
            m -= m.mean(0)                                  # remove raw-tap DC offset
            mono = np.clip(np.round(m.mean(1)), -32768, 32767).astype("<i2")

            # Pull whatever reference LVA has produced (non-blocking); pad with
            # silence to exactly one frame. Drain extra to avoid unbounded lag.
            try:
                while True:
                    chunk = os.read(ref_fd, MONO_BYTES * 4)
                    if not chunk:
                        break
                    ref_buf += chunk
                    if len(ref_buf) > MONO_BYTES * 8:
                        break
            except OSError as e:
                if e.errno not in (errno.EAGAIN, errno.EWOULDBLOCK):
                    raise
            if len(ref_buf) >= MONO_BYTES:
                ref_bytes = bytes(ref_buf[:MONO_BYTES]); del ref_buf[:MONO_BYTES]
                active += 1
            else:
                ref_bytes = silence

            clean = proc.apply_dsp_to_audio(mono.tobytes(), ref_bytes)
            if not clean:
                clean = silence
            os.write(clean_fd, clean)
            frames += 1
            if args.debug and frames % 250 == 0:
                _LOG.debug("frames=%d (%.0fs) ref-active=%d", frames, frames * FR / 16000, active)
    except KeyboardInterrupt:
        pass
    finally:
        try:
            mic.terminate()
        except Exception:
            pass
        _LOG.info("stopped after %d frames (ref-active %d)", frames, active)


if __name__ == "__main__":
    main()
