# deploy/voice — Voice satellite build artifacts

This directory contains the overlay and sidecar for running a
[linux-voice-assistant](https://github.com/OHF-Voice/linux-voice-assistant)
(LVA) voice satellite on a Brilliant Control panel.

## Contents

- `LVA_REF` — upstream LVA commit SHA pinned for this build
- `lva-overlay/` — fork overlay copied over the upstream clone at build time
- `aec_daemon.py` — on-panel AEC sidecar (runs under py3.10)

## How the build works

`scripts/build_voice_payload.sh` (a later task) will:
1. Clone `github.com/OHF-Voice/linux-voice-assistant` at the SHA in `LVA_REF`
2. Copy `lva-overlay/` over the clone (replacing/adding files)
3. Package the result for deployment to `/var/lva/` on the panel

## aec_daemon.py

Runs under the panel's own Python 3.10
(`/data/switch-embedded/env/bin/python3`) because it needs the panel's
`audio_dsp` package (a cpython-310 Cython module). LVA runs under py3.11.
FIFOs bridge the two processes. Full install steps come in a later task.
