# deploy/voice — build/deploy reference

> **To enable and use voice in Home Assistant, see [docs/voice.md](../../docs/voice.md).**
> The HACS integration handles download, install, and repair automatically.
> This file is the **build/deploy reference** for building the ~57 MB payload manually.

Build artifacts for running a
[linux-voice-assistant](https://github.com/OHF-Voice/linux-voice-assistant)
(LVA) **Home Assistant ESPHome voice satellite** on a Brilliant Control panel:
on-panel wake word + mic capture + speaker playback. STT, the LLM, and TTS are
entirely your HA Assist pipeline — the panel is backend-agnostic.

## Contents

- `LVA_REF` — upstream LVA commit pinned for this build
- `lva-overlay/` — our fork overlay (ALSA `arecord` mic shim, GStreamer player,
  split wake/STT mic path, instant-wake) copied over the upstream clone at build time
- `aec_daemon.py` — on-panel AEC sidecar (runs under the panel's py3.10 because it
  needs the panel's `audio_dsp` Cython module); used only when `VOICE_ENABLE_AEC=on`

## 1. Build the payload (dev machine, fetch-only)

```sh
scripts/build_voice_payload.sh
```
Produces `custom_components/brilliant_mqtt/voice_payload/brilliant-voice-payload-<ver>.tar.gz`
(~57 MB compressed): a stripped Python 3.11 (LVA needs ≥3.11; the panel's own
py3.10 is reserved for the closed bus libs), the LVA py3.11 deps, vendored native
libs, the LVA fork, our supervisor, and the AEC daemon. Nothing is compiled.

## 2. Deploy to a panel (`/var/brilliant-voice/`, OTA-persistent)

Extract the tarball to `/var/brilliant-voice/`, install the env + unit, enable:
```sh
# on the panel
tar xzf brilliant-voice-payload-<ver>.tar.gz -C /var/brilliant-voice
cp /var/brilliant-voice/brilliant-voice.service /etc/systemd/system/
cat > /etc/brilliant-voice.env <<'ENV'        # chmod 600
BRILLIANT_PANEL=<slug>
VOICE_NAME=<friendly name>
# VOICE_HA_HOST=ha.example=10.0.0.5           # only if the panel can't resolve HA's URL host
ENV
systemctl daemon-reload && systemctl enable --now brilliant-voice
```
The supervisor (`python3 -m brilliant_voice`, panel py3.10) re-applies the nft
accept for the ESPHome port + the optional `/etc/hosts` mapping at every start
(both are tmpfs / OTA-replaced), then launches and supervises LVA (bundled py3.11)
and — when `VOICE_ENABLE_AEC=on` — the AEC daemon.

## 3. Home Assistant

After manual deploy: LVA advertises `_esphomelib._tcp` over zeroconf → HA
auto-discovers it and prompts you to add the ESPHome device. Assign an Assist
pipeline under **Settings → Voice assistants**. See [docs/voice.md](../../docs/voice.md)
for the full connection guide and troubleshooting.

## Configuration (`/etc/brilliant-voice.env`)

When deploying manually, write this file (mode `0600`). When using the HACS
integration, it is written automatically.

`BRILLIANT_PANEL` (required), `VOICE_NAME`, `VOICE_API_PORT` (6053),
`VOICE_WAKE_WORD` (okay_nabu), `VOICE_MIC_DEVICE` (default), `VOICE_SND_DEVICE`
(plug:dmix_48000), `VOICE_HA_HOST` (optional `host=ip`), `VOICE_ENABLE_AEC` (off),
`VOICE_AEC_*`, `LOG_LEVEL`. See `src/brilliant_voice/config.py` for the full
set and [docs/voice.md § Configuration](../../docs/voice.md#configuration) for
explanations.

## Resource caps & coexistence

The unit caps memory/CPU (`Nice=5`, `MemoryMax=300M`, `CPUQuota=100%`,
`OOMScoreAdjust=500`) so wake inference can never starve the touchscreen UI. The
mic is shared via ALSA dsnoop, so LVA coexists with the panel's built-in Alexa.

## Validated (pilot, the pilot panel 2026-06-23)

systemd `brilliant-voice` active → supervisor → LVA on :6053; nft accept + hosts
mapping re-applied at startup; LVA connected to HA (`assist_satellite … = idle`).
