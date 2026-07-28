# Brilliant MQTT Fleet Manager

![release_badge](https://img.shields.io/github/v/release/joyfulhouse/brilliant-mqtt?style=for-the-badge)
![release_date](https://img.shields.io/github/release-date/joyfulhouse/brilliant-mqtt?style=for-the-badge)
[![License](https://img.shields.io/github/license/joyfulhouse/brilliant-mqtt?style=for-the-badge)](LICENSE)

{% if prerelease %}
### NB!: This is a beta version
{% endif %}

> **Which Brilliant?** This integration is for **Brilliant Smart Home Control** — the in-wall touchscreen control panels (1–4 switch and plug-in models) made by **Brilliant NextGen, Inc.** ([brilliant.tech](https://www.brilliant.tech), San Mateo, CA). It is **not** affiliated with the Australian **"Brilliant Smart"** lighting brand (smart plugs/bulbs/cameras) or any other "Brilliant" product. It replaces the panel's HomeKit-Controller path with a local MQTT / Home Assistant bridge.

## What It Does

This companion integration manages the on-panel `brilliant-mqtt` agent's lifecycle across your Brilliant Control fleet from the Home Assistant UI:

- **First deploy** — push the agent to a panel over SSH and start it under systemd.
- **OTA updates** — update the agent on each panel when a new release is available.
- **Automatic repair** — detect and recover the agent after a panel firmware update (which replaces the panel OS, removing the agent).
- **Explicit uninstall** — remove the agent from a panel on demand. Removing the Home Assistant integration alone leaves running panel agents untouched.

The Brilliant Control panels themselves appear as **native MQTT-Discovery entities** published directly by the agent — lights, switches, dimmers, power sensors, presence, panel controls, and BLE-mesh loads — independent of this integration. Removing the integration leaves the device entities fully intact.

## Installation

Install via HACS: add `joyfulhouse/brilliant-mqtt` as a custom repository (category: **Integration**), install **Brilliant MQTT Fleet Manager**, restart Home Assistant, then add the integration once under **Settings → Devices & Services**. The official Home Assistant Mosquitto add-on is the recommended MQTT prerequisite, while an existing local, remote, or hosted broker is equally supported.

See [docs/ha-integration.md](https://github.com/joyfulhouse/brilliant-mqtt/blob/main/docs/ha-integration.md) for the full guide, including SSH prerequisites and per-panel configuration.
