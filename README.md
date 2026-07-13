# brilliant-mqtt

Local Home Assistant control for Brilliant smart-home touchscreen panels — no
cloud, no HomeKit pairing, no flaky BLE.

[![GitHub Release][releases-shield]][releases]
[![License][license-shield]](LICENSE)
[![HACS][hacs-shield]][hacs]
[![CI][ci-shield]][ci]
[![Quality Scale][quality-shield]][quality]
[![Project Maintenance][maintenance-shield]][maintenance]
[![GitHub Sponsors][sponsors-shield]][sponsors]
[![Ko-fi][kofi-shield]][kofi]

> For **Brilliant NextGen** in-wall panels ([brilliant.tech](https://www.brilliant.tech)) —
> not the Australian "Brilliant Smart" plug/bulb brand. See [Credits](#credits).

## What It Does

A small agent runs on each Brilliant panel and bridges it to your MQTT broker.
Home Assistant discovers everything automatically — no YAML:

- **Control your lights and switches.** Every load wired to a panel, plus the
  house-wide Brilliant mesh dimmers and plug-ins, as ordinary HA lights and
  switches with dimming.
- **See what your house is doing.** Live per-circuit power for every load,
  motion from mesh dimmers and panel faceplates, room occupancy, device
  temperatures, and fault alerts.
- **Manage the panels themselves.** Screen, brightness, volumes, mic mute,
  night mode, child lock, screensaver, firmware auto-update — all from HA.
- **Talk to your house** *(optional)*. Any panel can become a local wake-word
  voice satellite for HA Assist, using its built-in mic and speaker.

Everything stays local: the agent taps the panel's own internal message bus —
the same one Brilliant's HomeKit bridge uses — and runs alongside Alexa and
HomeKit without breaking them. It survives panel firmware updates.

## Features

| Area | What you get |
|---|---|
| Lighting & switches | Panel loads and BLE-mesh loads as HA lights/switches, with brightness |
| Energy | Live wattage per circuit — including always-on gangs you can't see anywhere else |
| Motion & presence | Motion from every mesh dimmer/switch and panel faceplate, plus panel-in-use occupancy |
| Panel settings | 25+ entities per panel: screen, audio, privacy, governance switches |
| Voice satellite | Local wake word → your own HA Assist pipeline (opt-in per panel) |
| Reliability | Auto-reconnect, watchdogs, and retained MQTT state — entities survive HA restarts, panel reboots, and firmware updates. A bus-health watchdog self-recovers a panel whose Brilliant bus stays wedged. |

Compared to pairing the panels through HomeKit Controller (the usual
workaround): no dropped pairings or `setup_retry`, far more entities (power,
motion, panel internals), and one clean set of mesh entities instead of
per-panel duplicates.

## Prerequisites

- One or more Brilliant Smart Home Control panels with **Root SSH Login**
  enabled (a supported toggle in the panel's settings)
- An MQTT broker (e.g. Mosquitto) that Home Assistant is connected to
- Home Assistant with HACS (for the recommended install path)

## Installation

**Recommended:** install the companion integration via HACS — it walks you
through onboarding, installs the agent on each panel over SSH, keeps it
updated from HA, and auto-repairs after firmware updates.

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=joyfulhouse&repository=brilliant-mqtt&category=integration)

| Option | Best for | Guide |
|---|---|---|
| **HACS integration** *(recommended)* | Most users | [docs/ha-integration.md](docs/ha-integration.md) |
| **Manual deploy** | No Home Assistant, or shell/Ansible preference | [deploy/README.md](deploy/README.md) |

Full guide, including broker setup: **[INSTALL.md](INSTALL.md)**.

## Configuration

Sensible defaults out of the box; everything tunable lives in one place:

- **[docs/CONFIGURATION.md](docs/CONFIGURATION.md)** — every entity, env var,
  and MQTT topic
- **[docs/voice.md](docs/voice.md)** — voice satellite setup
- **[docs/README.md](docs/README.md)** — full documentation index

## Supported Equipment

- **Brilliant Smart Home Control panels** — 1-, 2-, 3-, and 4-switch in-wall
  models and the plug-in Smart Home Control
- **Loads wired to panels** — dimmers, switches, and always-on circuits
- **Brilliant BLE-mesh accessories** — Smart Dimmer Switches and Smart Plugs
  house-wide, published once by an elected panel (with automatic failover)
- **Panel sensors** — faceplate motion (PIR), ambient light, touch, mic/speaker

## Automation Examples

Motion-activated dining lights, using a mesh dimmer's built-in motion sensor
(entity names follow your own panel and load names):

```yaml
automation:
  - alias: "Dining room: lights on motion"
    triggers:
      - trigger: state
        entity_id: binary_sensor.brilliant_ble_mesh_dining_room_lights_motion
        to: "on"
    actions:
      - action: light.turn_on
        target:
          entity_id: light.brilliant_ble_mesh_dining_room_lights
```

Alert when a circuit's power looks wrong (space heater left on):

```yaml
automation:
  - alias: "Office heater left on"
    triggers:
      - trigger: numeric_state
        entity_id: sensor.brilliant_office_heater_power
        above: 500
        for: "01:00:00"
    actions:
      - action: notify.mobile_app
        data:
          message: "The office heater has been on for an hour."
```

## Troubleshooting

Common problems — entities missing, sensors lagging, motion not firing,
broker/ACL issues — are covered in
**[docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md)**.

## Development

```bash
git clone https://github.com/joyfulhouse/brilliant-mqtt.git
cd brilliant-mqtt
uv sync
uv run ruff check && uv run mypy --strict src tests && uv run pytest
```

Python 3.10 (panel-locked), uv, ruff, mypy --strict, pytest; all bus/broker
I/O sits behind Protocol seams so the unit suite runs on any machine. See
[docs/DEVELOPMENT.md](docs/DEVELOPMENT.md) and
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md). Implementation agents: read
[CLAUDE.md](CLAUDE.md) first.

## Support

- Join the [JoyfulHouse Discord](https://discord.gg/gc4eTPwxjJ) for support and discussion across all JoyfulHouse Home Assistant integrations and libraries.
- **Issues:** <https://github.com/joyfulhouse/brilliant-mqtt/issues>

## Support Development

If this project is useful to you, please consider supporting its development:

- [GitHub Sponsors][sponsors]
- [Ko-fi][kofi]

## License

This project is licensed under the **MIT** License — see [LICENSE](LICENSE)
for details.

## Credits

- Built for **Brilliant Smart Home Control** panels by **Brilliant NextGen,
  Inc.** ([brilliant.tech](https://www.brilliant.tech), San Mateo, CA). This
  project is not affiliated with or endorsed by Brilliant NextGen, and is not
  related to the Australian **"Brilliant Smart"** lighting brand.
- The agent uses the same on-panel message-bus APIs Brilliant's own HomeKit
  bridge uses — nothing is modified or jailbroken; Root SSH is an official
  panel feature.

<!-- Badge links -->
[releases-shield]: https://img.shields.io/github/release/joyfulhouse/brilliant-mqtt.svg?style=for-the-badge
[releases]: https://github.com/joyfulhouse/brilliant-mqtt/releases
[license-shield]: https://img.shields.io/github/license/joyfulhouse/brilliant-mqtt.svg?style=for-the-badge
[hacs-shield]: https://img.shields.io/badge/HACS-Custom-41BDF5.svg?style=for-the-badge
[hacs]: https://github.com/hacs/integration
[ci-shield]: https://img.shields.io/github/actions/workflow/status/joyfulhouse/brilliant-mqtt/ci.yml?style=for-the-badge&label=CI
[ci]: https://github.com/joyfulhouse/brilliant-mqtt/actions
[quality-shield]: https://img.shields.io/badge/Quality%20Scale-Platinum-5c2d91.svg?style=for-the-badge
[quality]: https://developers.home-assistant.io/docs/core/integration-quality-scale/
[maintenance-shield]: https://img.shields.io/badge/maintainer-%40btli-blue.svg?style=for-the-badge
[maintenance]: https://github.com/btli
[sponsors-shield]: https://img.shields.io/badge/sponsor-GitHub-EA4AAA.svg?style=for-the-badge&logo=githubsponsors&logoColor=white
[sponsors]: https://github.com/sponsors/btli
[kofi-shield]: https://img.shields.io/badge/Ko--fi-donate-FF5E5B.svg?style=for-the-badge&logo=ko-fi&logoColor=white
[kofi]: https://ko-fi.com/bryanli
