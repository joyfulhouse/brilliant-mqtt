# brilliant-mqtt

Bridge Brilliant Control in-wall panels to MQTT / Home Assistant over each
panel's own internal message bus.

[![GitHub Release][releases-shield]][releases]
[![License][license-shield]](LICENSE)
[![HACS][hacs-shield]][hacs]
[![CI][ci-shield]][ci]
[![Quality Scale][quality-shield]][quality]
[![Project Maintenance][maintenance-shield]][maintenance]
[![GitHub Sponsors][sponsors-shield]][sponsors]
[![Ko-fi][kofi-shield]][kofi]

> **Which Brilliant?** This integration is for **Brilliant Smart Home Control** — the in-wall touchscreen control panels (1–4 switch and plug-in models) made by **Brilliant NextGen, Inc.** ([brilliant.tech](https://www.brilliant.tech), San Mateo, CA). It is **not** affiliated with the Australian **"Brilliant Smart"** lighting brand (smart plugs/bulbs/cameras) or any other "Brilliant" product. It replaces the panel's HomeKit-Controller path with a local MQTT / Home Assistant bridge.

## Start here

| What you want to do | Where to go |
|---|---|
| Install the bridge | **[INSTALL.md](INSTALL.md)** |
| Add via HACS (recommended) | **[docs/ha-integration.md](docs/ha-integration.md)** |
| Enable voice satellite | **[docs/voice.md](docs/voice.md)** |
| Browse all docs | **[docs/README.md](docs/README.md)** |

## What It Does

- An on-panel Python agent connects to the **panel's own internal message bus** — the same bus Brilliant's HomeKit bridge uses — and mirrors state to your MQTT broker as **Home Assistant MQTT-Discovery entities**.
- HA commands translate back into bus calls, giving you bidirectional control, live power monitoring, and full panel internals (screen, mic, volumes, occupancy…).
- The bridge runs on the panel itself (Python 3.10, resource-capped systemd unit in `/var`) so it **survives firmware OTA** and co-exists with the panel's built-in Alexa and HomeKit.

## Why this over HomeKit Controller?

Brilliant panels can pair into Home Assistant via **HomeKit Controller**, but that path is fragile and shallow. This bridge taps the panel's own bus directly:

| | HomeKit Controller | brilliant-mqtt |
|---|---|---|
| **Stability in HA** | Entities stick in `setup_retry`; pairings drop across HA restarts | Retained MQTT discovery + state — entities recover instantly across HA restarts |
| **Connection resilience** | Opaque HAP/BLE link; manual re-pair when it wedges | Auto-reconnect + re-reconcile, a ~2 s diff-poll, a stale-stream watchdog, periodic resync, and a reconnect-storm circuit breaker |
| **Loads exposed** | Lights, switches, faceplate motion/occupancy | All of that **plus** per-circuit power (incl. always-on gangs), dimmer/internal temperatures, and fault sensors |
| **BLE-mesh loads** | Each panel re-exposes them → duplicate entities | One **elected publisher** → a single clean set, with heartbeat failover |
| **Mesh-load motion** | Not exposed | Each mesh dimmer/switch's integrated **PIR motion** (+ score, thresholds) as `binary_sensor`s |
| **Panel internals** | None | Mic mute, screen on/brightness, volumes, night mode, child lock, identify, in-use occupancy, camera/privacy, CPU temp, firmware, Wi-Fi/Internet/NTP diagnostics |
| **Integration model** | Closed HAP accessory | Standard MQTT — retained, observable, scriptable, per-panel availability/LWT |
| **Responsiveness** | BLE/HAP round-trips | Direct bus tap + optimistic command echo + hot diff-poll |

It isn't a destructive replacement: the bridge uses the **same bus APIs**
Brilliant's own HomeKit peripheral does and runs alongside it, so you can **keep
HomeKit paired as a fallback** while migrating automations onto the MQTT entities.

## Features

### Reliability
- Auto-reconnecting bus client with post-reconnect re-reconcile
- Fast diff-publishing state poll (~2 s worst-case staleness even if bus pushes silently die)
- Stale-stream watchdog + reconnect-storm circuit breaker
- Retained discovery + state so HA restarts recover instantly

### Visibility
- Full snapshot (`get_all()`) plus push notifications, published as retained MQTT state
- HA MQTT Discovery: entities appear automatically, grouped per panel, with availability/LWT
- Whole-home power monitoring: live per-circuit wattage (including always-on gangs), dimmer temperatures, and fault sensors
- Panel controls and presence: mic mute, screen on/brightness, volumes, night mode, child lock, identify, panel-in-use occupancy, camera/privacy state, Wi-Fi diagnostics

### Control
- Bidirectional: HA light/switch commands (including brightness scaling) drive the physical loads
- BLE mesh loads: plug-in switches and mesh dimmers house-wide published by a single elected panel (priority-based MQTT leader election with heartbeat failover) — one stable set of HA entities, no per-panel duplicates, no single point of failure

### Voice satellite (opt-in)
Turn any panel into a **local wake-word voice satellite** — on-panel microphone,
speaker, and wake word; STT, conversation agent, and TTS run in your own HA
Assist pipeline. Enable per panel during onboarding or from the panel's device
page. See **[docs/voice.md](docs/voice.md)**.

For the full entity list and configuration options, see [docs/CONFIGURATION.md](docs/CONFIGURATION.md).

## Installation

Two ways to deploy and manage the agent:

| Option | Best for | Guide |
|---|---|---|
| **HACS companion integration** *(recommended)* | Most users — guided onboarding, auto-repair after OTA, one-click updates, voice satellite | [docs/ha-integration.md](docs/ha-integration.md) |
| **Manual deploy** | No Home Assistant, or shell/Ansible preference | [deploy/README.md](deploy/README.md) |

**Start here: [INSTALL.md](INSTALL.md)** — prerequisites checklist, broker setup, then the deploy choice above.

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=joyfulhouse&repository=brilliant-mqtt&category=integration)

## Documentation

| Document | What |
|---|---|
| [docs/README.md](docs/README.md) | Documentation index |
| [INSTALL.md](INSTALL.md) | Full installation guide |
| [docs/ha-integration.md](docs/ha-integration.md) | HACS integration — onboarding, updates, repair |
| [docs/voice.md](docs/voice.md) | Voice satellite guide |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the bridge works and why |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Env vars, MQTT topics, broker ACL |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common problems and fixes |

## Development

```bash
git clone https://github.com/joyfulhouse/brilliant-mqtt.git
cd brilliant-mqtt
uv sync
uv run ruff check && uv run mypy --strict src tests && uv run pytest
```

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md). Python 3.10 (panel-locked), uv,
ruff, mypy --strict, pytest; TDD with all bus/broker I/O behind Protocol seams
so the unit suite runs on any machine.

Implementation agents: read [CLAUDE.md](CLAUDE.md) for the required reading
order, the non-negotiables, and what remains of the plan.

## Support

- **Issues:** <https://github.com/joyfulhouse/brilliant-mqtt/issues>

## Support Development

If this project is useful to you, please consider supporting its development:

- [GitHub Sponsors][sponsors]
- [Ko-fi][kofi]

## License

This project is licensed under the **MIT** License — see [LICENSE](LICENSE)
for details.

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
