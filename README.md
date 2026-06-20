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

> **Status:** implemented and **verified live on real hardware**: HA
> discovery, telemetry, bidirectional control, and LWT/auto-restart all
> exercised against a production panel, broker, and Home Assistant. Agents
> start at [CLAUDE.md](CLAUDE.md), humans at [docs/](docs/README.md).

## What It Does

An on-panel Python 3.10 agent connects to the panel's internal Apache Thrift
message bus — the same bus Brilliant's own HomeKit bridge is a client of — and
mirrors device state to the home's central MQTT broker as Home Assistant
MQTT-Discovery entities, translating HA commands back into bus calls. It
replaces the unreliable HomeKit Controller path (entities stuck in
`setup_retry`, lost across HA restarts) with robust local read + control.

The panels expose no public API, no maintained HA integration, and no Matter,
so this is built from first principles on the panel's own client library — see
[docs/ARCHITECTURE.md](docs/ARCHITECTURE.md).

## Why this over HomeKit Controller?

Brilliant panels *can* be paired into Home Assistant via **HomeKit Controller**,
but that path is fragile and shallow. This bridge taps the panel's own message
bus directly and publishes everything as native MQTT-Discovery entities:

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

As implemented and verified live on real panels:

- Live state: full `get_all()` snapshot plus push notifications, published as
  retained MQTT state.
- HA MQTT Discovery: entities appear automatically, grouped per panel, with
  availability/LWT.
- Bidirectional control: HA light/switch commands (including brightness
  scaling) drive the physical loads.
- Whole-home power monitoring: live per-circuit wattage (including always-on
  gangs), dimmer temperatures, and fault sensors.
- Panel controls and presence: microphone mute, screen on/brightness, volumes,
  night mode, child lock, identify, panel-in-use occupancy, camera/privacy
  state, and Wi-Fi diagnostics — tidied with HA entity categories.
- BLE mesh loads: Brilliant plug-in switches and mesh dimmers house-wide,
  published by a single elected panel (priority-based MQTT leader election
  with heartbeat failover) — one stable set of HA entities, no per-panel
  duplicates, no single point of failure.
- Reliability first: auto-reconnecting bus client with post-reconnect
  re-reconcile, a fast diff-publishing state poll (~2 s worst-case staleness
  even if bus pushes silently die), a stale-stream watchdog, periodic full
  re-sync, and retained discovery + state so HA restarts recover instantly.
- Panel-safe: resource-capped systemd unit in `/var`, surviving firmware OTA;
  HomeKit stays paired as fallback.

## Installation

See **[INSTALL.md](INSTALL.md)** for the complete guide, including enabling
**root SSH** on the panel (an official, opt-in Brilliant feature) and **MQTT
broker setup** (standalone, or Home Assistant's Mosquitto add-on if you have no
broker). The bridge deploys to each panel either via the companion integration
below or the documented manual path.

[![Open your Home Assistant instance and open a repository inside the Home Assistant Community Store.](https://my.home-assistant.io/badges/hacs_repository.svg)](https://my.home-assistant.io/redirect/hacs_repository/?owner=joyfulhouse&repository=brilliant-mqtt&category=integration)

## Home Assistant companion integration

An optional **HACS custom integration** manages the agent's lifecycle across
your fleet from the Home Assistant UI — first deploy to a panel, version
**updates** (OTA), automatic **repair** after a panel firmware update, and
removal — while the devices themselves stay native MQTT-Discovery entities
published by the agent. Install it via HACS (custom repository
`joyfulhouse/brilliant-mqtt`, category Integration) or the release zip; add one
panel per config entry (per-panel root password, TOFU host-key pinning).

See **[docs/ha-integration.md](docs/ha-integration.md)** for the full guide.

## Quick Start (development)

```bash
git clone https://github.com/joyfulhouse/brilliant-mqtt.git
cd brilliant-mqtt
uv sync
uv run ruff check && uv run mypy --strict src tests && uv run pytest
```

Implementation agents: read [CLAUDE.md](CLAUDE.md) for the required reading
order, the non-negotiables, and what remains of the plan.

## Documentation

| Document | What |
|---|---|
| [docs/README.md](docs/README.md) | Documentation index |
| [docs/ARCHITECTURE.md](docs/ARCHITECTURE.md) | How the bridge works and why |
| [docs/CONFIGURATION.md](docs/CONFIGURATION.md) | Env contract, MQTT topics, broker ACL |
| [docs/TROUBLESHOOTING.md](docs/TROUBLESHOOTING.md) | Common problems and fixes |
| [INSTALL.md](INSTALL.md) | Full installation guide |

## Development

See [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md). Python 3.10 (panel-locked),
uv, ruff, mypy --strict, pytest; TDD with all bus/broker I/O behind Protocol
seams so the unit suite runs on any machine.

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
