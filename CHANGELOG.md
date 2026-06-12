# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **BLE mesh loads via elected publisher (Milestone 11)**: Brilliant's
  plug-in switches and mesh dimmers (the bus's whole-home virtual `ble_mesh`
  device — 12 controllable loads) are now bridged to HA under a
  publisher-agnostic `brilliant/mesh/...` namespace and one
  "Brilliant BLE Mesh" HA device. Exactly one panel publishes them, elected
  by priority over a retained MQTT claim with heartbeat failover and
  higher-priority preemption (`MESH_PRIORITY`, `MESH_HEARTBEAT_SECONDS`) —
  no duplicate entities across the fleet, no single point of failure, zero
  HA churn when leadership moves. Mesh loads reporting the `-1` power
  sentinel get no junk power sensor. Verified live on the pilot panel:
  12 entities discovered, HA control round-trip ~2 s, leadership re-acquired
  across restarts.

### Fixed

- **Realtime state (power/motion/occupancy)**: live diagnosis found the panel
  lib's notification stream can die *silently* — pushes stop and the
  observer's `get_all()` mirror freezes, so even the periodic resync
  republished stale data. Three-layer fix: a fast scoped `get_device` poll
  (`HOT_POLL_SECONDS`, default 2 s) publishing only payload diffs; a
  processor-reconnect hook that re-subscribes and re-reconciles after gaps;
  and a stale-stream watchdog (`BUS_STALE_SECONDS`, default 900 s) that
  rebuilds the session. Sensor changes now reach HA in ~1–3 s worst-case
  instead of minutes. All state publishes flow through one diff cache, so
  the fast cadence adds no MQTT traffic while values are unchanged.
- Commanded writes now echo state optimistically: the bus does not push
  notifications for some panel variables (e.g. `muted`), so the bridge
  republishes the updated state immediately after a successful write — HA
  reflects aux controls instantly instead of waiting for the 5-min resync.
- Multi-gang aux entities are named per load ("Lights Power",
  "Backyard Lamps Power") instead of colliding into `power` / `power_2`.
- `handle_notification` must be a coroutine: the panel lib's inbound dispatcher
  awaits handler methods, so the previous sync override raised
  `TypeError: object NoneType can't be used in 'await' expression` on every bus
  push (found and fixed during the live pilot).

### Added

- Extended entities (Milestone 10): per-circuit power/temperature/fault
  sensors (incl. always-on gangs), panel controls (mic mute, screen +
  brightness, volumes, night mode, child lock, faceplate LED, identify),
  presence & privacy (panel-in-use occupancy, camera/privacy state, lux
  enable), and Wi-Fi/CPU diagnostics — driven by a declarative
  variable-entity table; HA entity categories + disabled-by-default flags
  keep the fleet tidy; panel firmware surfaces as device `sw_version`.
- Live pilot (2026-06-12): bridge running under systemd; HA
  discovery, telemetry, bidirectional control, and LWT/auto-restart verified
  against the real panel, broker, and Home Assistant.
- Operational logging: reconcile summary, inbound-command trace, bus
  set-variable responses (`LOG_LEVEL=DEBUG` for the full trace).

- **Full bridge implementation (Milestones 3–7):** normalized device model,
  HA entity mapping + MQTT-Discovery payloads, HA-command → bus variable-set
  translation, bridge orchestrator behind `BusClient`/`MqttClient` Protocol
  seams, env-driven config, real `RPCObserver` (deferred panel imports) and
  aiomqtt adapters with LWT, and a supervised entrypoint — comprehensive
  off-panel test suite, `mypy --strict` across src and tests, `py.typed`.
- Milestone-1 live PoC findings (`docs/reference/poc-findings.md`): verified
  connection recipe, real bus schema, device-scoping decision.

- Research: the Brilliant panel local control surface (internal Thrift message
  bus; no off-box API exists).
- Reference docs: introspected message-bus API (`RPCObserver` / ttypes) and the
  on-panel deployment/OTA guide.
- Design spec and task-by-task implementation plan for the message-bus → MQTT
  bridge (operator-internal).
- Project skeleton: uv project, Python 3.10 pin (panel runtime), ruff,
  mypy --strict, pytest.
- Reference systemd unit and manual pilot deploy guide (`deploy/`).
- JoyfulHouse OSS docs standard: LICENSE (MIT), INSTALL.md, CHANGELOG.md,
  FUNDING.yml, CODEOWNERS, and the canonical `docs/` set.

[Unreleased]: https://github.com/joyfulhouse/brilliant-mqtt/commits/main
