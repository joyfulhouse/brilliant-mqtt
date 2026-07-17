# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

### Added

- **Panel reboot with pre-reboot diagnostics.** A new `brilliant_mqtt.reboot`
  service and a per-panel **Reboot panel** button capture a diagnostics bundle
  over SSH and *then* reboot the panel. The panels wedge two ways in practice —
  an uptime-decay wedge that ignores relay commands until a reboot, and Wi-Fi
  power-save packet starvation (MQTT keep-alive timeouts → LWT → unavailable
  cycles) — and the panel's journald is **volatile** (a `/run` tmpfs holding
  only the current boot), so a reboot destroys all evidence. The bundle is
  therefore always captured *before* the reboot and persisted to
  `<config>/brilliant_mqtt/diagnostics/<panel>/<UTC-timestamp>.log` (newest 14
  kept per panel); it collects uptime/memory/`/var` usage, the current-boot and
  bridge-unit journals, kernel Wi-Fi/OOM lines, the `wlan0` link + power-save
  state, connman services, and the bridge unit status — each probe failure-
  tolerant so one dead probe never aborts the bundle. The reboot's inevitable
  mid-command SSH disconnect is treated as success. Diagnostics capture is
  best-effort and never blocks the reboot; the service takes
  `collect_diagnostics` (default on) and `journal_lines` (100–2000, default
  400). Intended for a scheduled staggered overnight reboot automation.
- **Brilliant scenes and modes in Home Assistant** *(in pilot)*. An opt-in
  scene/mode bridge (`SCENE_BRIDGE_ENABLED`) runs on each panel agent's
  existing bus and MQTT sessions: it publishes the panel's scene and mode
  catalogs, fires `brilliant_mqtt_scene` / `brilliant_mqtt_mode` HA events on
  native executions, and accepts `brilliant_mqtt.run_scene` /
  `brilliant_mqtt.set_mode` service calls with **confirmed execution** — a
  command succeeds only after a matching execution record is observed, never
  on a bare bus write. Each panel gains a **Scene** select and a **Run
  selected scene** button. The bridge only catalogs and executes *existing*
  scene/mode IDs; it never creates or edits Brilliant configuration. See
  [the scene bridge guide](docs/brilliant-panel/home-assistant-integration.md).
- **HA MQTT control plane v1.** A versioned, HA-owned contract under
  `brilliant/ha-control/v1/...`: a retained manifest of labeled HA entities
  (with areas, Brilliant-room overrides, and capability-derived commands),
  retained per-entity state, and fenced non-retained command/result exchanges
  with idempotency keys and expiry. The scene/mode topics ride the same
  contract. No panel transport consumes the generic entity manifest yet — it
  is groundwork, not a native-tile feature.
- **Legacy HA-mirror cleanup CLI.** `brilliant_mqtt.cleanup_legacy_mirror`
  inventories (dry-run-first) and, on a separate operator decision, deletes
  peripherals persisted by the retired HA-mirror experiments, using strict
  case-sensitive ID and display-name allowlists and fail-closed verification.
  See [HA mirror retirement and cleanup](docs/ha-mirror.md).
- **Virtual Control research toolkit** (`tools/brilliant_vc/`) with
  reference-only systemd units and SHA-256 staged-tree manifests in
  `deploy/`. Research tooling for the blocked native-tile track — fail-closed
  gates, one-shot approval markers, pinned manifests, non-root runtime; not
  installed or started by any repository automation. See
  [the toolkit overview](docs/brilliant-panel/virtual-control-toolkit.md).

### Deprecated

- **The physical-Control HA mirror is retired and must stay inactive.**
  `brilliant-ha-mirror` is hidden from install selection, its installer fails
  closed, and config entries migrate to version 3 with a verified uninstall
  and a redacted Repair when absence cannot be proven. The supported
  replacement is the HA-owned control plane and scene bridge. See
  [HA mirror retirement and cleanup](docs/ha-mirror.md).

### Fixed

- **Agent unit disables panel Wi-Fi power-save at start** — root cause of the
  MQTT keepalive flap / entity churn. The on-panel systemd unit now runs
  `ExecStartPre=-/bin/sh -c 'iw dev wlan0 set power_save off'` before the
  bridge. Wi-Fi power-save drops inbound packets → MQTT keepalive timeouts →
  LWT → ~5-minute unavailable cycles (confirmed live: 20 min with power-save
  off held bridge errors at 0 vs constant before). The `-` prefix keeps it
  strictly best-effort so a missing/failing `iw` can never delay or block the
  bridge, and it rolls out durably across panel reboots via the existing
  redeploy/repair service.

## [0.5.6] - 2026-07-05

### Fixed

- **Bus client-name collision could lock a panel out of its message bus.** The
  bridge registered on the panel's message bus under a fixed name, so a connect
  that timed out mid-handshake could leave a stale "ghost" registration that
  rejected every reconnect with `NameInUseError` — the bridge never recovered
  (all bus-derived entities stuck `unavailable`) until the panel's `message_bus`
  or the panel itself was restarted, and it re-formed on the next timeout. Each
  bridge session now uses a unique client name, so a stale ghost can no longer
  block reconnection and the bridge self-recovers on its normal reconnect.

## [0.5.5] - 2026-07-04

### Added

- **Bus-health watchdog.** A rare failure mode — the panel's internal
  Brilliant message bus wedges mid-handshake, and only a reboot clears it, not
  a bridge restart — now self-recovers instead of needing someone to notice
  and reboot the panel by hand. The bridge now stamps a lightweight heartbeat
  every time it successfully reads the bus, and a new, independent watchdog
  daemon reboots the panel once that heartbeat has gone stale for **30
  minutes** — but only while the bridge is still running and the network is
  up (a plain network outage stays the existing Wi-Fi watchdog's job, so the
  two never fight over the same reboot). Guarded against reboot-looping the
  same way the Wi-Fi watchdog is. Opt in per panel with the new **Bus
  watchdog** switch; see
  [CONFIGURATION.md → Bus-health watchdog](docs/CONFIGURATION.md#bus-health-watchdog).

## [0.5.0] - 2026-07-02

### Changed

- **Mesh-load motion now actually works.** The firmware's motion latch on
  BLE-mesh loads never fires (verified live: scores of 255 with a threshold
  of 45 never tripped it), so the bridge now derives the **Motion** sensor
  from the score stream: motion turns **on** when `motion_score` reaches the
  device's **Motion High Threshold** and turns **off** after a configurable
  hold window (`MOTION_DERIVED_HOLD_S`, default 60 s) with no new spikes.
  Validated against mmwave ground truth (~85% episode recall with ~zero
  false triggers in an 11 h pilot). Enabled by default and inert wherever
  **Motion Score Reporting** is off; set `MOTION_DERIVED_ENABLED=0` to
  restore the raw firmware value. **Motion Low Threshold** no longer affects
  the published sensor (it still writes to the device).

### Fixed

- **Motion threshold range**: the **Motion High/Low Threshold** number
  entities were capped at an assumed 0–100; the real scale is 8-bit
  **0–255** (observed live during calibration).

## [0.4.0] - 2026-07-02

### Added

- **Screen wake-on-motion**: **Wake Screen on Motion** and **Sleep Screen
  After Motion Stops** switches, plus a **Screen Off Timeout** number.
- **Screensaver & lock-screen widgets**: **Screensaver** and **Show Time &
  Date** switches, plus four widget toggles — **Weather Widget**, **Music
  Widget**, **Device Status Widget**, **Solar Savings Widget** (disabled by
  default).
- **Touch-slider & intercom controls**: **Touch Sliders** and **Intercom
  Broadcasts** switches, plus a **Slider Double-Tap Timeout** number (disabled
  by default).
- **Audio & governance switches** (all disabled by default — opt in per
  panel): **Speaker Ducking**, **Low Temperature Mode**, **Firmware
  Auto-Update**, and **Remote Assistance**.
- **Wi-Fi watchdog install switch**: a new **Wi-Fi watchdog** switch on each
  panel's device (parity with the voice satellite switch) installs and
  removes the on-panel Wi-Fi watchdog daemon over SSH.

## [0.3.1] - 2026-07-01

### Added

- **Motion settings now stick.** The panel firmware silently reverts the motion
  *enable* flags (mesh motion scoring; faceplate PIR / screen / light motion
  detection) to defaults within minutes, so turning them on from Home Assistant
  never lasted. The agent now remembers the last value you commanded for the
  motion controls and re-asserts any that drift — batched per device,
  rate-limited bus-wide, durable across panel reboots and firmware updates
  (state lives under `/var`). Enabled by default; see the
  [reconciler settings](docs/CONFIGURATION.md#motion-desired-state-reconciler)
  to tune or disable it. Re-asserted values are echoed to Home Assistant
  immediately, so motion switches no longer blip OFF in history each time the
  firmware fights back.

## [0.3.0] - 2026-06-23

### Added

- **On-panel voice satellite (opt-in, per panel).** A Brilliant panel can now run
  a Home Assistant **ESPHome voice satellite** — on-panel wake word, microphone
  capture, and speaker playback — which Home Assistant discovers automatically
  over zeroconf. Speech-to-text, the conversation agent, and text-to-speech all
  run in your HA Assist pipeline, so the panel stays backend-agnostic. Enable it
  per panel during onboarding (a new **Enable voice satellite** toggle, with a
  **Wake word** choice and an optional Home-Assistant-host override for segmented
  networks), or later with the **Voice satellite** switch and **Wake word** select
  on the panel's device. The integration downloads the voice payload from the
  matching GitHub release asset and installs it over SSH under `/var`
  (OTA-persistent); the Repair flow redeploys it if it goes missing. Acoustic echo
  cancellation is bundled but ships **off** — it is needed only for barge-in, and
  tuning it is a follow-up.

## [0.2.4] - 2026-06-21

### Fixed

- **Agent update now shows progress.** The bridge Update entity never declared
  the `PROGRESS` feature, so Home Assistant ignored its in-progress state and the
  install card showed nothing while the agent deployed. It now renders a
  determinate progress bar through the deploy stages (connect → upload payload →
  write config → restart).

## [0.2.3] - 2026-06-21

### Added

- **Faceplate motion-detection controls (bundled on-panel agent → 0.2.0).** The
  agent now exposes the panel faceplate's motion-detection subsystem as
  **disabled-by-default** entities: switches for **Screen Motion Detection**,
  **PIR Score Reporting**, and **Light Motion Detection**, plus **PIR Motion
  High/Low Threshold** numbers. These let you pick a panel's motion source and
  tune its sensitivity from Home Assistant (enable the ones you need under the
  panel's device). `movement_detected` is driven by whichever detection mode is
  enabled; the PIR thresholds take effect once PIR Score Reporting is on.

## [0.2.2] - 2026-06-21

### Fixed

- **Onboarding now installs the on-panel agent.** Adding a not-yet-installed
  panel previously created the Home Assistant entry but never deployed the
  agent — so the panel published nothing and only the management entities
  appeared, leaving the bridge dark until a separate redeploy. The final
  onboarding step now SSH-installs the agent (push payload → write unit/env →
  enable) before the entry is created; on failure the step stays open with a
  `cannot_install` error and no entry is created.
- **The Repair button now lays down the agent code when it's missing.**
  `inspect_panel` reports whether the agent's runnable code is present, and a
  repair (button or auto-repair) deploys the payload before enabling the unit —
  so it can bootstrap or heal a code-less panel instead of enabling a service
  whose program isn't there. An already-installed panel keeps the light path
  (restore unit/env + enable, no re-upload).

## [0.2.1] - 2026-06-21

### Changed

- **Smarter panel onboarding (HA integration)**: the config flow is now
  **detection-first** — step 1 asks only for the panel **IP + root password**,
  connects once (TOFU host-key pin), and **adopts** an already-installed agent by
  reading its config back (name, broker, mesh priority) with no further questions
  and no changes to the panel. A not-yet-installed panel continues to an **MQTT
  broker** step (pre-filled from the most recent panel) and a **Panel Name** step
  (free-form, slugified for MQTT topics, e.g. "Office Bath" → `office-bath`). The
  **Reconfigure** flow is broadened to edit host / root password / broker / mesh
  priority and **push the change to the panel** (re-render env + restart); the
  panel name stays immutable. Reconfigure refuses to write to a host already
  running a **different** panel's agent (guards a mistyped IP from clobbering
  another controller). The repair path is unchanged — it still always regenerates
  the env from entry data and never reads it back.

## [0.2.0] - 2026-06-21

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

[Unreleased]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.5.6...HEAD
[0.5.6]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.5.5...v0.5.6
[0.5.5]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.5.0...v0.5.5
[0.5.0]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.4.0...v0.5.0
[0.4.0]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.3.1...v0.4.0
[0.3.1]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.3.0...v0.3.1
[0.3.0]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.2.4...v0.3.0
[0.2.4]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.2.3...v0.2.4
[0.2.3]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.2.2...v0.2.3
[0.2.2]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.2.1...v0.2.2
[0.2.1]: https://github.com/joyfulhouse/brilliant-mqtt/compare/v0.2.0...v0.2.1
[0.2.0]: https://github.com/joyfulhouse/brilliant-mqtt/releases/tag/v0.2.0
