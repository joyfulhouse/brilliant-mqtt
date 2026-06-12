# Troubleshooting

Common problems with brilliant-mqtt and how to resolve them.

> **Status:** validated against a live pilot panel (2026-06-12).

## Common Issues

### Entities never appear in Home Assistant

- **Silent ACL denial** is the most likely cause: Mosquitto drops denied
  publishes without any error. Verify the `brilliant` user's ACL grants
  `homeassistant/#` (write) and `brilliant/#` (rw).
- Confirm broker credentials in `/etc/brilliant-mqtt.env` and that the panel
  VLAN is allowed to reach the broker (`<broker>:1883`).
- Check the agent is running: `systemctl status brilliant-mqtt` on the panel.

### Entities show as unavailable / panel `offline`

- The availability topic is an LWT — `offline` means the agent died or lost the
  broker. systemd restarts it (`Restart=always`); check
  `journalctl -u brilliant-mqtt` for the crash cause.
- If the agent is up but entities stay unavailable, the broker may have dropped
  the retained `online` publish — restart the unit to re-reconcile.

### Commands from HA don't drive the load

- Check the startup log line `reconcile: N devices -> M entities, K command
  topics subscribed` (`journalctl -u brilliant-mqtt`). If it is missing, the
  initial reconcile did not complete and no command subscriptions exist; the
  periodic resync (default 5 min) re-runs reconcile and self-heals.
- Verify the ACL allows the `brilliant` user to **read** `brilliant/#`
  (subscriptions to `<...>/set` are silently denied otherwise).
- Run with `LOG_LEVEL=DEBUG` and watch the full trace: mqtt receipt →
  translated command → `SetVariableResponse` from the bus.
- If the light turns on/off by itself right after a command, suspect the
  panel's own motion logic or an HA automation (e.g. Adaptive Lighting)
  re-asserting state — not the bridge.

### Sensors lag or freeze (power/motion/occupancy stale for minutes)

- The panel lib's notification stream can **die silently** while the process
  keeps running — pushes stop and even the bridge's bus reads serve a frozen
  in-process mirror (live pilot finding, 2026-06-12). Three mitigations are
  built in: a fast scoped poll (`HOT_POLL_SECONDS`, default 2 s) that bounds
  staleness, a reconnect hook that re-reconciles after bus gaps (look for
  `bus processor reconnected` warnings in the journal), and a watchdog that
  rebuilds the session when no push arrives for `BUS_STALE_SECONDS`.
- If sensors still lag: confirm the deployed code includes the hot poll
  (`journalctl` shows the version at deploy; `HOT_POLL_SECONDS=0` disables
  it), then run with `LOG_LEVEL=DEBUG` and watch for `state publish` lines —
  a healthy bridge emits one within a poll interval of any bus change.
- A motion entity that never triggers may simply be a PIR that never sees
  anyone (verify by watching `movement_detected` on the bus); illuminance
  reads 0 while `enable_lux=0` on the panel (the default everywhere).

### Bridge broken after a panel firmware update

- Firmware OTA replaces `/data/switch-embedded` — including the closed-source
  bus libraries the bridge imports. If their API drifted, the bridge can fail
  at startup. Re-run the read-only PoC smoke checks and re-validate before
  letting the rest of the fleet update — see
  [reference/deployment.md](reference/deployment.md).

### Panel UI feels sluggish

- The unit is resource-capped (`MemoryMax`, `CPUQuota`, `Nice`) precisely so
  the bridge cannot degrade the touchscreen. If caps were edited, restore them;
  check `systemctl show brilliant-mqtt | grep -E 'Memory|CPU'`.

### Removing a panel's entities from HA

- Discovery topics are retained: publish an **empty retained payload** to each
  `homeassistant/<component>/<unique_id>/config` topic, then stop the unit.

## Enabling Debug Logging

Set `LOG_LEVEL=DEBUG` in `/etc/brilliant-mqtt.env`, then:

```bash
systemctl restart brilliant-mqtt
journalctl -u brilliant-mqtt -f
```

## Getting Help

Open an issue at <https://github.com/joyfulhouse/brilliant-mqtt/issues> with
logs and reproduction steps.
