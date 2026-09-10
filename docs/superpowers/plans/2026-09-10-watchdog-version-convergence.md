# 2026-09-10 — Watchdog version convergence + Wi-Fi roam lock

Two fleet findings from the 0.10.0 post-rollout check, both fixed in 0.10.1.

## Finding 1 — Update-entity installs leave the bus watchdog stale

`manager.async_update_agent` (the per-panel **Update** entity) ships app+vendor
only. The bus/Wi-Fi watchdog relays (`_relay_watchdog`) run only from
`async_repair` and `_refresh_staged_copies`, and even there they redeploy the
watchdog code ONLY when `payload_present` is False (run.py missing). Result after
the 0.10.0 rollout: 13/14 panels run bridge 0.10.0 with bus-watchdog 0.9.x
(no `bounded.py` / `phase_record.py`). Fail-safe, but violates the 0.10.0
"upgrade bridge + bus watchdog together" contract.

### Task A — release-version convergence for the watchdogs

1. `panel_ops`: `deploy_wifi_watchdog(shell, local_dir, version)` and
   `deploy_bus_watchdog(shell, local_dir, version)` write `{DIR}/VERSION` (the
   bundled payload version, `agent_payload/VERSION`) AFTER the swap. Inspect
   commands append `cat {DIR}/VERSION 2>/dev/null || true`; the parsers expose
   `version: str | None` on `WifiWatchdogState` / `BusWatchdogState` (last
   non-`key=value` line, stripped; None when absent). Add
   `restart_wifi_watchdog` / `restart_bus_watchdog` (`systemctl restart <svc>`).
2. `manager`: `_WatchdogRelaySpec` gains `restart`; `_PayloadState` gains
   `version`. `_relay_watchdog(shell, spec, version)` redeploys when
   `not state.payload_present or state.version != version`, then ensure_unit +
   enable, and **restarts the service only when it redeployed** (enable --now
   does not reload running code). No redeploy → ensure_unit + enable as today.
3. `manager`: new `_relay_selected_components(shell, *, context)` consolidating
   the wifi/bus/hue-ca relay blocks from `async_repair` and
   `_refresh_staged_copies` (same warnings, same swallow semantics). Call it
   from BOTH existing sites AND from `async_update_agent` after
   `ensure_configs` and before `panel_ops.restart` (progress 85). Relay failures
   never fail the update.
4. `components._install_watchdog` passes the bundled version to `deploy`.
5. Tests (`ha/tests/test_panel_ops.py`, `ha/tests/test_manager.py`): inspect
   parses/omits version; deploy writes VERSION last; relay redeploys + restarts
   on version skew and on None (legacy install without marker); no redeploy when
   versions match (update existing relay tests' inspect stubs to carry the
   fixture version `0.2.0`); update path relays selected watchdogs and skips
   unselected ones; update still succeeds when the relay fails.
6. Docs: `docs/ha-integration.md` (Update entity now converges watchdogs),
   `docs/reference/deployment.md` (VERSION marker), CHANGELOG `Unreleased` →
   Fixed.

## Finding 2 — panels deaf to broadcast: U6-LR access points (RESOLVED, no code)

Initial read (a firmware-roam bug on the BCM43430) was WRONG. Live-verified
2026-09-10 14:00–14:45 UTC with host-level `tcpdump` on all 14 panels + the
chip's `wl counters`: every panel associated to one of the three UniFi
**U6-LR** APs (1st_Floor, 2nd_Floor, ADU; fw 6.7.57.15670, ~15 d uptime)
received ZERO inbound broadcast/multicast frames at the host, while panels on
the U6-M-Pro / U6-IW / U6-Lite / U7-Pro APs received hundreds per 10 s. The
station firmware still counted the frames (`rxdfrmmcast`), with zero
CCMP/replay/undecrypt errors, so the AP was emitting bc/mc that the station
could not use. A soft restart of each U6-LR (UniFi `cmd/devmgr restart`)
restored delivery for all its panels immediately (ADU: 4/4 panels ~210
frames/10 s; 1st/2nd floor likewise; office→bath broadcast ARP `REACHABLE`).

Why it looked like a panel problem: HA's end0.52 leg must broadcast-ARP a panel
after any HomeKit disconnect; a deaf panel never answers, so the entry stays
`FAILED` until the panel itself sends an ARP request HA can snoop. Panels whose
HA neighbor entry had been garbage-collected (the two bath panels, later the
entryway) stayed unreachable → HomeKit `setup_retry` → server0 HAP-watchdog
reboot every 6 h. `wl roam_off 1` was applied fleet-wide as a mitigation and
**reverted** (roam_off=0 everywhere) once the AP cause was proven.

Follow-ups (operator, outside this repo): the U6-LRs report no newer firmware
in the controller; if the fault recurs, schedule a periodic soft restart of
the three U6-LRs (homelab ansible/cron via the same API call) or enable
`proxy_arp` on the `joyful.house.iot` WLAN as an ARP-only safety net.

**Task B (Wi-Fi roam lock component) is DROPPED** — it does not address the
cause and would make the panels sticky clients.

## Release

Both agent and integration → **0.10.1** (agent code unchanged; the version bump
is what makes every panel's Update entity offer the install that converges the
watchdog). Gates: root + `ha/` both green; payload rebuilt; parity test updated.
