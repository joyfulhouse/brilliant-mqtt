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

## Finding 2 — panels go deaf to broadcast after a firmware Wi-Fi roam

Live-verified on adu-bath (10.100.0.32) and office-bath (10.100.0.11), the two
panels the server0 HAP watchdog kept rebooting (7× in 48 h): after the BCM43430
firmware (brcmfmac, fw 7.45.98.125, `wl roam_off=0`) roams between APs, the
station keeps receiving unicast + beacons but ZERO broadcast/multicast data
frames (`wl counters rxdfrmmcast` stops; sibling panel on the same BSSID keeps
counting). HA (on-link via end0.52) can then only ARP the panel after the panel
speaks first → HomeKit setup_retry → HAP-watchdog reboot. A host-initiated
`wl reassoc <bssid>` restores reception; `wl roam_off 1` prevents the trigger.
Fleet mitigation applied at runtime on all 14 panels (lost on reboot).

### Task B — `wifi_roam_lock` component (opt-in, default off)

1. `deploy/brilliant-wifi-roam-lock.service` (Type=simple, Restart=always,
   Nice=15, MemoryMax=8M, CPUQuota=2%, After=wpa_supplicant.service
   network.target): `ExecStart=/bin/sh -c 'while :; do /usr/sbin/wl roam_off 1
   >/dev/null 2>&1; sleep 300; done'` (re-asserts every 5 min so a firmware
   reset can't undo it); `ExecStopPost=-/usr/sbin/wl roam_off 0` restores
   roaming on uninstall. Copy it in `scripts/build_payload.sh` into
   `agent_payload/`.
2. `const`: `COMPONENT_WIFI_ROAM_LOCK = "wifi_roam_lock"`,
   `PANEL_WIFI_ROAM_LOCK_DIR = f"{PANEL_VAR_DIR}/wifi_roam_lock"`,
   `PANEL_WIFI_ROAM_LOCK_UNIT_FILE`, `WIFI_ROAM_LOCK_SERVICE_NAME`.
3. `panel_ops`: `WIFI_ROAM_LOCK_INSPECT_COMMAND` (unit/enabled/active/payload
   where payload = staged unit copy under PANEL_WIFI_ROAM_LOCK_DIR),
   `WifiRoamLockState`, `inspect_wifi_roam_lock`, `ensure_wifi_roam_lock_unit`
   (/etc + staged copy + daemon-reload), `enable_wifi_roam_lock`,
   `uninstall_wifi_roam_lock` (disable --now, rm unit + dir, daemon-reload).
4. `components.REGISTRY` row (label "Wi-Fi roam lock", locked=False,
   default_enabled=False); `switch.WifiRoamLockSwitch` registered like the
   others; `diagnostics._SAFE_COMPONENTS`; `panel_inspection._OWNED_SERVICES`.
5. `manager`: `_relay_wifi_roam_lock(shell)` (ensure_unit + enable; no code
   payload) wired into `_relay_selected_components` (repair / refresh / update).
6. `strings.json` + `translations/en.json`: component checkbox label +
   description, switch name, `wifi_roam_lock_failed` exception
   (`test_translation_contract` enforces parity). Config-flow component list
   must pick it up (verify `flow/schemas.py` builds from `components.optional()`).
7. Docs: `docs/CONFIGURATION.md` new "Wi-Fi roam lock" section (what, why,
   the sticky-client trade-off, how to verify with `wl roam_off` /
   `wl counters`), `docs/ha-integration.md` entity table row,
   `docs/TROUBLESHOOTING.md` entry ("HA can't reach a panel until the panel
   talks first / HomeKit setup_retry"), CHANGELOG `Unreleased` → Added.
8. Tests: `ha/tests/test_wifi_roam_lock_component.py` (panel_ops recipes +
   registry row + present/install/remove), manager relay tests (repair /
   refresh / update: relayed when selected, untouched when not), switch test.

## Release

Both agent and integration → **0.10.1** (agent code unchanged; the version bump
is what makes every panel's Update entity offer the install that converges the
watchdog). Gates: root + `ha/` both green; payload rebuilt; parity test updated.
