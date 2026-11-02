# Brilliant i350 / aarch64 panels

**Status: not supported.** brilliant-mqtt is qualified only on the original
armv7l (NXP i.MX6) Brilliant Control. The Home Assistant integration refuses
any other architecture with `unsupported_architecture` ("This panel
architecture is not supported."). The gate is deliberate. Removing it would
not show that the bridge works on an i350. It would only stop the integration
from checking. Tracking issue:
[#155](https://github.com/joyfulhouse/brilliant-mqtt/issues/155).

The newer i350 panels report `aarch64` from `uname -m` and run a MediaTek
kernel (`5.15.x-mtk`) with Python 3.10.x. The panel Python version is
compatible. Everything else listed below is unverified.

## What is and isn't architecture-bound

| Part | Architecture dependency | i350 status |
|---|---|---|
| Bridge agent payload (`brilliant_mqtt`, vendored `aiomqtt`/`paho-mqtt`) | None. It is pure Python with no native code. | Should load. It still depends on Brilliant's message-bus library on the panel. |
| Brilliant message-bus library (`lib.message_bus_api`, compiled Cython `.so`) | Ships with the firmware and is built for the panel's own architecture | Unknown. Whether it imports, and whether its schema matches what the bridge maps, must be measured (gates 1 and 3). |
| Wi-Fi watchdog last-resort reset (GPIO 2/5 + `sdhci-esdhc-imx` unbind) | i.MX6 only. It copies Brilliant's i.MX6 reset sequence. | Skipped with a logged warning when the i.MX sdhci driver is absent. The guarded reboot still runs. The soft reconnect and service-restart rungs need `connmanctl` and `systemctl`. |
| Voice satellite payload | armv7l only (prebuilt armv7hf Python, wheels and libraries) | Refused before anything on the panel is touched. |
| Hue CA recovery (`brilliant_hue_ca`) | Hardcoded `python3.10` site-packages path and Hue peripheral paths | Unverified. The diagnostic reports whether each path is present. |

## Run the read-only diagnostic

[`scripts/brilliant-panel/panel_diagnostics.sh`](../../scripts/brilliant-panel/panel_diagnostics.sh)
is a POSIX `sh` script that runs read-only on the panel. It writes nothing,
starts or stops nothing, never connects to the message bus, and never reads
credentials, keys, network identity, Wi-Fi configuration or certificate
contents. Run it from a workstation checkout of this repo:

```bash
ssh -o NumberOfPasswordPrompts=1 root@<panel-ip> 'sh -s' \
  < scripts/brilliant-panel/panel_diagnostics.sh > i350-diagnostics.txt
```

Review `i350-diagnostics.txt` before you share it. It contains one header line
followed by `key=value` lines:

| Keys | What they show |
|---|---|
| `timeout_present`, `timeout_version`, `timeout_mode` | Which `timeout` flavour bounds the probes (`plain`, `busybox_t` or `unbounded`) |
| `machine`, `kernel_release` | `uname -m` / `uname -r` |
| `panel_python_present`, `panel_python_version` | The firmware Python at `/data/switch-embedded/env/bin/python3` |
| `release_tag` | The firmware `release_tag` from `/etc/release_info.json` |
| `site_packages_dir` | The firmware site-packages directory, or `missing` |
| `bus_library_import` | Whether `RPCObserver` imports (`ok`, `failed: …`, `failed: timed out`). The library is imported only (isolated `python3 -I -B`), never connected. The import is bounded at 20 s unless `timeout_mode=unbounded`. |
| `bus_library_so_count`, `bus_library_elf_sample`, `bus_library_elf_class`, `bus_library_elf_machine`, `bus_library_elf_arch` | How many native `.so` files are under the bus library's `lib/` package (`unknown` without `find`), and the ELF class/machine of one sampled `.so` (`2`/`183` = AArch64, `1`/`40` = 32-bit ARM). The sample is the first `lib/message_bus_api/*.so`, else the first `.so` under `lib/`; its path is reported relative to site-packages. Without `od` the ELF fields are `unknown`. |
| `bus_socket_present` | Whether `/var/run/brilliant/server_socket` exists |
| `message_bus_enabled`, `message_bus_active` | `systemctl is-enabled` / `is-active message_bus.service` |
| `var_mount_*`, `data_mount_*`, `var_free_kib`, `data_free_kib` | Filesystem type and options of the mount entries for `/var` and `/data` (the last entry for each path in `/proc/mounts`), and the free space on each |
| `wlan0_present`, `iw_present`, `connmanctl_present` | Wi-Fi interface and tool presence. The tools are looked up only, never run. |
| `mmc_drivers`, `imx_sdhci_driver_present` | The SD/MMC host drivers registered on the panel, and whether the i.MX driver that the watchdog reset targets is present |
| `mem_total_kib`, `cpu_count` | Memory and CPU count |
| `hue_site_packages_present`, `hue_peripherals_ini_present`, `hue_ca_bundle_present` | Whether the Hue CA recovery paths exist (presence only) |

## Pre-release check on the existing fleet

The Wi-Fi watchdog now skips its GPIO/usdhc reset when
`/sys/bus/platform/drivers/sdhci-esdhc-imx` is absent. Before releasing that
change, run the diagnostic once on the designated armv7 pilot and confirm it
reports `imx_sdhci_driver_present=1`. That confirms the guard keeps the reset
on the existing fleet. If it reports `0`, stop: the guard would silently
downgrade the watchdog on qualified panels.

## Qualification gates

aarch64 is added to the supported set only after all five gates pass, in
order, on a designated i350 pilot panel:

1. **Diagnostic passes.** The diagnostic reports `bus_library_import=ok`, an
   AArch64 bus library, a present bus socket, an enabled and active
   `message_bus.service`, an rw mount reported for `/var` with enough free space, and a
   `release_tag` that matches the integration's firmware pattern
   (`_FIRMWARE_VERSION` in `custom_components/brilliant_mqtt/panel_inspection.py`,
   currently `^v[0-9]+(?:\.[0-9]+){3}$`).
2. **`/var` survives OTA.** A file placed in `/var` is still there after a
   firmware update on the pilot. The bridge, its units and its rollback
   baselines all live in `/var`. See [`/var` persistence](var-persistence.md).
3. **The bus schema matches.** A read-only bus-schema dump (device and variable
   names, types and ranges) matches what the bridge maps. See
   [PoC findings](../reference/poc-findings.md) and the
   [message-bus API](../reference/message-bus-api.md).
4. **Designated-panel pilot.** On the pilot, verify discovery, telemetry,
   commands, LWT, the stale-stream watchdog, the systemd resource caps, the
   Wi-Fi and bus watchdogs, and rollback. Have the rollback paths ready before
   you start:
   [INSTALL.md → Uninstall / rollback](../../INSTALL.md#uninstall--rollback),
   [deployment → Rollback](../reference/deployment.md#rollback) and
   [canary rollback](../canary-rollback.md).
5. **Only then add `aarch64`** to `_SUPPORTED_ARCHITECTURES`, with tests and a
   CHANGELOG entry that names the qualified hardware and firmware.
