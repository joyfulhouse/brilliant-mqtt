# Acquisition and evidence

## Representative build

All 14 SSH-enabled panels were queried read-only on 2026-07-11. Each reported:

- architecture: ARMv7l;
- firmware: `v26.06.03.1`;
- active OSTree commit: `2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b`;
- rollback: `v26.05.20.2`.

The fifteenth panel, sunroom, has a failed display and cannot enable the on-screen Root SSH option. It is the only acquisition coverage gap. Because every reachable panel uses the same signed OSTree commit, one software corpus from the pilot is representative of the fleet. Per-panel `/var` state is not assumed identical.

## Collected paths

The pilot archive contains:

| Path | Approximate size | Role |
|---|---:|---|
| `/data/switch-ui` | 24 MB | Monolithic native Qt/QML UI ELF and release marker |
| `/data/switch-embedded` | 242 MB | Python 3.10 runtime, first-party Cython services, generated Thrift types, configuration, and third-party dependencies |
| `/var` | 378 MB | Persistent service state, object-store cache, histories, Brilliant MQTT/voice deployments, caches, and runtime configuration |
| Selected `/etc/systemd/system` files | small | Launch topology for message bus, UI, and updater |
| `/etc/default/update_manager` | small | OTA branch configuration |
| `/usr/sbin/update_manager` | small | Brilliant OTA manager executable |

The acquisition is a regular-file/symlink tar stream produced by BusyBox tar and compressed with local Zstandard. BusyBox tar preserves mode, ownership identifiers, timestamps, and symlinks, but does not support the GNU `--xattrs` or `--acls` options. Extended attributes and ACLs are therefore outside this corpus.

## Local layout

```text
artifacts/brilliant-panel/v26.06.03.1/
├── raw/          # immutable compressed capture + checksum
├── extracted/    # working tree
└── analysis/     # strings, Ghidra project, generated inventories, images
```

The entire `artifacts/brilliant-panel/` subtree is gitignored. Verify before every acquisition:

```bash
git check-ignore -v artifacts/brilliant-panel/v26.06.03.1/raw/pilot-corpus.tar.zst
```

## Reproduce the capture

The tracked helper never accepts a password on the command line and refuses to write unless its destination is ignored:

```bash
SSHPASS='<panel-root-password>' \
  scripts/brilliant-panel/acquire.sh v26.06.03.1 <pilot-ip>
```

Preview the path set without connecting:

```bash
scripts/brilliant-panel/acquire.sh --dry-run v26.06.03.1 <pilot-ip>
```

In this environment, per-panel passwords are decrypted from SOPS host vars only for the lifetime of the SSH process. They are not written to the artifact tree.

## Integrity record

Record these after the archive finishes:

```bash
zstd -t artifacts/brilliant-panel/v26.06.03.1/raw/pilot-corpus.tar.zst
sha256sum artifacts/brilliant-panel/v26.06.03.1/raw/pilot-corpus.tar.zst
sha256sum artifacts/brilliant-panel/v26.06.03.1/extracted/data/switch-ui/switch-ui
```

The UI ELF is independently identified as:

| Property | Value |
|---|---|
| Format | ELF32, little-endian, ARM EABI5, PIE, dynamically linked |
| Build ID | `dc5f6341437fe008266dc49fcac16d517e41f6c3` |
| SHA-256 | `ddf6c9cd226325d8a3db30e407ca48fe263c6447007ba3926bbd09686acbb58f` |
| Size | 25,070,892 bytes |
| UI source revision marker | `d4f0252cc5d35fba91b9ecdd9b6e1729a00bef9d` |
| Embedded-stack source revision marker | `2332ced103d755d48d2302b592f95f8e7b6c66f5` |
| Symbols | stripped; `.gnu_debuglink` names `switch-ui.debug`, not installed beside the executable |

The complete archive is identified as:

| Property | Value |
|---|---|
| Archive SHA-256 | `53126220e9c161df70e13374b599270ee1c4bd4d2ce69c5b5360409f41dfc1e6` |
| Decompressed tar size | 655,026,176 bytes |
| Tar entries | 29,487 |

The ignored archive checksum is also stored next to the archive. A hash authenticates the local capture but does not grant redistribution rights.

## Analysis tools and outputs

| Tool | Result |
|---|---|
| `file`, `rabin2`, `strings` | ELF properties, sections, imports, Qt types, endpoints, source paths, embedded QML, and resource paths |
| Ghidra 12.1.2 | Successful ARM analysis; 15,210 functions recognized in the stripped UI ELF |
| Ghidra embedded-media analyzer | 426 PNG resources extracted locally |
| ImageMagick | Ignored contact sheet used to classify the visual system; no artwork is tracked |
| Generated Thrift Python | 106 `PeripheralType` values and 96 named peripheral-interface packages recoverable without decompilation |
| Existing bus snapshots | Current-build graph of 36 devices; pilot has 25 own peripherals, `ble_mesh` has 40 |

## `/var` sensitivity rules

The `/var` archive is explicitly authorized for analysis and must still be treated as sensitive. It can contain:

- device and home identifiers;
- Wi-Fi and partner-integration state;
- device certificates, PKCS#11/SoftHSM material, and HomeKit state;
- object-store blobs and cached art;
- user/account metadata and room/device names;
- journals, histories, request samples, voice-related state, and crash dumps;
- locally deployed HA/MQTT credentials in adjacent configuration paths.

Before deriving a tracked file from `/var`:

1. Search it for tokens, passwords, keys, authorization headers, emails, SSIDs, IPs, device IDs, home IDs, and certificate material.
2. Replace dynamic identifiers with semantic placeholders.
3. Prefer counts and schema names to raw values.
4. Never copy databases, logs, media, certificates, or raw object-store blobs into `docs/`.
5. Check `git diff --cached` and `git status --ignored` before commit.

See the sanitized [`/var` persistence map](var-persistence.md) for the captured layout, provenance split, and integration consequences. The persisted capture contained no `/var/log` files; live journal observations are separate evidence.

## Confidence limitations

- Static presence does not prove a feature is enabled, reachable, or cloud-independent.
- A generated interface describes the shape a peripheral may have, not which variables are externally settable on a specific instance.
- The UI includes dormant, demo, factory, partner, feature-flagged, and unsupported screens.
- The 2026-07-06 bus snapshot is from the same firmware and provides current-build structure, but a live value can change at any moment.
- No destructive firmware modification or broad command fuzzing is part of this analysis.
