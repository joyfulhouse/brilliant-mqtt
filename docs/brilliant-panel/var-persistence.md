# `/var` persistence map

## Why `/var` matters

The firmware deployments under `/data` are replaced as a unit by OSTree, while `/var` survives deployment changes. This makes `/var` both the correct home for community services and the source of several lifecycle hazards: credentials persist, hosted peripherals can outlive the process that created them, and an OTA does not automatically remove stale community payloads.

The captured pilot `/var` contains 16,938 regular files and 2,066 symlinks. It is retained only in the ignored artifact corpus. This document records structure, counts, formats, and integration consequences; it intentionally does not record values, home/device identifiers, network names, keys, tokens, cached images, or account metadata.

## Footprint and provenance

The top-level footprint separates native panel state from community-installed software:

| Path | Approximate size | Provenance / purpose |
|---|---:|---|
| `/var/brilliant-voice` | 210 MB | Community HA voice satellite, bundled Python/runtime libraries, audio processing |
| `/var/spike` | 82 MB | Community voice/wake-word model and Python payload |
| `/var/lva-py311.tar.gz` | 37 MB | Community deployment/staging archive |
| `/var/np.tgz` | 18 MB | Community deployment/staging archive |
| `/var/brilliant` | 12 MB | Native Brilliant persistent state, history, object cache, and HomeKit data |
| `/var/lva` | 6.1 MB | Community local voice assistant payload |
| `/var/stage.tgz`, `/var/wakestage.tgz` | 5.2/4.9 MB | Community deployment/staging archives |
| `/var/brilliant-mqtt` | 2.0 MB | Community MQTT bridge payload and state |
| `/var/lib` | 472 KB | Native operating-system/service state |
| `/var/device_variables` | 36 KB | Native device identity, PKI, SSH, and persistent variables |

Therefore the archive size is not a good measure of the vendor firmware's mutable state. Most bytes are local extensions and their self-contained runtimes. The vendor-specific `/var/brilliant`, `/var/device_variables`, and selected `/var/lib` trees are small but much more sensitive.

## Native Brilliant state

### `/var/brilliant/object_store`

The object store contains 65 files totaling approximately 10.9 MB:

- a `disk_cache/cache_data` index;
- 32 content-addressed cache entries;
- 32 JSON metadata records;
- cached payloads identified as JPEG images in this capture.

The two-level prefix directories and opaque content identifiers indicate a content-addressed cache rather than a human-authored configuration tree. The metadata can associate cached objects with remote service state. Neither cache payloads nor metadata belong in diagnostics, Git, MQTT attributes, or issue reports.

**Integration consequence:** do not use the object store as an API. Use the message-bus object-store peripheral and its typed variables where required. Directly editing cache files risks index inconsistency and couples the integration to an implementation detail.

### Device history

One device-scoped `.history` directory contained one approximately 1.24 MB opaque/long-record file. This is evidence of retained historical state, but its presence alone does not establish a supported replay or audit interface.

**Integration consequence:** the bridge should publish state from the live bus mirror and retained MQTT state, not scrape history files. Any future decoder must first establish record framing and privacy rules on a disposable copy.

### HomeKit state

The native tree contains a HomeKit token, setup identifier, and a small HomeKit state directory. These are pairing credentials/state, not general device identifiers.

**Integration consequence:** preserve this tree during bridge, voice, and HA-mirror installation or removal. Never copy its values into logs, diagnostics, backups intended for community sharing, or HA entities. HomeKit can remain an independent fallback while HA behavior is validated.

### Samples and flags

The capture contains `alexa_request_samples`, `alexa_response_samples`, and `wakeword_samples` directories; they were empty in this snapshot. `device-flags` is a small native configuration file.

**Integration consequence:** empty sample directories do not prove voice content is never persisted. Collection and support tooling should exclude these paths categorically rather than relying on current emptiness.

## Operating-system persistent state

| Path | Captured shape | Sensitivity / relevance |
|---|---|---|
| `/var/device_variables/pki` | Certificate, CSR, and private key | Device identity; private key must never leave the ignored corpus |
| `/var/lib/softhsm` | Token objects, locks, and generation state | Hardware-emulated credential store used by native services |
| `/var/lib/connman` | Seven files, about 30 KB | Wi-Fi/network profiles and service history; may contain network credentials or SSIDs |
| `/var/lib/bluetooth` | 29 small files | Adapter and paired-device cache; reveals hardware identifiers/topology |
| `/var/lib/update_manager` | Five small state files | OTA/update-manager state across reboots |
| `/var/volatile` | Runtime symlink target | Volatile logs/tmp, not durable configuration |

`/var/log` had no files in the archive. Runtime journals observed during live validation are not represented by this persisted capture, so the corpus is not a complete historical log source.

## Community-service state

Community services correctly install under `/var`, outside the signed OSTree deployment:

```text
/var/brilliant-mqtt/     MQTT bridge payload, watchdog, state, and vendored dependencies
/var/brilliant-voice/    HA voice satellite runtime and audio dependencies
/var/lva/                local voice assistant runtime
/var/spike/              voice/wake-word models and libraries
```

This layout allows an OTA to replace `/data/switch-embedded` and `/data/switch-ui` without deleting the integrations. It also means service installers must explicitly manage versioning, migration, rollback, and uninstall. Staging archives at `/var` root are community deployment artifacts rather than native Brilliant files and can consume significant storage if never pruned.

**Integration consequences:**

1. Keep executable payloads and writable state in separate subdirectories so upgrades can be atomic without erasing state.
2. Treat the current `/data/switch-embedded/env/bin/python3` as a firmware dependency and revalidate imports after every OTA.
3. Use resource-capped systemd units; persistence does not imply spare CPU or RAM.
4. Remove obsolete staging archives only through an explicit installer-owned cleanup policy.
5. Never infer that an unfamiliar `/var` directory is vendor-owned solely from its name.

## Hosted-peripheral persistence

Live testing showed that peripherals hosted on a Control's own device can remain in persistent object-store state after the host process exits and across reboot. This explains why a crashed or replaced HA-mirror leader can leave native-UI phantoms.

Required lifecycle behavior:

- use deterministic peripheral IDs;
- explicitly delete no-longer-hosted peripherals with a valid timestamp;
- perform leader handoff cleanup before the replacement host publishes duplicates;
- reconcile desired versus persisted peripherals at startup;
- test process stop, reboot, leader loss, entity removal, and rename independently;
- retain a manual cleanup path that does not require deleting the entire object store.

Do not solve stale peripherals by clearing `/var/brilliant/object_store`; that store is shared native state, not an HA-mirror database.

## Collection and disclosure policy

The full `/var` corpus is useful for local interoperability analysis but is never a normal support attachment. A shareable diagnostic should be allowlisted and generated from live typed APIs. At minimum it must exclude:

- `/var/device_variables`, ConnMan, SoftHSM, Bluetooth cache, and HomeKit state;
- object-store payloads/metadata and all `.history` content;
- voice/request/wake-word samples, even when currently empty;
- bridge/voice configuration or state fields that may contain broker/API credentials;
- journals or logs until values and identifiers are redacted.

For reproducibility, identify the ignored corpus by the archive hash in [Acquisition and evidence](acquisition.md), not by publishing any file from it.
