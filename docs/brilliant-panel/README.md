# Brilliant Control panel internals

This directory documents the software and interaction architecture of the Brilliant Control in-wall panel, with an emphasis on local Home Assistant support. The primary evidence is firmware `v26.06.03.1`, OSTree commit `2174d3882504c03bf9c7b3f78f0cad4a5ae039f7a32f0bb9c5eca02dc0370b0b`, acquired from the designated pilot on 2026-07-11.

The central finding is that the panel is not a collection of isolated integrations. It is a typed, home-wide device graph joined by an Apache Thrift message bus. The native Qt UI, Brilliant's HomeKit service, BLE mesh, automation engine, and every cloud connector consume the same graph. That makes the message bus the correct local extension point for Home Assistant.

## Read this first

| Document | Purpose |
|---|---|
| [Acquisition and evidence](acquisition.md) | Corpus scope, hashes, fleet coverage, privacy controls, and reproducibility |
| [`/var` persistence map](var-persistence.md) | Native persistent state, community payloads, sensitive stores, and integration consequences |
| [Software architecture](software-architecture.md) | OSTree, systemd, uWSGI services, message bus, device graph, and persistence |
| [UI/UX information architecture](ui-information-architecture.md) | Home navigation, settings hierarchy, screen families, visual language, and interaction patterns |
| [Peripheral and control surfaces](peripheral-surfaces.md) | Firmware type system, high-value variables, command paths, scenes, media, and sensors |
| [Peripheral type catalog](peripheral-type-catalog.md) | Complete firmware `PeripheralType` enum for `v26.06.03.1` |
| [Cloud and local boundaries](cloud-boundaries.md) | What remains local, what depends on Brilliant or partner clouds, and replacement strategies |
| [Home Assistant support matrix](home-assistant-support-matrix.md) | Implemented, partially implemented, missing, inappropriate, and recommended capabilities |
| [Validation runbook](validation-runbook.md) | Safe static, read-only, telemetry, and write-validation procedures |

Existing low-level references remain authoritative for the already-proven Python bus client contract:

- [PoC findings](../reference/poc-findings.md)
- [Message-bus API](../reference/message-bus-api.md)
- [Bridge architecture](../ARCHITECTURE.md)
- [HA mirror](../ha-mirror.md)

## Evidence labels

The documents use these labels so static capability is not confused with working hardware:

| Label | Meaning |
|---|---|
| **Live** | Observed on a running panel or validated through an existing controlled probe |
| **Schema** | Present in generated Thrift types or an installed module, but not necessarily instantiated |
| **UI** | Present in the native UI ELF, embedded QML, resources, or Qt meta-object registration |
| **Inference** | A conclusion drawn from multiple artifacts that still needs a live probe |
| **Unsupported here** | Firmware can represent it, but no native instance exists in this 15-panel installation |

## High-value conclusions

1. **Local loads and panel settings are already on the right path.** `brilliant-mqtt` reads and writes the same variables as the native UI and HomeKit vassal.
2. **The panel UI is broader than the live hardware graph.** The firmware contains first-class UI and schemas for shades, climate, locks, garages, cameras, security, valves, music, and energy. Most are partner-backed virtual peripherals in this home, not panel hardware.
3. **Rooms, scenes, modes, groups, and shortcuts are first-class IA concepts.** These are the most important remaining cross-system semantics for making HA the central hub.
4. **The reverse HA mirror is architecturally native.** Hosting HA entities as Brilliant peripherals uses the same pattern as Hue, LIFX, SmartThings, Ring, Sonos, and other bundled adapters.
5. **Physical slider and gesture bindings are configuration objects, not ordinary state variables.** They can target lights, groups, scenes, or modes. The lack of a press-event variable explains why simple bus observation cannot expose every gesture as an HA event.
6. **Camera/intercom is a media subsystem, not a boolean camera entity.** It combines raw camera hardware, GStreamer, WebRTC/SDP session state, RTSP, remote-media peripherals, and privacy gates. It should be isolated from the core bridge.
7. **Cloud removal is capability-specific.** Wired loads, mesh loads, panel controls, MQTT, and HA mirror operation are local. Partner account linking, some media relay paths, weather/art catalogs, Alexa, and OTA discovery remain cloud-backed unless separately replaced.

## Corpus handling

Raw panel files, `/var`, logs, object-store data, decompiler projects, and extracted media are under `artifacts/brilliant-panel/`, which is gitignored. They may contain credentials, account state, identifiers, home topology, and personal content. Only sanitized findings and cryptographic identifiers are tracked.

The proprietary firmware is analyzed for interoperability. This repository does not redistribute the raw Brilliant binaries or extracted artwork.
