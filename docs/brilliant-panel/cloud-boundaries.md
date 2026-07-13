# Cloud and local boundaries

The panel is local-first at the hardware/control layer but cloud-backed at the account, catalog, partner, and fleet-management layers. “Runs on the panel” does not automatically mean “works indefinitely without cloud”; the distinction depends on where authoritative configuration and credentials live.

## Capability boundary

| Capability | Local runtime path | Cloud dependency | Independence assessment |
|---|---|---|---|
| Wired light/switch control | Gangbox peripheral → local bus → UART/load | None for routine control | Strong local candidate; implemented |
| Wired power/temp/fault | Gangbox telemetry → bus | None | Strong local candidate; implemented |
| Brilliant BLE mesh control | `ble_mesh` virtual device, BLE proxy/topology, panel-to-panel graph | Provisioning/firmware may use Brilliant services | Routine control is local; implemented with elected publisher |
| Faceplate motion/lux | Faceplate peripheral | None | Local; implemented |
| Screen/audio/settings | Hardware/UI/config peripherals | None for routine changes | Local; implemented subset |
| Rooms/groups/scenes/modes | Configuration virtual device + execution engine | Configuration may synchronize through Brilliant services/mobile app | Existing scene/mode execution uses a local bridge; Office hardware acceptance remains pending |
| HomeKit | On-panel HAP vassal over LAN | None after pairing | Local fallback; failures observed in service lifecycle, not cloud transport |
| MQTT bridge | On-panel bus client → local broker | None | Local community path |
| HA scene/mode bridge | Existing panel bus/MQTT session → local HA | None beyond HA/broker availability for cached execution | Supported non-hosting reverse path; pending Office hardware acceptance |
| Native HA tiles | Physical hosting rejected; Virtual Control unproven | Virtual Control provisioning/runtime may require Brilliant cloud | Blocked behind explicit feasibility and WAN-isolation gates |
| Native Alexa | Local wake word/audio plus Amazon OAuth/AVS | Amazon and Brilliant token exchange | Cloud-dependent assistant |
| Google Assistant linking | UI/account linking and external assistant | Google/Brilliant account services | Cloud-dependent |
| Partner integrations | On-panel adapters plus partner APIs/tokens | Usually partner and/or Brilliant cloud | Prefer native HA integrations |
| Weather | Weather peripheral and lock-screen widget | Data source likely cloud | Cache may survive temporarily; not independent |
| Art libraries | Local object-store cache and art configuration | Catalog/custom-art acquisition likely cloud | Cached display local; catalog lifecycle cloud-backed |
| Solar estimates/savings | Local schema/UI and configuration | Weather/model/account inputs may be cloud-derived | Partially local; source validation required |
| Intercom inside home | Local media stack, remote bridge, RTSP/WebRTC | Remote monitoring/relay may use cloud | Local panel-to-panel path likely viable; prove signaling and TURN behavior |
| Remote video access | Camera/media session stack | Account authorization and relay likely cloud | Do not claim independence without a network-isolation test |
| OTA | `update_manager` → Brilliant OSTree remote | Brilliant update service | Local mirror exists; panels are not yet universally pinned to it |
| Mobile onboarding/home membership | Bootstrap, auth, property/user configuration | Brilliant cloud | Cloud-dependent provisioning path |
| Vendor remote assistance | reverse SSH / support flow | Brilliant support infrastructure | Cloud/vendor dependent and security-sensitive |

## Strong local core

The following path does not require partner or Brilliant web APIs during normal operation:

```text
physical touch / PIR / gang UART / BLE mesh
                    ↓
          local Brilliant message bus
             ↙                 ↘
       native Qt UI       brilliant-mqtt / scene bridge
                                  ↓
                         local MQTT and Home Assistant
```

This core should remain the project's priority. It covers dependable
lights/switches, sensor telemetry, panel settings, physical controls, and local
HA automations. It does not currently render selected HA entities as native
panel tiles; physical-Control hosting was rejected and Virtual Control remains
gated. See the [HA integration guide](home-assistant-integration.md).

## Cloud-owned graph participants

The full bus graph contains `cloud` and partner virtual devices. These are useful because they reveal interface schemas and the native hosting model, but they are not evidence that the panel has a secret local protocol to every partner device. In many cases the on-panel adapter calls a partner cloud API and reflects the result into a Brilliant peripheral.

Static UI strings and installed modules identify clients or auth flows for Amazon/Alexa, Google/Nest, Ring, TP-Link, SmartThings, Sonos, Hue, LIFX, Ecobee, Schlage, Somfy, Wemo, Hunter Douglas, Bluesound, and others. HA should generally connect to those systems directly. Existing Brilliant scenes can then trigger configured HA actions without re-hosting each entity on a physical Control.

This inversion has three benefits:

1. HA becomes the authoritative integration hub.
2. Brilliant's existing scenes/modes remain available in its native UI while HA receives local events and can request confirmed execution.
3. Loss of Brilliant's partner cloud adapters no longer removes the device from the wall panel.

## Configuration synchronization risk

Rooms, names, scene catalogs, device groups, user/home membership, and hosted peripheral persistence involve the object store and Brilliant's distributed graph. A function can execute locally yet still be difficult to recreate after factory reset without cloud bootstrap.

For durable independence, back up sanitized structural data and preserve:

- the signed OSTree repository and boot artifacts;
- panel release/commit inventory;
- room and relevant configuration schemas;
- HA control label/area mapping and scene-action configuration;
- community agent packages and systemd units;
- broker and HA configuration in the operator's secret stores;
- documented recovery procedures that do not require copying device credentials into Git.

Do not publish home IDs, device certificates, HomeKit pairing state, account tokens, or raw object-store databases as a “backup.” They are sensitive and may also be cryptographically tied to one device.

## OTA independence

The updater follows a signed Brilliant OSTree remote and honors firmware governance variables. The local mirror preserves released commits and GPG metadata, which is necessary but not sufficient for a fully independent update path. A safe transition requires:

1. serve the mirror without changing object/signature contents;
2. point one canary panel at the mirror;
3. verify polling, download, signature validation, deployment, reboot, rollback, and bridge compatibility;
4. record the exact active/rollback commits;
5. only then change more panels or restrict vendor egress.

The HA `Firmware Auto-Update` switch controls whether the panel updates; it does not select or validate a replacement remote. Keep it opt-in/disabled-by-default in HA because turning it off has security implications.

## Media independence test

Camera/intercom should be evaluated with staged network isolation, not assumptions from binaries:

1. record the local bus and socket endpoints during an in-home panel-to-panel call;
2. identify ICE candidates, STUN/TURN use, RTSP listeners, and remote-bridge traffic;
3. repeat while blocking internet but allowing panel-to-panel and HA traffic;
4. distinguish same-LAN live view, in-home broadcast, and remote mobile viewing;
5. measure CPU, memory, latency, and privacy state before considering an HA camera/notify adapter.

Until that test is complete, document local media support as promising but unproven.

## Recommended ownership model

| Domain | Preferred authority |
|---|---|
| Brilliant wired loads and panel hardware | Brilliant local bus, bridged to HA |
| Brilliant mesh accessories | Brilliant mesh leader, bridged once to HA |
| Third-party devices | Native HA integration |
| Cross-ecosystem automation | Home Assistant |
| Devices shown on Brilliant panels | Existing Brilliant graph; native HA tiles blocked pending Virtual Control gates |
| Rooms/areas | HA area is operator authority; room mapping prepares the HA manifest but does not create tiles |
| Firmware artifacts | Signed Brilliant commits mirrored locally |
| Secrets | SOPS/HA secret storage, never panel-analysis docs |
| HomeKit | Local fallback during migration and for recovery |

This model uses Brilliant for what the hardware uniquely does well and HA for integration breadth, automation, history, and replacement of cloud-dependent partner adapters.
