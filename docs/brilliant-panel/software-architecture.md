# Software architecture

## Platform

The Brilliant Control is an embedded Linux appliance, not Android:

| Layer | Observed implementation |
|---|---|
| SoC | Freescale/NXP i.MX6 DualLite, two ARM Cortex-A9 cores |
| OS | Brilliant Embedded Linux 4.0.11, Yocto kirkstone / Boot2Qt |
| Kernel | `5.15.71-brl+yocto` family |
| Deployment | Signed OSTree, stable branch `linux/brilliant-imx6/brilliant-embedded-image/stable` |
| UI | 25 MB ARM C++ Qt 5 Quick/QML executable at `/data/switch-ui/switch-ui` |
| Service runtime | Python 3.10.9, uWSGI emperor/vassals, mostly Cython-compiled first-party modules |
| Local RPC | Apache Thrift binary protocol over `/var/run/brilliant/server_socket` |
| Persistent state | `/var` on the shared OSTree var partition |

`/data/switch-ui` and `/data/switch-embedded` belong to the selected OSTree deployment and are replaced by OTA. `/var` persists across deployments. This is why community agents must live under `/var`, while importing the current firmware's Python environment through its stable `/data/switch-embedded/env/bin/python3` path.

## Boot and process topology

`message_bus.service` is the control-plane supervisor:

1. export device flags and initialize PKCS#11/SoftHSM state;
2. initialize ALSA controls;
3. run `/data/switch-embedded/run.py --skip_emperor` to generate process configuration;
4. start the uWSGI emperor against `/var/run/brilliant/processes`;
5. fork vassals through a zygote/fork-server socket.

`run.py` initially writes only `message_bus.ini`. Once that vassal runs under
Emperor, its internal `PeripheralProcessManager.run_bootstrap()` writes the
other enabled default-process INIs and activates embedded startables. The
method name is historical; it does not start only the bootstrap peripheral.
This lifecycle matters for any isolated Virtual Control. See the
[recovered VC runtime contract](virtual-control-runtime-contract.md).

The native `switch_ui_app.service` starts separately after the message bus and executes `/data/switch-ui/switch-ui`. It has a systemd watchdog and an aggressive restart policy whose repeated failure action is a panel reboot. This reinforces an important integration rule: do not inject into, replace, or preload the UI process. Use the bus.

Observed uWSGI vassals on the pilot include:

| Family | Vassals |
|---|---|
| Core | `message_bus`, `monitor`, `bootstrap`, `analytics`, `wifi`, `hardware` |
| Automation/state | `execution`, `control_notification`, `config_peripherals`, `object_store_peripheral`, `discovery_peripheral` |
| Physical hardware | `faceplate_peripheral`, `gangbox_peripherals`, `voice`, `art` |
| Bridges | `homekit`, Brilliant virtual-device services, and dynamically selected third-party adapters |

The installed package tree contains 7,116 files, including 618 ARM Cython extensions. First-party-heavy areas are:

| Package | ARM extensions | Purpose |
|---|---:|---|
| `peripherals` | 372 | Physical peripherals, virtual devices, partner integrations, hosting framework |
| `lib` | 179 | Bus observer, protocol processor, networking, storage, queueing, users, versioning |
| `bridge` | 6 | Bootstrap/provisioning bridges |
| `bus` | 5 | Message-bus server implementation |
| `monitors` | 5 | uWSGI/kernel/socket monitoring |
| `thrift_types` | 0 | 505 readable generated Python schema files |

## Architectural flow

```text
                         whole-home device graph
                                   │
                    /var/run/brilliant/server_socket
                                   │
             ┌─────────────────────┼─────────────────────┐
             │                     │                     │
      Qt/QML switch-ui      uWSGI peripherals      community agents
             │                     │                     │
  screens / gestures /      gangbox, BLE mesh,     brilliant-mqtt
  settings / media UI       execution, HomeKit,    scene bridge
                            partner adapters        watchdogs
                                   │                     │
                         local hardware/cloud       local MQTT / HA
```

The message bus owns the canonical graph. A `Device` is a bus participant or namespace; a `Peripheral` is a typed capability instance; a `Variable` is a string-encoded value plus timestamp and `externally_settable` permission. This distinction matters:

- a physical Control is one `Device` with many peripherals;
- each wired gang is a separate LIGHT, GENERIC_ON_OFF, or ALWAYS_ON peripheral;
- all Brilliant mesh accessories live under the home-wide `ble_mesh` virtual device;
- partner ecosystems are separate virtual devices such as `hue_bridge`, `ring_virtual_device`, `sonos`, and `ecobee`;
- configuration catalogs—rooms, scenes, modes, groups, property information—are also peripherals.

The pilot snapshot contained 36 devices: 15 Controls, `ble_mesh`, Brilliant/configuration/cloud virtual devices, partner virtual devices, and mobile-app participants. A per-panel bridge must therefore scope ordinary physical entities to its owning Control; otherwise every panel republishes the whole home.

## RPC and synchronization model

The on-box Python client is `lib.message_bus_api.observer_interface.RPCObserver`. It wraps a `SinglePeerProcessor`, generated Thrift clients, subscription matching, notification application, and reconnect callbacks. Confirmed operations include:

- `get_all`, `get_device`, and `get_peripheral`;
- `subscribe` and `unsubscribe`;
- `request_set_variables_in_peripheral`;
- `register_virtual_device` and peripheral hosting through the peripheral framework;
- inbound `handle_notification`, `handle_home_id_updated`, and set-variable dispatch.

The Qt UI implements its own C++ Thrift client. Static evidence includes `MessageBusClient`, `StateManager`, `SubscriptionFilter`, `SetVariablesRequestResult`, generated `MessageBusService` and `PeripheralService` methods, and the same server-socket path. The HomeKit vassal and community bridge use the same conceptual variable-write path as the UI.

### Important reliability behavior

`RPCObserver.get_all()` and `get_device()` read the observer's notification-fed local mirror. They are not independent server round trips. If the notification stream silently dies, both push updates and subsequent reads can be stale. The bridge's reconnect hook, hot diff poll, stream-staleness watchdog, optimistic command echo, and bus-health watchdog are responses to a live-observed failure mode, not speculative hardening.

## Peripheral hosting model

The firmware ships a reusable peripheral-host framework. Bundled Hue, LIFX, SmartThings, Ring, Schlage, Ecobee, Nest, Sonos, Somfy, Wemo, TP-Link, Hunter Douglas, and other adapters translate external APIs into native Brilliant peripherals. A hosted peripheral supplies:

- a stable name/registry key;
- a `PeripheralType` and interface variables;
- externally settable variables with push callbacks;
- room assignment and display metadata;
- lifecycle registration and deletion.

The deprecated HA mirror attempted to apply this pattern to a physical Control.
Live testing rejected that ownership model: it co-managed real hardware, added
bus load, threatened physical responsiveness, and did not reliably admit or
propagate tiles. The supported [HA scene/mode bridge](home-assistant-integration.md)
does not host a peripheral. Native HA tiles require the distinct Virtual
Control path to pass its feasibility gates.

One lifecycle trap is live-verified: own-device hosted peripherals persist in object store across host exit and reboot. A leader or test host must explicitly delete peripherals during handoff and teardown, or the UI retains phantoms.

## UI process architecture

The UI ELF is stripped but structurally rich:

- Qt Quick/QML, Qt Quick Controls, Qt Multimedia, Qt Network, and Qt WebEngine;
- GStreamer player, RTSP server, WebRTC/SDP, Farstream, ALSA/PortAudio, and input/udev bindings;
- a direct Thrift message-bus client and state manager;
- embedded QML source, 225 registered QML types, including 148 `*Screen` types;
- 426 embedded PNG resources plus fonts, sounds, SVGs, and JavaScript;
- controller/model classes for rooms, devices, groups, shortcuts, weather, art, media, mesh, security, and configuration.

The UI uses a stack-navigation manager rather than independent applications. Most screens are wrappers around C++ QML types that read models from the bus and issue typed variable writes through the state manager.

## Physical hardware and media

| Hardware | Software surface |
|---|---|
| Wired gangs | Gangbox UART status/config plus LIGHT, GENERIC_ON_OFF, or ALWAYS_ON peripherals |
| Capacitive sliders | `brilliant_captouch` input device plus per-gang/mesh `slider_config` blobs |
| PIR/ambient sensor | `faceplate_peripheral` motion/lux variables and thresholds |
| Display/backlight | Hardware peripheral screen variables plus Linux backlight sysfs |
| Microphone/speaker | ALSA/PortAudio, native Alexa pipeline, HA voice satellite, GStreamer playback |
| Camera | OV9732 1280×720 raw Bayer device, GStreamer conversion, WebRTC/RTSP session stack |
| BLE radio | Brilliant mesh virtual device, accessory configuration, proxy/DFU/topology management |
| Wi-Fi | ConnMan/wpa_supplicant plus WIFI peripheral state and commands |

The camera and intercom are intentionally not modeled as a simple local file or toggle. The UI and interfaces describe `remote_sessions`, streaming configuration, SDP, call targets/states, live view, broadcast selection, audio routing, and privacy/configuration gates.

## Networking and cloud-facing surfaces

The local bus has no off-panel TCP listener. Panel-to-panel and media features use other services, including remote bridge WebSockets and RTSP/WebRTC-related ports. The firewall defaults to drop with allow rules for selected management, bridge, media, voice, and high ephemeral ports.

The UI and service stack contain clients for Brilliant web/object-store services and partner APIs. Presence of a URL is evidence of a dependency path, not evidence that all state transits the cloud. Wired load control and bus propagation continue locally after the graph is established.

## Resource constraints

Live measurements show roughly 1 GB RAM, no swap, and a two-core CPU that can already run near saturation. The message-bus vassal can consume a large fraction of one core. New panel-resident features should therefore:

- stay in separate resource-capped systemd services;
- avoid polling the full graph at high frequency;
- elect one host for home-wide responsibilities;
- avoid embedding browsers or heavyweight transcoding in the core bridge;
- fail independently of the native UI and message bus;
- preserve HomeKit as a fallback until equivalent behavior is validated.
