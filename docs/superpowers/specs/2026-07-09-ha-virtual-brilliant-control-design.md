# Home Assistant Virtual Brilliant Control — Design

**Status:** Approved in conversation on 2026-07-09
**Target:** A redistributable Home Assistant add-on that enrolls as a software-only Brilliant Control and mirrors manually selected Home Assistant entities into Brilliant panels.

## 1. Purpose

The existing `brilliant-mqtt` project exports physical Brilliant panel state to Home Assistant. This feature implements the reverse direction: selected Home Assistant entities become first-class, controllable devices in the Brilliant panel UI.

The feature must support the Brilliant device classes needed for a complete home-control experience:

- lights and switches;
- locks;
- thermostats and HVAC controls;
- shades and covers;
- security systems and their sensors;
- cameras and video doorbells;
- live video, doorbell pop-ups, and two-way audio where the underlying Home Assistant integration supports them.

Entity selection is manual. The add-on must not automatically propagate all Home Assistant entities.

The Brilliant app is used for the one-time enrollment flow. Runtime control through the Brilliant cloud or mobile app is not required and must not become a dependency of panel control.

## 2. Constraints Established by Panel Research

Read-only inspection and previous pilot experiments establish the following constraints:

1. A raw message-bus `register_peripheral` call creates a record that renders, but does not bind command routing to the registering client. Panel interactions therefore revert.
2. Hosting a peripheral on a physical panel's `CONTROL` device makes the host a competing device manager and degrades the real gangbox loads.
3. Claiming an existing home-wide virtual device lease would also make the add-on responsible for that device's existing peripherals and risks fleet-wide breakage.
4. `VIRTUAL_CONTROL` provisioning is app/cloud-token gated and is classified as cloud-relayed, which does not meet the local-runtime objective.
5. Brilliant already models `LOCK`, `THERMOSTAT`, `SHADE`, `SECURITY_SYSTEM`, `CAMERA`, and `DOORBELL` as separate first-class peripheral schemas.
6. Cameras use session-oriented `RemoteStreamingConfiguration` and `RemoteMediaSessions` structures. The panel UI contains RTSP and WebRTC receivers; cameras are not represented by a static URL alone.
7. A one-time Brilliant app/account enrollment is acceptable, provided normal state, command, and media traffic remains local.
8. The runtime may execute off-panel as a Home Assistant add-on/container.
9. This design does not authorize changes to a running panel. Active enrollment and pilot tests require a separately approved runbook.

## 3. Approaches Considered

### 3.1 Clean-room synthetic `CONTROL` device

The add-on enrolls as a software-only Brilliant `CONTROL`, participates in the Brilliant LAN and message bus, owns its peripherals, and implements the supported peripheral schemas itself.

This is the selected production architecture. It is the only approach that provides one ownership and lifecycle model across every required device class while remaining redistributable.

### 3.2 User-extracted native runtime under ARM/QEMU

A development-only harness runs firmware binaries extracted locally from a panel. It is useful as a behavioral oracle for protocol capture and differential testing, but is firmware-fragile and cannot be a redistributed product dependency.

This is an optional private conformance tool, not part of the shipped add-on.

### 3.3 Multiple third-party protocol facades

This would emulate Wemo/LIFX for lights, Schlage for locks, Ecobee for climate, Ring for cameras/security, and similar ecosystems for other domains.

It is rejected as the primary design because it fragments identity and lifecycle, depends on multiple cloud-authenticated or certificate-pinned integrations, and cannot provide consistent support for all required device classes. A protocol facade may still be used as a temporary diagnostic tool for a single class.

## 4. System Architecture

```text
Brilliant app
   | one-time pairing/enrollment
   v
+---------------- Home Assistant add-on ----------------+
| Identity & Enrollment                                 |
|   device ID, certificate, home membership             |
|                                                       |
| Virtual Control Core                                  |
|   Brilliant discovery, peer and message-bus protocols |
|   ownership and peripheral lifecycle                  |
|                                                       |
| Peripheral Providers                                  |
|   light/switch | lock | climate | shade | security    |
|   camera | doorbell                                   |
|                                                       |
| HA Adapter + Reconciler                               |
|   manual mappings, commands, state and recovery       |
|                                                       |
| Media Gateway                                         |
|   HA/go2rtc <-> Brilliant RTSP/WebRTC sessions        |
+-----------------------+-------------------------------+
                        | LAN-local runtime
                        v
                   Brilliant panels
```

The virtual control owns every mirrored peripheral. No physical panel, existing third-party device, or existing virtual-device lease is co-managed.

## 5. Components

### 5.1 Identity and Enrollment

The enrollment component:

- obtains or generates the stable Brilliant device identifier and key material as required by the observed enrollment protocol;
- participates in the app-approved pairing or peer-provisioning flow;
- persists the assigned identity, certificate, home membership, and bootstrap information in protected add-on storage;
- detects identity loss or inconsistency and enters recovery mode instead of creating a duplicate control;
- supports explicit unenrollment and local credential destruction.

Enrollment is a one-time account-visible action. Production acceptance requires commands, state, discovery, and media to remain LAN-local after enrollment; this is verified at the relevant delivery gates rather than assumed from the current research.

### 5.2 Virtual Control Core

The core is a clean-room implementation of the minimum Brilliant protocols required to:

- advertise and discover Brilliant LAN services;
- authenticate as the enrolled control;
- join and maintain the home peer graph;
- expose a message-bus endpoint;
- register, update, and delete owned peripherals;
- receive set-variable requests and session commands;
- publish device and peripheral notifications;
- reconnect and reconcile after network or process interruption.

The first project gate is proving that a synthetic control can enroll, join the home, publish one owned test peripheral, survive restart, receive a command from another panel, and continue its local control path during a controlled internet outage. Domain work does not begin until this gate passes.

### 5.3 Home Assistant Adapter

The HA adapter uses Home Assistant APIs to:

- enumerate entities for manual selection;
- subscribe only to mapped entity state changes;
- invoke domain services for panel-originated commands;
- resolve capabilities and supported features;
- obtain camera stream sources through supported Home Assistant interfaces;
- report mapping and command errors to the add-on UI.

It must exclude Brilliant-origin entities from reverse mappings to prevent mirror loops.

### 5.4 Mapping Registry and Reconciler

Each manual mapping is a durable record containing:

```text
mapping_id
HA entity_id
Brilliant peripheral_id and peripheral_type
display name and Brilliant room assignment
declared capabilities
device-specific options
availability and last confirmed state
schema version
```

Identifiers remain stable across rename, restart, and upgrade. Changing a display name does not create a new peripheral. Removing a mapping explicitly withdraws or deletes the corresponding peripheral without affecting other mappings.

The reconciler compares configured mappings, HA state, and Brilliant registrations. It repairs missing registrations, updates changed metadata, removes deleted mappings, and publishes a full confirmed snapshot after reconnect.

### 5.5 Peripheral Providers

Providers isolate Brilliant schema translation from transport and HA APIs.

- **Light/switch:** power, brightness, color temperature, and color when both sides support them.
- **Lock:** locked state, lock/unlock, battery, jam/error status, and event history required by the panel UI.
- **Climate:** ambient and target temperatures, heat/cool ranges, HVAC mode, fan mode, and capability limits.
- **Shade/cover:** position, tilt, continuous movement, open, close, and stop.
- **Security system:** current mode, supported arm modes, sensors, alarm events, and the required authorization exchange.
- **Camera:** metadata, streaming configuration, media session lifecycle, and availability.
- **Doorbell:** camera behavior plus ring/motion events, pop-up configuration, answer/dismiss state, and optional paired lock/security actions.

Each provider owns validation, conversion, command handling, and confirmed-state publication for its domain.

### 5.6 Media Gateway

The media gateway uses go2rtc and a small Brilliant session controller.

When a panel opens a camera, the controller creates a bounded session and publishes the corresponding Brilliant streaming and remote-session structures. The panel connects directly to the add-on using RTSP or WebRTC. Dismissal, disconnect, idle timeout, or source failure tears down the session and clears its bus state.

Doorbell events create the Brilliant notification state required for panel pop-ups. Answer and dismiss actions update the session owner and notification state.

Two-way audio is enabled only when the Home Assistant camera source and Brilliant client negotiate a compatible audio path. Cameras without talkback remain available for live view. Media credentials and URLs are short-lived and session-scoped.

## 6. Data and Command Flows

### 6.1 Home Assistant to Brilliant

```text
HA state event
 -> provider validates and normalizes state
 -> reconciler updates owned peripheral variables
 -> virtual control publishes bus notification
 -> Brilliant panels update their UI
```

### 6.2 Brilliant to Home Assistant

```text
Panel interaction
 -> virtual control receives set-variable request
 -> provider validates capability and authorization
 -> HA adapter invokes the appropriate service
 -> provider waits for resulting HA state
 -> confirmed state is published to Brilliant
```

Lights and shades may display a short-lived pending value for responsiveness. Locks and security systems never publish optimistic success. Their final state is updated only after Home Assistant confirms the operation.

### 6.3 Camera Session

```text
Panel opens camera
 -> media controller creates a session
 -> go2rtc exposes RTSP/WebRTC media
 -> session variables are published
 -> panel connects directly to the add-on
 -> dismissal, idle timeout or failure closes the session
```

## 7. Reliability and Lifecycle

- If Home Assistant is unavailable, mapped peripherals become offline while retaining their last known values. Commands are rejected rather than queued.
- If the Brilliant connection fails, HA subscriptions remain active. Reconnect uses backoff and publishes a full state snapshot before accepting new commands.
- A rejected or timed-out HA command republishes the confirmed HA state and reports failure. Request delivery is never treated as success.
- A media failure clears only the affected session and marks that camera unavailable.
- Restart and upgrade reload the same identity and mapping records, reconcile idempotently, and avoid duplicate controls or peripherals.
- Mapping deletion closes active sessions, removes retained state, and withdraws the one target peripheral.
- Loss of enrollment identity enters explicit recovery mode.

## 8. Security

- Device certificates, HA credentials, and bootstrap data use add-on-protected storage with restrictive permissions.
- Manual mappings are the only entities exposed to Brilliant.
- Media listeners are restricted to the panel network and use native Brilliant authentication where available.
- Camera URLs and credentials are short-lived and scoped to one session.
- Unlock, disarm, and media actions are audited with origin, target, result, and timestamp.
- PINs, alarm codes, tokens, and camera credentials are never persisted in mappings or logs.
- Security codes are passed ephemerally to Home Assistant.
- The native/QEMU conformance oracle runs in a separate development profile without production credentials.
- Shipped code is independently authored and released under the project's MIT license using only license-compatible open-source dependencies.
- Proprietary binaries, decompiled source, vendor assets, certificates, and private keys are never committed or packaged. User-extracted firmware remains outside the repository and is used only by the private conformance profile.

## 9. Add-on Configuration Experience

The add-on UI provides:

- enrollment status and explicit pair/recover/unenroll actions;
- manual entity selection;
- Brilliant type, display name, room, and capability overrides;
- camera source, live-view, doorbell, and talkback options;
- validation that prevents unsupported entity/type combinations;
- health for HA connectivity, Brilliant connectivity, reconciliation, command failures, and active media sessions;
- per-mapping disable and delete controls.

Bulk automatic propagation is intentionally excluded. Bulk import may be added later only as a review-and-confirm workflow that still creates explicit mappings.

## 10. Delivery Gates

### Gate 1: Protocol and enrollment

Implement discovery, pairing, identity persistence, and minimum peer/message-bus transport. Enroll one synthetic control. It must appear once and survive restart without panel file changes or an existing-device claim.

Capture the enrolled control's traffic and verify that discovery, state publication, and panel-originated commands use LAN paths. Repeat the command and state tests with internet egress denied while preserving the local network. A cloud dependency during steady-state command or state handling fails the gate.

If the pairing flow requires hardware attestation that cannot be implemented cleanly, production work stops. The private native/QEMU oracle may be used to characterize the missing exchange, but the design does not fall back to redistributed firmware.

### Gate 2: Ownership and routing

Publish one test light, prove rendering on the Office Panel, and prove Office Panel commands reach the add-on. Teardown must return the home graph to its baseline.

### Gate 3: Core providers

Add lights, switches, locks, climate, and shades with schema, state, command, availability, and failure tests.

### Gate 4: Camera live view

Validate RTSP and WebRTC session creation and teardown first with a synthetic feed, then with real HA camera sources. Traffic capture and controlled internet denial must show that live media remains on the LAN after enrollment.

### Gate 5: Doorbells, talkback, and security

Add ring/motion pop-ups, answer/dismiss state, bidirectional audio, lock pairing, alarm modes, and safety tests for failed unlock/disarm operations.

### Gate 6: Multi-panel soak

Exercise multiple mappings and panels, add-on and panel restarts, HA/network interruptions, identity stability, reconciliation, latency, and cleanup.

## 11. Verification Strategy

- Golden wire fixtures from read-only captures.
- Unit tests for each provider and serialized structure.
- Differential tests against the private, locally extracted native/QEMU reference runtime.
- Simulated HA and Brilliant peers for timeouts, malformed requests, reconnects, and partial failures.
- Office Panel pilot runs under a written baseline/abort/rollback runbook.
- Second-panel validation only after the Office pilot is stable.
- Upgrade and rollback tests for mappings, certificates, identity, and active-session cleanup.

The release remains experimental until enrollment, ownership, removal, restart recovery, lock/security failure behavior, and media-session cleanup pass on real hardware.

## 12. Success Criteria

The design succeeds when:

1. one app-enrolled synthetic control has a stable identity and passes the local-runtime outage tests;
2. manually mapped HA entities render as the correct native Brilliant types on panels;
3. commands are routed to the add-on and confirmed from HA state;
4. locks and security systems never report false success;
5. cameras support live view, doorbell pop-ups, and two-way audio when the source supports it;
6. restart, network loss, mapping deletion, unenrollment, and rollback leave no duplicate or orphaned peripherals;
7. the distributed add-on is MIT-licensed and contains no proprietary Brilliant code, binaries, assets, certificates, or private keys.
