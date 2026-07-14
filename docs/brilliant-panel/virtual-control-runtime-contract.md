# Virtual Control runtime contract

This document records the launcher and ownership contract recovered from
Brilliant firmware `v26.06.03.1` for the proposed Home Assistant Virtual
Control. It is the implementation companion to the
[Virtual Control gate runbook](runbooks/virtual-control-gates.md) and the
[native slider status ledger](native-ha-slider-validation-status.md).

## Decision

The firmware contains a coherent path for an officially provisioned
`VIRTUAL_CONTROL` (`DeviceType 6`) to own a normal `LIGHT` and therefore act as
the native endpoint for an HA-backed slider target. The repository now has a
redacted, deterministic candidate manifest for that runtime. It still does not
have a safe start implementation, an official VC identity, or live proof that
the candidate joins the home and remains online.

The current result is therefore:

> Runtime topology understood; live start intentionally blocked.

No panel, cloud endpoint, HA entity, slider, load, or scene was changed while
recovering the contracts below. All runtime probes used synthetic IDs and
credentials in an ARM container with networking disabled, a read-only firmware
mount, temporary storage, and no device mounts.

## Stock process lifecycle

The shipped `run.py` does not create every vassal up front. Its `pre_exec()`
generates flagfiles and hosted-startable configurations, then writes only the
message-bus vassal. Once that vassal is running under uWSGI Emperor, the
message bus constructs `PeripheralProcessManager`. Its `run_bootstrap()` method
admits every enabled default process and activates embedded startables.

```text
isolated uWSGI Emperor
  -> message_bus.ini
       -> bus.message_bus                     (priority -10, non-root)
          -> embedded remote_bridge
          -> PeripheralProcessManager.run_bootstrap()
               -> discovery_peripheral.ini    (non-root)
               -> config_peripherals.ini      (candidate; non-root)
               -> bootstrap.ini               (non-root)
```

The method name `run_bootstrap()` is easy to misread: it does not mean “start
only `BootstrapPeripheral`.” A no-start probe with only message bus, discovery,
and bootstrap enabled produced both client INIs. A stock-path uWSGI smoke then
confirmed this sequence at runtime:

1. Before Emperor start, the process directory contained only
   `message_bus.ini`.
2. The message-bus vassal became loyal and created the isolated UNIX socket.
3. Its process manager created `discovery_peripheral.ini` and `bootstrap.ini`.
4. Both client vassals were spawned and derived the isolated local bus address.

The earlier manual experiment that pre-created three vassals was useful for
isolating uWSGI behavior, but it is not the faithful launcher shape. A future
launcher must preserve the message-bus-first transition.

## Four-process E2E candidate

The captured inventory has 38 process configurations. The data-only candidate
enables four and disables 34:

| Process | Module | Role | Privilege |
|---|---|---|---|
| `message_bus` | `bus.message_bus` | Owns the DeviceType-6 graph and embeds `remote_bridge` | Captured config is non-root, priority `-10` |
| `discovery_peripheral` | `peripherals.discovery.discovery_peripheral` | Publishes remote-bridge discovery metadata | Captured config is non-root |
| `config_peripherals` | `peripherals.lib.peripheral_service.peripheral_host` | Candidate source of the VC-owned Device Configuration link | Captured config is non-root |
| `bootstrap` | `peripherals.bootstrap.bootstrap_peripheral` | Loads official bootstrap parameters and requests target-home assignment | Captured config is non-root |

`config_peripherals` hosts four stock startables together:

| ID | Type | Constructor-only result | E2E consequence |
|---|---:|---|---|
| `art_config_peripheral` | 16 | `ART_CONFIGURATION`; adds defaults | Must be tolerated and inventoried, not mistaken for the light's config link |
| `device_config_peripheral` | 19 | `DEVICE_CONFIGURATION`; adds defaults | Exact candidate for `configuration_peripheral_id` |
| `motion_detection_config_peripheral` | 20 | `MOTION_DETECTION_CONFIGURATION`; no defaults | Additional stock configuration record |
| `alarm_config_peripheral` | 48 | `ALARM_CONFIGURATION`; no defaults | Additional stock configuration record |

The Device Configuration constructor subscribes to the owning device's
hardware slider-count and gangbox-UART variables so it can initialize physical
slider configuration. Those sources will be absent on a Virtual Control. The
constructor succeeds without them, but absence behavior has not been observed
on real ARM hardware. Starting the whole host is therefore a bounded live gate,
not a static conclusion.

The single-light topology snapshot is now schema 2. It records each
`peripheral_type`, allows the three other stock configuration records, and
selects exactly one VC-owned type-19 record whose ID is
`device_config_peripheral`. It still fails closed on a shared physical config,
the home-wide `brilliant_virtual_device_configuration`, a missing type-19
record, a renamed record, or multiple type-19 records.

## Local message-bus addressing

For co-located vassals, set `message_bus_server_socket_path` to the isolated
socket and leave `message_bus_address_override` unset. Bootstrap, discovery,
and configuration hosts then derive a URL of this form:

```text
unix://%2Frun%2Fbrilliant-vc%2Fserver_socket
```

Captured parser probes established the boundary:

| Input | Result |
|---|---|
| `unix://%2Ftmp%2Fvc%2Fserver_socket` | Valid UNIX path `/tmp/vc/server_socket` |
| raw `/tmp/vc/server_socket` | Rejected as an unrecognized scheme |
| `unix:///tmp/vc/server_socket` | Parser accepts the scheme but yields an empty UNIX path |
| `tcp://127.0.0.1:15455` | Valid TCP address, but unnecessary for co-located clients |

A global `message_bus_address_override` is wrong for this topology. It is also
passed into the embedded `RemoteBridge`, causing that component to dial the
same message-bus endpoint it hosts. That self-dial destabilized the analysis
runtime. The candidate manifest therefore renders the override as JSON `null`
and never emits it as a flag.

## Runtime path surface

Every writable path must be isolated from the physical Control. Schema-3
preflight now covers all path flags observed in the generated flagfiles, not
only state/config/socket paths:

| Flag or artifact | Candidate location or rule |
|---|---|
| `mb_state_dir` | Dedicated persistent state directory |
| `cert_dir` | Dedicated runtime certificate directory containing only `device.key` and `device.cert` |
| `process_configs_dir` | Dedicated Emperor watch directory |
| `process_flagfiles_dir` | Dedicated generated flagfile directory |
| `startable_host_configs_dir` | Dedicated embedded-startable configuration directory |
| `message_bus_server_socket_path` | Dedicated nonexistent socket below `/run/brilliant-vc` |
| `uwsgi_stats_socket_path` | A second, distinct nonexistent socket below `/run/brilliant-vc` |
| `saved_bootstrap_parameters_path` | Official private bootstrap blob; runtime ownership unresolved |
| `log_output_directory` | Dedicated empty log directory |
| `error_log_storage_dir` | Dedicated empty error directory |
| `trace_dir` | Dedicated empty trace directory |
| `release_info_filepath` | Existing bounded read-only release metadata |
| `tracking_branch_filepath` | Existing bounded read-only update-channel metadata |
| `art_preload_dir` | Existing read-only stock art catalog required by the grouped config host |

The preflight rejects symlinks, hard links, broad modes, stale contents,
duplicate directories, either pre-existing socket, the physical
`/var/run/brilliant/server_socket`, and any writable path below the physical
Control's protected roots. It pins 15 launcher/configuration files, including
the grouped configuration host and its four startables. The sanitized snapshot
template is
[`virtual-control-launcher-snapshot-v3.example.json`](virtual-control-launcher-snapshot-v3.example.json).

## Privilege and credential handoff

Captured `ProcessManager` output for a nonprivileged vassal includes numeric
drop fields:

```ini
user_override = 65534
group_override = 65534
```

The stock INI and flagfiles were mode `0644`, but the materializer correctly
creates the private key and certificate as owner-only mode `0600`. If those
files remain owned by root, a non-root vassal cannot read them. Running all
vassals as root would bypass the symptom while violating the captured privilege
model and expanding the impact of proprietary code; it remains prohibited.

The candidate pins a dedicated service account name, `brilliant-vc`, but does
not create the account or change file ownership. Before a launcher can exist,
a reviewed handoff must prove all of the following:

- raw provisioner outputs remain root-only;
- only the saved bootstrap blob and validated PEM pair are copied or transferred
  to the dedicated runtime principal;
- state, socket, generated-process, and log directories are writable only by
  that principal and the supervising root Emperor where required;
- the root orchestration lock lives separately at
  `/run/brilliant-vc-control/single-light-pilot.lock`, so the light pilot does
  not require ownership of the service user's socket directory;
- no physical `brilliant` service account, physical certificate directory, or
  physical message-bus path is reused; and
- rollback removes only the disposable VC's files.

Until this exists, schema-3 preflight reports
`runtime_user_credential_handoff_unresolved` after certificate materialization.

## Remote-bridge hardware isolation

The candidate uses an alternate port (`15455` by default), keeps strict peer
authentication enabled, disables Bluetooth provisioning, disables the
provisioning IP listener, disables the BLE debug listener, and proposes
`stub_ble_peripheral=true`. That combination allowed the
message-bus vassal to become loyal in the device-less emulator without
contending for D-Bus or BLE hardware.

`stub_ble_peripheral` is a captured firmware flag, but its behavior on a
co-hosted physical panel is not yet validated. The candidate must not start
until a bounded Office test proves that the stub still permits remote-bridge
home synchronization while opening no physical Bluetooth, UART, gangbox,
faceplate, or load-control surface.

## ARM emulator boundary

The stock-lifecycle smoke ran as the non-root `nobody` UID with a synthetic
DeviceType-6 ID, synthetic certificate, canonical serialized
`BootstrapParameters`, no network, and no hardware. It reached these states:

- message bus loyal: yes;
- isolated socket created: yes;
- discovery/bootstrap INIs generated by the captured process manager: yes;
- discovery client derived the correct percent-encoded UNIX address: yes;
- bootstrap payload parsed: no; and
- QEMU target signal 7 (`SIGBUS`) at the first client/message-bus exchange: yes.

The same boundary occurred with the earlier manually staged client. This is not
evidence of a firmware bug or failed official bootstrap. It means user-mode ARM
emulation cannot validate the first multi-process Thrift exchange for this
binary. Bootstrap parsing, target-home assignment, configuration registration,
and remote peer propagation require a bounded real-ARM test after official
provisioning, credential handoff, and separate live-start approval.

## Repository implementation

`tools.brilliant_vc.vassal_manifest` is intentionally data-only. It returns a
redacted manifest with:

- all 38 process names and the exact 34-process disable set;
- message-bus-first lifecycle and embedded startables;
- the type-19 Device Configuration candidate;
- every isolated path/port flag;
- `message_bus_address_override=null`;
- a private device-ID placeholder rather than an identity; and
- all remaining blockers with `contains_start_primitive=false` and
  `start_permitted=false`.

It has no firmware import, private-file read, subprocess, command/argv builder,
socket, write, apply, or start method. `launcher_preflight` validates schema 3
and the expanded path/hash surface, but also has no start primitive.

## Remaining live sequence

The remaining order is strict:

1. Complete the official-app inventory and obtain an official token scoped to
   `/provisioning/virtual-control-self-bootstrap`.
2. Obtain fresh approval for exactly one account-visible disposable VC write
   and confirm the supported removal screen before submitting it.
3. Provision once; validate and materialize the official identity locally.
4. Implement and review the dedicated runtime-principal credential handoff.
5. Pass schema-3 preflight and review the redacted candidate manifest.
6. Obtain separate approval to start the bounded runtime on Office. The agent
   must still not trigger a light, slider, load, or scene.
7. Observe DeviceType-6 home assignment, remote-bridge/discovery health,
   resource limits, and the exact VC-owned configuration set; stop on any
   unexpected owner or physical-device access.
8. Only then start the one-light pilot, confirm the online tile on two panels,
   and confirm native picker admission.
9. Snapshot one named physical slider and obtain separate native-UI binding
   approval. Physical gestures remain separately prohibited unless the user
   later authorizes them; the operator, not the agent, performs any gesture.
10. Restore the binding and remove the light/VC through supported paths, with
    two later absence observations.

This sequence can confirm whether HA can become the central local hub while
preserving Brilliant's native room/tile/slider experience. It does not yet
establish WAN independence: initial official provisioning is cloud-backed, and
post-provisioning locality remains a dedicated acceptance gate.
