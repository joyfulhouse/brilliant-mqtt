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
redacted candidate manifest, fail-closed credential handoff, schema-5
mixed-ownership preflight, a no-start runtime preparer, and a bounded
reference systemd unit. It still does not have an official VC identity, a
reviewed on-panel account/install, or live proof that the candidate joins the
home and remains online.

The current result is therefore:

> Runtime topology understood; live start intentionally blocked.

No panel, cloud endpoint, HA entity, slider, load, or scene was changed while
recovering the contracts below. Runtime preparation probes used synthetic IDs
and credentials in an ARM container with networking disabled, a read-only
firmware mount, temporary storage, and no physical device mounts. A later
read-only Office query established systemd compatibility facts only; it did not
copy, install, enable, start, stop, or restart a unit.

## Stock process lifecycle

The shipped `run.py` does not create every vassal up front. Its `pre_exec()`
generates flagfiles and hosted-startable configurations, then writes only the
message-bus vassal. Once that vassal is running under uWSGI Emperor, the
message bus constructs `PeripheralProcessManager`. Its `run_bootstrap()` method
admits every enabled default process and activates embedded startables.

```text
isolated non-root uWSGI Emperor (`brilliant-vc`)
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
isolating uWSGI behavior, but it is not the faithful launcher shape. The
reference launcher preserves the message-bus-first transition and runs the
Emperor itself as the dedicated non-root principal.

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

Every writable path must be isolated from the physical Control. Schema-5
preflight now covers all path flags observed in the generated flagfiles, not
only state/config/socket paths:

| Flag or artifact | Candidate location or rule |
|---|---|
| `mb_state_dir` | Dedicated persistent state directory |
| `cert_dir` | `/data/brilliant-vc-credentials/certificates`; root-owned, dedicated-group-readable, and not service-writable |
| `process_configs_dir` | Dedicated mode-`0700` Emperor watch directory owned by `brilliant-vc` |
| `process_flagfiles_dir` | Dedicated generated flagfile directory |
| `startable_host_configs_dir` | Dedicated embedded-startable configuration directory |
| `message_bus_server_socket_path` | Dedicated nonexistent socket below `/run/brilliant-vc` |
| `uwsgi_stats_socket_path` | A second, distinct nonexistent socket below `/run/brilliant-vc` |
| `saved_bootstrap_parameters_path` | `/data/brilliant-vc-credentials/bootstrap`; root-owned mode `0640`, readable only by the dedicated runtime group |
| `log_output_directory` | Dedicated empty log directory |
| `error_log_storage_dir` | Dedicated empty error directory |
| `trace_dir` | Dedicated empty trace directory |
| `release_info_filepath` | Existing root-owned, single-link, mode-`0644` bounded release metadata |
| `tracking_branch_filepath` | Existing root-owned, single-link, mode-`0644` bounded update-channel metadata |
| `art_preload_dir` | Existing root-owned, real-directory, mode-`0755` stock art catalog required by the grouped config host |

The preflight rejects symlinks, hard links, broad modes, stale contents,
duplicate directories, either pre-existing socket, the physical
`/var/run/brilliant/server_socket`, and any writable path below the physical
Control's protected roots. It pins 20 exact files and modes: the firmware
modules and configuration helpers used by preparation, the grouped
configuration host and its four startables, `run.py`, `process-default.ini`,
`run_startable.py`, uWSGI, `/usr/bin/python3.10`, and the immutable approval
mover. Source/config files must be `0644`; binaries and extension modules must
be `0755`. `configs.socket_parameters` is included because preparation executes
its flag-discovery helper. The sanitized schema-5 template is
[`virtual-control-launcher-snapshot-v5.example.json`](virtual-control-launcher-snapshot-v5.example.json).

## Privilege and credential handoff

Captured `ProcessManager` output for a nonprivileged vassal includes numeric
drop fields:

```ini
user_override = 65534
group_override = 65534
```

The stock physical service starts Emperor as root and makes
`/var/run/brilliant` mode `0777`. Its dropped message bus then writes child INIs
into the watched process directory. Copying that shape for a new service user
would make a root Emperor consume files writable by that user. The candidate
therefore does not use a root Emperor.

The networkless ARM smoke was rerun with Docker UID/GID `65534:65534`, so
`run.pre_exec`, Emperor, message bus, and child vassals all shared one non-root
identity. After `pre_exec`, the process, flagfile, hosted-startable, and error
directories were tightened from the stock umask result to mode `0700`.
Message bus still became loyal, created the isolated socket, and generated the
discovery/bootstrap INIs. This proves the non-root supervisor shape up to the
same QEMU exchange boundary; it does not prove live home assignment.

The repository handoff uses three disjoint roots:

| Root | Owner/mode | Contents and authority |
|---|---|---|
| `/data/brilliant-vc-private` | root, `0700` | Raw four-file provisioner output and root-only materialized PEM pair |
| `/data/brilliant-vc-credentials` | root:`brilliant-vc`, `0750` | Canonical device ID, bootstrap, and `certificates/`; files are `0640` |
| `/data/brilliant-vc` and `/run/brilliant-vc` | `brilliant-vc`, `0700` | Writable state, generated configs, logs, sockets, and traces |

`tools.brilliant_vc.runtime_handoff` revalidates the device ID/metadata and the
materialized key/certificate match, validity, CN, non-CA status, and bounds. A
dry run writes nothing. Apply exclusively creates only `device_id`,
`bootstrap`, `certificates/device.key`, and `certificates/device.cert`; it
keeps each file owner-only until its contents are fsynced, then applies group
read access. It fsyncs the files/directories, verifies byte equality, is
idempotent for an exact existing copy, rejects drift, and rolls back only a
directory it just created.
It cannot create an account, connect to a panel, import firmware, build a
command, or start a process.

The runtime account must be a non-root `brilliant-vc` user with exactly one
passwd record for its UID, exactly one same-name dedicated group record for its
GID, no foreign group members, a locked password, home `/nonexistent`, and
shell `/usr/sbin/nologin`; the home path must not exist. Root preflight requires
the shadow database to be root-owned mode `0600` or `0640` and checks the single
locked password entry without materializing password hashes as Python strings.
The non-root preparer independently checks its real UID, effective GID, unique
passwd/group records, lack of supplementary groups, home, and shell.
Staging must also show no pre-existing process under that UID. The service can
read credentials through its group but cannot modify or delete them. The later
single-light lease remains separate at
`/run/brilliant-vc-control/single-light-pilot.lock`; bootstrap approval uses
`/run/brilliant-vc-approval` and cannot collide with it.

Schema-5 preflight distinguishes three no-start states:

1. no root-only PEM pair: `identity_materialization_required`;
2. PEM pair present but no exact runtime copy:
   `runtime_credential_handoff_required`; and
3. exact handoff present:
   `nonroot_service_install_and_compatibility_validation_required`.

All states retain `start_permitted=false`. The handoff implementation resolves
the file-access design but has not been applied to an official identity. The
repository's preparer and service are reference artifacts, not authorization
or evidence of an on-panel install.

## No-start preparation and bounded bootstrap service

`tools.brilliant_vc.runtime_prepare` implements the one-shot step between
schema-5 preflight and uWSGI. Its dry run:

- re-hashes the 20 pinned firmware/runtime/launch-chain files and rejects a
  symlink, owner mismatch, non-regular file, size drift, race, or any mode that
  differs from its exact `0644`/`0755` contract;
- requires the exact non-root runtime identity and empty, disjoint mode-`0700`
  service roots;
- requires the existing stock art catalog to be a root-owned, non-symlink
  directory with exact mode `0755`, outside every service-writable root;
- revalidates the exact root:`brilliant-vc` credential layout, device ID,
  bootstrap bounds, and PEM key/certificate/CN/validity/non-CA contract; and
- builds the exact four-enabled/34-disabled firmware flags in memory, while
  printing a redacted device ID, the non-secret credential-bundle digest, and
  `fresh_start_approval_required`.

Apply mode additionally requires a root:`brilliant-vc` mode-`0640` approval
less than ten minutes old. Its exact schema is
[`virtual-control-start-approval.example.json`](virtual-control-start-approval.example.json).
The tracked file is schema documentation only: its timestamp and all-zero
credential digest are deliberate invalid placeholders and must not be used as
an approval.
The approval is Office/build/run specific, caps runtime at 600 seconds, binds
the exact four-file runtime credential digest, and sets both
`physical_device_actions_permitted` and `hosted_light_permitted` to false. Its
root-owned mode-`0750` directory is not service-writable. Before repository code
runs, systemd invokes the pinned stock mover for one privileged step and
atomically renames the root:`brilliant-vc` mode-`0640` source to the consumed
marker. The non-root preparer requires the source to be absent, validates the
marker without modifying it, and compares its digest to the credentials. A
failed preparation keeps the marker or a non-empty generated surface, so a
blind retry fails closed. The journal result records the approval run ID,
approval SHA-256, and credential-bundle SHA-256 without credential contents.

The captured `process_configs.get_all_configs()` creates fresh config objects
whose `flagfile` paths reflect the gflags values at construction time. The
first real-firmware spike exposed that reusing the pre-parse objects silently
left `/tmp/flagfiles` references. The implementation now:

1. inventories all 38 names without evaluating peripheral capture properties;
2. temporarily exposes only message bus, discovery, grouped config, and
   bootstrap while stock gflag discovery imports modules;
3. restores stock enumeration, parses the isolated flags, and creates a new
   exact four-config set with the final paths; and
4. exposes only that rebuilt set while calling the captured `run.pre_exec()`.

This also prevents `pre_exec` from importing or generating flagfiles for the 34
disabled processes. The preparer never calls `run_emperor`, uWSGI, a shell, a
socket, or any Brilliant managed-process start. Selected firmware imports do
perform one libc lookup through `/sbin/ldconfig -p`; the isolated smoke
allowlisted only that exact helper and rejected every other child-process path.
The public result therefore says `contains_emperor_start_primitive=false` and
`emperor_started=false`, not that no operating-system helper can execute.

After `pre_exec`, the preparer accepts only this surface:

```text
/data/brilliant-vc/process-config/message_bus.ini
/data/brilliant-vc/flagfiles/{message_bus,discovery_peripheral,config_peripherals,bootstrap}_flagfile
/data/brilliant-vc/startable-configs/message_bus
/data/brilliant-vc/startable-configs/config_peripherals
/data/brilliant-vc/errors/                         # empty
```

It rejects any missing/extra file, symlink, hard link, empty/oversized file,
wrong owner, or content drift. It parses every flagfile and INI and requires the
isolated paths, exact disabled set, DeviceType-6 flags, non-root numeric vassal
overrides, alternate port, strict authentication, BLE stub, four grouped
configuration startables, and absent global message-bus override. It then
tightens generated directories to `0700` and files to `0600`, checks the
complete persistent-root inventory again, and requires state, logs, traces,
errors, and `/run/brilliant-vc` to remain empty.

The captured ARM firmware passed the current schema-5/20-file apply path in a read-only,
networkless, device-less container as synthetic UID/GID `12345:12345`. It
produced four flagfiles, both hosted-startable configs, only
`message_bus.ini`, exact non-root overrides, and no Emperor process. The
expected GLib/D-Bus-unavailable warnings were consistent with the container's
absence of device mounts and a system bus. This is stronger preparation proof
than the earlier mock, but it still does not parse an official bootstrap or
exercise the live message bus.

[`deploy/brilliant-vc-pilot.service`](../../deploy/brilliant-vc-pilot.service)
is the separate start-bearing reference. It has no `[Install]` section and is
not packaged, installed, enabled, or started by repository automation. One
`ExecStartPre=!` uses only the pinned immutable OS mover as root; no repository
Python runs root. The next pre-start, direct uWSGI Emperor, and every vassal run
as `brilliant-vc`. The chain uses pinned `/usr/bin/python3.10`, uWSGI,
`process-default.ini`, and `run_startable.py` paths directly and never loads the
stock `emperor.ini`, zygote, or delegated-launch socket. A networkless ARM
`--show-config` parse contained only the requested direct Emperor options and
no fork/zygote option. The unit never restarts, stops after 600 seconds, kills
the whole control group, caps memory/CPU/tasks/files, removes all capabilities,
hides physical devices and the Brilliant/D-Bus/udev paths, mounts firmware,
staged app, credentials, and metadata read-only, and grants runtime writes only
to its two isolated service-owned roots. The approval directory appears in the
systemd write allowlist solely for the root mover; DAC keeps it non-writable to
`brilliant-vc`.

The staged repository subset is part of the trust boundary. Before unit
verification it must contain only the reviewed runtime modules under
`/var/brilliant-vc/app`, with root ownership, non-writable files/directories,
no symlinks or bytecode, and SHA-256 values matched to the reviewed repository
revision through
[`brilliant-vc-pilot-app-manifest.sha256`](../../deploy/brilliant-vc-pilot-app-manifest.sha256).
`ReadOnlyPaths=/var/brilliant-vc/app` preserves that surface inside the sandbox;
it is not a substitute for staging review.

The unit's `MemoryMax`, `CPUQuota`, `TasksMax`, 600-second deadline, and
control-group kill policy are the authoritative aggregate bounds for Emperor
and every vassal. The separate runtime monitor's main-PID samples are
supplementary evidence; they do not replace those cgroup-wide limits.

A read-only Office check on 2026-07-13 reported systemd `250 (250.5+)`; the
stock message bus and existing MQTT bridge remained active. The panel also
already uses `MemoryMax` and `CPUQuota` in deployed community units. Its
`systemd-analyze` rejects stdin-backed units, and this no-deploy pass did not
write a temporary unit, so a real `systemd-analyze verify` against the staged
file remains mandatory before installation. Account creation, code staging,
unit installation, start, runtime monitoring, and cleanup are all still live
gates.

This unit is deliberately **bootstrap-only**. Its approval forbids a hosted
light and its 600-second deadline is shorter than the single-light pilot's
1,800-second registration/cleanup budget. It must not be stretched, restarted,
or reused for VC5. After a bootstrap run, persistent state and generated files
are intentionally non-empty, so the first-run preparer fails closed on retry.

The required separate profile is now implemented in
[`coordinated-session-design.md`](coordinated-session-design.md),
[`deploy/brilliant-vc-session.service`](../../deploy/brilliant-vc-session.service),
and `tools.brilliant_vc.{session_approval,session_prepare,session_coordinator}`.
It has a different consumed marker, fixed VC2/password inputs, empty evidence
and control roots, exact source/vendor staging gate, and an absolute
2,520-second deadline. It binds the direct uWSGI PID/executable/UID/GID/cgroup,
requires two normalized scoped topologies ten seconds apart, records VC3/VC4,
hosts one approval-bound light while monitoring, and proves two-read deletion.
It leaves VC5 `not_run`. The profile has passed focused repository tests, a
production-default 14-app/19-vendor manifest rehearsal, captured-ARM no-start
preparation, the exact captured-uWSGI option/`--pidfile` parse, and an
off-panel systemd 252 unit verify. It is not staged, installed, authorized, or
live evidence. The 2026-07-14 read-only Office check found every session path
and the `brilliant-vc` account absent while the native bus, UI, and MQTT bridge
remained active; only the exact on-panel systemd 250 verify is still a staging
gate.

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

The stock-lifecycle smoke ran with the entire container and Emperor at non-root
UID/GID `65534:65534`, using a synthetic DeviceType-6 ID, synthetic
certificate, canonical serialized `BootstrapParameters`, no network, and no
hardware. It reached these states:

- message bus loyal: yes;
- generated watch/flag/startable/error directories at mode `0700`: yes;
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
- a dedicated non-root Emperor sharing the vassal identity, never root;
- the type-19 Device Configuration candidate;
- every isolated path/port flag and the root-group credential layout;
- `message_bus_address_override=null`;
- a private device-ID placeholder rather than an identity; and
- all remaining blockers with `contains_start_primitive=false` and
  `start_permitted=false`.

It has no firmware import, private-file read, subprocess, command/argv builder,
socket, write, apply, or start method. `launcher_preflight` validates schema 5,
the expanded path/hash surface, non-root supervisor contract, and exact
root/service ownership split, but also has no start primitive.

`tools.brilliant_vc.runtime_handoff` writes only the exact isolated credential
root. `runtime_prepare` is the second bounded writer: after systemd consumes one
approval, it validates the marker and generates only the validated
service-owned pre-start surface. The identity
materializer remains confined to root-private source/output directories.

The reference service is the only new artifact containing an Emperor start.
It is not packaged or installed by automation and is non-enableable by default
because it has no `[Install]` section. The static manifest still
reports `start_permitted=false`; its blockers now distinguish credential
handoff, preparation, service installation/compatibility, fresh bootstrap-only
approval, real-ARM validation, and supported removal.

## Remaining live sequence

The remaining order is strict:

1. Complete the official-app inventory and obtain an official token scoped to
   `/provisioning/virtual-control-self-bootstrap`.
2. Obtain fresh approval for exactly one account-visible disposable VC write
   and confirm the supported removal screen before submitting it.
3. Provision once; validate and materialize the official identity locally.
4. Create/review the dedicated non-root account and same-name private group,
   then dry-run, review, and apply the implemented credential handoff. Verify
   the exact root/dedicated-group ownership without exposing contents.
5. Review the implemented preparer/reference unit, stage the code without
   enabling it, run on-panel `systemd-analyze verify`, dry-run the preparer as
   the exact service account, and pass schema-5 preflight. Confirm the physical
   services are unchanged.
6. Create a fresh bootstrap-only approval from the documented schema and obtain
   separate authorization to start the bounded runtime on Office.
   The agent must still not trigger a light, slider, load, or scene.
7. Observe DeviceType-6 home assignment, remote-bridge/discovery health,
   resource limits, and the exact VC-owned configuration set; stop on any
   unexpected owner or physical-device access.
8. Stop: the bootstrap-only unit cannot host the 1,800-second light pilot.
   Review and separately authorize the implemented clean-root coordinated
   session; never reuse the bootstrap marker or roots.
9. Under that separately authorized session, start the one-light pilot only after VC3/VC4
   pass, then confirm the online tile on two panels and picker admission.
10. Snapshot one named physical slider and obtain separate native-UI binding
   approval. Physical gestures remain separately prohibited unless the user
   later authorizes them; the operator, not the agent, performs any gesture.
11. Restore the binding and remove the light/VC through supported paths, with
    two later absence observations.

This sequence can confirm whether HA can become the central local hub while
preserving Brilliant's native room/tile/slider experience. It does not yet
establish WAN independence: initial official provisioning is cloud-backed, and
post-provisioning locality remains a dedicated acceptance gate.
