# Coordinated Virtual Control session design

Status: **repository implementation and off-panel validation complete; no live
authorization**. This document does not authorize staging,
installation, start, light registration, slider binding, or a panel gesture.
The Office panel remains unchanged until the live gates in the Virtual Control
runbook are separately approved.

## Why this is a separate service

`brilliant-vc-pilot.service` and its approval are intentionally limited to a
600-second bootstrap observation. That approval says
`hosted_light_permitted=false`; its marker, generated files, and writable roots
are one-shot. Extending its timeout or reusing its consumed marker would erase
the distinction between proving an isolated Brilliant Virtual Control and
writing a native `LIGHT` into that control's home graph.

The coordinated session therefore gets a different unit, approval schema,
marker directory, service-owned evidence root, and absolute deadline. It still
uses the same official VC identity, root-owned runtime credential handoff,
schema-5 firmware pins, four-process topology, and direct non-root uWSGI
command.

## Chosen lifecycle

The reference unit keeps uWSGI Emperor as systemd's `ExecStart` main process
and runs one non-root coordinator as `ExecStartPost`:

```text
non-root exact app/vendor validator
  -> root stock mover
  -> non-root session preparer
  -> non-root uWSGI Emperor + four-process VC
  -> non-root coordinator
       -> validate the consumed approval and exact VC2 input ledger
       -> validate the Emperor PID/executable/UID and isolated socket
       -> wait for two matching scoped topology observations
       -> record VC3 and VC4 in a new session ledger
       -> monitor the exact Emperor PID while hosting one HA-backed LIGHT
       -> delete the LIGHT and prove two scoped absence observations
       -> fsync a redacted terminal result
  -> coordinator exits
  -> one-second active drain expires and systemd kills the whole cgroup
```

This shape preserves systemd as owner of the Emperor and every vassal. It does
not add a Python parent that launches firmware, and it avoids a multi-unit
target whose children could outlive the one-shot approval. systemd 250 executes
`ExecStartPost` commands serially after a successful `ExecStart`, treats a
non-zero post-start result as service failure, and applies `TimeoutStartSec` to
the complete activating phase. `RuntimeMaxSec` begins only after activation,
so it is a one-second post-coordinator drain, not the primary session budget.

## Exact time budget

The approval has one absolute 2,520-second budget measured from
`approved_at_s`:

| Phase | Maximum | Contract |
|---|---:|---|
| Approval consumption, preparation, and direct launch | 60 s reserve | Any delay reduces the bootstrap window; time is never added back. |
| Bootstrap and two stable topology observations | 600 s | Must finish with at least the full pilot budget remaining. |
| One-light lifecycle, including cleanup | 1,800 s | The existing pilot reserves its final 120 s for disconnect, deletion, and two absence reads. |
| Internal failure/serialization reserve | 60 s | Coordinator must finish by the approval deadline. |
| systemd emergency margin | 60 s | `TimeoutStartSec=2580`; it is not usable pilot time. |
| Post-success drain | 1 s | `RuntimeMaxSec=1` stops Emperor after the coordinator has proved cleanup. |

The coordinator refuses to register a light unless at least 1,800 seconds
remain before the approval deadline. A slow bootstrap therefore fails closed
instead of shortening the required acceptance window.

## One-shot approval

The implemented schema is exact and rejects extra fields or JSON type
confusion. Its deliberately non-usable tracked example is
[`coordinated-session-approval.example.json`](coordinated-session-approval.example.json).
It contains:

- schema version, `approved=true`, issuance time, safe run ID, Office panel,
  pinned firmware, and purpose
  `coordinated_virtual_control_single_light_session`;
- exact aggregate, bootstrap, and pilot limits (`2520`, `600`, and `1800`);
- the four-file runtime credential-bundle SHA-256;
- the SHA-256 of a sanitized VC0-through-VC2 gate ledger;
- an optional SHA-256 for a fixed-path MQTT password file;
- canonical stable UUID, display name, room ID, physical Office device ID,
  broker host/port, and optional broker username; and
- `hosted_light_permitted=true` together with
  `physical_device_actions_permitted=false`,
  `slider_binding_permitted=false`, and
  `panel_gestures_permitted=false`.

The root-issued source is
`/run/brilliant-vc-session-approval/session-approval.json`, root:`brilliant-vc`
mode `0640`, in a root-owned mode-`0750` directory. The same pinned stock mover
renames it to `session-approval-consumed.json` before repository code runs. The
non-root preparer requires the source to be absent. Start-phase validation
requires issuance within ten minutes; later coordinator checks allow only the
same consumed marker and only until its absolute session deadline.

The approval does not authorize agent-generated commands or gestures. MQTT
command publication is reachable only from an operator's later native input;
until separate gesture permission exists, the live session itself remains
prohibited.

## Fixed input and output roots

Inputs are immutable for the session:

```text
/data/brilliant-vc-session-input/             root:brilliant-vc 0750
  gate-ledger-vc2.json                        root:brilliant-vc 0640
  mqtt-password                               root:brilliant-vc 0640  # optional
```

The input directory has no other entries. The ledger must contain immutable
VC0, VC1, and VC2 pass records for the approval's run ID, no VC3-or-later
record, and only sanitized evidence. The approval binds its bytes. The optional
password is bounded, non-empty UTF-8 and is never printed; its presence and
digest must match the approval exactly.

Outputs are service-owned and empty before every attempt:

```text
/data/brilliant-vc-session/                   brilliant-vc 0700
  gate-ledger.json                            brilliant-vc 0600
  topology.json                               brilliant-vc 0600  # sanitized summary
  monitor.jsonl                               brilliant-vc 0600
  session-result.json                         brilliant-vc 0600
/run/brilliant-vc-session/                    brilliant-vc 0700
  single-light-pilot.lock                     brilliant-vc 0600
```

No raw journal line, room ID, certificate, bootstrap blob, broker password, HA
token, or unredacted VC/Office ID may enter an output. `topology.json` contains
only a redacted owner, counts, peripheral IDs/types/roles, requested-room
presence, and the SHA-256 of the complete in-memory normalized snapshot. A
failed attempt leaves its consumed marker and non-empty roots for review; no
automatic cleanup or retry is allowed.

## Coordinator state machine

1. Revalidate the consumed approval, account, runtime credentials, fixed
   inputs, empty outputs, absolute deadline, and exact uWSGI PID file.
2. Require `/proc/<pid>/exe` to be the pinned uWSGI binary, the real/effective
   UID and GID to be `brilliant-vc`, and the PID start time to remain stable.
3. Open only `/run/brilliant-vc/server_socket`; reject the physical Control
   socket, symlink escape, or any alternate root.
4. Repeatedly make bounded scoped reads until the bus owner is the approved
   DeviceType-6 VC, the room catalog contains the approved room, and exactly
   one VC-owned type-19 `device_config_peripheral` exists. Require two complete,
   byte-equivalent normalized snapshots ten seconds apart.
5. Extend the bound VC2 ledger with VC3 (launch/isolation) and VC4
   (stable topology/readiness) pass records and atomically persist it.
6. Start redaction-safe exact-PID monitoring and acquire the process-wide
   service-owned light lease. The monitor exclusively creates its JSONL,
   binds its first and every later sample to the same PID/start time/name, and
   reports rather than killing the bus so deletion can run first.
7. Host exactly one stable `ha_vc_<uuid>` `LIGHT` through the existing
   retained-state-fenced MQTT control-plane mapping. Do not bind a slider and
   do not synthesize a native push.
8. On normal deadline, broker loss, signal, monitor abort, or exception, delete
   the light while its VC bus is still alive and require two scoped absence
   reads 30 seconds apart.
9. While the light is active, revalidate the consumed marker, absolute wall
   deadline, and exact Emperor identity once per second. A guard or monitor
   failure requests pilot stop and waits for cleanup before returning failure.
10. Fsync a terminal result containing only approval/run digests, redacted IDs,
    gate status, monitor counters, cleanup outcome, and failure class. Return
    non-zero unless cleanup was proven and no guard failed.

If systemd's emergency timeout or an external stop kills the bus before local
deletion can finish, the session is failed and supported official removal plus
two later graph snapshots are mandatory. `ExecStopPost` may report that state,
but it must not claim cleanup by querying an already-dead bus.

## Implementation status

1. **Implemented:** `session_approval.py` validates the exact one-shot schema,
   permissions, digests, identity, broker plan, start freshness, and absolute
   active deadline.
2. **Implemented:** `runtime_prepare.py` has a typed approval-validator
   injection point; its normal CLI remains pinned to the bootstrap-only schema.
3. **Implemented:** `session_prepare.py` validates the exact VC2 ledger,
   optional password digest, disjoint empty roots, runtime credentials, and
   calls only captured `run.pre_exec`.
4. **Implemented:** `staged_runtime.py` rejects extra, linked, writable,
   mis-owned, or digest-drifted source/vendor files. The service mounts the
   staged app, MQTT vendor tree, and manifest read-only.
5. **Implemented:** the scoped topology probe and live light lifecycle are
   public library entry points; the standalone pilot CLI still requires root
   for `--apply`.
6. **Implemented:** `session_coordinator.py` performs exact process binding,
   stable topology, VC3/VC4 progression, concurrent monitoring, the one-light
   lifecycle, active guards, cleanup, and private terminal evidence. It never
   records VC5 pass.
7. **Implemented:**
   [`deploy/brilliant-vc-session.service`](../../deploy/brilliant-vc-session.service)
   is reference-only, has no `[Install]`, uses the separate marker/roots,
   remains activating while the coordinator runs, and gives systemd a
   60-second emergency margin. The exact staged manifest is
   `deploy/brilliant-vc-session-app-manifest.sha256`.
8. **Implemented and validated off-panel:** the frozen source set passed the
   captured-ARM no-start preparer, exact direct-uWSGI option/`--pidfile` parse,
   production-default source/vendor staging gate, and systemd 252 unit verify.
9. **Pending live review only:** stage without enabling and run Office
   `systemd-analyze verify`. Installation/start still require separate approval.

The systemd lifecycle assumptions are from the panel's systemd 250-compatible
[`systemd.service(5)`](https://www.freedesktop.org/software/systemd/man/250/systemd.service.html):
post-start commands are part of activation and `TimeoutStartSec`, while
`RuntimeMaxSec` applies after activation.

## 2026-07-14 validation evidence

All executable validation below was isolated from the Office panel unless a
step is explicitly marked read-only:

1. In a network-disabled ARMv7 container, the captured firmware tree was
   mounted read-only, no physical devices were mounted, all identity material
   was synthetic, and unexpected child-process primitives were guarded. The
   shared no-start preparer ran as synthetic `12345:12345`, consumed its test
   approval, generated exactly one `message_bus.ini`, four flagfiles, and two
   hosted-startable configs, enforced `0700` directories/`0600` files, and
   reported `emperor_started=false`.
2. The captured uWSGI `2.1+brl1` ARM binary parsed the session service's exact
   direct command plus the inspection-only `--show-config` flag as non-root
   `12345:12345`. Its normalized config included the private Emperor directory,
   vassal include, stats socket, PID file, socket mode, termination behavior,
   and log path. It contained no stock `emperor.ini`, zygote, fork-server, or
   delegated-launch option. Because this uWSGI's `--show-config` continues into
   runtime, an empty Emperor was deliberately bounded to five seconds and then
   terminated; no vassal, network, or device was available.
3. A production-default staging rehearsal copied only the 14 reviewed app
   files, 19 reviewed `aiomqtt`/Paho files, and the committed manifest into an
   ephemeral `/var/brilliant-vc` tmpfs. The non-importing validator reported 14
   app files, 19 vendor files, no process start, and manifest SHA-256
   `ff7adcf390b97542ba3c2d37110fe833168fb9ce54345be2dca28ff2408f3a15`.
4. The exact unit passed `systemd-analyze verify` under systemd
   `252.39-1~deb12u2`. This is a syntax/lifecycle compatibility check, not a
   substitute for the panel's systemd 250 verifier.
5. A read-only Office check confirmed systemd `250 (250.5+)`,
   `/usr/bin/python3.10`, `/usr/bin/mv.coreutils`, captured uWSGI,
   `process-default.ini`, and `/usr/bin/sha256sum`. The native message bus, UI,
   and `brilliant-mqtt` remained active. Their systemd cgroups use
   `/system.slice/<unit>.service`; the message-bus PID's unified cgroup entry
   exactly matched that shape. The session unit, staged manifest, input/output
   roots, approval root, and `brilliant-vc` account were not created.

These checks close the repository and captured-binary compatibility work. They
do not prove official provisioning, live bootstrap, home assignment, online
rendering, picker admission, binding, gestures, command routing, restart
behavior, WAN independence, or supported removal.

## Remaining live proof after implementation

Repository implementation and off-panel tests cannot establish official
provisioning, target-home bootstrap, cross-panel rendering, slider-picker
visibility, HA command/state round trips, WAN independence, or supported
removal. Those remain ordered operator gates. The user's existing observation
proves that legacy injected HA lights render in Backyard and appear in the
slider picker, but their offline state does not prove this isolated VC session
or end-to-end control path.
