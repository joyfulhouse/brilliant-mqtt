# Live outstanding-issue investigation, 2026-10-23

This follow-up records read-only production observations and release-readiness
checks. It does not qualify a release, repair a network, or authorize fleet
rollout. Private identities, topology, credentials, logs, and packet transcripts
are excluded. All eight residual issues remain open.

## Repository and candidate state

- PR #215 remains open and mergeable at
  `e4d3263d9079916672b6b627e3591ceadd673d59`; all eight hosted checks passed.
- Main and published tag `v0.11.2` point to
  `d990adb75bf725798726fcd6d67948c337fc6aa3`. PR #215 is not that release.
- An isolated worktree preserves the original dirty checkout. Blob comparisons
  found all 133 inspected application, documentation, configuration, and test
  paths in the dirty set match historical main commit `c52be4d` (the delivered
  `0.10.2` work). This is selected-file equivalence, not equivalence of the whole
  checkout. No new product implementation was recovered from that dirty set.
- Fresh validation of PR #215 passed: agent Python 3.10, 2,803 tests passed and
  one skipped; HA Python 3.14, 2,011 passed and six skipped. Ruff, formatting,
  strict mypy, and rebuilt payload parity passed. The agent suite passed after
  rerunning with `umask 0022`; its initial run under inherited `0077` failed
  permission-sensitive fixtures, without demonstrating a product regression.
  The HA suite passed under its inherited environment.
- PR #167 remains open and was not modified. Its reviewed documentation target
  is the existing tagged release, which excludes #215. Its agent CI check failed
  on an asynchronous scene-event assertion. Its target and checks must be
  refreshed for the eventual final candidate.

### Deployment decision: not ready

PR #215 changes component code while retaining version `0.11.2` and release
ordinal `4`, already assigned to the published release. The admission policy
rejects different digests at the same known ordinal. Passing CI is not an
admission override. No release metadata, selector, journal, or baseline was
changed to bypass that refusal.

The candidate component digests are:

| Component | SHA-256 code digest |
| --- | --- |
| Bridge | `1def8f3281dbc8a4e7214852cc3ac6264f8fe6cd363b14f887710bedba0f985c` |
| Wi-Fi watchdog | `f432adaf3a2be5e5d2e34c102a23d44a063672b6ce36103f223f7aae6bada706` |
| Bus watchdog | `c47bc8968f7db96698af9a4bfcc5164efa4a721fe4a1c0eccbb3b4376d11afce` |

Normal release preparation needs a reviewed distinct release identity, a
synchronized final bundle, and exact loaded-HA parity. A branch build must not be
represented as the published release. The current pilot's admission has not been
executed; a version label alone does not establish its stored ordinal.

## Pilot baseline and #164

The designated pilot was reached through its existing pinned SSH identity.
The initial read was at approximately 01:56 UTC:

| Observation | Result |
| --- | --- |
| Architecture and interpreter | `armv7l`, Python `3.10.9` |
| Firmware | Active `v26.09.23.1`; OSTree rollback `v26.09.09.1` present |
| Installed agent label | `0.11.2` |
| Layout | Legacy fixed directories; no `current` selector or `releases` directory |
| Bridge / bus watchdog / native bus / native UI | Active; each reported zero systemd restarts |
| Wi-Fi watchdog | Unit absent, inactive |
| Runtime wireless power saving | Off |
| Bridge resources | About 20.1 MB reported `MemoryCurrent`; 96 MiB limit, 20% CPU quota, nice 10 |
| Host resources | 267 MiB available memory; about 4.5 GiB free on `/var`; load approximately 1.95/2.07/1.94 |
| Installed HA integration | Reports `0.11.2` on HA Core `2026.10.2` |
| Provisioning diagnostic | Count zero, phase null; installed diagnostic lacks a retained-baseline listing |

These resource values are a snapshot, not a responsiveness benchmark or soak.
An `update_manager` process exists; its presence alone neither proves nor excludes
an active firmware update. Physical recovery access, native-control behavior,
HomeKit fallback, and absence of an ongoing update/DFU/calibration have not been
fully qualified for mutation.

A read-only invocation of the canonical `code_digest` function hashed the fixed
trees without invoking legacy identity enrollment. The bridge process's
`PYTHONPATH` selects the fixed app/vendor trees, and the watchdog selects its
fixed package tree. Filesystem digests were:

| Component | Observed pilot SHA-256 code digest |
| --- | --- |
| Bridge | `2eb2e0736f3bb924eb41c4597eaf2ed28a48eff34c65b50f507d4bc4678379b8` |
| Bus watchdog | `ffe26bb49fbcbf5e42dffba063096d914461a8b47bd8d2997a39055f3b271c70` |
| Wi-Fi watchdog | No code tree found |

These differ from the proposed candidate. Hashing files does not independently
identify already-loaded Python bytecode or prove the complete integration bundle
matches HA. No exact incumbent release ordinal was established.

The rollback directory exists and a bounded listing found no files within its
first two levels. Neither that listing nor idle provisioning diagnostics proves
absence or integrity of HA's retained records. No records were removed or
finalized. The immutable-layout manifest check cannot qualify this legacy layout;
the supported legacy path and complete named baseline must be verified before
mutation. The standard legacy identity collector can write enrollment records
and was deliberately not run as a read-only probe.

No candidate was deployed. Upgrade, fresh deployment-correlated health, live
rollback, response measurements, and the minimum one-day final-byte soak remain
unperformed. #164 remains open, including the network dependency below.

## #173: current neighbor-resolution failure

The previous issue discussion already localized the immediate failure to
unresolved broker-host neighbor entries; temporary normally-aging neighbor
entries restored connections but did not repair ARP delivery. Current observations
reproduce the failure without repeating that intervention.

At approximately 01:57 UTC, three affected clients lacked resolved broker-host
neighbor entries. The affected clients and a working comparison associate with
the same AP according to the controller. Controller signal snapshots ranged
from -48 to -57 dBm; these values are not proof of reliable delivery. Runtime
power saving was off on the two affected panels reached through existing SSH
pins; their optional Wi-Fi watchdog units were absent. Their bridge services were
active with zero restarts, and available logs contained repeated MQTT timeouts.
Some journal files were unreadable by the installed journal reader, limiting log
coverage. No timeout rate was inferred from that partial log output.

### Bounded passive captures

Overlapping captures ran approximately 01:58:35-01:59:21 UTC. Each had a 45-second
budget, a 1,000-packet cap, a 96-byte snapshot limit, and a filter limited to ARP
and MQTT TCP connection-control packets. Collection did not generate test load
commands, change a neighbor entry, or restart a service.

| Capture point | Observed result |
| --- | --- |
| Affected panel | 15 outbound SYNs across five source-port attempts; no inbound handshake reply and no captured ARP |
| Broker VLAN interface | 28 ARP requests from the broker host for that affected panel; no corresponding reply |
| Pilot, on another AP | 23 of those broker-to-suspect ARP requests observed |
| Capture integrity | All three reported zero kernel drops; none reached the packet cap |

The broker capture printed local time at UTC-07:00 while panel captures printed
UTC. The offset was verified and normalized for comparison; broker NTP reported
synchronized. No sub-millisecond cross-host timing claim is made. The pilot is
on the same VLAN but a different AP, so it is a delivery comparison, not an
equivalent same-AP control.

A later broker-namespace TCP snapshot showed three affected clients in
`SYN_RECV` and one same-AP comparison in `ESTABLISHED`. A separate bounded
20-second broker-namespace capture observed six incoming SYNs and 13 SYN-ACK
transmissions for the captured suspect, with zero kernel drops. That later
capture corroborates continued broker-side handshake attempts; it is not the
same observation window as the three simultaneous captures.

**Supported finding:** broker neighbor discovery remains unresolved while
ordinary connection retries continue. ARP requests are visible on broker egress
and at another wireless receiver but absent at the affected panel's capture
point. Broker-namespace SYN-ACK observation does not prove physical egress when
neighbor resolution is incomplete.

**Unresolved cause:** the exact loss point between broker egress, switching,
AP forwarding, and the affected panel's receive path. Missing trusted access to
the relevant AP/switch capture boundary prevents finer localization. Missing
host-key pins also prevented SSH collection on the same-AP comparison and a
third affected panel; verification was not bypassed.

The initial HA snapshot reported all 14 configured bridges online with no problem
flag. This is a status snapshot, not proof of a fresh exchange: direct TCP and
packet evidence takes precedence for this incident. No new retained-state
freshness guarantee is inferred.

No infrastructure change, station reconnect, neighbor seeding, or repair was
performed. There is no post-repair recurrence observation or command-delivery
qualification. #173 remains open.

## #214: new reporter evidence

After the prior audit, the reporter attached redacted integration diagnostics to
the issue. They report one active journal in `pending_config_commit`, an `armed`
retained baseline for the ownerless affected panel, and a separate `soak` baseline
for the working panel. The installed diagnostics do not supply the typed recovery
failure for the ownerless panel.

This establishes that one active journal is in `pending_config_commit`; its
association with the affected transaction still requires verification, because
the diagnostics do not identify that journal's panel. It does not identify why
recovery failed. This evidence is from the reporter's installation, distinct from
the local pilot. An `armed` baseline is not a verified restored baseline and cannot use
ownerless finalization. Preserve both the active journal and retained records.
The dedicated follow-up investigation owns code-level reproduction and any
justified diagnostic or recovery fix; #215's bounded waits and upload changes do
not establish the original failure's root cause.

## Remaining qualification boundaries

### #153: deployed upstream reflection behavior

The deployed diyHue image index is
`sha256:5501ec39c4bf73e8f5b750988151b05e1fd4a0ef1602d8c98a9eaf80c0674e75`.
The relevant deployed `services/homeAssistantWS.py`,
`lights/protocols/homeassistant_ws.py`, `services/stateFetch.py`, and
`HueEmulator3.py` files exactly match upstream diyhue/diyHue commit
`3ee0461711d6caad6ba1125360c5534c2047c87e`. The image has no revision label;
this is a four-file source match, not proof of whole-image Git provenance.

HA WebSocket events update an intermediate `latest_states` cache. The Hue light
objects are updated separately by `stateFetch.syncWithLights`: sequential refresh,
a 10-second sleep, then activity-dependent waiting in one-second steps for up to
300 iterations. Recent Hue API use shortens that wait. The 10-second sleep is
neither a measured universal cadence nor a command-latency bound. The adapter
still forwards commands directly through HA's WebSocket services.

Two passive observation windows of 45 and 30 seconds saw no qualifying natural
on/off/brightness changes among ten mapped lights. The second window explicitly
confirmed authentication and subscription. No Hue GET was issued by the observer
because no eligible event occurred. There are no reflection-latency samples or
panel-display measurements. Future GET-based measurement must account for its
own effect on the activity-dependent scheduler. No load was actuated and no
runtime hook or logging change was installed.

### Other evidence and limits

The deployed broker reports Mosquitto `2.1.2`. Its inspected configuration has no
explicit `max_packet_size`, `message_size_limit`, or `include_dir` directives.
This does not prove that every frame is accepted; no supported frame was shown
to be permanently rejected, and no poison frame was published.

Static review confirms that the watchdog's 300-second dead-writer retention
does not carry independent cross-sample lease-revocation history. A failed marker
write or a lease missing while the writer is alive cannot by itself establish
permanent revocation after that writer dies. No production fault was injected.
The mode adapter likewise exposes no typed request-relative no-op confirmation;
its bounded textual RPC receipt is diagnostic and gates no confirmation.

The available private inventory establishes no designated local i350/aarch64
test target. This does not prove that no other such panel exists; the issue
reporter's hardware is a separate installation. The ARMv7 pilot cannot qualify
that architecture.

| Issue | Evidence still required |
| --- | --- |
| #113 | Reproducible permanent rejection of a supported frame, followed by an explicit durability, ordering, capacity, replay, and recovery contract before quarantine changes |
| #142 | Request-relative execution/no-op evidence; a matching stale mirror remains insufficient |
| #143 | Independent watchdog-verifiable lease revocation across recovered-but-unrecorded writer death; no production read-only-filesystem fault injection |
| #153 | Qualified upstream event-driven reflection and separate event-to-cache versus physical/display measurements |
| #155 | Designated i350/aarch64 hardware, compatible native runtime and adapter diagnostics, followed by pilot qualification; architecture gate retained |

No issue was closed based on these observations. Further public updates must
retain the distinction between source readiness, live evidence, and completed
qualification.
