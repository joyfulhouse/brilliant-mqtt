# Outstanding issue investigation, 2026-10-22

This audit distinguishes verified code defects from completed work and work
requiring live hardware or infrastructure evidence. Issue numbers refer to
`joyfulhouse/brilliant-mqtt`. Code fixes require regression tests and review;
an issue is not resolved merely because its symptoms have a plausible cause.

## Work already delivered

| Issues | Evidence |
| --- | --- |
| #147, #148 | PR #154 delivered the broadcast/ARP and Wi-Fi qualification runbooks. Documentation completion does not establish infrastructure recovery. |
| #149, #150 | PR #158 and #163 implemented priority admission, maintenance fairness/backoff, and supersession of unissued intents. Current admission/scheduling/repair suites pass 70 tests. |
| #165 | PR #168 wrapped managed updates in release admission and journaled canary rollback. Release-identity and canary regressions verify the safeguards. |

## Corrections included in this change

The following table records the root causes and contracts implemented by the
fixes, regression coverage, and documentation in this change. The original
rollback failure in #214 remains open as described below.

| Issues | Root cause and required behavior |
| --- | --- |
| #212 | Address repair uses the old password, while password repair uses the old address. Verify the new pair against the existing SSH identity and save both atomically. Partial manual uninstall can separately leave an invalid snapshot; retain the baseline safety refusal. |
| #213 | Successful onboarding retains a soak baseline. The next update correctly requires explicit finalization, but reports an unrelated transaction as recovering. Explain the retained-baseline requirement without deleting the rollback guarantee automatically. |
| #214 | Onboarding waits indefinitely for the shared operation lock and bypasses the recovery deadline when settling an existing journal. Bound both waits. The original rollback failure needs its redacted phase and typed failure evidence; do not infer it solely from a running agent. Recursive SFTP also performs local file I/O on HA's event loop. |
| #198, #199 | Both reboot guards read arbitrary state paths without a size/type limit, measure bad-state expiry with wall time, and discard future timestamps. Reject unsafe files, measure expiry monotonically, and preserve the rolling cap across clock corrections. |
| #205, #208 | Guard boundary/error tests are incomplete; a refused Wi-Fi reboot logs repeatedly every poll. Pin the boundaries and suppress duplicate refusal notices while retaining storage retries. |
| #201 | Scene delivery selects all in-memory records even when their own snapshots have not persisted. Publication must be gated by durable coverage for both events and results. |
| #194 | Retained-ledger loading uses an unbounded executor read. Bound loading and retain ownership of stalled work so retries cannot accumulate threads; sweep only inactive temporary files. |
| #197, #200 | Scene tests assume a loop turn or a fixed startup delay implies persistence and delivery. Wait on observable conditions and test delayed writes. |
| #189, #184 | Ignored `transition` keys disable safe command merging; teardown can discard a merged replacement with insufficient diagnostics. Preserve setter semantics and cancellation behavior while adding coverage. |
| #180 | Watchdogs predating 0.10.1 have no VERSION file. Any fallback must be restricted to a verified same-panel, strict legacy bridge version; ambiguous identities must still be refused. |
| #188 | Auto-admission auditing precedes baseline capture. Audit text must distinguish admission from actual deployment. |
| #191 | The manager omits `snapshot_probe_failed` from safe operator details. Expose a sanitized retry-after-unit-settles hint. |
| #202 | Stopped legacy recovery lacks combined-watchdog and stranded-unit cases. Verify both and document safe cleanup. |
| #203, #204 | Behavior-preserving helper/constructor cleanup remains; preserve the existing safety assertions. |
| #102, #114 | Add supervisor reader-death reconnect coverage and an explicit finite aiomqtt timeout contract test. |
| #85, #137 | Fixed connect-failure backoff, missing persistent watchdog shutdown markers, and indefinite fenced bus starts need bounded retry/escalation policies with reset tests. |
| #81 | Mesh discovery uses a common device identifier. Per-peripheral device grouping must retain entity identities and remain leader independent. |
| #179 | Existing policy deliberately makes a strictly greater known release ordinal authoritative. Clarify that version-string restrictions apply to unknown-ordinal admission. |
| #169 | A restored onboarding baseline can outlive both its active journal and its HA owner. Add explicit fresh-credential reauthorization against the retained SSH identity, fleet-wide ownership checks, and confirmed finalization. Persist nonsecret origin metadata so an interrupted ownerless finalization requires fresh authorization on retry. |

## Evidence or design still required

| Issue | Completion requirement |
| --- | --- |
| #113 | A poison-frame quarantine changes QoS-1 replay, durability, and capacity policy. Define an explicit retained/quarantine contract before changing delivery; current payloads have no demonstrated permanent broker rejection. |
| #142 | Conservative same-value mode confirmation is an accepted limitation pending request-relative evidence. Do not infer execution from a stale mirror. |
| #143 | The read-only-filesystem reboot residual needs independently observable lease revocation. A failed marker write alone cannot prove it. |
| #153 | The event-reflection proposal is delivered, but upstream implementation and qualification remain. The issue expressly requires more than a documentation merge. |
| #155 | i350/aarch64 support needs native-library/runtime diagnostics and pilot qualification. Removing the architecture gate alone does not prove compatibility. |
| #164 | Release readiness requires designated-panel deployment, rollback, resource, and soak evidence, beyond synthetic code tests. |
| #173 | The observed TCP return-path failure needs correlated captures and an evidenced infrastructure repair observed beyond the recurrence window. Agent changes cannot establish that recovery. |
| #214 | The bounded-wait and local-upload defects are fixed separately. Keep the original `restart_recovery; recovery_failed` report open until redacted integration diagnostics identify the journal phase and typed failure. A running agent alone does not prove rollback or ownership recovery. Preserve its journal and retained baseline. |

## Verification

The complete agent gate passes: 2,803 tests passed and one skipped, with Ruff,
formatting, and strict mypy clean across 175 source files. The complete
integration gate passes: 2,011 tests passed and six skipped, with lint,
formatting, and strict mypy clean across 83 files. The tracked payload was rebuilt
from source, and parity checks pass. Independent specification and quality
reviews cover each implementation group and the interactions between them.

Regression tests include real subprocess termination with cancellation-resistant
cleanup, watchdog SIGTERM log markers, hostile state-file swaps, held filesystem
writes and reads, overlapping temporary-file namespaces, and restored-baseline
finalization across interruption and fresh authorization. These establish local
software behavior; they do not replace the live qualification requirements.

The first PR CI run exposed another immediate scene-delivery assertion after
reconnect. A deterministic held-write variant reproduces it; the test now waits
for observable delivery and task completion before its exact count assertion.

Production panel credentials and access are not present in this checkout. No
live panel deployment, network repair, physical actuation, or soak is claimed by
this audit.

## #214 follow-up: October 22 reporter diagnostics

The reporter's 18:45 UTC comment and redacted attachment were reviewed after
PR #215. The attachment shows one active `pending_config_commit` transaction,
an affected panel's `armed` retained baseline with no recovery deployment ID,
and only the other panel in the fleet. The dismissed repair is not evidence of
storage cleanup. The original diagnostics do not identify the active transaction
or export its stored error; they cannot prove which panel owns that journal.

For an orphaned onboarding transaction, `pending_config_commit` first checks
ownership and then reconstructs recovery artifacts and connects with the stored
SSH pin. Only afterward does rollback request `restore_requested`. **If the active
journal belongs to the affected baseline**, the surviving `armed` state narrows
the investigation to a failure before durable restore intent (including failure
to persist intent), or an attempt that did not reach that boundary. The attachment
does not establish that association or an SSH, storage, lookup, or panel-side root
cause. A running agent and retained MQTT entities do not establish commit
or restoration. No raw private storage or panel logs were obtained.

A reproduced observability defect is fixed separately: diagnostics omitted the
active transaction identity and any already stored error. Diagnostics now identify
its `name`, `panel`, `operation`, and allowlisted `last_error.stage`/
`last_error.code`. Unknown stored tokens are exported as `unknown`; passwords,
addresses, keys, snapshots, and remote exception text are not included in these
new fields. Error capture is unchanged: a failure before an error is durably
recorded can still show `last_error: null`. Previously absent errors cannot be
reconstructed retroactively.

Regression tests reproduce missing active identity/error evidence and exercise a
real stored pending journal with an armed filesystem baseline. A connection
failure leaves that baseline armed and its uncaptured error null; diagnostics do
not authorize rollback or ownerless finalization. This follow-up performs no new
recovery writes and changes no recovery timeout or mutation policy.

The original #214 failure remains unproven. Preserve the execution journal and
both retained baselines. Obtain updated diagnostics and the existing HA-side
repair/log code, with secrets redacted, before selecting a recovery action.
An armed ownerless baseline is ineligible for the restored-only finalization
path. Existing release admission remains mandatory. This follow-up makes no
production access, deployment, recovery, or soak claim.
