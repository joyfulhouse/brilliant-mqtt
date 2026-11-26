# Mesh write feedback

Mesh lights and switches expose request feedback alongside their existing
confirmed state. This change does **not improve physical response time**.
It makes the conservative confirmation window visible. A bus ACK acknowledges
transport acceptance; it does **not** prove physical actuation.

The shared retained `brilliant/mesh/<peripheral>/state` document always contains
three additional keys for mesh LIGHT/SWITCH primaries:

| Key | Meaning |
| --- | --- |
| `mesh_write_status` | `idle`, `pending`, `unconfirmed`, `contradicted`, `failed`, or `superseded`; none is a success flag. |
| `mesh_requested` | Normalized native target names and string values while pending; `{}` otherwise. |
| `mesh_write_deadline` | Fixed UTC Unix seconds while pending; JSON `null` otherwise. |

For example, a synthetic dimmer awaiting an OFF observation may publish:

```json
{
  "state": null,
  "brightness": 153,
  "mesh_write_status": "pending",
  "mesh_requested": {"on": "0"},
  "mesh_write_deadline": 2000000080.0
}
```

`mesh_requested` describes a target, not an observed value. It appears when
the current owner's confirmation window starts at actual native acceptance or
the unresolved caller deadline. There is no requested phase while waiting for
the device write lock. Brightness-only requests contain only the normalized
`intensity` target; they do not invent an `on` target.

During `pending`, primary `state` stays JSON `null` (unknown), while brightness
and auxiliary readings keep their existing observed values. Matching mirror
observations alone do not confirm a write early: the owner waits 80 seconds
and requires matching native evidence captured after acceptance, before the
deadline, and no more than 10 seconds old when expiry is handled. Confirmation
then publishes the observed primary value and returns feedback to `idle`.
**Idle means no tracked confirmation, never successful completion.**

A post-return contradicting observation publishes `contradicted` with the
observed primary state. Native rejection, including a late rejection, publishes
`failed`. A five-second caller timeout leaves the native RPC unresolved and
starts a provisional pending window. Matching mirror values cannot confirm
that unresolved request. Actual native acceptance starts a **fresh 80-second
window from the native return**, even after the caller timed out or its first
window expired. Native acceptance alone never confirms the target.

An actual native timeout or native cancellation is distinct from an unresolved
caller timeout. It retains the original fixed window and requires evidence
beyond the actual return fence; it cannot extend an expired window. With no
eligible matching evidence at expiry, `unconfirmed` keeps primary `state` null
and retains ordinary auxiliary readings. A genuinely later trusted primary
capture can resume normal observed state and idle feedback.

If no caller-timeout window exists yet, the settled inconclusive outcome
starts its original window at actual return. Later processing cannot renew it.

Captures carry immutable source, generation, sequence and capture time. Reads
use their start time, not their delivery time. Fields merge independently:
a power-only report cannot refresh an `on` match, and a newer power value
does not hide an older unshadowed `on` contradiction. Only explicit reliable
notification masks contribute target evidence; copied mirror fields and
unreliable masks cannot confirm. Complete reads can contribute versioned
missing-field evidence. Reconnect, removal and binding replacement revoke old
evidence and owners, so delayed reads cannot revive a retired request.

Canceling an already-issued command worker deliberately abandons its intent:
feedback immediately becomes `unconfirmed` and primary state unknown. That
command **never rearms**. Its physical RPC retains the shared device lock
through real completion; primary state stays unknown through that completion
and requires a genuinely later trusted primary capture to recover. A successor
owner inherits only this outstanding physical-completion hold, not the old
targets or evidence. This conservative tradeoff can keep state unknown even
when the abandoned RPC ultimately succeeded. It avoids a second independent
deadline timer. Cancellation before native issue creates no confirmation
window; ordinary caller timeout is not worker abandonment.

A newer command synchronously revokes the old intent. Existing observation
publishes during the replacement bus call can show `superseded` with an empty
request; otherwise the old retained request can remain during that in-flight
window. Only the current owner, command and exact native ticket may start a
window or publish its outcome. Native settlement reduces synchronously before
the caller resumes; a delayed caller cannot overwrite newer native evidence.
Feedback work adds no await before native issue.

On startup or leadership acquisition the agent publishes `idle`, `{}`, and
`null`, unrelated to any request from a prior process. This does not establish
that a pre-crash request completed. Withdrawal and session teardown revoke
timers and publications and join bounded cleanup; an ex-leader sends no
terminal clear or shared mesh offline message. There is at most one timer,
one publication worker and one publish child per owned peripheral. Obsolete
pre-handoff children are canceled and joined before a replacement publication;
a child that cannot settle within the existing bound triggers session recovery.

All mesh state uses one current-owner publication lane. It retains one current
terminal obligation and one replaceable latest ordinary state. A terminal
reason gets an MQTT attempt before ordinary idle debt; a new command or window
can revoke the old obligation. Failed attempts retain bounded debt for existing
change, reconcile or session-recovery triggers. This does not promise delivery
of every transient reason. A publish returning means acceptance under the
existing MQTT QoS contract, not subscriber receipt. A packet already handed to
the MQTT transport/broker cannot be retracted; ownership checks and cancellation
prevent obsolete local work but cannot undo retained packets already sent.

## Home Assistant diagnostic

Each mesh primary has one additional MQTT sensor named `<load name> Write
status`, with unique ID `<existing sanitized base>_mesh_write_status`. It is a
diagnostic entity disabled by default. Enable it in the entity settings to
inspect the status and the `mesh_requested` / `mesh_write_deadline` attributes.
It uses the same state topic and expires after 80 seconds without messages.
Existing primary entities and consumers ignoring the new keys retain their
prior behavior.

On rollback or downgrade, an older agent does not remove this diagnostic's
retained discovery configuration, so an orphaned entity can remain. Remove
it manually using the same convention as a stale peripheral's discovery:
publish an empty retained payload to
`homeassistant/sensor/<existing sanitized base>_mesh_write_status/config`.

The status template uses guarded lookups. Old agents with absent fields,
missing/null/unrecognized status, and malformed pending deadlines render
literal `unknown`. Pending deadlines must be finite non-boolean numbers and
at most 80 seconds in the future. A deadline at or before the consumer's UTC
time renders `unconfirmed`, including retained replay after a crash. Excessive
future skew renders `unknown`. The deadline is computed once when armed;
republishing never renews it. An injectable wall clock supplies only this wire
deadline; confirmation and observation age still use the monotonic clock.
Wall-clock skew can expire feedback early or make it unknown, but cannot
extend the resolver or prove actuation.

MQTT templates evaluate on messages, not continuously. The terminal expiry
publish handles the live case; `expire_after` bounds silence when the publisher
dies. Unavailable, unknown, expired, or idle feedback never implies success.

Template branch tests use stock Jinja2 with explicit HA input stubs:
`value_json` is a decoded mapping, `now()` returns an aware UTC datetime, and
`as_timestamp()` converts that instant to float Unix seconds. Jinja's real
`number`, `boolean`, and `mapping` tests and `tojson` filter are used. Jinja2 is
a development dependency only. **HA-core template validation not performed**;
these tests do not establish compatibility with HA's template environment.
