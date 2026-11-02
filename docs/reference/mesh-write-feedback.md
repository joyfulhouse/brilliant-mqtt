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

`mesh_requested` describes a target, not an observed value. It appears only
when the existing pending record is armed after the bus call returns or its
caller deadline expires. There is no requested phase while waiting for the
device write lock. Brightness-only requests contain only the normalized
`intensity` target; they do not invent an `on` target.

During `pending`, primary `state` stays JSON `null` (unknown), while brightness
and auxiliary readings keep their existing observed values. Matching mirror
observations alone do not confirm a write early: the unchanged resolver waits
80 seconds and requires a recent matching observation. Confirmation then
publishes the observed primary value and returns feedback to `idle`.
**Idle means no tracked confirmation, never successful completion.**

A contradicting observation publishes `contradicted` with the observed primary
state. The exception is a write whose native RPC is still running past the
caller deadline (see below): its differing observations are ignored. A bounded
write rejection publishes `failed` with the last observation. With no fresh
matching observation at expiry, a terminal `unconfirmed` document keeps
primary `state` null and retains ordinary auxiliary readings. The next bus
observation resumes normal observed state and idle feedback.

A newer command synchronously revokes the old record. Existing observation
publishes during the replacement bus call can show `superseded` with an empty
request; otherwise the old retained request can remain during that in-flight
window. On completion, only the newest command can arm pending or publish
failure. No feedback publish or new await precedes the native bus call.

On startup or leadership acquisition the agent publishes `idle`, `{}`, and
`null`, unrelated to any request from a prior process. This does not establish
that a pre-crash request completed. Withdrawal and session teardown revoke
and join feedback publications; an ex-leader sends no terminal clear or shared
mesh offline message. The separate pre-existing confirmation-timer teardown
sweep remains a follow-up.

## Writes that outlive the caller deadline

A write still running at the bus adapter's 5 second caller deadline is
unresolved, not failed. The native RPC keeps running detached, and the
bridge arms normal pending feedback with a placeholder receipt. The admission
ticket that carried the write later reports the RPC's outcome (issue #225):

- **Still running:** the mirror still carries the pre-write value, so a
  differing observation proves nothing and is ignored. A matching
  observation does not count toward confirmation either, because it comes
  before any acceptance. Feedback stays `pending`, and unless a late
  acceptance re-arms the window, it closes `unconfirmed` at expiry.
- **Late failure:** publishes `failed` with the observed state immediately,
  as an immediate rejection does.
- **Late acceptance:** re-arms a full 80 second window with a new
  `mesh_write_deadline`. Observations made before the acceptance do not
  count toward confirmation. After this, a contradicting observation
  publishes `contradicted` as usual.
- **Late timeout or cancellation:** reveals nothing about actuation. The
  original deadline stays, and contradiction handling resumes.
- **Native timeout before the deadline:** the panel library's own "No
  response received!" has already settled. Pending feedback is armed and a
  contradicting observation publishes `contradicted` on the next poll.

A late outcome for a write that was superseded by a newer command, or that
belongs to a withdrawn session, is ignored.

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
