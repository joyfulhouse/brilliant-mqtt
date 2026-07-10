# Synthetic CONTROL Protocol Gate — Result

**Date:** 2026-07-09
**Plan:** `docs/superpowers/plans/2026-07-09-synthetic-control-protocol-characterization.md`
**Design:** `docs/superpowers/specs/2026-07-09-ha-virtual-brilliant-control-design.md`
**Outcome:** **STOP-CLEAN-ROOM**

## Question

Can an off-panel, clean-room software `CONTROL` enroll into a Brilliant home over
the LAN peer-provisioning path (`search_for_available_homes → knock_on_home →
request_provisioning_with_code → join_home`) **without** a proprietary runtime
dependency, a Brilliant-issued device credential, or a device-unique
cryptographic input? A fully-known, standard-primitive profile would authorize
writing the guarded enrollment plan. Any device-unique key/certificate/attestation
requirement is a STOP.

## Evidence method

All observations are **read-only** and value-free. No message-bus write, peripheral
registration, pairing request, account mutation, enrollment, service restart, or
panel filesystem write was performed. Evidence sources:

- **Structural metadata** from the pilot panel's compiled provisioning modules,
  read via binary symbol/string extraction (no import, no instantiation, no
  socket): `peripherals/bootstrap/device_provisioning_client…so` and
  `peripherals/bootstrap/bootstrap_peripheral…so`.
- **Readable Thrift interface types**: `thrift_types/bootstrap/ttypes.py`
  (`BootstrapPeripheralInterface`, type 37).
- Prior read-only introspection of the provisioning/bootstrap/lease stack
  (see the project mirror-PoC record).

No raw device IDs, home IDs, certificates, keys, or tokens are reproduced here.

## Profile

| Fact | Status | Evidence (value-free) |
|---|---|---|
| Discovery service | **known** | `_init-brilliant._tcp.local.` / `_brilliant._tcp.local.` (discovery types; prior LAN obs). Live multicast re-confirmation not run from the analysis host (reaches the panel via routed path, not L2). |
| Provisioning method set | **known** | `search_for_available_homes`, `knock_on_home`, `request_provisioning_with_code`, `join_home` all present as `DeviceProvisioningClient` symbols. |
| Thrift type graph | **known** | `BootstrapPeripheralInterface` (type 37): `available_homes`, `authentication_code`, `target_home_id`, `server_authentication_token`, `pivot_home`, `out_of_band_data`, `enable_out_of_band_provisioning`. |
| Framing | **unknown** | No loopback capture. A conformance capture requires a native provisioning client, which cannot be constructed clean-room (see commitment/attestation below), so framing was not — and cannot cleanly be — characterized. Moot given the STOP. |
| Protocol | **unknown** | Same as framing. Moot given the STOP. |
| TLS | **known (peer-cert TLS present)** | Commitment path consumes `get_peer_tls_info` / `tls_info`; provisioning peers exchange TLS certificate fingerprints. |
| Commitment | **BLOCKER — non-standard, device-certificate-bound** | `DeviceProvisioningClient._get_secret_and_commitment_for_home` derives the commitment from a `random_secret` **combined with `certificate_fingerprint_base64` / `get_my_certificate_fingerprint` / `peer_certificate_fingerprint`**. The peer *verifies* it ("Failed to verify commitment from …"). It is **not** a pure function of two synthetic byte strings — it consumes the device's own mTLS certificate fingerprint, a device-unique input outside the two synthetic inputs the classifier admits. Does not match `{hmac-sha256, sha256-client-server, sha256-server-client}`. |
| Hardware attestation / device credential | **BLOCKER — required** | Enrollment is bound to the device's mTLS certificate identity (fingerprint commitment above). `BootstrapPeripheral._join_home` enforces authorization: symbols `UNAUTHORIZED`, `InvalidTokenError`, and *"Received improperly signed token. Rejecting out-of-band home assignment."* All three provisioning routes require a Brilliant-issued credential: **SERVER** → `server_authentication_token` (cloud/account-minted), **PEER** → verified device certificate fingerprint + per-device authorization, **OUT_OF_BAND** → a *properly-signed* bootstrap token (`decode_token` / `signature`; loaded from a bootstrap-token file, app/cloud-minted). No unauthenticated / test / bypass provisioning mode exists. |
| Removal path | **unknown** | Not investigated; moot given the STOP. |

## Verdict: STOP-CLEAN-ROOM

Blockers:

1. **commitment** — device-mTLS-certificate-fingerprint-bound with a random secret; not a standard primitive over two synthetic inputs.
2. **hardware_attestation / device credential** — every provisioning route (SERVER, PEER, OUT_OF_BAND) requires a Brilliant-issued credential (server auth token, verified device certificate + per-device authorization, or a Brilliant-signed bootstrap token). A clean-room control cannot mint or present any of them.

## Interpretation

The clean-room synthetic-`CONTROL` architecture (design §3.1) is **not viable** as a
redistributable, credential-free product. A software control cannot complete the
LAN peer-join because the pairing commitment is bound to a device mTLS certificate
the peer verifies, and `_join_home` additionally requires a properly-signed
authorization token. This is the **same identity/authorization wall** the earlier
`VIRTUAL_CONTROL` self-bootstrap investigation hit (a Brilliant-signed,
account/app-minted provisioning credential), reached here through the *local*
peer-provisioning door rather than the cloud one. Both doors gate on a
Brilliant-issued device identity.

Per the plan, this result **ends the clean-room architecture** unless new read-only
evidence resolves every blocker. It does **not** authorize a live pairing action.
The protocol-lab tooling remains a value-free instrument for re-confirming this
finding or characterizing any future firmware that relaxes the certificate/token
requirement.
