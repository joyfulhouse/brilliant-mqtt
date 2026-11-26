# Plugin metadata contract, version 1

This is Phase 2 slice 1 of [the component-plugin design](component-plugins.md)
([#283](https://github.com/joyfulhouse/brilliant-mqtt/issues/283)). It supplies
pure metadata checks in `plugin_manifest.py` and `plugin_catalog.py`, plus an
internal integration option, `plugins_enabled`, which defaults to `false` and
accepts only the boolean `true` as opt-in. `FleetManager.plugin_catalog` is
`None` while disabled and an empty catalog while enabled. No option is exposed
in forms, no plugin component is registered, and no catalog entries ship.
Neither state can install, enable, update or execute a plugin.

## Manifest

The manifest has exactly the fields illustrated in component-plugins section 3.
All fields are required, including empty capability arrays. Unknown fields at
any level fail validation. Input is strict UTF-8 JSON, at most 65,536 bytes,
with duplicate object keys, non-JSON numeric constants and nesting deeper than
16 levels refused. Lists have at most 128 items and string lists reject repeats.
Metadata strings are printable ASCII, nonempty and at most 2,048 characters;
IDs match `[a-z][a-z0-9_]{0,63}`. Schema and API versions must be integer `1`
(booleans do not count). Release versions are exact `major.minor.patch` decimal
triples without leading zeroes; aliases, ranges and prerelease tags are refused
in this first contract.

Artifact SHA-256 values are 64 lowercase hex characters. URLs require HTTPS,
no user information or fragment, and an exact reviewed scheme/host/port origin
for artifacts. Limits are 64 MiB compressed bytes, 256 MiB expanded bytes and
4,096 files, all positive integers. These are metadata ceilings, not an
extractor implementation. Entrypoints are canonical relative paths without
parent traversal, backslashes or command arguments. A later staging boundary
must prove the named entry is a regular file in the verified inventory.

Compatibility requires nonempty architecture and firmware-profile lists;
architectures are `armv7l` or `aarch64`, Python is `3.10`, and manager/bridge
APIs are `1`. A compiled-in `ValidationPolicy` supplies reviewed firmware,
runtime adapter, resource-profile and health-profile IDs and exact origins.
All its allowlists default empty. Tests use explicit fake profiles; these are
not shipped catalog entries or qualified firmware.

Capabilities are declarations, not runtime grants or recorded consent:

- `network_egress` names exact reviewed HTTPS origins; listeners remain empty.
- Microphone, camera and personal media must be `false` until their detailed
  capture and external cessation contracts exist. Display takeover is boolean.
- Provider accounts and plugin-delivered secrets remain empty.
- HA layout is `none` or `configured_dashboard`, reads may contain
  `configured_entities`, and actions remain empty.
- Bus reads may contain `own_panel_summary`; writes may contain
  `own_wired_on_intensity`. They declare semantic scope and create no bus path.
- Hardware is limited to `display`, `backlight` and `touchscreen`.
- Media disk bytes and lifetime seconds must both be integer zero.

Ownership surfaces are the screen surfaces listed in section 3; topics are
restricted to `plugins/<manifest id>`. Replacements have exactly `target`,
`kind`, `identity` and `firmware_adapter`, with only `systemd` syntax supported.
Repeated replacements fail. A replacement must match a complete reviewed
`Replacement` tuple; the default replacement allowlist is empty.

## Catalog

Catalogs are reviewed data bundled with an integration release. `parse_catalog`
is a pure validator of that already trusted input, not an external catalog
loader or signature verifier. Publisher/key IDs identify review provenance;
including an ID does not authenticate an external document. Third-party
signatures, expiry, monotonic persistence and common activation admission are
later slices. Do not expose this parser as approval of a downloaded catalog.

A version 1 catalog object has exactly:

| Field | Value |
| --- | --- |
| `schema_version` | Integer `1` |
| `revision` | Integer from 1 through 2,147,483,647 |
| `entries` | Array of exact release records |
| `revoked_manifest_sha256` | Array of manifest digests |
| `revoked_artifact_sha256` | Array of artifact digests |
| `revoked_publisher_keys` | Array of publisher/key IDs |

Every release record has exactly:

| Field | Value |
| --- | --- |
| `id`, `version`, `publisher` | Identity matching the parsed manifest |
| `publisher_key_id` | Reviewed publisher/key ID |
| `review_revision` | Integer from 1 through 2,147,483,647 |
| `manifest_sha256` | SHA-256 of the **exact raw manifest bytes** |
| `artifact_sha256`, `artifact_size_bytes` | Exact digest and positive size matching the manifest |
| `license` | Nonempty printable license identifier/expression |
| `source_url` | HTTPS source link |
| `support_owner` | Support owner ID |
| `firmware_evidence` | Nonempty array of HTTPS evidence links |
| `minimum_safe_version` | Exact minimum release version for this plugin ID |

Catalogs use the same strict JSON, size, list and nesting bounds as manifests.
A repeated ID/version or manifest digest is refused. All records for one plugin
ID must agree on its minimum safe version; a release below that version or
matching any revocation is refused. The supplied digest-to-manifest-byte map
must match the catalog exactly. Semantically equivalent JSON with different
whitespace does not match a pin. `verify_artifact_bytes` compares exact bytes
and SHA-256 with the authenticated manifest's artifact descriptor. It performs
no I/O. Future download, cache and staging paths must call identity checks at
each boundary and add their own streaming/extraction bounds.

## Protected-core guard

`guard_protected_core` is a pure function over a parsed manifest and compiled-in
firmware topology. `UnitTopology` records canonical unit identity, aliases,
dependencies and canonical resolved paths. They come from reviewed firmware
qualification, never manifest/catalog claims. A qualified profile must supply
all external-supervisor edges and resolve filesystem links before constructing
these records. This slice does not discover a real panel topology.

The guard checks each replacement's identity and symbolic target, resolves its
aliases and full dependency closure, and also protects every prerequisite of
a protected core node. Unknown edges, duplicate/ambiguous aliases and cycles
fail closed. Protected bus/supervisor/process, bridge, helper/rescue and
watchdog names cannot be overridden by a replacement allowlist. Unit templates
and instances are compared by base name. Unit files and drop-ins, bus/firmware
paths, bridge state/runtime paths and recovery paths are protected, including
parent paths that would contain them and both `/run` and `/var/run` forms.
Double-leading-slash paths are refused. Owning a whole systemd unit search,
control, transient or generator directory is refused; an independently qualified
optional child unit file remains eligible. Canonical custom paths attached to
protected firmware-profile units also protect overlapping paths and their
prerequisites.
Shared unit-type drop-ins and hyphen-prefix drop-ins affecting the protected
bus/bridge/watchdog families are protected too; an independently qualified
optional unit's own drop-in is not a shared core override.
No replacement path beneath `/var/brilliant-mqtt` is permitted in this slice;
future plugin-owned release storage is not a stock replacement target.

These checks validate metadata only. They make no claim about local recovery,
firmware compatibility, resource enforcement, consent, runtime isolation or
stock handback. Helpers/rescue, journals, HA mediation and all panel execution
remain out of scope. No pilot is selected or contacted and no plugin is enabled.
