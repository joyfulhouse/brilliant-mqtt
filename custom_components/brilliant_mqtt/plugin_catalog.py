"""Reviewed-release catalog contracts; no fetching, install or panel operations."""

from __future__ import annotations

import hashlib
from collections.abc import Mapping
from dataclasses import dataclass

from .const import DEFAULT_PLUGINS_ENABLED, OPT_PLUGINS_ENABLED
from .plugin_manifest import (
    DEFAULT_POLICY,
    MAX_ARTIFACT_BYTES,
    MAX_DOCUMENT_BYTES,
    PluginManifest,
    PluginValidationError,
    ValidationPolicy,
    digest,
    https_url,
    identifier,
    integer,
    object_fields,
    parse_document,
    parse_manifest,
    sequence,
    strings,
    text,
    version,
)


@dataclass(frozen=True)
class PluginCatalog:
    revision: int
    manifests: tuple[PluginManifest, ...]


def _version_order(value: str) -> tuple[int, ...]:
    return tuple(int(part) for part in value.split("."))


def parse_catalog(
    raw: bytes,
    manifests: Mapping[str, bytes],
    policy: ValidationPolicy = DEFAULT_POLICY,
) -> PluginCatalog:
    """Validate data bundled with a reviewed integration, never a remote trust root.

    Manifest SHA-256 covers exact UTF-8 bytes, not reserialized JSON. Catalog
    metadata is not a signature and is never accepted from an arbitrary source.
    """
    document = object_fields(
        parse_document(raw),
        {
            "schema_version",
            "revision",
            "entries",
            "revoked_manifest_sha256",
            "revoked_artifact_sha256",
            "revoked_publisher_keys",
        },
    )
    integer(document["schema_version"], 1, 1)
    revision = integer(document["revision"], 1, 2**31 - 1)
    revoked_manifests = frozenset(
        digest(item) for item in strings(document["revoked_manifest_sha256"])
    )
    revoked_artifacts = frozenset(
        digest(item) for item in strings(document["revoked_artifact_sha256"])
    )
    revoked_keys = frozenset(
        identifier(item) for item in strings(document["revoked_publisher_keys"])
    )
    releases: set[tuple[str, str]] = set()
    required_digests: set[str] = set()
    minimum_versions: dict[str, str] = {}
    parsed: list[PluginManifest] = []
    for item in sequence(document["entries"]):
        record = object_fields(
            item,
            {
                "id",
                "version",
                "publisher",
                "publisher_key_id",
                "review_revision",
                "manifest_sha256",
                "artifact_sha256",
                "artifact_size_bytes",
                "license",
                "source_url",
                "support_owner",
                "firmware_evidence",
                "minimum_safe_version",
            },
        )
        plugin_id = identifier(record["id"])
        release_version = version(record["version"])
        publisher = identifier(record["publisher"])
        key_id = identifier(record["publisher_key_id"])
        integer(record["review_revision"], 1, 2**31 - 1)
        manifest_digest = digest(record["manifest_sha256"])
        artifact_digest = digest(record["artifact_sha256"])
        artifact_size = integer(record["artifact_size_bytes"], 1, MAX_ARTIFACT_BYTES)
        text(record["license"])
        https_url(record["source_url"])
        identifier(record["support_owner"])
        evidence = strings(record["firmware_evidence"])
        if not evidence:
            raise PluginValidationError("missing_firmware_evidence")
        for url in evidence:
            https_url(url)
        minimum = version(record["minimum_safe_version"])
        if plugin_id in minimum_versions and minimum_versions[plugin_id] != minimum:
            raise PluginValidationError("inconsistent_minimum_safe_version")
        minimum_versions[plugin_id] = minimum
        if (
            (plugin_id, release_version) in releases
            or manifest_digest in required_digests
            or manifest_digest in revoked_manifests
            or artifact_digest in revoked_artifacts
            or key_id in revoked_keys
            or _version_order(release_version) < _version_order(minimum)
        ):
            raise PluginValidationError("ineligible_plugin_release")
        releases.add((plugin_id, release_version))
        required_digests.add(manifest_digest)
        manifest_raw = manifests.get(manifest_digest)
        if (
            type(manifest_raw) is not bytes
            or not manifest_raw
            or len(manifest_raw) > MAX_DOCUMENT_BYTES
            or hashlib.sha256(manifest_raw).hexdigest() != manifest_digest
        ):
            raise PluginValidationError("plugin_manifest_digest_mismatch")
        manifest = parse_manifest(manifest_raw, policy)
        if (
            manifest.id != plugin_id
            or manifest.version != release_version
            or manifest.publisher != publisher
            or manifest.artifact.sha256 != artifact_digest
            or manifest.artifact.size_bytes != artifact_size
        ):
            raise PluginValidationError("plugin_catalog_identity_mismatch")
        parsed.append(manifest)
    if set(manifests) != required_digests:
        raise PluginValidationError("unexpected_plugin_manifests")
    return PluginCatalog(revision, tuple(parsed))


def verify_artifact_bytes(manifest: PluginManifest, raw: bytes) -> None:
    """Verify already-bounded bytes for any future download/cache/staging caller."""
    if (
        type(raw) is not bytes
        or len(raw) != manifest.artifact.size_bytes
        or hashlib.sha256(raw).hexdigest() != manifest.artifact.sha256
    ):
        raise PluginValidationError("plugin_artifact_identity_mismatch")


def configured_plugin_catalog(options: Mapping[str, object]) -> PluginCatalog | None:
    """Internal opt-in only. No user-facing selection or catalog entries yet."""
    if options.get(OPT_PLUGINS_ENABLED, DEFAULT_PLUGINS_ENABLED) is not True:
        return None
    return PluginCatalog(1, ())
