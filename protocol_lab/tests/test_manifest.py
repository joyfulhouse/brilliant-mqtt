from pathlib import Path

import pytest

from brilliant_protocol_lab.manifest import build_manifest


def test_manifest_refuses_a_root_inside_repository(tmp_path: Path) -> None:
    private = tmp_path / "protocol_lab" / "private"
    private.mkdir(parents=True)
    with pytest.raises(ValueError, match="outside the repository"):
        build_manifest(private, tmp_path)


def test_manifest_contains_hashes_not_contents(tmp_path: Path) -> None:
    repository = tmp_path / "repo"
    private = tmp_path / "firmware"
    repository.mkdir()
    private.mkdir()
    (private / "module.so").write_bytes(b"vendor-secret-payload")
    entries = build_manifest(private, repository)
    assert entries[0].relative_path == "module.so"
    assert entries[0].size == len(b"vendor-secret-payload")
    assert "vendor-secret-payload" not in repr(entries)
