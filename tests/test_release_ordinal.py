"""Committed release authority metadata participates in deployment manifests."""

from pathlib import Path

import pytest

from tests.test_bundle_manifest import _load_helper, _run


def test_release_ordinal_is_committed_and_manifested() -> None:
    root = Path(__file__).parents[1]
    source = root / "deploy/RELEASE_ORDINAL"
    payload = root / "custom_components/brilliant_mqtt/agent_payload"
    assert source.read_bytes() == (payload / "RELEASE_ORDINAL").read_bytes()
    assert int(source.read_text()) > 0
    manifest = _run("payload-release", payload)
    assert manifest.returncode == 0
    assert "RELEASE_ORDINAL\t" in manifest.stdout


@pytest.mark.parametrize("value", ["0", "-1", "true", "1.5", "", "01"])
def test_manifest_rejects_invalid_release_ordinals(tmp_path: Path, value: str) -> None:
    (tmp_path / "RELEASE_ORDINAL").write_text(value)
    helper = _load_helper()
    with pytest.raises(helper.ManifestError, match="release_ordinal"):
        helper.release_ordinal(tmp_path)
