from __future__ import annotations

import hashlib
import os
from pathlib import Path

import pytest

from tools.brilliant_vc.staged_runtime import (
    EXPECTED_APP_ENTRIES,
    EXPECTED_VENDOR_ENTRIES,
    StagedRuntimeError,
    validate_staged_runtime,
)

_REPOSITORY_ROOT = Path(__file__).resolve().parents[1]
_COMMITTED_MANIFEST = _REPOSITORY_ROOT / "deploy/brilliant-vc-session-app-manifest.sha256"


def _stage(tmp_path: Path) -> tuple[Path, Path, Path]:
    tmp_path.chmod(0o755)
    os.chown(tmp_path, os.getuid(), os.getgid())
    app = tmp_path / "app"
    vendor = tmp_path / "vendor"
    manifest = tmp_path / "session-app-manifest.sha256"
    (app / "tools/brilliant_vc").mkdir(parents=True, mode=0o755)
    (vendor / "aiomqtt").mkdir(parents=True, mode=0o755)
    for directory in (app, app / "tools", app / "tools/brilliant_vc", vendor, vendor / "aiomqtt"):
        directory.chmod(0o755)
        os.chown(directory, os.getuid(), os.getgid())
    files = {
        app / "tools/__init__.py": b"",
        app / "tools/brilliant_vc/__init__.py": b"",
        app / "tools/brilliant_vc/session_coordinator.py": b"SAFE = True\n",
        vendor / "aiomqtt/__init__.py": b"__version__ = 'synthetic'\n",
    }
    for path, content in files.items():
        path.write_bytes(content)
        path.chmod(0o644)
        os.chown(path, os.getuid(), os.getgid())
    manifest.write_text(
        "".join(
            f"{hashlib.sha256(content).hexdigest()}  {path}\n"
            for path, content in sorted(files.items(), key=lambda item: str(item[0]))
        ),
        encoding="ascii",
    )
    manifest.chmod(0o644)
    os.chown(manifest, os.getuid(), os.getgid())
    return app, vendor, manifest


def _validate(app: Path, vendor: Path, manifest: Path) -> dict[str, object]:
    return validate_staged_runtime(
        app_root=app,
        vendor_root=vendor,
        manifest_path=manifest,
        required_uid=os.getuid(),
        required_gid=os.getgid(),
        allowed_app_roots=(app,),
        allowed_vendor_roots=(vendor,),
        allowed_manifest_paths=(manifest,),
        expected_app_entries=(
            "tools/__init__.py",
            "tools/brilliant_vc/__init__.py",
            "tools/brilliant_vc/session_coordinator.py",
        ),
        expected_vendor_entries=("aiomqtt/__init__.py",),
    ).to_public_dict()


def test_exact_root_owned_staged_app_and_vendor_validate_without_writes(tmp_path: Path) -> None:
    app, vendor, manifest = _stage(tmp_path)
    before = {path: path.stat().st_mtime_ns for path in (app, vendor, manifest)}

    result = _validate(app, vendor, manifest)

    assert result["app_file_count"] == 3
    assert result["vendor_file_count"] == 1
    assert result["staging_valid"] is True
    assert len(str(result["manifest_sha256"])) == 64
    assert {path: path.stat().st_mtime_ns for path in before} == before


def test_committed_manifest_matches_the_complete_reviewed_repository_surface() -> None:
    manifest = {}
    for line in _COMMITTED_MANIFEST.read_text(encoding="ascii").splitlines():
        digest, target = line.split("  ", maxsplit=1)
        assert target not in manifest
        manifest[target] = digest

    expected = {
        f"/var/brilliant-vc/app/{relative}": _REPOSITORY_ROOT / relative
        for relative in EXPECTED_APP_ENTRIES
    }
    vendor_root = _REPOSITORY_ROOT / "custom_components/brilliant_mqtt/agent_payload/vendor"
    expected.update(
        {
            f"/var/brilliant-vc/vendor/{relative}": vendor_root / relative
            for relative in EXPECTED_VENDOR_ENTRIES
        }
    )

    assert manifest.keys() == expected.keys()
    assert all(
        hashlib.sha256(source.read_bytes()).hexdigest() == manifest[target]
        for target, source in expected.items()
    )


@pytest.mark.parametrize("drift", ["extra", "symlink", "mode", "digest", "target"])
def test_staging_rejects_inventory_link_mode_digest_or_target_drift(
    tmp_path: Path, drift: str
) -> None:
    app, vendor, manifest = _stage(tmp_path)
    coordinator = app / "tools/brilliant_vc/session_coordinator.py"
    if drift == "extra":
        (app / "tools/brilliant_vc/extra.py").write_text("unexpected\n", encoding="utf-8")
    elif drift == "symlink":
        coordinator.unlink()
        coordinator.symlink_to(app / "tools/__init__.py")
    elif drift == "mode":
        coordinator.chmod(0o664)
    elif drift == "digest":
        coordinator.write_text("CHANGED = True\n", encoding="utf-8")
    else:
        source = str(coordinator)
        manifest.write_text(
            manifest.read_text(encoding="ascii").replace(source, f"{source}.other"),
            encoding="ascii",
        )

    with pytest.raises(StagedRuntimeError):
        _validate(app, vendor, manifest)
