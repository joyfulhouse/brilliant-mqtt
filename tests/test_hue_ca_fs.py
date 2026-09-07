from pathlib import Path

import pytest

from brilliant_hue_ca.fs import RealFileSystem


def test_real_fs_read_append_exists(tmp_path: Path) -> None:
    fs = RealFileSystem()
    p = tmp_path / "bundle.pem"
    p.write_text("A\n")
    assert fs.exists(str(p)) is True
    assert fs.exists(str(tmp_path / "nope")) is False
    assert fs.read_text(str(p)) == "A\n"
    fs.append_text(str(p), "B\n")
    assert fs.read_text(str(p)) == "A\nB\n"


def test_real_fs_write_text_overwrites(tmp_path: Path) -> None:
    fs = RealFileSystem()
    p = tmp_path / "state.json"
    fs.write_text(str(p), "first")
    assert fs.read_text(str(p)) == "first"
    # write_text must overwrite (unlike append_text), so the second write
    # replaces the file rather than growing it.
    fs.write_text(str(p), "second")
    assert fs.read_text(str(p)) == "second"


def test_real_fs_write_text_creates_missing_parent_dir(tmp_path: Path) -> None:
    fs = RealFileSystem()
    p = tmp_path / "sub" / "dir" / "state.json"
    fs.write_text(str(p), "x")
    assert fs.read_text(str(p)) == "x"


def test_real_fs_write_text_leaves_no_tmp_on_success(tmp_path: Path) -> None:
    fs = RealFileSystem()
    p = tmp_path / "state.json"
    fs.write_text(str(p), "X")
    # atomic replace consumes the temp file; only the final file remains
    assert [q.name for q in tmp_path.iterdir()] == ["state.json"]


def test_real_fs_write_text_is_atomic_preserving_old_on_replace_failure(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    fs = RealFileSystem()
    p = tmp_path / "state.json"
    fs.write_text(str(p), "OLD")

    def boom(_src: str, _dst: str) -> None:
        raise OSError("simulated crash during rename")

    monkeypatch.setattr("brilliant_hue_ca.fs.os.replace", boom)
    with pytest.raises(OSError):
        fs.write_text(str(p), "NEW")
    # The write is atomic: a failure at the replace step leaves the previous
    # content intact — never a truncated/torn "NEW" that a naive in-place write
    # would produce.
    assert p.read_text(encoding="utf-8") == "OLD"


def test_real_fs_glob_finds_nested(tmp_path: Path) -> None:
    fs = RealFileSystem()
    nested = tmp_path / "a" / "b" / "certs"
    nested.mkdir(parents=True)
    (nested / "hue-bridge-ca-certs.pem").write_text("X")
    found = fs.glob(str(tmp_path), "hue-bridge-ca-certs.pem")
    assert found == str(nested / "hue-bridge-ca-certs.pem")
    assert fs.glob(str(tmp_path), "absent.pem") is None
