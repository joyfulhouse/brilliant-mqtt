from pathlib import Path

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


def test_real_fs_glob_finds_nested(tmp_path: Path) -> None:
    fs = RealFileSystem()
    nested = tmp_path / "a" / "b" / "certs"
    nested.mkdir(parents=True)
    (nested / "hue-bridge-ca-certs.pem").write_text("X")
    found = fs.glob(str(tmp_path), "hue-bridge-ca-certs.pem")
    assert found == str(nested / "hue-bridge-ca-certs.pem")
    assert fs.glob(str(tmp_path), "absent.pem") is None
