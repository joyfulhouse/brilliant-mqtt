"""Tests: bus-liveness heartbeat writer."""

from __future__ import annotations

from pathlib import Path

from brilliant_mqtt.heartbeat import write_heartbeat


def test_writes_epoch_and_creates_parent(tmp_path: Path) -> None:
    p = tmp_path / "sub" / "bus-heartbeat"
    write_heartbeat(str(p), lambda: 1751630400.5)
    assert p.read_text().strip() == "1751630400.5"


def test_overwrites(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    write_heartbeat(str(p), lambda: 1.0)
    write_heartbeat(str(p), lambda: 2.0)
    assert p.read_text().strip() == "2.0"


def test_empty_path_is_noop(tmp_path: Path) -> None:
    write_heartbeat("", lambda: 1.0)  # must not raise, must not create anything


def test_never_raises_on_unwritable(tmp_path: Path) -> None:
    # parent is a file, so mkdir/replace will fail — must be swallowed
    blocker = tmp_path / "blocker"
    blocker.write_text("x")
    write_heartbeat(str(blocker / "hb"), lambda: 1.0)  # no exception
