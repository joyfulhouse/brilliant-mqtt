from __future__ import annotations

from pathlib import Path

from brilliant_bus_watchdog.health import heartbeat_age


def test_fresh(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("100.0")
    assert heartbeat_age(str(p), now=130.0, started_at=0.0) == 30.0


def test_stale(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("100.0")
    assert heartbeat_age(str(p), now=2000.0, started_at=0.0) == 1900.0


def test_missing_file_measures_from_start(tmp_path: Path) -> None:
    # no file: age is now - started_at, so a never-seen heartbeat only ages
    # relative to the watchdog's own start (not epoch 0)
    assert heartbeat_age(str(tmp_path / "nope"), now=500.0, started_at=200.0) == 300.0


def test_unparsable_measures_from_start(tmp_path: Path) -> None:
    p = tmp_path / "hb"
    p.write_text("garbage")
    assert heartbeat_age(str(p), now=500.0, started_at=200.0) == 300.0
