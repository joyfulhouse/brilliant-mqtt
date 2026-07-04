"""Heartbeat staleness — pure, single read, never raises."""

from __future__ import annotations


def heartbeat_age(path: str, *, now: float, started_at: float) -> float:
    """Seconds since the bridge last stamped *path*. If the file is absent or
    unparsable, age is measured from *started_at* (the watchdog's own start) so
    a never-seen heartbeat can't read as infinitely stale right after boot."""
    try:
        with open(path, encoding="utf-8") as f:
            stamped = float(f.read().strip())
    except (OSError, ValueError):
        return now - started_at
    return now - stamped
