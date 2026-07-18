"""On-panel oneshot that keeps the injected diyHue CA in the pinned Hue bundle
across firmware OTA and restarts the local Hue coordinator when it re-appends."""

from __future__ import annotations

__all__ = ["config", "coordinator", "fs", "reconcile", "run"]
