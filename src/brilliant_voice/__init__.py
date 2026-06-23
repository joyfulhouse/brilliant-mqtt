"""On-panel voice agent: a Home Assistant Assist satellite (mic + speaker) with
on-panel wake word, built on the vendored Wyoming stack.

Runs as the ``brilliant-voice`` systemd service alongside the message-bus
bridge. Like the bridge it is shipped vendored to ``/var`` (the panel has no
pip) and imports no panel-only or heavy ML libraries at module scope — the
wake-word runtime lives behind the supervised subprocesses, so this package's
logic stays unit-testable on any machine.
"""

from __future__ import annotations

__version__ = "0.1.0"
