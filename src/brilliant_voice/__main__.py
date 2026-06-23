"""Entrypoint: wire real adapters and run the supervised voice-satellite loop.

``python -m brilliant_voice`` on the panel. The process also runs under systemd
``Restart=always`` so a hard crash is recovered by the init system. The
in-process supervisor handles soft child exits without a full process restart.
"""

from __future__ import annotations

import logging
import signal
import threading

from brilliant_voice import firewall, hosts
from brilliant_voice.config import VoiceSettings
from brilliant_voice.supervisor import child_specs, supervise

log = logging.getLogger(__name__)


def main() -> None:
    """Read settings, apply startup side-effects, and run the supervisor."""
    settings = VoiceSettings.from_env()
    logging.basicConfig(
        level=settings.log_level,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )

    log.info(
        "brilliant-voice starting: panel=%s name=%r api_port=%d aec=%s",
        settings.panel,
        settings.name,
        settings.api_port,
        settings.enable_aec,
    )

    # Idempotent root startup tasks — re-applied each run because /etc/nftables
    # and /etc/hosts are part of the OTA-replaced filesystem.
    firewall.ensure_port_accept(settings.api_port)
    hosts.ensure_host_mapping(settings.ha_host)

    specs = child_specs(settings)

    # A threading.Event lets the SIGTERM handler signal the supervisor's
    # keep_running predicate.  KeyboardInterrupt is caught inside supervise()
    # itself so children are terminated before the process exits.
    stop_event = threading.Event()

    def _handle_sigterm(signum: int, frame: object) -> None:
        log.info("SIGTERM received — stopping supervisor")
        stop_event.set()

    signal.signal(signal.SIGTERM, _handle_sigterm)

    supervise(specs, keep_running=lambda: not stop_event.is_set())

    log.info("brilliant-voice stopped")


if __name__ == "__main__":
    main()
