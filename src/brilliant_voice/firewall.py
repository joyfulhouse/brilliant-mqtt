"""Ensure the panel's nftables host firewall accepts the satellite port.

The panel runs an nftables ``inet firewall`` / ``filter-input`` chain with
``policy drop`` that accepts only ``tcp dport {22, 5000-5010, 5455-5456, 6455,
8554}`` and ``>= 32768`` — so the LVA ESPHome native API port (default 6053) is
silently dropped until we add an explicit accept (live-verified: this, not the
UniFi zone firewall, is what blocked HA→panel connections). ``/etc/nftables`` is
part of the OTA-replaced deployment, so the agent re-applies this rule at every
startup rather than persisting it on disk.
"""

from __future__ import annotations

import subprocess
from collections.abc import Callable

#: Runs an ``nft`` sub-command (argv after the ``nft`` program) and returns stdout.
NftRunner = Callable[[list[str]], str]

_TABLE = ("inet", "firewall")
_CHAIN = "filter-input"


def _default_run_nft(argv: list[str]) -> str:
    return subprocess.run(["nft", *argv], capture_output=True, text=True, check=True).stdout


def ensure_port_accept(port: int, *, run_nft: NftRunner = _default_run_nft) -> bool:
    """Idempotently accept inbound ``tcp/<port>`` on the panel filter-input chain.

    Returns ``True`` when a rule was added, ``False`` when an identical
    single-port accept was already present. The presence check matches the whole
    ``tcp dport <port> accept`` rule so a port inside an existing set (e.g. 22)
    or a numeric superstring (10700 vs 1070) is never mistaken for our rule.
    """
    rule_text = f"tcp dport {port} accept"
    listing = run_nft(["list", "chain", *_TABLE, _CHAIN])
    if rule_text in listing:
        return False
    run_nft(["add", "rule", *_TABLE, _CHAIN, "tcp", "dport", str(port), "accept"])
    return True
