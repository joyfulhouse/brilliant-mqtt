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
    single-port accept was already present.

    The presence check is token-aware per (non-comment) line, not a raw
    substring: a line must contain the consecutive tokens
    ``tcp dport <port> accept``.  This way a port inside an existing set
    (``tcp dport { 22, 6053 } accept``), the same port with a different verb
    (``tcp dport 6053 drop``), or — the case a broad substring gets WRONG — a
    *commented-out* rule (``# tcp dport 6053 accept``) is never mistaken for a
    live single-port accept.  Treating a disabled rule as active would skip the
    add and leave the port closed (satellite unreachable); a numeric superstring
    (``10700`` vs ``1070``) is likewise excluded.
    """
    listing = run_nft(["list", "chain", *_TABLE, _CHAIN])
    if _has_single_port_accept(listing, port):
        return False
    run_nft(["add", "rule", *_TABLE, _CHAIN, "tcp", "dport", str(port), "accept"])
    return True


def _has_single_port_accept(listing: str, port: int) -> bool:
    """True when ``listing`` already has a live ``tcp dport <port> accept`` rule.

    Matches on token boundaries per line: the consecutive tokens
    ``("tcp", "dport", str(port), "accept")`` must appear in some non-comment
    line's whitespace-split tokens.  This tolerates nftables' variable spacing
    while rejecting set members, numeric superstrings, other verbs, and
    commented-out (disabled) rules.
    """
    target = ("tcp", "dport", str(port), "accept")
    for line in listing.splitlines():
        if line.lstrip().startswith("#"):
            continue  # a disabled rule must not count as present
        tokens = line.split()
        for i in range(len(tokens) - len(target) + 1):
            if tuple(tokens[i : i + len(target)]) == target:
                return True
    return False
