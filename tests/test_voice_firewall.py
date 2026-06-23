"""Panel nftables rule management: idempotent accept for the satellite port."""

from __future__ import annotations

from brilliant_voice.firewall import ensure_port_accept


class FakeNft:
    """Records nft invocations and serves a canned `list chain` output."""

    def __init__(self, existing_listing: str) -> None:
        self.listing = existing_listing
        self.calls: list[list[str]] = []

    def __call__(self, argv: list[str]) -> str:
        self.calls.append(argv)
        if argv[:2] == ["list", "chain"]:
            return self.listing
        return ""


def test_adds_rule_when_absent() -> None:
    # A chain whose ruleset has the port only inside an unrelated set, not as a
    # standalone accept — the agent must still add the explicit single-port rule.
    nft = FakeNft("chain filter-input { tcp dport { 22, 5000-5010 } accept }")
    added = ensure_port_accept(10700, run_nft=nft)
    assert added is True
    assert [
        "add",
        "rule",
        "inet",
        "firewall",
        "filter-input",
        "tcp",
        "dport",
        "10700",
        "accept",
    ] in nft.calls


def test_idempotent_when_present() -> None:
    nft = FakeNft("chain filter-input { tcp dport 10700 accept }")
    added = ensure_port_accept(10700, run_nft=nft)
    assert added is False
    assert not any(c[:1] == ["add"] for c in nft.calls)


def test_distinct_port_not_confused_with_substring() -> None:
    # 10700 present must NOT satisfy a request for 1070 (substring guard).
    nft = FakeNft("chain filter-input { tcp dport 10700 accept }")
    assert ensure_port_accept(1070, run_nft=nft) is True
