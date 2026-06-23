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


def test_port_only_in_set_is_not_single_port_accept() -> None:
    # The port appears as a SET MEMBER, never as a standalone "dport <port> accept".
    # Token-aware matching must treat this as absent and add the explicit rule.
    nft = FakeNft("chain filter-input { tcp dport { 22, 6053, 8554 } accept }")
    added = ensure_port_accept(6053, run_nft=nft)
    assert added is True
    add_rule = ["add", "rule", "inet", "firewall", "filter-input", "tcp", "dport", "6053", "accept"]
    assert add_rule in nft.calls


def test_same_port_different_verb_not_mistaken_for_accept() -> None:
    # A DROP rule on the same port must not be read as an accept (would otherwise
    # leave the port closed). Token-aware matching requires the `accept` verb.
    nft = FakeNft("chain filter-input { tcp dport 6053 drop }")
    assert ensure_port_accept(6053, run_nft=nft) is True


def test_idempotent_tolerates_variable_spacing() -> None:
    # nftables may render with extra spacing/indentation; the token-aware check
    # still recognises the existing single-port accept (no duplicate added).
    nft = FakeNft("chain filter-input {\n    tcp dport 6053 accept\n}")
    assert ensure_port_accept(6053, run_nft=nft) is False
    assert not any(c[:1] == ["add"] for c in nft.calls)


def test_commented_out_rule_does_not_count_as_present() -> None:
    # A *disabled* (commented) rule whose text contains "tcp dport 6053 accept"
    # must NOT be treated as a live accept. A broad substring check would skip
    # the add here and leave the port closed (satellite unreachable); the
    # token-aware, comment-skipping check correctly adds the real rule.
    nft = FakeNft("chain filter-input {\n    # tcp dport 6053 accept\n}")
    added = ensure_port_accept(6053, run_nft=nft)
    assert added is True
    add_rule = ["add", "rule", "inet", "firewall", "filter-input", "tcp", "dport", "6053", "accept"]
    assert add_rule in nft.calls


def test_accept_on_another_line_is_not_matched() -> None:
    # `tcp dport 6053` and a bare `accept` on a SEPARATE line must NOT be matched
    # as a single-port accept (the old raw-substring check could be fooled by
    # cross-line/cross-rule text); requires the four tokens on ONE line.
    nft = FakeNft("chain filter-input {\n    tcp dport 6053 counter\n    accept\n}")
    assert ensure_port_accept(6053, run_nft=nft) is True
