"""Tests for brilliant_voice.hosts — /etc/hosts idempotent mapping helper."""

from __future__ import annotations

import pytest

from brilliant_voice.hosts import ensure_host_mapping

# ---------------------------------------------------------------------------
# In-memory /etc/hosts fake
# ---------------------------------------------------------------------------


class FakeHosts:
    """Holds an in-memory /etc/hosts text; records whether write was called."""

    def __init__(self, initial: str = "") -> None:
        self._text = initial
        self.written: list[str] = []

    def read(self) -> str:
        return self._text

    def write(self, text: str) -> None:
        self._text = text
        self.written.append(text)


# ---------------------------------------------------------------------------
# Empty / whitespace spec — no-op
# ---------------------------------------------------------------------------


def test_empty_spec_returns_false_no_io() -> None:
    fh = FakeHosts("127.0.0.1\tlocalhost\n")
    result = ensure_host_mapping("", read=fh.read, write=fh.write)
    assert result is False
    assert fh.written == []


def test_whitespace_only_spec_returns_false_no_io() -> None:
    fh = FakeHosts("127.0.0.1\tlocalhost\n")
    result = ensure_host_mapping("   ", read=fh.read, write=fh.write)
    assert result is False
    assert fh.written == []


# ---------------------------------------------------------------------------
# Malformed spec → ValueError
# ---------------------------------------------------------------------------


def test_no_equals_raises() -> None:
    fh = FakeHosts()
    with pytest.raises(ValueError, match="missing '='"):
        ensure_host_mapping("homeassistant.local", read=fh.read, write=fh.write)


def test_empty_hostname_raises() -> None:
    fh = FakeHosts()
    with pytest.raises(ValueError, match="empty hostname"):
        ensure_host_mapping("=1.2.3.4", read=fh.read, write=fh.write)


def test_empty_ip_raises() -> None:
    fh = FakeHosts()
    with pytest.raises(ValueError, match="empty ip"):
        ensure_host_mapping("host=", read=fh.read, write=fh.write)


# ---------------------------------------------------------------------------
# Injection defense: internal whitespace / control chars → ValueError
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "spec",
    [
        "ha local=1.2.3.4",  # internal space in hostname
        "ha\tlocal=1.2.3.4",  # internal tab in hostname
        "ha.local=1.2.3.4 evil.host",  # extra field smuggled into ip side
        "ha.local=1.2.3.4\t10.0.0.1",  # tab-separated second value in ip side
        "ha\x00.local=1.2.3.4",  # NUL control char in hostname
        "ha.local=1.2.3.4\x7f",  # DEL control char in ip
    ],
)
def test_internal_whitespace_or_control_raises(spec: str) -> None:
    """A hand-written VOICE_HA_HOST with internal whitespace/control chars must
    be rejected so it can never corrupt the /etc/hosts line."""
    fh = FakeHosts("127.0.0.1\tlocalhost\n")
    with pytest.raises(ValueError, match="whitespace/control chars"):
        ensure_host_mapping(spec, read=fh.read, write=fh.write)
    # Nothing written — the bad spec never touches the file.
    assert fh.written == []


def test_embedded_newline_in_ip_raises() -> None:
    """An embedded newline (line-injection attempt) is rejected, not written as a
    second /etc/hosts line."""
    fh = FakeHosts("127.0.0.1\tlocalhost\n")
    with pytest.raises(ValueError, match="whitespace/control chars"):
        ensure_host_mapping("ha.local=1.2.3.4\nevil 6.6.6.6", read=fh.read, write=fh.write)
    assert fh.written == []


# ---------------------------------------------------------------------------
# New mapping appended
# ---------------------------------------------------------------------------

_EXISTING = "127.0.0.1\tlocalhost\n::1\t\tlocalhost ip6-localhost\n"


def test_new_mapping_appended_returns_true() -> None:
    fh = FakeHosts(_EXISTING)
    result = ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    assert result is True
    assert len(fh.written) == 1


def test_new_mapping_appended_line_format() -> None:
    fh = FakeHosts(_EXISTING)
    ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    new_text = fh.written[0]
    # Appended line must be tab-separated "ip\thostname"
    assert "192.168.1.10\thomeassistant.local\n" in new_text


def test_pre_existing_lines_preserved() -> None:
    fh = FakeHosts(_EXISTING)
    ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    new_text = fh.written[0]
    assert "127.0.0.1\tlocalhost" in new_text
    assert "::1" in new_text


def test_appended_to_content_without_trailing_newline() -> None:
    """Content lacking a trailing newline must not produce a blank line."""
    initial = "127.0.0.1\tlocalhost"  # no trailing \n
    fh = FakeHosts(initial)
    ensure_host_mapping("ha.local=10.0.0.1", read=fh.read, write=fh.write)
    new_text = fh.written[0]
    # Must not have two consecutive newlines
    assert "\n\n" not in new_text
    assert "10.0.0.1\tha.local\n" in new_text


# ---------------------------------------------------------------------------
# Idempotent: mapping already present
# ---------------------------------------------------------------------------


def test_idempotent_exact_match_returns_false() -> None:
    initial = _EXISTING + "192.168.1.10\thomeassistant.local\n"
    fh = FakeHosts(initial)
    result = ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    assert result is False
    assert fh.written == []


def test_idempotent_multiple_hostnames_on_same_ip_line() -> None:
    """hostname present alongside other names on the same ip line → already present."""
    initial = _EXISTING + "192.168.1.10\tha homeassistant.local ha.home\n"
    fh = FakeHosts(initial)
    result = ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    assert result is False
    assert fh.written == []


def test_comment_line_not_considered_present() -> None:
    """A commented-out entry must not count as the mapping being present."""
    initial = _EXISTING + "# 192.168.1.10\thomeassistant.local\n"
    fh = FakeHosts(initial)
    result = ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    assert result is True
    assert len(fh.written) == 1


# ---------------------------------------------------------------------------
# Different ip for same hostname is NOT considered present
# ---------------------------------------------------------------------------


def test_different_ip_same_hostname_appends() -> None:
    initial = _EXISTING + "10.0.0.1\thomeassistant.local\n"
    fh = FakeHosts(initial)
    result = ensure_host_mapping("homeassistant.local=192.168.1.10", read=fh.read, write=fh.write)
    assert result is True
    assert len(fh.written) == 1
    assert "192.168.1.10\thomeassistant.local\n" in fh.written[0]
    # The old entry must still be there
    assert "10.0.0.1\thomeassistant.local" in fh.written[0]
