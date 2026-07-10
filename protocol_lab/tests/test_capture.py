import hashlib
import hmac
import json
import runpy
import sys
from collections.abc import Callable
from pathlib import Path
from typing import cast

import pytest

from brilliant_protocol_lab.capture import classify_transport

_TOOLS_DIR = Path(__file__).resolve().parents[1] / "tools"


def _load_classify(
    vectors: list[dict[str, str]], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> Callable[[list[dict[str, str]]], dict[str, object]]:
    # classify_commitment.py runs its CLI (argparse + file read + print) at module
    # scope, so it needs a real input file and a valid argv to execute cleanly
    # under runpy; the returned `classify` function is then reusable directly.
    input_path = tmp_path / "vectors.json"
    input_path.write_text(json.dumps(vectors), encoding="utf-8")
    monkeypatch.setattr(sys, "argv", ["classify_commitment.py", str(input_path)])
    namespace = runpy.run_path(
        str(_TOOLS_DIR / "classify_commitment.py"), run_name="classify_commitment_under_test"
    )
    return cast(Callable[[list[dict[str, str]]], dict[str, object]], namespace["classify"])


def _vector(client: bytes, server: bytes, output: bytes) -> dict[str, str]:
    return {"client": client.hex(), "server": server.hex(), "output": output.hex()}


def test_classify_identifies_hmac_sha256_when_all_vectors_match(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    pairs = (
        (b"\x00" * 32, b"\x11" * 32),
        (b"\x22" * 32, b"\x33" * 32),
        (b"\x55" * 32, b"\xaa" * 32),
    )
    vectors = [
        _vector(client, server, hmac.new(client, server, hashlib.sha256).digest())
        for client, server in pairs
    ]
    classify = _load_classify(vectors, tmp_path, monkeypatch)
    result = classify(vectors)
    assert result == {
        "commitment": "hmac-sha256",
        "hardware_attestation": False,
        "vector_count": 3,
    }


def test_classify_returns_none_when_no_candidate_matches_all_vectors(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    client, server = b"\x00" * 32, b"\x11" * 32
    vectors = [_vector(client, server, hashlib.sha256(client + server).digest())]
    vectors.append(_vector(b"\x22" * 32, b"\x33" * 32, b"\x00" * 32))
    classify = _load_classify(vectors, tmp_path, monkeypatch)
    result = classify(vectors)
    assert result == {"commitment": None, "hardware_attestation": None, "vector_count": 2}


def test_classifies_framed_strict_binary() -> None:
    message = bytes.fromhex("80010001") + b"synthetic"
    frame = len(message).to_bytes(4, "big") + message
    result = classify_transport(frame)
    assert result.framing == "framed"
    assert result.protocol == "binary"
    assert result.tls is False


def test_classifies_tls_client_hello_without_guessing_inner_protocol() -> None:
    result = classify_transport(bytes.fromhex("1603030020") + b"x" * 32)
    assert result.tls is True
    assert result.framing == "unknown"
    assert result.protocol == "unknown"


def test_unknown_bytes_remain_unknown() -> None:
    result = classify_transport(b"not-a-protocol")
    assert (result.framing, result.protocol, result.tls) == (
        "unknown",
        "unknown",
        "unknown",
    )
