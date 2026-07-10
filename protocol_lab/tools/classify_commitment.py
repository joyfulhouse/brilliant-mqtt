from __future__ import annotations

import argparse
import hashlib
import hmac
import json
from pathlib import Path


def candidates(client: bytes, server: bytes) -> dict[str, bytes]:
    return {
        "hmac-sha256": hmac.new(client, server, hashlib.sha256).digest(),
        "sha256-client-server": hashlib.sha256(client + server).digest(),
        "sha256-server-client": hashlib.sha256(server + client).digest(),
    }


def classify(vectors: list[dict[str, str]]) -> dict[str, object]:
    matching = set(candidates(b"\0" * 32, b"\1" * 32))
    for vector in vectors:
        client = bytes.fromhex(vector["client"])
        server = bytes.fromhex(vector["server"])
        output = bytes.fromhex(vector["output"])
        if len(client) != 32 or len(server) != 32 or len(output) != 32:
            raise ValueError("every synthetic vector field must be 32 bytes")
        matching.intersection_update(
            name for name, candidate in candidates(client, server).items() if candidate == output
        )
    primitive = next(iter(matching)) if len(matching) == 1 else None
    return {
        "commitment": primitive,
        "hardware_attestation": False if primitive is not None else None,
        "vector_count": len(vectors),
    }


parser = argparse.ArgumentParser()
parser.add_argument("input", type=Path)
arguments = parser.parse_args()
raw = json.loads(arguments.input.read_text(encoding="utf-8"))
if not isinstance(raw, list):
    raise SystemExit("vector input must be a list")
print(json.dumps(classify(raw), sort_keys=True))
