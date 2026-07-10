from __future__ import annotations

import hashlib
import re
from collections.abc import Mapping, Sequence

_SECRET_KEYS = ("password", "token", "secret", "private", "credential", "certificate")
_HEX_ID = re.compile(r"^[0-9a-fA-F]{32}$")
_PEM = re.compile(r"-----BEGIN [A-Z0-9 ]+-----")


def safe_id(value: str) -> str:
    digest = hashlib.sha256(value.lower().encode()).hexdigest()[:12]
    return f"id:{digest}"


def sanitize(value: object, key: str = "") -> object:
    if isinstance(value, Mapping):
        return {
            str(child_key): sanitize(child, str(child_key)) for child_key, child in value.items()
        }
    if isinstance(value, Sequence) and not isinstance(value, (str, bytes, bytearray)):
        return [sanitize(child, key) for child in value]
    if isinstance(value, (bytes, bytearray)):
        return f"<redacted-bytes:{len(value)}>"
    if isinstance(value, str):
        if any(fragment in key.lower() for fragment in _SECRET_KEYS) or _PEM.search(value):
            return f"<redacted:{len(value)}>"
        if _HEX_ID.fullmatch(value):
            return safe_id(value)
    return value
