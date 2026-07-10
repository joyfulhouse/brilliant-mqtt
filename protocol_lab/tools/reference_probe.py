from __future__ import annotations

import importlib
import json
import re

from brilliant_protocol_lab.oracle import MODULE_NAMES, collect_structure
from brilliant_protocol_lab.redaction import sanitize

result = sanitize(collect_structure(importlib.import_module, MODULE_NAMES))
encoded = json.dumps(result, sort_keys=True, separators=(",", ":"))
if "-----BEGIN" in encoded:
    raise SystemExit("oracle output contains PEM-shaped material")
if re.search(r"[A-Za-z0-9_-]{12,}\.[A-Za-z0-9_-]{12,}\.", encoded):
    raise SystemExit("oracle output contains JWT-shaped material")
if re.search(r"(?<!id:)[0-9a-f]{32}", encoded):
    raise SystemExit("oracle output contains an unredacted identifier")
print(encoded)
