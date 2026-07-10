from __future__ import annotations

import argparse
import dataclasses
import json
from pathlib import Path

from brilliant_protocol_lab.capture import classify_transport

parser = argparse.ArgumentParser()
parser.add_argument("capture", type=Path)
arguments = parser.parse_args()
result = classify_transport(arguments.capture.read_bytes())
print(json.dumps(dataclasses.asdict(result), sort_keys=True))
