from __future__ import annotations

import argparse
import asyncio
import json
from dataclasses import asdict

from brilliant_protocol_lab.mdns import browse_read_only

parser = argparse.ArgumentParser()
parser.add_argument("--interface", required=True)
parser.add_argument("--timeout", type=float, default=15.0)
arguments = parser.parse_args()
observations = asyncio.run(browse_read_only(arguments.interface, arguments.timeout))
print(json.dumps([asdict(item) for item in observations], sort_keys=True))
