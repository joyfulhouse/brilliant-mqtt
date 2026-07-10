from __future__ import annotations

import argparse
import asyncio
import importlib
import inspect
import json
from collections.abc import Callable
from pathlib import Path

PAIRS = tuple(
    (bytes([left]) * 32, bytes([right]) * 32) for left, right in ((0, 17), (34, 51), (85, 170))
)


def resolve(path: str) -> Callable[[bytes, bytes], object]:
    module_name, separator, qualname = path.partition(":")
    if not separator:
        raise ValueError("callable path must be module:qualified.name")
    value: object = importlib.import_module(module_name)
    for part in qualname.split("."):
        value = getattr(value, part)
    if not callable(value):
        raise TypeError(f"{path} is not callable")
    return value


async def invoke(function: Callable[[bytes, bytes], object]) -> list[dict[str, str]]:
    vectors: list[dict[str, str]] = []
    for client, server in PAIRS:
        result = function(client, server)
        if inspect.isawaitable(result):
            result = await result
        if not isinstance(result, bytes) or len(result) != 32:
            raise TypeError("commitment callable must return exactly 32 bytes")
        vectors.append({"client": client.hex(), "server": server.hex(), "output": result.hex()})
    return vectors


parser = argparse.ArgumentParser()
parser.add_argument("callable_path")
parser.add_argument("output", type=Path)
arguments = parser.parse_args()
if arguments.output.parent != Path("/private/tmp"):
    raise SystemExit("output must be directly under /private/tmp")
arguments.output.write_text(
    json.dumps(asyncio.run(invoke(resolve(arguments.callable_path))), indent=2) + "\n",
    encoding="utf-8",
)
