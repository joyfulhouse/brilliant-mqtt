from __future__ import annotations

import argparse
import asyncio
from pathlib import Path

MAX_CAPTURE = 4 * 1024 * 1024


async def record(port: int, output: Path) -> None:
    if output.parent != Path("/private/tmp"):
        raise ValueError("capture must be directly under /private/tmp")
    completed = asyncio.Event()

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            data = await asyncio.wait_for(reader.read(MAX_CAPTURE + 1), timeout=5)
            if len(data) > MAX_CAPTURE:
                raise ValueError("capture exceeded 4 MiB")
            await asyncio.to_thread(output.write_bytes, data)
        finally:
            writer.close()
            await writer.wait_closed()
            completed.set()

    server = await asyncio.start_server(handle, "127.0.0.1", port)
    async with server:
        await asyncio.wait_for(completed.wait(), timeout=30)


parser = argparse.ArgumentParser()
parser.add_argument("--port", type=int, required=True)
parser.add_argument("--output", type=Path, required=True)
arguments = parser.parse_args()
asyncio.run(record(arguments.port, arguments.output))
