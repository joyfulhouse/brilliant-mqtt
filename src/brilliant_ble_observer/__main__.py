"""Command-line entry point for passive service and explicit discovery probe."""

from __future__ import annotations

import argparse
import asyncio
import logging
import signal
from collections.abc import Sequence

from .config import Settings
from .run import run_probe, run_service

_LOG_FORMAT = "%(asctime)s %(levelname)s %(name)s: %(message)s"


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(prog="python -m brilliant_ble_observer")
    commands = parser.add_subparsers(dest="command")
    commands.add_parser("service", help="run the passive BLE observer")
    probe = commands.add_parser("probe", help="run one bounded discovery session")
    probe.add_argument("--seconds", type=float, default=10.0)
    return parser


def _get_running_loop() -> asyncio.AbstractEventLoop:
    return asyncio.get_running_loop()


async def run_service_with_signals(settings: Settings) -> None:
    """Translate SIGTERM/SIGINT into the supervisor's clean stop event."""
    stop_event = asyncio.Event()
    loop = _get_running_loop()
    installed: list[signal.Signals] = []
    for selected_signal in (signal.SIGTERM, signal.SIGINT):
        try:
            loop.add_signal_handler(selected_signal, stop_event.set)
        except NotImplementedError:
            continue
        installed.append(selected_signal)
    try:
        await run_service(settings, stop_event=stop_event)
    finally:
        for selected_signal in installed:
            loop.remove_signal_handler(selected_signal)


def main(argv: Sequence[str] | None = None) -> None:
    """Load strict settings, then run passive service or the explicit probe."""
    args = _parser().parse_args(argv)
    settings = Settings.from_env()
    logging.basicConfig(level=settings.log_level, format=_LOG_FORMAT)
    if args.command == "probe":
        asyncio.run(
            run_probe(
                adapter=settings.adapter,
                seconds=args.seconds,
            )
        )
        return
    asyncio.run(run_service_with_signals(settings))


if __name__ == "__main__":
    main()
