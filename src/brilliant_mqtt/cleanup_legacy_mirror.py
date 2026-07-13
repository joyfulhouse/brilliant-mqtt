"""Dry-run-first cleanup for persistent legacy HA-mirror peripherals.

The module is importable off-panel. Brilliant's closed-source modules are
loaded only by :meth:`NativeCleanupClient.start`, after CLI safety validation.
"""

from __future__ import annotations

import argparse
import asyncio
import inspect
import json
import os
import secrets
import stat
import tempfile
import time
from collections import Counter
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import dataclass, replace
from pathlib import Path
from typing import Any, Protocol, cast

from brilliant_mqtt.bus import normalize_peripheral
from brilliant_mqtt.model import BrilliantDevice

ALLOWED_ID_PREFIXES = ("ha_", "ha-pilot-", "zzz_mirror_")
ALLOWED_NAME_PREFIXES = ("HA ", "HA_PILOT_", "ZZZ Mirror ")
# The July 2026 room-assignment pilot used display labels as peripheral IDs.
# Keep these fail-closed instead of broadening the ID prefix allowlist to every
# user-visible name beginning with "HA ".
EXACT_LEGACY_ID_NAMES = frozenset(
    {
        "HA Backyard Lamp 1",
        "HA Backyard Lamp 2",
        "HA Backyard Lamp 3",
        "HA Balcony Lamp 1",
        "HA Balcony Lamp 2",
    }
)
SAFE_REPORT_ROOT = Path("/data/brilliant-mqtt/cleanup")

_SOCKET_PATH = "/var/run/brilliant/server_socket"
_CONNECT_TIMEOUT_S = 10.0
_CONNECT_POLL_S = 0.25
_NATIVE_CLOSE_STEP_TIMEOUT_S = 2.0
_CLI_CLOSE_TIMEOUT_S = 5.0
_RETAINED_CLEANUP_TASKS: set[asyncio.Task[Any]] = set()


@dataclass(frozen=True)
class OwnDeviceSnapshot:
    """One scoped read of the panel's own device."""

    owning_device_id: str
    peripherals: tuple[BrilliantDevice, ...]


@dataclass(frozen=True)
class CandidateRecord:
    """The only peripheral metadata allowed in a cleanup report."""

    id: str
    name: str
    type: int


@dataclass(frozen=True)
class CleanupReport:
    """Sanitized cleanup inventory/result."""

    timestamp_ms: int
    owning_device_id: str
    candidates: tuple[CandidateRecord, ...]
    deleted_ids: tuple[str, ...]
    remaining_ids: tuple[str, ...]
    success: bool

    @staticmethod
    def public_fields() -> set[str]:
        """Return the fixed, deliberately small public report schema."""
        return {
            "timestamp_ms",
            "owning_device_id",
            "candidates",
            "deleted_ids",
            "remaining_ids",
            "success",
        }


class CleanupClient(Protocol):
    """Narrow runtime seam; it cannot host peripherals or write variables."""

    async def start(self) -> None:
        """Connect to the local message bus."""
        ...

    async def snapshot_own_device(self) -> OwnDeviceSnapshot:
        """Read only the owning Control device."""
        ...

    async def delete_peripheral(
        self, device_id: str, peripheral_id: str, deletion_time_ms: int
    ) -> None:
        """Delete one peripheral from the owning device."""
        ...

    async def close(self) -> None:
        """Boundedly close the bus session."""
        ...


ReportWriter = Callable[[Path, str], None]
PathValidator = Callable[[Path, Path], Path]


def is_candidate(device: BrilliantDevice) -> bool:
    """Return true only when both case-sensitive allowlist dimensions match."""
    peripheral_id = device.peripheral_id
    name = device.name
    prefix_match = (
        isinstance(peripheral_id, str)
        and isinstance(name, str)
        and peripheral_id.startswith(ALLOWED_ID_PREFIXES)
        and name.startswith(ALLOWED_NAME_PREFIXES)
    )
    exact_legacy_match = (
        isinstance(peripheral_id, str)
        and isinstance(name, str)
        and peripheral_id == name
        and peripheral_id in EXACT_LEGACY_ID_NAMES
    )
    return prefix_match or exact_legacy_match


def select_candidates(devices: Sequence[BrilliantDevice]) -> tuple[BrilliantDevice, ...]:
    """Select unique candidates in stable order, excluding duplicate IDs.

    A duplicate ID makes every occurrence ambiguous and therefore ineligible
    for deletion. This is intentionally stricter than ordinary de-duplication.
    """
    id_counts = Counter(
        device.peripheral_id for device in devices if isinstance(device.peripheral_id, str)
    )
    return tuple(
        device
        for device in devices
        if is_candidate(device) and id_counts[device.peripheral_id] == 1
    )


def build_report(
    *,
    timestamp_ms: int,
    owning_device_id: str,
    candidates: Sequence[BrilliantDevice],
    deleted_ids: Sequence[str],
    remaining_ids: Sequence[str],
    success: bool,
) -> CleanupReport:
    """Build a report without copying variables, values, blobs, or reprs."""
    return CleanupReport(
        timestamp_ms=timestamp_ms,
        owning_device_id=owning_device_id,
        candidates=tuple(
            CandidateRecord(
                id=candidate.peripheral_id,
                name=candidate.name,
                type=candidate.peripheral_type,
            )
            for candidate in candidates
        ),
        deleted_ids=tuple(deleted_ids),
        remaining_ids=tuple(remaining_ids),
        success=success,
    )


def canonical_report_json(report: CleanupReport) -> str:
    """Serialize the fixed report schema as compact, sorted JSON."""
    value = {
        "timestamp_ms": report.timestamp_ms,
        "owning_device_id": report.owning_device_id,
        "candidates": [
            {"id": candidate.id, "name": candidate.name, "type": candidate.type}
            for candidate in report.candidates
        ],
        "deleted_ids": list(report.deleted_ids),
        "remaining_ids": list(report.remaining_ids),
        "success": report.success,
    }
    return json.dumps(value, ensure_ascii=True, separators=(",", ":"), sort_keys=True)


def validate_report_path(path: Path, safe_root: Path = SAFE_REPORT_ROOT) -> Path:
    """Resolve and validate an apply report path without creating anything."""
    try:
        if not path.is_absolute() or ".." in path.parts or not path.name:
            raise ValueError
        safe_root_resolved = safe_root.resolve(strict=True)
        parent = path.parent.resolve(strict=True)
        if not safe_root_resolved.is_dir() or not parent.is_dir():
            raise ValueError
        if parent != safe_root_resolved and safe_root_resolved not in parent.parents:
            raise ValueError
        if path.is_symlink():
            raise ValueError
        if path.exists() and not path.is_file():
            raise ValueError
        parent_mode = parent.stat().st_mode
        if parent_mode & (stat.S_IWUSR | stat.S_IWGRP | stat.S_IWOTH) == 0:
            raise ValueError
        if not os.access(parent, os.W_OK):
            raise ValueError
    except (OSError, RuntimeError, ValueError) as exc:
        raise ValueError("unsafe report path") from exc
    return path


def write_report_atomic(path: Path, payload: str) -> None:
    """Atomically replace *path* with a mode-0600 canonical report."""
    fd = -1
    temporary_path: str | None = None
    try:
        fd, temporary_path = tempfile.mkstemp(prefix=f".{path.name}.", dir=path.parent)
        os.fchmod(fd, 0o600)
        with os.fdopen(fd, "w", encoding="utf-8", newline="\n") as output:
            fd = -1
            output.write(payload)
            output.write("\n")
            output.flush()
            os.fsync(output.fileno())
        os.replace(temporary_path, path)
        temporary_path = None
    finally:
        if fd >= 0:
            os.close(fd)
        if temporary_path is not None:
            try:
                os.unlink(temporary_path)
            except FileNotFoundError:
                pass


def _empty_failure_report(timestamp_ms: int) -> CleanupReport:
    return CleanupReport(
        timestamp_ms=timestamp_ms,
        owning_device_id="",
        candidates=(),
        deleted_ids=(),
        remaining_ids=(),
        success=False,
    )


class _VerificationReadError(RuntimeError):
    """Internal error carrying only a sanitized conservative report."""

    def __init__(self, report: CleanupReport) -> None:
        super().__init__("cleanup verification failed")
        self.report = report


class _ReportPersistenceError(RuntimeError):
    """Internal write error carrying the exact sanitized attempted report."""

    def __init__(self, report: CleanupReport) -> None:
        super().__init__("cleanup report persistence failed")
        self.report = report


async def run_cleanup(
    client: CleanupClient,
    *,
    apply: bool,
    report_path: Path | None,
    now_ms: Callable[[], int],
    sleep: Callable[[float], Awaitable[None]],
    report_writer: ReportWriter,
) -> tuple[int, CleanupReport]:
    """Inventory, optionally delete serially, and verify from a second read."""
    initial = await client.snapshot_own_device()
    candidates = select_candidates(initial.peripherals)
    candidate_ids = tuple(candidate.peripheral_id for candidate in candidates)

    if not apply:
        return 0, build_report(
            timestamp_ms=now_ms(),
            owning_device_id=initial.owning_device_id,
            candidates=candidates,
            deleted_ids=(),
            remaining_ids=candidate_ids,
            success=True,
        )

    if report_path is None:
        raise ValueError("apply requires a validated report path")

    if not candidates:
        report = build_report(
            timestamp_ms=now_ms(),
            owning_device_id=initial.owning_device_id,
            candidates=(),
            deleted_ids=(),
            remaining_ids=(),
            success=True,
        )
        report_writer(report_path, canonical_report_json(replace(report, success=False)))
        return 0, report

    prepared_report = build_report(
        timestamp_ms=now_ms(),
        owning_device_id=initial.owning_device_id,
        candidates=candidates,
        deleted_ids=(),
        remaining_ids=candidate_ids,
        success=False,
    )
    report_writer(report_path, canonical_report_json(prepared_report))

    delete_failed = False
    for index, candidate in enumerate(candidates):
        try:
            await client.delete_peripheral(
                initial.owning_device_id,
                candidate.peripheral_id,
                now_ms(),
            )
        except Exception:
            delete_failed = True
            break
        if index < len(candidates) - 1:
            await sleep(1.0)

    try:
        verified = await client.snapshot_own_device()
    except Exception as exc:
        conservative = build_report(
            timestamp_ms=now_ms(),
            owning_device_id=initial.owning_device_id,
            candidates=candidates,
            deleted_ids=(),
            remaining_ids=candidate_ids,
            success=False,
        )
        try:
            report_writer(report_path, canonical_report_json(conservative))
        except Exception as write_error:
            raise _ReportPersistenceError(conservative) from write_error
        raise _VerificationReadError(conservative) from exc

    observed_ids = {
        peripheral.peripheral_id
        for peripheral in verified.peripherals
        if isinstance(peripheral.peripheral_id, str)
    }
    if verified.owning_device_id != initial.owning_device_id:
        remaining_ids = candidate_ids
    else:
        remaining_ids = tuple(
            candidate_id for candidate_id in candidate_ids if candidate_id in observed_ids
        )
    deleted_ids = tuple(
        candidate_id for candidate_id in candidate_ids if candidate_id not in remaining_ids
    )
    success = not delete_failed and not remaining_ids
    report = build_report(
        timestamp_ms=now_ms(),
        owning_device_id=initial.owning_device_id,
        candidates=candidates,
        deleted_ids=deleted_ids,
        remaining_ids=remaining_ids,
        success=success,
    )
    durable_report = report if not success else replace(report, success=False)
    try:
        report_writer(report_path, canonical_report_json(durable_report))
    except Exception as exc:
        raise _ReportPersistenceError(durable_report) from exc
    return (0 if success else 1), report


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m brilliant_mqtt.cleanup_legacy_mirror",
        description=(
            "Inventory legacy HA-mirror peripherals; use --apply to delete verified candidates."
        ),
    )
    parser.add_argument(
        "--apply", action="store_true", help="delete candidates after safe preflight"
    )
    parser.add_argument("--snapshot", help="absolute apply report path")
    return parser


def _native_client_factory() -> CleanupClient:
    return NativeCleanupClient()


def _retain_cleanup_task(task: asyncio.Task[Any]) -> None:
    """Retain and eventually consume a cancellation-resistant cleanup task."""
    if task in _RETAINED_CLEANUP_TASKS:
        return
    _RETAINED_CLEANUP_TASKS.add(task)

    def consume(done: asyncio.Task[Any]) -> None:
        _RETAINED_CLEANUP_TASKS.discard(done)
        try:
            done.result()
        except BaseException:
            pass

    task.add_done_callback(consume)


async def _wait_for_cleanup_task(task: asyncio.Task[Any], timeout_s: float) -> bool:
    """Wait within a hard wall-clock bound, never joining after cancel/timeout."""
    try:
        done, _pending = await asyncio.wait({task}, timeout=timeout_s)
    except asyncio.CancelledError:
        task.cancel()
        _retain_cleanup_task(task)
        raise
    if task not in done:
        task.cancel()
        _retain_cleanup_task(task)
        return False
    try:
        task.result()
    except BaseException:
        return False
    return True


async def _bounded_close(client: CleanupClient, timeout_s: float) -> bool:
    close_task = asyncio.create_task(client.close(), name="legacy-mirror-cleanup-close")
    _retain_cleanup_task(close_task)
    return await _wait_for_cleanup_task(close_task, timeout_s)


async def async_main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[[], CleanupClient] = _native_client_factory,
    effective_uid: Callable[[], int] = os.geteuid,
    safe_root: Path = SAFE_REPORT_ROOT,
    path_validator: PathValidator = validate_report_path,
    now_ms: Callable[[], int] = lambda: int(time.time() * 1000),
    sleep: Callable[[float], Awaitable[None]] = asyncio.sleep,
    report_writer: ReportWriter = write_report_atomic,
    close_timeout_s: float = _CLI_CLOSE_TIMEOUT_S,
) -> int:
    """Run the CLI with injectable off-panel seams."""
    args = _parser().parse_args(argv)
    raw_report_path: str | None = args.snapshot
    report_path = None if raw_report_path is None else Path(raw_report_path)

    if args.apply:
        try:
            if (
                effective_uid() != 0
                or raw_report_path is None
                or raw_report_path.endswith("/")
                or report_path is None
            ):
                raise ValueError("apply preflight rejected")
            report_path = path_validator(report_path, safe_root)
        except (OSError, ValueError):
            print(canonical_report_json(_empty_failure_report(now_ms())))
            return 1

    client: CleanupClient | None = None
    report: CleanupReport | None = None
    code = 1
    try:
        client = client_factory()
        await client.start()
        code, report = await run_cleanup(
            client,
            apply=args.apply,
            report_path=report_path,
            now_ms=now_ms,
            sleep=sleep,
            report_writer=report_writer,
        )
    except asyncio.CancelledError:
        raise
    except _ReportPersistenceError as exc:
        report = exc.report
        code = 1
    except _VerificationReadError as exc:
        report = exc.report
        code = 1
    except Exception:
        report = _empty_failure_report(now_ms())
        code = 1
    finally:
        if client is not None:
            closed = await _bounded_close(client, close_timeout_s)
            if not closed:
                code = 1
                if report is None:
                    report = _empty_failure_report(now_ms())
                elif report.success:
                    report = replace(report, success=False)

    if report is None:
        report = _empty_failure_report(now_ms())
        code = 1
    if code == 0 and args.apply and report_path is not None:
        try:
            report_writer(report_path, canonical_report_json(report))
        except Exception:
            report = replace(report, success=False)
            code = 1
    print(canonical_report_json(report))
    return code


def main(
    argv: Sequence[str] | None = None,
    *,
    client_factory: Callable[[], CleanupClient] = _native_client_factory,
    close_timeout_s: float = _CLI_CLOSE_TIMEOUT_S,
) -> int:
    """Synchronous entry point with non-joining abandoned-task teardown."""
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        return loop.run_until_complete(
            async_main(
                argv,
                client_factory=client_factory,
                close_timeout_s=close_timeout_s,
            )
        )
    finally:
        try:
            _close_main_loop(loop)
        finally:
            asyncio.set_event_loop(None)


def _close_main_loop(loop: asyncio.AbstractEventLoop) -> None:
    """Close *loop* without joining explicitly abandoned cleanup tasks."""
    tracked = {
        task for task in _RETAINED_CLEANUP_TASKS if task.get_loop() is loop and not task.done()
    }
    ordinary = asyncio.all_tasks(loop) - tracked
    for task in ordinary:
        task.cancel()
    if ordinary:
        loop.run_until_complete(asyncio.gather(*ordinary, return_exceptions=True))
    loop.run_until_complete(loop.shutdown_asyncgens())

    for task in tuple(_RETAINED_CLEANUP_TASKS):
        if task.get_loop() is not loop:
            continue
        _RETAINED_CLEANUP_TASKS.discard(task)
        if task.done():
            try:
                task.result()
            except BaseException:
                pass
        else:
            task.cancel()
            pending_task = cast(Any, task)
            pending_task._log_destroy_pending = False
    loop.close()


async def _maybe_await(value: object) -> object:
    if inspect.isawaitable(value):
        return await cast(Awaitable[object], value)
    return value


def _make_cleanup_observer_class(base: Any) -> Any:
    async def handle_notification(self: Any, notification: Any) -> None:
        del self, notification

    return type("_CleanupObserver", (base,), {"handle_notification": handle_notification})


class NativeCleanupClient:
    """Scoped reader and direct native MessageBusClient deletion adapter."""

    def __init__(self) -> None:
        self._observer: Any = None
        self._processor: Any = None

    async def start(self) -> None:
        """Connect after importing panel-only modules at the runtime boundary."""
        import lib.protocol.message_bus_peer_service as mbps
        from lib.message_bus_api.observer_interface import RPCObserver
        from lib.protocol.processor import SinglePeerProcessor

        loop = asyncio.get_running_loop()
        observer_class = _make_cleanup_observer_class(RPCObserver)
        observer = observer_class(loop)
        processor = SinglePeerProcessor(
            socket_path=_SOCKET_PATH,
            my_name=f"brilliant_mqtt_cleanup-{secrets.token_hex(4)}",
            handler=mbps.PeripheralServer(observer),
            client_class=mbps.MessageBusClient,
            loop=loop,
        )
        self._observer = observer
        self._processor = processor
        await processor.start()
        deadline = loop.time() + _CONNECT_TIMEOUT_S
        while not processor.is_connected():
            if loop.time() >= deadline:
                raise TimeoutError("message bus connection timed out")
            await asyncio.sleep(_CONNECT_POLL_S)
        await observer.start(processor, None)

    async def snapshot_own_device(self) -> OwnDeviceSnapshot:
        """Use get_device(own_id), never the whole-home get_all call."""
        if self._observer is None:
            raise RuntimeError("cleanup client is not started")
        owning_device_id = str(self._observer.get_owning_device_id())
        raw_device = await self._observer.get_device(owning_device_id)
        raw_peripherals = None if raw_device is None else getattr(raw_device, "peripherals", None)
        if raw_peripherals is None:
            raise RuntimeError("owning device snapshot unavailable")
        peripherals = tuple(
            normalize_peripheral(owning_device_id, peripheral_id, raw_peripheral)
            for peripheral_id, raw_peripheral in dict(raw_peripherals).items()
        )
        return OwnDeviceSnapshot(owning_device_id, peripherals)

    async def delete_peripheral(
        self, device_id: str, peripheral_id: str, deletion_time_ms: int
    ) -> None:
        """Call the acquired firmware's MessageBusClient deletion method."""
        if self._processor is None or getattr(self._processor, "client", None) is None:
            raise RuntimeError("cleanup client is not started")
        await _maybe_await(
            self._processor.client.delete_peripheral(
                device_id,
                peripheral_id,
                deletion_time_ms,
            )
        )

    async def close(self) -> None:
        """Attempt both shutdown stages, with a bound on each stage."""
        first_error: BaseException | None = None
        cancellation: asyncio.CancelledError | None = None
        observer, processor = self._observer, self._processor
        self._observer = None
        self._processor = None
        for target in (observer, processor):
            if target is None:
                continue
            shutdown_task = asyncio.create_task(target.shutdown())
            _retain_cleanup_task(shutdown_task)
            try:
                completed = await _wait_for_cleanup_task(
                    shutdown_task,
                    _NATIVE_CLOSE_STEP_TIMEOUT_S,
                )
            except asyncio.CancelledError as exc:
                cancellation = exc
                continue
            if not completed and first_error is None:
                first_error = RuntimeError("native shutdown step failed")
        if cancellation is not None:
            raise cancellation
        if first_error is not None:
            raise RuntimeError("native cleanup client close failed") from first_error


if __name__ == "__main__":
    raise SystemExit(main())
