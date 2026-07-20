"""Cancellation-safe helpers for bounded fail-closed cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any

_CLEANUP_TIMEOUT_SECONDS = 60.0


async def _cancel_and_drain[T](task: asyncio.Task[T]) -> None:
    """Cancel a timed-out cleanup through its async finalizer, then consume it."""
    task.cancel()
    # A cancellation delivered to a stalled command can move the coroutine into an
    # async ``finally`` (the SSH close). Give it that turn, then cancel that finalizer
    # too if it stalls. PanelShell operations do not suppress cancellation.
    await asyncio.sleep(0)
    if not task.done():
        task.cancel()
    await asyncio.gather(task, return_exceptions=True)


async def _bounded_cleanup[T](cleanup: Coroutine[Any, Any, T]) -> T:
    """Run one safety cleanup under a deadline covering all of its finalizers."""
    cleanup_task = asyncio.create_task(
        cleanup,
        name="brilliant-mqtt-fail-closed-cleanup",
    )
    try:
        done, _pending = await asyncio.wait(
            {cleanup_task},
            timeout=_CLEANUP_TIMEOUT_SECONDS,
        )
    except BaseException:
        await _cancel_and_drain(cleanup_task)
        raise
    if cleanup_task in done:
        return cleanup_task.result()
    await _cancel_and_drain(cleanup_task)
    raise TimeoutError(f"fail-closed cleanup exceeded {_CLEANUP_TIMEOUT_SECONDS:g} seconds")


async def shield_and_drain[T](cleanup: Coroutine[Any, Any, T]) -> T:
    """Run bounded *cleanup* before propagating any new cancellation.

    ``asyncio.shield()`` alone leaves its inner task running when the caller is
    cancelled.  A safety cleanup must also be drained so the caller cannot close its
    SSH session underneath it.  The deadline lives inside the shielded task, so outer
    cancellation cannot disarm it and a stalled command or finalizer cannot block HA
    shutdown forever.  Callers remain responsible for mapping or re-raising failures.
    """
    task = asyncio.create_task(_bounded_cleanup(cleanup))
    interrupted: asyncio.CancelledError | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError as error:
            interrupted = interrupted or error
    try:
        result = task.result()
    except BaseException as cleanup_error:
        if interrupted is not None:
            raise interrupted from cleanup_error
        raise
    if interrupted is not None:
        raise interrupted
    return result


async def shielded_cleanup_after_failure[T](
    error: BaseException, cleanup: Coroutine[Any, Any, T]
) -> T:
    """Drain cleanup and preserve an original cancellation-class failure.

    Ordinary operation failures may still be mapped by the caller when cleanup
    succeeds.  A cancellation, ``KeyboardInterrupt``, or ``SystemExit`` is re-raised
    here only after cleanup completes, even if another cancellation arrives meanwhile.
    """
    try:
        result = await shield_and_drain(cleanup)
    except BaseException as cleanup_error:
        if not isinstance(error, Exception):
            raise error from cleanup_error
        raise
    if not isinstance(error, Exception):
        raise error
    return result
