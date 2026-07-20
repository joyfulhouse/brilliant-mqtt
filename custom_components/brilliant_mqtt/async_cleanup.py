"""Cancellation-safe helpers for bounded fail-closed cleanup."""

from __future__ import annotations

import asyncio
from collections.abc import Coroutine
from typing import Any


async def shield_and_drain[T](cleanup: Coroutine[Any, Any, T]) -> T:
    """Run *cleanup* to completion before propagating any new cancellation.

    ``asyncio.shield()`` alone leaves its inner task running when the caller is
    cancelled.  A safety cleanup must also be drained so the caller cannot close its
    SSH session underneath it.  Callers invoke this only from an exception handler and
    remain responsible for re-raising that original exception afterward.
    """
    task = asyncio.create_task(cleanup)
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
