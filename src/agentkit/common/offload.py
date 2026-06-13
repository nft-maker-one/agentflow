"""CPU-offload helper — move blocking/CPU-bound work off the event loop.

The runtime is a single asyncio event loop (one per process). Any
*synchronous* CPU work executed inline on that loop — jsonschema
validation, JSON parsing, user ``python_script`` bodies — freezes
**every** coroutine in the process until it returns (see
``docs/CONCURRENCY.md`` §5).

:func:`maybe_offload` runs such work in the default thread pool via
:func:`asyncio.to_thread`, so the loop stays responsive. For small
inputs the thread hand-off costs more than it saves, so callers pass a
cheap ``size_hint`` (e.g. ``len(text)``) and work below ``threshold``
runs inline.
"""

from __future__ import annotations

import asyncio
from collections.abc import Callable
from typing import TypeVar

T = TypeVar("T")

#: Inputs smaller than this (by the caller's ``size_hint`` metric, e.g.
#: characters/bytes) run inline — the thread hand-off isn't worth it.
DEFAULT_OFFLOAD_THRESHOLD: int = 8192


async def maybe_offload(
    fn: Callable[..., T],
    *args: object,
    size_hint: int | None = None,
    threshold: int = DEFAULT_OFFLOAD_THRESHOLD,
) -> T:
    """Run ``fn(*args)`` inline (small input) or in a thread (large input).

    Parameters
    ----------
    fn, *args
        The synchronous callable and its positional arguments.
    size_hint
        Cheap proxy for the input size (e.g. ``len(text)``). When
        ``None`` the work is always offloaded. When provided and below
        ``threshold`` the work runs inline to avoid thread overhead.
    threshold
        Size below which to run inline. Defaults to
        :data:`DEFAULT_OFFLOAD_THRESHOLD`.
    """
    if size_hint is not None and size_hint < threshold:
        return fn(*args)
    return await asyncio.to_thread(fn, *args)
