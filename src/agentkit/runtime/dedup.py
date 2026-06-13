"""TTL-based event-id deduplication store.

Implements Doc03 §6.1 Gate 4 — local in-memory deduplication
keyed on ``event_id`` with a sliding TTL window.

For multi-instance / multi-process deployments the v0.2 plan is
to back this with Redis ``SET event_id ts NX PX <window_ms>``;
the in-process implementation here is the *correct* default for
single-Runtime setups and is what unit tests use.
"""

from __future__ import annotations

import time
from collections import OrderedDict
from typing import Final


class DedupStore:
    """O(1) ``event_id`` cache with sliding TTL.

    Implementation: ``OrderedDict`` mapping ``event_id → expires_at``.
    Lookups touch ``time.monotonic()`` once and pop expired keys lazily;
    we don't run a sweeper thread because the eviction stays bounded
    by usage.

    Thread/coroutine safety: not concurrency-safe by itself — caller
    is expected to hold the AgentInstance's lock (or rely on
    asyncio's single-threaded model).
    """

    def __init__(self, *, window_ms: int = 300_000, max_entries: int = 100_000) -> None:
        if window_ms < 0:
            raise ValueError("window_ms must be >= 0")
        if max_entries < 1:
            raise ValueError("max_entries must be >= 1")
        self._window_s: Final[float] = window_ms / 1000.0
        self._max_entries: Final[int] = max_entries
        self._entries: OrderedDict[str, float] = OrderedDict()

    def __len__(self) -> int:
        return len(self._entries)

    def seen(self, event_id: str) -> bool:
        """Return True iff ``event_id`` was already seen within the window.

        If the entry has expired, treat it as unseen *and* refresh the
        slot so the second arrival counts as the new "first seen".
        Inserts a new entry on first sight.
        """
        if not event_id:
            raise ValueError("event_id must be non-empty")
        now = time.monotonic()
        deadline = self._entries.get(event_id)
        if deadline is not None and deadline > now:
            # Hit — fresh entry within the window.
            return True

        # Either never seen OR expired. Insert as fresh.
        if deadline is not None:
            self._entries.pop(event_id, None)
        self._entries[event_id] = now + self._window_s
        self._evict_if_needed(now)
        return False

    # ----------------------------------------------------------------
    # Internals
    # ----------------------------------------------------------------

    def _evict_if_needed(self, now: float) -> None:
        # Lazy eviction: drop expired heads until we're under the cap
        # OR we've evicted everything that could be expired.
        while self._entries:
            oldest_id, expiry = next(iter(self._entries.items()))
            if expiry <= now:
                self._entries.popitem(last=False)
                continue
            break

        # Hard cap: drop oldest entries to stay under max_entries.
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
