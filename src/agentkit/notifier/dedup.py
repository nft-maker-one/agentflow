"""Notification dedup — Phase 1 in-memory + interface for Redis backend later.

Doc08 §4.1 — collapse / suppress within a sliding window.
The hash key is hashed over ``rule.id`` plus whatever
``DedupSpec.by`` keys reference (e.g. ``run_id``).

For Phase 1 we ship the in-memory implementation; production
deployments will plug in a Redis-backed one in Phase 2 (same
Protocol).
"""

from __future__ import annotations

import hashlib
import time
from collections import OrderedDict
from typing import Any, Protocol, runtime_checkable

from agentkit.notifier.models import DedupSpec, NotificationRule


# ============================================================
# Protocol — what the Notifier expects
# ============================================================


@runtime_checkable
class DedupBackend(Protocol):
    """Minimal contract; Phase 2 RedisDedupBackend satisfies it."""

    def should_send(
        self, rule: NotificationRule, *, context: dict[str, Any],
    ) -> bool:
        """Return ``True`` if the notification can go out, ``False``
        if it's deduped within the configured window.
        """
        ...


# ============================================================
# In-memory implementation
# ============================================================


class InMemoryDedupBackend:
    """Sliding-window dedup using ``time.monotonic()`` + LRU map.

    Not concurrency-safe across processes — fine for single-process
    Phase 1 demos and unit tests.
    """

    def __init__(self, *, max_entries: int = 10_000) -> None:
        self._entries: OrderedDict[str, float] = OrderedDict()
        self._max_entries = max_entries

    def __len__(self) -> int:
        return len(self._entries)

    def should_send(
        self, rule: NotificationRule, *, context: dict[str, Any],
    ) -> bool:
        spec = rule.dedup
        if spec is None or spec.window_seconds <= 0:
            return True
        key = self._key_for(rule, spec, context)
        now = time.monotonic()
        deadline = self._entries.get(key)
        if deadline is not None and deadline > now:
            return False  # still within window — suppress
        self._entries[key] = now + spec.window_seconds
        self._evict_expired(now)
        return True

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    @staticmethod
    def _key_for(
        rule: NotificationRule,
        spec: DedupSpec,
        context: dict[str, Any],
    ) -> str:
        parts = [rule.id]
        for f in spec.by:
            value = context.get(f, "")
            if isinstance(value, (dict, list)):
                value = repr(value)
            parts.append(f"{f}={value}")
        digest = hashlib.sha1(  # noqa: S324 — non-crypto hashing
            "|".join(parts).encode("utf-8"),
        ).hexdigest()
        return f"dedup:{digest[:16]}"

    def _evict_expired(self, now: float) -> None:
        # Drop expired heads until we're back under the cap.
        while self._entries:
            oldest_key, expiry = next(iter(self._entries.items()))
            if expiry <= now:
                self._entries.popitem(last=False)
                continue
            break
        while len(self._entries) > self._max_entries:
            self._entries.popitem(last=False)
