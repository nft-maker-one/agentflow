"""In-process token-bucket rate limiter for the LLM Gateway.

Keyed on ``(provider, model, agent_tag)`` per ``Doc06 §8``.
Default off: a :class:`RateLimit` with both ``rpm == 0`` and
``tpm == 0`` makes :meth:`RateLimiter.acquire` a no-op.

Two dimensions are enforced *independently*:

* **rpm** — requests per minute (one acquisition costs 1 token)
* **tpm** — tokens per minute (one acquisition costs N tokens)

If either bucket is empty the limiter either waits (``strategy=wait``)
or raises (``strategy=fail_fast``).

For Phase 2 we'll add a Redis-backed shared bucket; the in-process
version is sufficient for single-Runtime deployments and for tests.
"""

from __future__ import annotations

import asyncio
import time
from dataclasses import dataclass

from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.models import RateLimit


@dataclass
class _Bucket:
    """A simple monotonic-clock token bucket.

    ``capacity`` tokens regenerate over ``window_s`` (60s for rpm/tpm).
    """

    capacity: float
    window_s: float
    tokens: float
    last_refill: float

    def refill(self, now: float) -> None:
        if self.capacity <= 0:
            return
        elapsed = now - self.last_refill
        if elapsed <= 0:
            return
        rate_per_s = self.capacity / self.window_s
        self.tokens = min(self.capacity, self.tokens + elapsed * rate_per_s)
        self.last_refill = now

    def try_take(self, n: float, now: float) -> bool:
        self.refill(now)
        if self.tokens >= n:
            self.tokens -= n
            return True
        return False

    def time_to_available(self, n: float, now: float) -> float:
        """Seconds until ``n`` tokens are available, given current refill rate."""
        self.refill(now)
        if self.tokens >= n:
            return 0.0
        if self.capacity <= 0:
            return float("inf")
        deficit = n - self.tokens
        rate_per_s = self.capacity / self.window_s
        return deficit / rate_per_s


def _make_key(provider: str, model: str, agent_tag: str | None) -> str:
    return f"{provider}::{model}::{agent_tag or ''}"


class RateLimiter:
    """Async in-process rate limiter shared by all callers in this Gateway.

    Methods:

    * :meth:`acquire` — block (or fail-fast) until enough capacity exists
      to admit a request consuming ``tokens_estimated`` tokens.
    """

    def __init__(self) -> None:
        self._buckets_rpm: dict[str, _Bucket] = {}
        self._buckets_tpm: dict[str, _Bucket] = {}
        # B9: one lock PER (provider, model, agent_tag) key instead of a
        # single global lock, so independent provider/model pairs check
        # their independent token buckets in parallel rather than
        # serializing every LLM call across the whole gateway.
        self._locks: dict[str, asyncio.Lock] = {}

    def _lock_for(self, key: str) -> asyncio.Lock:
        # Safe without its own guard: asyncio is single-threaded and there
        # is no ``await`` between the get and the assignment, so this is
        # atomic with respect to other coroutines.
        lock = self._locks.get(key)
        if lock is None:
            lock = asyncio.Lock()
            self._locks[key] = lock
        return lock

    async def acquire(
        self,
        rl: RateLimit | None,
        *,
        provider: str,
        model: str,
        agent_tag: str | None,
        tokens_estimated: int,
        budget_ms: int,
    ) -> None:
        """Reserve 1 request slot and ``tokens_estimated`` tokens.

        Behavior:

        * No :class:`RateLimit` configured (or both rpm/tpm == 0) → no-op.
        * ``strategy="wait"`` → sleep until both buckets admit, but no
          longer than ``budget_ms`` total — otherwise raises
          :class:`LLMError(RATE_LIMIT_429)`.
        * ``strategy="fail_fast"`` → raises immediately if either bucket
          can't admit right now.
        """
        if rl is None or (rl.rpm == 0 and rl.tpm == 0):
            return

        key = _make_key(provider, model, agent_tag)
        deadline = time.monotonic() + budget_ms / 1000.0

        while True:
            now = time.monotonic()
            async with self._lock_for(key):
                rpm_bucket = self._get_or_create_bucket(self._buckets_rpm, key, rl.rpm)
                tpm_bucket = self._get_or_create_bucket(self._buckets_tpm, key, rl.tpm)

                rpm_ok = rl.rpm == 0 or rpm_bucket.try_take(1, now)
                tpm_ok = rl.tpm == 0 or tpm_bucket.try_take(tokens_estimated, now)

                if rpm_ok and tpm_ok:
                    return

                # Roll back any partial deduction so we don't leak tokens.
                if rpm_ok and rl.rpm:
                    rpm_bucket.tokens += 1
                if tpm_ok and rl.tpm:
                    tpm_bucket.tokens += tokens_estimated

                wait_rpm = (
                    rpm_bucket.time_to_available(1, now) if not rpm_ok else 0.0
                )
                wait_tpm = (
                    tpm_bucket.time_to_available(tokens_estimated, now)
                    if not tpm_ok
                    else 0.0
                )
                wait_s = max(wait_rpm, wait_tpm)

            if rl.strategy == "fail_fast":
                raise LLMError(
                    LLMErrorClass.RATE_LIMIT_429,
                    f"rate limit exceeded ({key}); fail_fast strategy",
                    provider=provider,
                    model=model,
                )

            if time.monotonic() + wait_s > deadline:
                raise LLMError(
                    LLMErrorClass.RATE_LIMIT_429,
                    f"rate limit wait exceeded budget ({budget_ms}ms)",
                    provider=provider,
                    model=model,
                    retry_after_ms=int(wait_s * 1_000),
                )
            await asyncio.sleep(min(wait_s, 0.5))  # poll at most every 500ms

    @staticmethod
    def _get_or_create_bucket(
        store: dict[str, _Bucket], key: str, capacity_per_minute: int,
    ) -> _Bucket:
        bucket = store.get(key)
        if bucket is not None and bucket.capacity == capacity_per_minute:
            return bucket
        # Either missing or capacity changed — create fresh.
        now = time.monotonic()
        bucket = _Bucket(
            capacity=float(capacity_per_minute),
            window_s=60.0,
            tokens=float(capacity_per_minute),
            last_refill=now,
        )
        store[key] = bucket
        return bucket
