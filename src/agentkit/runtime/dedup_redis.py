"""Redis-backed event-id deduplication store.

Atomic check-and-set via ``SET key NX PX window_ms``, giving the same
FIFO-TTL semantics as the in-memory :class:`DedupStore` but shared
across multiple Runtime instances or processes.

Usage::

    store = RedisDedupStore(url="redis://localhost:6379/0", window_ms=60_000)
    await store.start()

    if await store.seen(event.id):
        return  # duplicate — drop

See ``Doc03_AgentRuntime.md §6.1 Gate 4`` for the deduplication contract.
"""

from __future__ import annotations

from redis.asyncio import Redis
from redis.exceptions import RedisError

from agentkit.common.logging import get_logger

log = get_logger(__name__)


class RedisDedupStore:
    """Redis-backed dedup with NX SET semantics.

    Drop-in async replacement for the in-memory :class:`DedupStore`
    for multi-instance deployments.
    """

    def __init__(
        self,
        *,
        redis: Redis | None = None,
        url: str = "redis://localhost:6379/0",
        namespace: str = "agentkit:dedup",
        window_ms: int = 60_000,
    ) -> None:
        self._redis: Redis | None = redis
        self._url = url
        self._namespace = namespace
        self._window_ms = window_ms
        self._owns_redis = redis is None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect and ping Redis. Raises :class:`RuntimeError` if unreachable."""
        if self._redis is None:
            self._redis = Redis.from_url(self._url, decode_responses=True)
        try:
            await self._redis.ping()
        except RedisError as exc:
            raise RuntimeError(f"redis unavailable: {exc}") from exc

    async def stop(self) -> None:
        """Close the client if this instance created it."""
        if self._owns_redis and self._redis is not None:
            try:
                await self._redis.aclose()
            except RedisError:
                log.exception("dedup_redis.close_failed")
            finally:
                self._redis = None

    # ------------------------------------------------------------------
    # Dedup interface
    # ------------------------------------------------------------------

    async def seen(self, event_id: str) -> bool:
        """Return True if already seen (duplicate), False on first sight.

        Uses ``SET key 1 NX PX window_ms`` — atomic check-and-set.
        ``SET NX`` returns ``None`` when the key already exists,
        meaning the event is a duplicate.

        Raises:
            RuntimeError: Redis is not started or the connection failed.
        """
        if self._redis is None:
            raise RuntimeError("redis unavailable")
        key = f"{self._namespace}:{event_id}"
        try:
            ok = await self._redis.set(key, "1", nx=True, px=self._window_ms)
        except RedisError as exc:
            raise RuntimeError(f"redis unavailable: {exc}") from exc
        # SET NX returns None when key already existed → duplicate
        return ok is None

    async def add(self, event_id: str) -> None:
        """Unconditional write — ensures the event_id is marked seen.

        Idempotent; used primarily in tests to pre-populate the store.
        """
        await self.seen(event_id)
