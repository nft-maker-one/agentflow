"""Integration tests for RedisDedupStore against a real Redis instance.

Skipped automatically if Redis is unreachable. To run::

    docker run --rm -p 6379:6379 redis:7
    pytest -m integration tests/integration/test_dedup_redis.py -v -s
"""

from __future__ import annotations

import asyncio
import os

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from agentkit.runtime import RedisDedupStore

pytestmark = pytest.mark.integration

REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")

# ---------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------


async def _redis_reachable() -> bool:
    """Return True iff the test Redis instance responds to PING."""
    try:
        r = Redis.from_url(REDIS_URL)
        await r.ping()
        await r.aclose()
        return True
    except (RedisError, OSError, ConnectionRefusedError):
        return False


async def _flush_namespace(namespace: str) -> None:
    """Delete all keys under the given namespace prefix."""
    r = Redis.from_url(REDIS_URL)
    cursor = 0
    while True:
        cursor, batch = await r.scan(cursor=cursor, match=f"{namespace}:*", count=200)
        if batch:
            await r.delete(*batch)
        if cursor == 0:
            break
    await r.aclose()


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------


@pytest.fixture
async def dedup():
    """A started RedisDedupStore with a clean test namespace."""
    if not await _redis_reachable():
        pytest.skip(f"Redis not reachable at {REDIS_URL}")

    namespace = "test:dedup:main"
    await _flush_namespace(namespace)

    store = RedisDedupStore(url=REDIS_URL, namespace=namespace, window_ms=60_000)
    await store.start()
    yield store
    await store.stop()
    await _flush_namespace(namespace)


# ---------------------------------------------------------------
# Tests
# ---------------------------------------------------------------


class TestRedisDedupStore:
    async def test_first_seen_returns_false(self, dedup: RedisDedupStore) -> None:
        """First encounter of an event_id should NOT be flagged as duplicate."""
        result = await dedup.seen("evt_first_001")
        assert result is False

    async def test_second_seen_returns_true(self, dedup: RedisDedupStore) -> None:
        """Second encounter of the same event_id within the window is a duplicate."""
        assert await dedup.seen("evt_dup_001") is False
        assert await dedup.seen("evt_dup_001") is True

    async def test_distinct_ids_are_independent(self, dedup: RedisDedupStore) -> None:
        """Different event_ids should not interfere with each other."""
        assert await dedup.seen("evt_a") is False
        assert await dedup.seen("evt_b") is False
        assert await dedup.seen("evt_a") is True
        assert await dedup.seen("evt_b") is True

    async def test_different_namespaces_are_independent(self) -> None:
        """The same event_id in different namespaces is treated independently."""
        if not await _redis_reachable():
            pytest.skip(f"Redis not reachable at {REDIS_URL}")

        ns_a = "test:dedup:ns_a"
        ns_b = "test:dedup:ns_b"
        await _flush_namespace(ns_a)
        await _flush_namespace(ns_b)

        store_a = RedisDedupStore(url=REDIS_URL, namespace=ns_a)
        store_b = RedisDedupStore(url=REDIS_URL, namespace=ns_b)
        await store_a.start()
        await store_b.start()
        try:
            # Same event_id — both namespaces see it as fresh
            assert await store_a.seen("shared_event") is False
            assert await store_b.seen("shared_event") is False
            # Within each namespace, second call is a duplicate
            assert await store_a.seen("shared_event") is True
            assert await store_b.seen("shared_event") is True
        finally:
            await store_a.stop()
            await store_b.stop()
            await _flush_namespace(ns_a)
            await _flush_namespace(ns_b)

    async def test_ttl_expiry_resets_seen(self) -> None:
        """After the TTL window expires the event_id is treated as fresh again."""
        if not await _redis_reachable():
            pytest.skip(f"Redis not reachable at {REDIS_URL}")

        namespace = "test:dedup:ttl"
        await _flush_namespace(namespace)
        store = RedisDedupStore(url=REDIS_URL, namespace=namespace, window_ms=200)
        await store.start()
        try:
            assert await store.seen("evt_ttl") is False   # first — fresh
            assert await store.seen("evt_ttl") is True    # second — duplicate
            await asyncio.sleep(0.35)                     # wait for TTL expiry
            assert await store.seen("evt_ttl") is False   # should be fresh again
        finally:
            await store.stop()
            await _flush_namespace(namespace)

    async def test_add_marks_event_as_seen(self, dedup: RedisDedupStore) -> None:
        """add() should pre-populate the store so subsequent seen() returns True."""
        await dedup.add("evt_add_001")
        assert await dedup.seen("evt_add_001") is True
