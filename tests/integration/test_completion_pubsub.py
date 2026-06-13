"""Integration tests for RedisCompletionNotifier against a real Redis instance.

Skipped automatically if Redis is unreachable. To run::

    docker run --rm -p 6379:6379 redis:7
    pytest -m integration tests/integration/test_completion_pubsub.py -v -s
"""

from __future__ import annotations

import asyncio
import os

import pytest
from redis.asyncio import Redis
from redis.exceptions import RedisError

from agentkit.orchestrator import RedisCompletionNotifier

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


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------


@pytest.fixture
async def notifier():
    """A started RedisCompletionNotifier pointing at the test namespace."""
    if not await _redis_reachable():
        pytest.skip(f"Redis not reachable at {REDIS_URL}")

    n = RedisCompletionNotifier(url=REDIS_URL, namespace="test:completion")
    await n.start()
    yield n
    await n.stop()


# ---------------------------------------------------------------
# Tests
# ---------------------------------------------------------------


class TestRedisCompletionNotifier:
    async def test_subscriber_receives_published_payload(
        self, notifier: RedisCompletionNotifier,
    ) -> None:
        """A subscriber that is active before publish should receive the payload."""
        run_id = "run_pubsub_001"
        async with notifier.subscribe(run_id) as ch:
            await notifier.publish_terminal(run_id, status="Succeeded")
            payload = await asyncio.wait_for(ch.recv(), timeout=5.0)

        assert payload["run_id"] == run_id
        assert payload["status"] == "Succeeded"
        assert "ended_at" in payload

    async def test_fanout_two_subscribers_both_receive(
        self, notifier: RedisCompletionNotifier,
    ) -> None:
        """Both subscribers should receive the same broadcast message (fan-out)."""
        run_id = "run_pubsub_002"
        async with notifier.subscribe(run_id) as ch1, notifier.subscribe(run_id) as ch2:
            await notifier.publish_terminal(run_id, status="Failed")
            p1 = await asyncio.wait_for(ch1.recv(), timeout=5.0)
            p2 = await asyncio.wait_for(ch2.recv(), timeout=5.0)

        assert p1["run_id"] == run_id
        assert p1["status"] == "Failed"
        assert p2["run_id"] == run_id
        assert p2["status"] == "Failed"

    async def test_no_publish_raises_timeout_error(
        self, notifier: RedisCompletionNotifier,
    ) -> None:
        """If no message is published, wait_for should raise TimeoutError."""
        run_id = "run_pubsub_003"
        async with notifier.subscribe(run_id) as ch:
            with pytest.raises(asyncio.TimeoutError):
                await asyncio.wait_for(ch.recv(), timeout=0.4)

    async def test_payload_contains_custom_ended_at(
        self, notifier: RedisCompletionNotifier,
    ) -> None:
        """Caller-supplied ended_at should be preserved in the payload."""
        run_id = "run_pubsub_004"
        custom_ts = "2024-01-15T12:00:00+00:00"
        async with notifier.subscribe(run_id) as ch:
            await notifier.publish_terminal(
                run_id, status="Succeeded", ended_at=custom_ts,
            )
            payload = await asyncio.wait_for(ch.recv(), timeout=5.0)

        assert payload["ended_at"] == custom_ts

    async def test_publish_returns_subscriber_count(
        self, notifier: RedisCompletionNotifier,
    ) -> None:
        """publish_terminal should return the number of active subscribers."""
        run_id = "run_pubsub_005"
        async with notifier.subscribe(run_id) as _ch:
            count = await notifier.publish_terminal(run_id, status="Succeeded")
        assert count == 1
