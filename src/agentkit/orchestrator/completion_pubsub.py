"""Cross-process run-completion signalling via Redis pub/sub.

Each terminal Run event is published to a per-run channel::

    agentkit:run:terminal:{run_id}

Any number of consumers can subscribe and await the signal, enabling
cross-process ``wait_for_completion`` without shared memory.

Usage (producer side, inside Orchestrator when a run terminates)::

    await notifier.publish_terminal(run_id, status="Succeeded")

Usage (consumer side, e.g. an API handler awaiting completion)::

    async with notifier.subscribe(run_id) as ch:
        payload = await asyncio.wait_for(ch.recv(), timeout=30.0)

See ``Doc05_Orchestrator.md`` for the completion-wait design.
"""

from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import UTC, datetime
from typing import Any

import orjson
from redis.asyncio import Redis
from redis.exceptions import RedisError

from agentkit.common.logging import get_logger

log = get_logger(__name__)


class _Channel:
    """Wraps a :class:`redis.asyncio.client.PubSub` for simple message consumption.

    Filters out Redis subscription-management messages and exposes a single
    ``recv()`` coroutine that returns the deserialized payload dict.
    """

    def __init__(self, pubsub: Any) -> None:
        self._pubsub = pubsub

    async def recv(self) -> dict:
        """Block until a terminal message arrives; return the parsed payload.

        Designed to be wrapped with :func:`asyncio.wait_for` for timeout control::

            payload = await asyncio.wait_for(ch.recv(), timeout=30.0)
        """
        while True:
            msg = await self._pubsub.get_message(
                ignore_subscribe_messages=True,
                timeout=0.1,
            )
            if msg is not None and msg.get("type") == "message":
                data = msg["data"]
                # data is bytes when decode_responses=False
                return orjson.loads(data)
            # Yield control briefly before retrying
            await asyncio.sleep(0)


class RedisCompletionNotifier:
    """Broadcasts run terminal events via Redis pub/sub.

    Channel naming: ``{namespace}:run:terminal:{run_id}``

    Payload schema (orjson-serialized)::

        {"run_id": str, "status": str, "ended_at": str (ISO-8601)}

    One :class:`Redis` client handles both publish and the pool backing
    each :func:`subscribe` context; redis-py keeps pub/sub sockets
    separate from normal-command sockets automatically.
    """

    def __init__(
        self,
        *,
        redis: Redis | None = None,
        url: str = "redis://localhost:6379/0",
        namespace: str = "agentkit",
    ) -> None:
        self._redis: Redis | None = redis
        self._url = url
        self._namespace = namespace
        self._owns_redis = redis is None

    # ------------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------------

    async def start(self) -> None:
        """Connect and ping Redis. Raises :class:`RuntimeError` if unreachable."""
        if self._redis is None:
            # decode_responses=False keeps payload bytes intact for orjson
            self._redis = Redis.from_url(self._url, decode_responses=False)
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
                log.exception("completion_pubsub.close_failed")
            finally:
                self._redis = None

    # ------------------------------------------------------------------
    # Pub/sub API
    # ------------------------------------------------------------------

    async def publish_terminal(
        self,
        run_id: str,
        *,
        status: str,
        ended_at: str | None = None,
    ) -> int:
        """Publish a single terminal notification.

        Args:
            run_id: Identifier of the completed run.
            status: Terminal status string, e.g. ``"Succeeded"`` or ``"Failed"``.
            ended_at: ISO-8601 timestamp; defaults to current UTC time.

        Returns:
            Number of subscribers that received the message.
        """
        if self._redis is None:
            raise RuntimeError("redis unavailable")
        if ended_at is None:
            ended_at = datetime.now(tz=UTC).isoformat()
        payload = orjson.dumps(
            {"run_id": run_id, "status": status, "ended_at": ended_at}
        )
        channel = self._channel(run_id)
        try:
            count: int = await self._redis.publish(channel, payload)
        except RedisError as exc:
            raise RuntimeError(f"redis unavailable: {exc}") from exc
        log.debug(
            "completion_pubsub.published",
            run_id=run_id,
            status=status,
            channel=channel,
            subscribers=count,
        )
        return count

    @asynccontextmanager
    async def subscribe(self, run_id: str):
        """Async context manager yielding a :class:`_Channel` with ``recv()``.

        The subscription is active for the duration of the ``async with`` block.
        Multiple concurrent subscribers on the same ``run_id`` all receive
        the same broadcast (fan-out).

        Example::

            async with notifier.subscribe(run_id) as ch:
                payload = await asyncio.wait_for(ch.recv(), timeout=30.0)
        """
        if self._redis is None:
            raise RuntimeError("redis unavailable")
        channel = self._channel(run_id)
        pubsub = self._redis.pubsub()
        await pubsub.subscribe(channel)
        try:
            yield _Channel(pubsub)
        finally:
            try:
                await pubsub.unsubscribe(channel)
                await pubsub.aclose()
            except RedisError:
                log.exception("completion_pubsub.unsubscribe_failed")

    # ------------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------------

    def _channel(self, run_id: str) -> str:
        return f"{self._namespace}:run:terminal:{run_id}"
