"""EventBus Protocol — the abstraction every adapter implements.

See ``Doc02 §2.2`` for the full contract. This file is the
*authoritative* shape of the bus interface; all adapters
(:mod:`agentkit.bus.kafka`, future NATS / Redis Streams) plug in
here and interact with the rest of the framework only via these
types.

**Design rule**: this module is pure typing — no implementation.
Adapters live in subpackages.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import Literal, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from agentkit.models.envelope import Envelope


# ============================================================
# Value objects (data carriers — no behavior)
# ============================================================


class PublishReceipt(BaseModel):
    """Acknowledgment returned by a successful publish."""

    model_config = ConfigDict(frozen=True)

    event_id: str
    topic: str
    partition: int | None = None
    offset: int | None = None


class SubscribeSpec(BaseModel):
    """Parameters describing a subscription request.

    See ``Doc02 §2.2``. ``topic_pattern`` supports framework-level
    wildcards (``*`` / ``#``); the adapter is responsible for
    translating to the broker-native form.
    """

    model_config = ConfigDict(frozen=True)

    topic_pattern: str = Field(min_length=1)
    group: str = Field(min_length=1)
    starting_position: Literal["earliest", "latest", "committed"] = "committed"
    batch_size: int = Field(default=16, ge=1, le=10_000)
    max_inflight: int = Field(default=16, ge=1, le=10_000)
    visibility_timeout_ms: int = Field(default=60_000, ge=1_000)


class DeliveredMessage(BaseModel):
    """An envelope plus delivery metadata (partition / offset).

    Returned by :meth:`Subscriber.messages`; passed back to
    :meth:`EventBus.ack` / :meth:`EventBus.nack` to commit / requeue.
    """

    model_config = ConfigDict(frozen=False)

    envelope: Envelope
    partition: int
    offset: int
    raw_key: str | None = None


class BrokerHealth(BaseModel):
    """Reported by :meth:`EventBus.health`."""

    model_config = ConfigDict(frozen=True)

    healthy: bool
    detail: str = ""


# ============================================================
# Protocols (the abstract bus interface)
# ============================================================


@runtime_checkable
class Subscriber(Protocol):
    """Handle returned by :meth:`EventBus.subscribe`.

    Iterating ``messages()`` yields :class:`DeliveredMessage`. The
    caller is responsible for ack-ing each message via the parent
    :class:`EventBus` instance.
    """

    spec: SubscribeSpec

    def messages(self) -> AsyncIterator[DeliveredMessage]:
        """Stream messages until the subscriber is closed.

        Returns an async iterator (not a coroutine) — the caller
        does ``async for msg in sub.messages(): ...``.
        """
        ...

    async def close(self) -> None:
        """Stop consuming and release resources."""
        ...


@runtime_checkable
class EventBus(Protocol):
    """The unified abstraction every backend adapter implements."""

    async def start(self) -> None:
        """Initialize the bus (open producer, ensure topics).

        Idempotent. Must be called before any :meth:`publish` or
        :meth:`subscribe`.
        """
        ...

    async def stop(self) -> None:
        """Drain in-flight, close producer/consumers, free resources."""
        ...

    # ---- publish ----

    async def publish(self, env: Envelope) -> PublishReceipt:
        """Publish a single envelope. Awaits broker ack."""
        ...

    async def publish_batch(self, envs: list[Envelope]) -> list[PublishReceipt]:
        """Publish a batch. Order within ``envs`` is preserved per partition.

        Returns receipts in the same order as inputs.
        """
        ...

    # ---- subscribe ----

    async def subscribe(self, spec: SubscribeSpec) -> Subscriber:
        """Create a subscription. Caller iterates ``Subscriber.messages()``."""
        ...

    # ---- delivery confirmation ----

    async def ack(self, sub: Subscriber, msg: DeliveredMessage) -> None:
        """Commit ``msg``. Subsequent restarts will skip it."""
        ...

    async def nack(
        self,
        sub: Subscriber,
        msg: DeliveredMessage,
        *,
        requeue: bool = True,
        reason: str = "",
    ) -> None:
        """Reject ``msg``. If ``requeue`` is False, send to DLQ instead."""
        ...

    # ---- DLQ ----

    async def send_to_dlq(self, env: Envelope, *, reason: str) -> None:
        """Route ``env`` to its DLQ topic (``<topic>.dlq``)."""
        ...

    # ---- ops ----

    async def health(self) -> BrokerHealth:
        """Return broker reachability."""
        ...

    async def consumer_lag(self, group: str, topic: str) -> int:
        """Return total consumer lag for ``(group, topic)`` — Autoscaler signal."""
        ...
