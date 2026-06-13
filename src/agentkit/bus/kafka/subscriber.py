"""Kafka subscriber implementation.

Wraps an :class:`aiokafka.AIOKafkaConsumer` to expose the framework's
:class:`agentkit.bus.Subscriber` Protocol.

We deliberately keep auto-commit OFF — the framework decides exactly
when to commit (after handler success), which is critical for
at-least-once semantics combined with Runtime's Dedup gating.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

from aiokafka import AIOKafkaConsumer
from aiokafka.errors import KafkaError

from agentkit.bus.errors import BusUnavailable
from agentkit.bus.interface import DeliveredMessage, SubscribeSpec
from agentkit.bus.kafka.serde import decode
from agentkit.common.logging import get_logger

log = get_logger(__name__)


class KafkaSubscriber:
    """A live Kafka subscription.

    Concrete classes implementing :class:`agentkit.bus.Subscriber` —
    we don't subclass the Protocol; structural typing is sufficient.
    """

    def __init__(self, consumer: AIOKafkaConsumer, spec: SubscribeSpec) -> None:
        self._consumer = consumer
        self.spec = spec
        self._closed = False

    async def messages(self) -> AsyncIterator[DeliveredMessage]:
        """Stream :class:`DeliveredMessage` until the subscriber is closed.

        Decoding errors are logged and skipped — the framework writes
        a metric / DLQ later (here we don't have a producer handle).
        """
        try:
            async for record in self._consumer:
                if self._closed:
                    break
                try:
                    envelope = decode(record.value)
                except Exception:
                    log.exception(
                        "bus.consume.decode_failed",
                        topic=record.topic,
                        partition=record.partition,
                        offset=record.offset,
                    )
                    continue

                yield DeliveredMessage(
                    envelope=envelope,
                    partition=record.partition,
                    offset=record.offset,
                    raw_key=record.key.decode("utf-8") if record.key else None,
                )
        except KafkaError as e:
            raise BusUnavailable(f"kafka consumer error: {e}") from e

    async def close(self) -> None:
        """Stop the consumer; idempotent."""
        if self._closed:
            return
        self._closed = True
        await self._consumer.stop()

    # ---- internals exposed for the adapter to commit/seek ----

    @property
    def consumer(self) -> AIOKafkaConsumer:
        return self._consumer
