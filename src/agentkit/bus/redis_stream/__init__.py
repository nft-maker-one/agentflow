"""Redis Streams EventBus adapter.

A durable, broker-backed :class:`~agentkit.bus.interface.EventBus` built
on Redis Streams. Unlike Kafka, a Redis consumer group is created with a
single O(1) ``XGROUP CREATE`` — no JoinGroup / SyncGroup / rebalance — so
subscribing is sub-millisecond. That makes it a far better fit than Kafka
for AgentKit's "many low-fan-out topics" pattern (one stream per topic,
typically a single consumer each), where Kafka's per-consumer group
coordination dominated deploy latency.
"""

from __future__ import annotations

from agentkit.bus.redis_stream.adapter import RedisStreamBus
from agentkit.bus.redis_stream.config import RedisStreamSettings

__all__ = ["RedisStreamBus", "RedisStreamSettings"]
