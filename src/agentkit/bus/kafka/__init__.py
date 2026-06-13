"""Kafka / Redpanda adapter for :class:`agentkit.bus.EventBus`.

Public surface::

    from agentkit.bus.kafka import KafkaEventBus, KafkaSettings
"""

from agentkit.bus.kafka.adapter import KafkaEventBus
from agentkit.bus.kafka.config import KafkaSettings

__all__ = ["KafkaEventBus", "KafkaSettings"]
