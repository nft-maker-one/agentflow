"""EventBus — data plane abstraction and adapters.

Public surface::

    from agentkit.bus import (
        EventBus, Subscriber, SubscribeSpec,
        DeliveredMessage, PublishReceipt, BrokerHealth,
        BusError, BusUnavailable, OversizedPayload,
        build_envelope, dlq_topic_for, derive_consumer_group,
    )

Concrete adapter (Kafka) lives in :mod:`agentkit.bus.kafka`.
"""

from agentkit.bus.builder import build_envelope
from agentkit.bus.errors import (
    BusError,
    BusUnavailable,
    OversizedPayload,
    PublishFailed,
)
from agentkit.bus.inprocess import (
    InProcessEventBus,
    InProcessSubscriber,
)
from agentkit.bus.interface import (
    BrokerHealth,
    DeliveredMessage,
    EventBus,
    PublishReceipt,
    SubscribeSpec,
    Subscriber,
)
from agentkit.bus.naming import (
    DLQ_SUFFIX,
    derive_consumer_group,
    dlq_topic_for,
    is_dlq_topic,
)

__all__ = [
    "DLQ_SUFFIX",
    "BrokerHealth",
    "BusError",
    "BusUnavailable",
    "DeliveredMessage",
    "EventBus",
    "InProcessEventBus",
    "InProcessSubscriber",
    "OversizedPayload",
    "PublishFailed",
    "PublishReceipt",
    "SubscribeSpec",
    "Subscriber",
    "build_envelope",
    "derive_consumer_group",
    "dlq_topic_for",
    "is_dlq_topic",
]
