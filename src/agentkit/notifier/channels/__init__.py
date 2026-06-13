"""Notifier channel adapters.

Defines the :class:`Channel` Protocol every concrete channel must
satisfy. Phase 1 ships :class:`LogChannel` (always available, used
as a fallback) and :class:`WebhookChannel` (httpx + HMAC sig).

Email / IM / Collab adapters are Phase 2 — they slot in here.
"""

from agentkit.notifier.channels.base import (
    Channel,
    DeliveryResult,
)
from agentkit.notifier.channels.log_channel import LogChannel
from agentkit.notifier.channels.webhook_channel import WebhookChannel

__all__ = [
    "Channel",
    "DeliveryResult",
    "LogChannel",
    "WebhookChannel",
]
