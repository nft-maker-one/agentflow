"""Notifier — rule-driven event notification (Doc08).

Public surface::

    from agentkit.notifier import (
        Notifier,
        NotificationRule,
        ChannelSpec,
        Severity,
        Channel,
        LogChannel,
        WebhookChannel,
        DeliveryResult,
        NotifierError,
        RuleEvalError,
        DispatchFailed,
        evaluate_when,
        resolve_alias,
    )
"""

from agentkit.notifier.aliases import (
    BUILTIN_DEFAULT_RULES,
    DEFAULT_SUBSCRIPTIONS,
    resolve_alias,
)
from agentkit.notifier.channels import (
    Channel,
    DeliveryResult,
    LogChannel,
    WebhookChannel,
)
from agentkit.notifier.errors import (
    DispatchFailed,
    NotifierError,
    RuleEvalError,
)
from agentkit.notifier.expressions import evaluate_when
from agentkit.notifier.models import (
    ChannelSpec,
    DedupSpec,
    EscalateStep,
    Notification,
    NotificationRule,
    Severity,
)
from agentkit.notifier.notifier import Notifier

__all__ = [
    "BUILTIN_DEFAULT_RULES",
    "DEFAULT_SUBSCRIPTIONS",
    "Channel",
    "ChannelSpec",
    "DedupSpec",
    "DeliveryResult",
    "DispatchFailed",
    "EscalateStep",
    "LogChannel",
    "Notification",
    "NotificationRule",
    "Notifier",
    "NotifierError",
    "RuleEvalError",
    "Severity",
    "WebhookChannel",
    "evaluate_when",
    "resolve_alias",
]
