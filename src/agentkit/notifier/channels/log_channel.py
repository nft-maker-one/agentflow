"""LogChannel — structured-log fallback channel.

Used as the meta-notify safety net (Doc08 §8.3) and as the
out-of-the-box default for built-in rules. Always available, no
external dependencies.

Severity → log level mapping:

    info / warning / error / critical
        → structlog ``info / warning / error / critical``
"""

from __future__ import annotations

from agentkit.common.logging import get_logger
from agentkit.notifier.channels.base import Channel, DeliveryResult
from agentkit.notifier.models import Notification

log = get_logger(__name__)


class LogChannel:
    """Logs the notification via structlog. Never fails."""

    kind: str = "log"

    def __init__(self) -> None:
        # Keep a ref to a logger so structured tags don't have to
        # be rebuilt every call. Each call binds notification-scoped
        # context on top.
        self._log = log.bind(channel="log")

    async def deliver(self, *, notification: Notification) -> DeliveryResult:
        bound = self._log.bind(
            notification_id=notification.notification_id,
            rule_id=notification.rule_id,
            run_id=notification.run_id or "",
            workflow_id=notification.workflow_id or "",
            trace_id=notification.trace_id or "",
            severity=notification.severity,
            topic=notification.topic,
        )
        message = notification.rendered_subject or "(no subject)"
        level = _severity_to_level(notification.severity)
        getattr(bound, level)(
            "notifier.log.deliver",
            subject=message,
            body_chars=len(notification.rendered_body),
        )
        return DeliveryResult(ok=True, detail=f"logged at level={level}")

    async def close(self) -> None:
        # Nothing to do.
        return None


def _severity_to_level(severity: str) -> str:
    if severity == "info":
        return "info"
    if severity == "warning":
        return "warning"
    if severity == "critical":
        return "critical"
    return "error"  # default for unknown severities
