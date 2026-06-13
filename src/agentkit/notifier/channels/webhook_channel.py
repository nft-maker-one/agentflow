"""WebhookChannel — HTTP POST with HMAC-SHA256 body signature.

Doc08 §3.3.2.

The receiving endpoint should verify::

    expected = hmac.new(SECRET, body, sha256).hexdigest()
    received = headers["X-AgentKit-Signature"]
    hmac.compare_digest(expected, received)

We default ``timeout=5s`` and **do NOT retry inside the channel** —
retry logic lives at the dispatcher level so behavior is the same
across all channel types (Doc08 §6.2 Phase 2 territory).
"""

from __future__ import annotations

import hashlib
import hmac
import json
from typing import Any

import httpx

from agentkit.common.logging import get_logger
from agentkit.notifier.channels.base import Channel, DeliveryResult
from agentkit.notifier.models import Notification

log = get_logger(__name__)

DEFAULT_TIMEOUT_S: float = 5.0


class WebhookChannel:
    """HTTP webhook delivery with optional HMAC body signature.

    Pass ``hmac_secret=None`` to disable signing (only recommended
    for local development against a trusted endpoint). Production
    deployments should always set a secret.
    """

    kind: str = "webhook"

    def __init__(
        self,
        *,
        hmac_secret: str | None = None,
        timeout_s: float = DEFAULT_TIMEOUT_S,
        client: httpx.AsyncClient | None = None,
    ) -> None:
        self._hmac_secret = hmac_secret
        self._timeout_s = timeout_s
        self._client = client or httpx.AsyncClient(timeout=timeout_s)
        self._owns_client = client is None

    # ------------------------------------------------------------
    # Channel Protocol
    # ------------------------------------------------------------

    async def deliver(self, *, notification: Notification) -> DeliveryResult:
        # Only one URL per notification — webhook is point-to-point.
        if not notification.targets:
            return DeliveryResult(ok=False, detail="no target URL")
        url = notification.targets[0]

        body = self._build_body(notification)
        body_bytes = json.dumps(body, ensure_ascii=False).encode("utf-8")
        headers: dict[str, str] = {
            "Content-Type": "application/json",
            "X-AgentKit-Notification-Id": notification.notification_id,
            "X-AgentKit-Severity": notification.severity,
            "X-AgentKit-Topic": notification.topic,
        }
        if self._hmac_secret is not None:
            sig = hmac.new(
                self._hmac_secret.encode("utf-8"),
                body_bytes,
                hashlib.sha256,
            ).hexdigest()
            headers["X-AgentKit-Signature"] = sig

        try:
            resp = await self._client.post(
                url, content=body_bytes, headers=headers,
            )
        except httpx.TimeoutException as e:
            log.warning(
                "notifier.webhook.timeout",
                url=url, notification_id=notification.notification_id,
                timeout_s=self._timeout_s,
            )
            return DeliveryResult(ok=False, detail=f"timeout: {e}")
        except httpx.HTTPError as e:
            log.warning(
                "notifier.webhook.http_error",
                url=url, notification_id=notification.notification_id,
                error=str(e),
            )
            return DeliveryResult(ok=False, detail=f"http error: {e}")

        ok = 200 <= resp.status_code < 300
        if not ok:
            log.warning(
                "notifier.webhook.bad_status",
                url=url, notification_id=notification.notification_id,
                status=resp.status_code,
            )
        return DeliveryResult(
            ok=ok,
            detail=f"status={resp.status_code}",
            status_code=resp.status_code,
        )

    async def close(self) -> None:
        if self._owns_client:
            await self._client.aclose()

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    @staticmethod
    def _build_body(notification: Notification) -> dict[str, Any]:
        # The body is a JSON envelope — receivers can pluck whatever
        # they need by field name. We deliberately keep the shape
        # stable so users can pin schemas.
        return {
            "notification_id": notification.notification_id,
            "rule_id": notification.rule_id,
            "severity": notification.severity,
            "topic": notification.topic,
            "subject": notification.rendered_subject,
            "body": notification.rendered_body,
            "workflow_id": notification.workflow_id,
            "run_id": notification.run_id,
            "trace_id": notification.trace_id,
            "event_id": notification.event_id,
            "created_at": notification.created_at.isoformat(),
        }
