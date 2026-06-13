"""Tests for LogChannel + WebhookChannel."""

from __future__ import annotations

import hashlib
import hmac
import json

import httpx
import pytest

from agentkit.notifier.channels import LogChannel, WebhookChannel
from agentkit.notifier.models import Notification


def _sample_notification(targets=None, **kwargs) -> Notification:
    if targets is None:
        targets = ["https://hooks.example.com/x"]
    base = {
        "rule_id": "rule_xx",
        "topic": "system.guard.alert.run.tokens",
        "channel": "webhook",
        "targets": targets,
        "template": "guard_exceeded_default",
        "rendered_subject": "[CRITICAL] Run failed: …",
        "rendered_body": "details here",
        "severity": "critical",
        "run_id": "run_xxx",
        "trace_id": "trc_yyy",
        "workflow_id": "wf_z",
        "event_id": "evt_q",
    }
    base.update(kwargs)
    return Notification(**base)


# ----------------------------------------------------------------
# LogChannel
# ----------------------------------------------------------------


class TestLogChannel:
    async def test_log_always_succeeds(self) -> None:
        ch = LogChannel()
        result = await ch.deliver(notification=_sample_notification(channel="log"))
        assert result.ok is True
        await ch.close()


# ----------------------------------------------------------------
# WebhookChannel — using httpx MockTransport
# ----------------------------------------------------------------


def _make_webhook_with_handler(
    handler, *, hmac_secret: str | None = "test-secret",
) -> WebhookChannel:
    transport = httpx.MockTransport(handler)
    client = httpx.AsyncClient(transport=transport)
    return WebhookChannel(hmac_secret=hmac_secret, client=client)


class TestWebhookChannel:
    async def test_post_with_signature(self) -> None:
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["url"] = str(request.url)
            captured["body"] = request.content
            captured["sig"] = request.headers.get("X-AgentKit-Signature")
            captured["sev"] = request.headers.get("X-AgentKit-Severity")
            return httpx.Response(200)

        channel = _make_webhook_with_handler(handler, hmac_secret="topsecret")
        notif = _sample_notification(targets=["https://api.example.com/hook"])
        result = await channel.deliver(notification=notif)
        await channel.close()

        assert result.ok
        assert result.status_code == 200
        assert captured["url"] == "https://api.example.com/hook"
        # Verify signature
        expected = hmac.new(
            b"topsecret", captured["body"], hashlib.sha256,
        ).hexdigest()
        assert captured["sig"] == expected
        assert captured["sev"] == "critical"

    async def test_unsigned_when_no_secret(self) -> None:
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["sig"] = request.headers.get("X-AgentKit-Signature")
            return httpx.Response(202)

        channel = _make_webhook_with_handler(handler, hmac_secret=None)
        await channel.deliver(notification=_sample_notification())
        await channel.close()

        assert captured["sig"] is None

    async def test_5xx_marks_failed(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(503, text="busy")

        channel = _make_webhook_with_handler(handler)
        result = await channel.deliver(notification=_sample_notification())
        await channel.close()

        assert result.ok is False
        assert result.status_code == 503

    async def test_4xx_marks_failed(self) -> None:
        def handler(request: httpx.Request) -> httpx.Response:
            return httpx.Response(404)

        channel = _make_webhook_with_handler(handler)
        result = await channel.deliver(notification=_sample_notification())
        await channel.close()
        assert result.ok is False
        assert result.status_code == 404

    async def test_no_target_url(self) -> None:
        channel = _make_webhook_with_handler(lambda r: httpx.Response(200))
        result = await channel.deliver(
            notification=_sample_notification(targets=[]),
        )
        await channel.close()
        assert result.ok is False

    async def test_body_is_well_formed_json(self) -> None:
        captured: dict = {}

        def handler(request: httpx.Request) -> httpx.Response:
            captured["body"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(200)

        channel = _make_webhook_with_handler(handler, hmac_secret=None)
        await channel.deliver(notification=_sample_notification())
        await channel.close()

        body = captured["body"]
        for k in ("notification_id", "severity", "topic", "subject", "body", "run_id"):
            assert k in body
        assert body["severity"] == "critical"
