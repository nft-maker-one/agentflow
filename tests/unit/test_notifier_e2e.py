"""End-to-end Notifier tests with MockBus.

Exercises:

* Default subscription set wiring (guard.exceeded / dlq.received / role.down)
* User rules with `when` filtering
* Workflow_id pinning (rule narrowing the bus subscription)
* Dedup: two identical events → one notification
* Channel fanout: log + webhook deliver simultaneously
* `Notifier.inject()` for direct test feeding (no Bus needed)
"""

from __future__ import annotations

import asyncio
import hashlib
import hmac
import json

import httpx
import pytest

from agentkit.bus.builder import build_envelope
from agentkit.notifier import (
    ChannelSpec,
    DedupSpec,
    LogChannel,
    Notifier,
    NotificationRule,
    WebhookChannel,
)
from tests.helpers.mock_bus import MockEventBus


# ----------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------


@pytest.fixture
async def bus() -> MockEventBus:
    b = MockEventBus()
    await b.start()
    return b


def _capture_webhook(captured: list) -> WebhookChannel:
    """Build a WebhookChannel that records every request body."""

    def handler(request: httpx.Request) -> httpx.Response:
        captured.append({
            "url": str(request.url),
            "body": json.loads(request.content.decode("utf-8")),
            "sig": request.headers.get("X-AgentKit-Signature"),
        })
        return httpx.Response(200)

    transport = httpx.MockTransport(handler)
    return WebhookChannel(
        hmac_secret="testsecret",
        client=httpx.AsyncClient(transport=transport),
    )


# ----------------------------------------------------------------
# Direct injection (no Bus loop) — fastest sanity tests
# ----------------------------------------------------------------


class TestInject:
    async def test_inject_triggers_log_channel(self, bus) -> None:
        notifier = Notifier(bus=bus, rules=[], include_builtin=True)
        # No need to start() — inject() bypasses subscription loop.
        env = build_envelope(
            topic="system.guard.alert.run.tokens",
            payload={
                "layer": "run",
                "dim": "tokens",
                "used": 200_142,
                "limit": 200_000,
            },
            run_id="run_xxx",
        )
        notifications = await notifier.inject(env)
        assert len(notifications) == 1
        n = notifications[0]
        assert n.channel == "log"
        assert n.severity == "critical"
        assert n.template == "guard_exceeded_default"
        # Subject should mention the layer/dim and run_id.
        assert "Guardrail" in n.rendered_subject or "guard" in n.rendered_subject.lower()
        assert n.run_id == "run_xxx"

    async def test_workflow_id_filter(self, bus) -> None:
        notifier = Notifier(
            bus=bus,
            include_builtin=False,
            rules=[
                NotificationRule(
                    on="run.failed",
                    workflow_id="wf_a",
                    channel=ChannelSpec(kind="log"),
                    to="stderr",
                ),
            ],
        )
        # Event from a *different* workflow — must NOT match.
        env_b = build_envelope(
            topic="workflow.wf_b.failed",
            payload={"reason": "boom"},
            workflow_id="wf_b",
        )
        assert await notifier.inject(env_b) == []

        # Event from the right workflow — must match.
        env_a = build_envelope(
            topic="workflow.wf_a.failed",
            payload={"reason": "boom"},
            workflow_id="wf_a",
        )
        out = await notifier.inject(env_a)
        assert len(out) == 1

    async def test_when_expression_filter(self, bus) -> None:
        captured: list = [None]

        notifier = Notifier(
            bus=bus,
            include_builtin=False,
            rules=[
                NotificationRule(
                    on="agent.researcher.out.summary",
                    when="payload.score >= 0.9",
                    channel=ChannelSpec(kind="log"),
                    to="stderr",
                ),
            ],
        )
        # Below threshold → no fire.
        env_lo = build_envelope(
            topic="agent.researcher.out.summary",
            payload={"score": 0.7},
        )
        assert await notifier.inject(env_lo) == []

        # Above threshold → fire.
        env_hi = build_envelope(
            topic="agent.researcher.out.summary",
            payload={"score": 0.95},
        )
        out = await notifier.inject(env_hi)
        assert len(out) == 1


# ----------------------------------------------------------------
# Webhook end-to-end (with HMAC verify)
# ----------------------------------------------------------------


class TestWebhookE2E:
    async def test_guardrail_alert_to_webhook(self, bus) -> None:
        captured: list[dict] = []
        webhook = _capture_webhook(captured)

        notifier = Notifier(
            bus=bus,
            include_builtin=False,
            channels={
                "log": LogChannel(),
                "webhook": webhook,
            },
            rules=[
                NotificationRule(
                    on="guard.exceeded",
                    severity="critical",
                    channel=ChannelSpec(kind="webhook"),
                    to="https://api.example.com/incidents",
                ),
            ],
        )

        env = build_envelope(
            topic="system.guard.alert.run.tokens",
            payload={
                "layer": "run",
                "dim": "tokens",
                "used": 220_000,
                "limit": 200_000,
            },
            run_id="run_burst",
        )
        out = await notifier.inject(env)

        assert len(out) == 1
        assert len(captured) == 1
        body = captured[0]["body"]
        assert body["severity"] == "critical"
        assert body["topic"] == "system.guard.alert.run.tokens"
        assert body["run_id"] == "run_burst"

        # Verify HMAC signature.
        raw = json.dumps(body, ensure_ascii=False).encode("utf-8")
        expected = hmac.new(b"testsecret", raw, hashlib.sha256).hexdigest()
        assert captured[0]["sig"] == expected

        await notifier.stop()


# ----------------------------------------------------------------
# Dedup
# ----------------------------------------------------------------


class TestDedup:
    async def test_dedup_collapses_repeated_events(self, bus) -> None:
        notifier = Notifier(
            bus=bus,
            include_builtin=False,
            rules=[
                NotificationRule(
                    on="dlq.received",
                    channel=ChannelSpec(kind="log"),
                    to="stderr",
                    dedup=DedupSpec(window_seconds=60, by=["topic"]),
                ),
            ],
        )

        env = build_envelope(
            topic="agent.outliner.in.topic.dlq",
            payload={"reason": "schema violation"},
        )
        out_first = await notifier.inject(env)
        out_second = await notifier.inject(env)
        out_third = await notifier.inject(env)

        assert len(out_first) == 1
        assert out_second == []
        assert out_third == []


# ----------------------------------------------------------------
# Bus subscription loop — full path
# ----------------------------------------------------------------


class TestBusLoop:
    async def test_publish_arrives_through_bus_subscription(self, bus) -> None:
        captured: list[dict] = []
        webhook = _capture_webhook(captured)

        notifier = Notifier(
            bus=bus,
            include_builtin=False,
            channels={"log": LogChannel(), "webhook": webhook},
            rules=[
                NotificationRule(
                    on="guard.exceeded",
                    channel=ChannelSpec(kind="webhook"),
                    to="https://hook",
                ),
            ],
        )
        await notifier.start()
        await asyncio.sleep(0.05)  # let subscriptions register

        await bus.publish(
            build_envelope(
                topic="system.guard.alert.agent.tokens",
                payload={"layer": "agent", "dim": "tokens", "used": 5, "limit": 4},
            ),
        )

        # Wait for the consumer loop to pick up + dispatch.
        for _ in range(40):
            await asyncio.sleep(0.05)
            if captured:
                break

        assert len(captured) == 1
        assert captured[0]["body"]["topic"] == "system.guard.alert.agent.tokens"

        await notifier.stop()


# ----------------------------------------------------------------
# Multiple rules on the same topic
# ----------------------------------------------------------------


class TestMultipleMatches:
    async def test_two_rules_same_topic_both_fire(self, bus) -> None:
        captured: list[dict] = []
        webhook = _capture_webhook(captured)

        notifier = Notifier(
            bus=bus,
            include_builtin=False,
            channels={"log": LogChannel(), "webhook": webhook},
            rules=[
                NotificationRule(
                    on="guard.exceeded",
                    channel=ChannelSpec(kind="log"),
                    to="stderr",
                ),
                NotificationRule(
                    on="guard.exceeded",
                    channel=ChannelSpec(kind="webhook"),
                    to="https://hook",
                ),
            ],
        )

        env = build_envelope(
            topic="system.guard.alert.run.cycles",
            payload={"layer": "run", "dim": "cycles", "used": 201, "limit": 200},
        )
        out = await notifier.inject(env)
        assert len(out) == 2
        # Webhook captured once, log fired silently.
        assert len(captured) == 1


# ----------------------------------------------------------------
# Audit
# ----------------------------------------------------------------


class TestAudit:
    async def test_audit_log_records_each_notification(self, bus) -> None:
        notifier = Notifier(bus=bus, include_builtin=True)
        env = build_envelope(
            topic="agent.x.in.topic.dlq",
            payload={"reason": "rejection"},
        )
        out = await notifier.inject(env)
        assert len(out) == 1
        # Audit list must contain a matching record.
        assert len(notifier.audit) == 1
        rec = notifier.audit[0]
        assert rec.status in ("sent", "failed")
        assert rec.topic == "agent.x.in.topic.dlq"
