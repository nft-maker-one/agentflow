"""The :class:`Notifier` — Bus consumer + dispatcher.

Lifecycle::

    notifier = Notifier(
        bus=bus,
        rules=[rule_1, rule_2, ...],     # Plus built-in defaults if include_builtin=True
        channels={                        # kind → instance
            "log": LogChannel(),
            "webhook": WebhookChannel(hmac_secret="..."),
        },
    )
    await notifier.start()                # subscribes to all matched topics
    ...
    await notifier.stop()                 # graceful drain

The dispatcher is intentionally simple: one Bus subscriber per
*unique* topic pattern, then in-process matching against every
rule. This avoids N×M subscription state and is plenty fast for
Phase 1 scale (≤ a few hundred rules).
"""

from __future__ import annotations

import asyncio
from collections.abc import Iterable

from agentkit.bus.errors import BusError
from agentkit.bus.interface import EventBus, SubscribeSpec, Subscriber
from agentkit.common.logging import get_logger
from agentkit.models.envelope import Envelope
from agentkit.notifier.aliases import (
    BUILTIN_DEFAULT_RULES,
    default_template_for,
    resolve_alias,
)
from agentkit.notifier.channels.base import Channel, DeliveryResult
from agentkit.notifier.channels.log_channel import LogChannel
from agentkit.notifier.dedup import DedupBackend, InMemoryDedupBackend
from agentkit.notifier.errors import TemplateRenderError
from agentkit.notifier.matcher import build_when_context, rule_matches
from agentkit.notifier.models import Notification, NotificationRule
from agentkit.notifier.templates import TemplateRenderer
from agentkit.observability import metrics

log = get_logger(__name__)


CONSUMER_GROUP = "grp.notifier"


class Notifier:
    """Bus-driven notification dispatcher."""

    def __init__(
        self,
        *,
        bus: EventBus,
        rules: Iterable[NotificationRule] | None = None,
        channels: dict[str, Channel] | None = None,
        renderer: TemplateRenderer | None = None,
        dedup: DedupBackend | None = None,
        include_builtin: bool = True,
    ) -> None:
        self._bus = bus
        self._rules: list[NotificationRule] = list(rules or [])
        if include_builtin:
            self._rules.extend(BUILTIN_DEFAULT_RULES)

        # Default channels: at least the log fallback so we never silently
        # drop a critical alert because the user forgot to wire a channel.
        if channels is None:
            channels = {"log": LogChannel()}
        elif "log" not in channels:
            channels = {**channels, "log": LogChannel()}
        self._channels: dict[str, Channel] = channels

        self._renderer = renderer or TemplateRenderer()
        self._dedup = dedup or InMemoryDedupBackend()

        # In-memory audit (Phase 2 → PG).
        self._audit: list[Notification] = []

        self._subscribers: list[Subscriber] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    @property
    def rules(self) -> list[NotificationRule]:
        return list(self._rules)

    @property
    def audit(self) -> list[Notification]:
        return list(self._audit)

    def add_rule(self, rule: NotificationRule) -> None:
        """Register a rule at runtime. Note: subscriptions don't auto-
        update — call ``start()`` after adding rules in bulk.
        """
        self._rules.append(rule)

    async def start(self) -> None:
        """Subscribe to all unique topic patterns covered by current rules."""
        if self._running:
            return
        patterns = self._unique_subscription_patterns()
        for p in patterns:
            try:
                sub = await self._bus.subscribe(
                    SubscribeSpec(
                        topic_pattern=p,
                        group=f"{CONSUMER_GROUP}.{p}",
                        starting_position="latest",
                    ),
                )
            except BusError:
                log.exception("notifier.subscribe_failed", pattern=p)
                continue
            self._subscribers.append(sub)
            self._tasks.append(asyncio.create_task(self._consume(sub, p)))
        self._running = True
        log.info(
            "notifier.start",
            rules=len(self._rules),
            patterns=patterns,
            channels=sorted(self._channels),
        )

    async def stop(self) -> None:
        if not self._running:
            return
        for sub in self._subscribers:
            try:
                await sub.close()
            except BusError:
                log.exception("notifier.subscriber_close_failed")
        for t in self._tasks:
            t.cancel()
        self._subscribers.clear()
        self._tasks.clear()
        # Close every channel — channels usually own HTTP clients etc.
        for ch in self._channels.values():
            try:
                await ch.close()
            except Exception:
                log.exception("notifier.channel_close_failed", kind=ch.kind)
        self._running = False
        log.info("notifier.stop")

    async def inject(self, envelope: Envelope) -> list[Notification]:
        """Manually feed an envelope into the dispatcher.

        Useful for tests AND for direct programmatic notifications
        (e.g. CLI ``agentkit notify test``). Returns the list of
        notifications that were dispatched (or attempted).
        """
        return await self._process(envelope)

    # ------------------------------------------------------------
    # Consumption loop
    # ------------------------------------------------------------

    async def _consume(self, sub: Subscriber, pattern: str) -> None:
        try:
            async for msg in sub.messages():
                env = msg.envelope
                try:
                    await self._process(env)
                except Exception:
                    log.exception(
                        "notifier.process_failed",
                        topic=env.topic, pattern=pattern,
                    )
                try:
                    await self._bus.ack(sub, msg)
                except BusError:
                    log.exception("notifier.ack_failed")
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------
    # Process one envelope
    # ------------------------------------------------------------

    async def _process(self, envelope: Envelope) -> list[Notification]:
        out: list[Notification] = []
        for rule in self._rules:
            outcome = rule_matches(rule, envelope)
            if not outcome.matched:
                metrics.notifier_rule_match_total.labels(
                    rule_id=rule.id, result="miss",
                ).inc()
                continue
            metrics.notifier_rule_match_total.labels(
                rule_id=rule.id, result="match",
            ).inc()

            # Dedup — keyed on the rule + chosen by-fields.
            ctx = build_when_context(envelope)
            if not self._dedup.should_send(rule, context=ctx):
                metrics.notifier_dedup_collapsed_total.labels(
                    rule_id=rule.id,
                ).inc()
                log.debug(
                    "notifier.deduped",
                    rule_id=rule.id, topic=envelope.topic,
                )
                continue

            notification = self._render(rule, envelope)
            out.append(notification)

            # Dispatch via the configured channel (fall back to log).
            channel = self._channels.get(rule.channel.kind)
            if channel is None:
                log.warning(
                    "notifier.unknown_channel_kind",
                    rule_id=rule.id, kind=rule.channel.kind,
                )
                channel = self._channels["log"]

            await self._dispatch(channel, notification)
            self._audit.append(notification)
        return out

    # ------------------------------------------------------------
    # Render
    # ------------------------------------------------------------

    def _render(
        self, rule: NotificationRule, envelope: Envelope,
    ) -> Notification:
        template = rule.template or default_template_for(rule.on)
        ctx = build_when_context(envelope)
        ctx.update(rule.template_vars)
        ctx.setdefault("severity", rule.severity)

        try:
            subject = self._renderer.render(template, kind="txt", context=ctx)
            body = self._renderer.render(template, kind="body", context=ctx)
        except TemplateRenderError as e:
            log.exception(
                "notifier.render_failed",
                rule_id=rule.id, template=template, error=str(e),
            )
            subject = f"[{rule.severity.upper()}] {envelope.topic}"
            body = f"(template render failed: {e})"

        return Notification(
            rule_id=rule.id,
            workflow_id=envelope.workflow_id,
            run_id=envelope.run_id,
            trace_id=envelope.trace_id,
            severity=rule.severity,
            topic=envelope.topic,
            event_id=envelope.event_id,
            channel=rule.channel.kind,
            targets=rule.to_list(),
            template=template,
            rendered_subject=subject.strip(),
            rendered_body=body,
        )

    # ------------------------------------------------------------
    # Dispatch
    # ------------------------------------------------------------

    async def _dispatch(
        self, channel: Channel, notification: Notification,
    ) -> None:
        import time  # noqa: PLC0415
        t_start = time.monotonic()
        try:
            result = await channel.deliver(notification=notification)
        except Exception as e:
            log.exception(
                "notifier.channel.crash",
                kind=channel.kind, notification_id=notification.notification_id,
            )
            notification.status = "failed"
            notification.failure_reason = str(e)
            metrics.notifier_sent_total.labels(
                channel=channel.kind, severity=notification.severity, result="failed",
            ).inc()
            metrics.notifier_send_duration_seconds.labels(
                channel=channel.kind,
            ).observe(time.monotonic() - t_start)
            return

        # Record dispatch metrics regardless of OK/failed.
        metrics.notifier_sent_total.labels(
            channel=channel.kind,
            severity=notification.severity,
            result="ok" if result.ok else "failed",
        ).inc()
        metrics.notifier_send_duration_seconds.labels(
            channel=channel.kind,
        ).observe(time.monotonic() - t_start)

        if result.ok:
            from agentkit.common.time import utcnow  # noqa: PLC0415
            notification.status = "sent"
            notification.sent_at = utcnow()
        else:
            notification.status = "failed"
            notification.failure_reason = result.detail

        log.info(
            "notifier.dispatch",
            notification_id=notification.notification_id,
            rule_id=notification.rule_id,
            kind=channel.kind,
            ok=result.ok,
            detail=result.detail,
            severity=notification.severity,
            topic=notification.topic,
        )

    # ------------------------------------------------------------
    # Internals — subscription planning
    # ------------------------------------------------------------

    def _unique_subscription_patterns(self) -> list[str]:
        seen: set[str] = set()
        for r in self._rules:
            if not r.enabled:
                continue
            pattern = resolve_alias(r.on, workflow_id=r.workflow_id)
            seen.add(pattern)
        return sorted(seen)
