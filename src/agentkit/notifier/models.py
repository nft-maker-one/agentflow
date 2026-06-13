"""Notifier data models — Doc08 §3.

The :class:`NotificationRule` is the user-facing DSL; everything
else hangs off it. Phase 1 keeps the most-actively-used pieces
(``channel``, ``to``, ``when``, ``dedup``) fully wired and reserves
forward-compat fields (``escalate``, ``rate_limit``, ``template``)
that the engine accepts but does not yet act on at scheduling time.
"""

from __future__ import annotations

from datetime import datetime, timedelta
from typing import Any, Literal, Union

from pydantic import BaseModel, ConfigDict, Field, field_validator

from agentkit.common.ids import new_notification_id, new_rule_id
from agentkit.common.time import utcnow

# ============================================================
# Type aliases / enums
# ============================================================

Severity = Literal["info", "warning", "error", "critical"]

ChannelKind = Literal["log", "webhook", "email", "im", "collab"]

# Notification target — string or list of strings, depending on
# channel kind. Webhook = single URL; Email = list of addresses.
ToTarget = Union[str, list[str]]


# ============================================================
# Channel spec
# ============================================================


class ChannelSpec(BaseModel):
    """One channel descriptor — Doc08 §3.3."""

    model_config = ConfigDict(extra="forbid")

    kind: ChannelKind
    options: dict[str, Any] = Field(default_factory=dict)


# ============================================================
# Dedup / escalate / rate-limit (forward-compat for Phase 2)
# ============================================================


class DedupSpec(BaseModel):
    """Dedup config — Phase 1 supports ``window`` + ``by``."""

    model_config = ConfigDict(extra="forbid")

    window_seconds: int = Field(default=300, ge=0)
    by: list[str] = Field(default_factory=lambda: ["rule_id"])
    strategy: Literal["collapse", "suppress"] = "suppress"


class EscalateStep(BaseModel):
    """One escalation hop — Phase 2 will execute these via timer scan."""

    model_config = ConfigDict(extra="forbid")

    after_seconds: int = Field(ge=1)
    to: ToTarget
    channel: ChannelSpec | None = None


class NotifierRateLimit(BaseModel):
    """Per-rule rate limit — declared now, enforced in Phase 2."""

    model_config = ConfigDict(extra="forbid")

    rule_rpm: int | None = None
    channel_rpm: int | None = None
    target_rpm: int | None = None


# ============================================================
# NotificationRule (the user-facing DSL)
# ============================================================


class NotificationRule(BaseModel):
    """One notification rule. See ``Doc08 §3.1``.

    Most users declare these inline in Workflow YAML
    (``notifications:`` block). Programmatic registration uses
    ``Notifier.register_rule(NotificationRule(...))``.
    """

    model_config = ConfigDict(extra="forbid")

    id: str = Field(default_factory=new_rule_id)

    # Topic / alias to subscribe to.
    on: str = Field(min_length=1)

    # Optional guard expression. Empty / None → always match.
    when: str | None = None

    # Restrict to one Workflow's events.
    workflow_id: str | None = None

    # Channel + recipients.
    channel: ChannelSpec
    to: ToTarget

    # Template selection. ``None`` → engine picks a built-in default
    # based on the resolved alias.
    template: str | None = None
    template_vars: dict[str, Any] = Field(default_factory=dict)

    # Phase 2 features — declared now, executed later.
    dedup: DedupSpec | None = None
    escalate: list[EscalateStep] = Field(default_factory=list)
    rate_limit: NotifierRateLimit | None = None

    severity: Severity = "warning"
    enabled: bool = True

    @field_validator("to")
    @classmethod
    def _normalize_to(cls, v: ToTarget) -> ToTarget:
        # Empty list / empty string is a config bug.
        if isinstance(v, list) and not v:
            raise ValueError("`to` must not be an empty list")
        if isinstance(v, str) and not v.strip():
            raise ValueError("`to` must not be an empty string")
        return v

    def to_list(self) -> list[str]:
        """Return ``to`` as a list, normalizing the singleton-string form."""
        if isinstance(self.to, str):
            return [self.to]
        return list(self.to)


# ============================================================
# Notification — the rendered, dispatched record
# ============================================================


class Notification(BaseModel):
    """One concrete notification ready for or already sent to a channel.

    Created by the matcher + renderer; consumed by the channel
    dispatcher; persisted (in Phase 2) to ``notification`` table.
    """

    model_config = ConfigDict(extra="forbid")

    notification_id: str = Field(default_factory=new_notification_id)
    rule_id: str
    workflow_id: str | None = None
    run_id: str | None = None
    trace_id: str | None = None

    severity: Severity = "warning"
    topic: str
    event_id: str | None = None

    channel: ChannelKind
    targets: list[str]

    template: str
    rendered_subject: str = ""
    rendered_body: str = ""

    status: Literal[
        "queued", "sent", "failed", "dropped", "deduped", "acked",
    ] = "queued"
    sent_at: datetime | None = None
    failure_reason: str | None = None

    created_at: datetime = Field(default_factory=utcnow)

    @property
    def short_id(self) -> str:
        return self.notification_id[-8:]


# ============================================================
# Dedup key shaping
# ============================================================


def dedup_window(spec: DedupSpec | None) -> timedelta:
    if spec is None:
        return timedelta(0)
    return timedelta(seconds=spec.window_seconds)
