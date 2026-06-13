"""Workflow-level directives: guardrails, triggers, notifications, bus override.

These are the IR-level *configuration* sections — distinct from
agent definitions and edges.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from agentkit.workflow.ir.agent import AgentGuardrail


class RunGuardrail(BaseModel):
    """Per-Run guardrail caps. See ``Doc07 §2.1``."""

    model_config = ConfigDict(extra="forbid")

    max_total_tokens: int = Field(default=200_000, ge=1)
    max_cycles_per_run: int = Field(default=200, ge=1)


class WorkflowGuardrail(BaseModel):
    """Top-level guardrail block in IR.

    ``cost_usd`` was intentionally dropped in Doc07 v0.2 — this is
    a developer framework, not a billing product. We track tokens
    + cycles only.
    """

    model_config = ConfigDict(extra="forbid")

    per_agent: AgentGuardrail | None = None
    per_run: RunGuardrail | None = None


class BusOverride(BaseModel):
    """Per-Workflow EventBus knobs. See ``Doc02 §9.2``."""

    model_config = ConfigDict(extra="forbid")

    ordering: Literal["per_run", "per_workflow"] = "per_run"
    topic_overrides: dict[str, dict[str, Any]] = Field(default_factory=dict)


class TriggerSpec(BaseModel):
    """How a Run can be started."""

    model_config = ConfigDict(extra="allow")  # forward-compat (cron/webhook etc.)

    kind: Literal["api", "cron", "webhook", "event"] = "api"
    cron: str | None = None
    webhook_path: str | None = None
    event_topic: str | None = None


class NotificationRule(BaseModel):
    """Workflow-level Notifier rule. See ``Doc08 §3``.

    For Phase 1 we keep only the most common fields; the Notifier
    module (Doc08) will subclass / extend later. Using ``extra='allow'``
    ensures forward-compat — extra fields written by future YAML
    survive round-trip.
    """

    model_config = ConfigDict(extra="allow")

    on: str = Field(min_length=1)  # alias or full topic
    channel: dict[str, Any] | str = "email"
    to: list[str] | str = Field(default_factory=list)
    when: str | None = None
    template: str | None = None
    severity: Literal["info", "warning", "error", "critical"] = "warning"
    enabled: bool = True
