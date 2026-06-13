"""Guardrail data models.

See ``Doc07 §2`` (quota model) and ``§4`` (reservation lifecycle).
``Reservation`` itself lives in :mod:`agentkit.llm.guardrail_iface`
so the LLM Gateway can reference it without importing this package.
"""

from __future__ import annotations

from typing import Literal

from pydantic import BaseModel, ConfigDict, Field

# Type aliases for layer / dim labels — kept simple to keep
# Lua scripts and Python sites in sync.
QuotaLayer = Literal["agent", "run"]
QuotaSource = Literal[
    "framework_default",
    "project_default",
    "workflow",
    "run_overlay",
]


# ============================================================
# Core quota structs — match Doc07 §2.1 / §2.4 verbatim
# ============================================================


class AgentGuardrail(BaseModel):
    """Per-Agent quota caps. See ``Doc07 §2.1``."""

    model_config = ConfigDict(extra="forbid")

    max_tokens_per_call: int = Field(default=8_000, ge=1)
    max_cycles: int = Field(default=5, ge=1)


class RunGuardrail(BaseModel):
    """Per-Run quota caps."""

    model_config = ConfigDict(extra="forbid")

    max_total_tokens: int = Field(default=200_000, ge=1)
    max_cycles_per_run: int = Field(default=200, ge=1)


# ============================================================
# Resolution audit trail
# ============================================================


class OverrideRecord(BaseModel):
    """One quota field's resolution origin — Doc07 §2.4 ``overrides``.

    Captured by :func:`resolve_guardrail_context` for audit /
    debugging ("why is my run capped at 50k?").
    """

    model_config = ConfigDict(extra="forbid")

    layer: QuotaLayer
    field: str
    source: QuotaSource
    value: int


class GuardrailContext(BaseModel):
    """The 'effective' quota pack for a single Run.

    Built once at ``Orchestrator.create_run`` time (or at
    ``init_run_quota`` for clients constructing the context
    directly), then passed to the Guardrail backend for the run's
    duration. Stored verbatim in the audit table (``run_quota_audit``)
    once the run terminates.
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str
    workflow_id: str
    workflow_version: int = 1
    owner: str | None = None
    project: str | None = None

    agent: AgentGuardrail
    run: RunGuardrail

    #: Per-agent (template_key → caps) overrides set on individual
    #: agents in the UI ("this agent only"). When present for the agent
    #: being checked they REPLACE the run-wide ``agent`` caps. Empty =
    #: every agent inherits the single ``agent`` default.
    agent_overrides: dict[str, AgentGuardrail] = Field(default_factory=dict)

    overrides: list[OverrideRecord] = Field(default_factory=list)


# ============================================================
# Usage snapshots
# ============================================================


class RunUsage(BaseModel):
    """A point-in-time view of a Run's consumed quota.

    Returned by ``RedisGuardrail.get_run_usage`` for the UI / CLI;
    persisted in ``run_quota_audit`` at finalization.
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str
    used_tokens: int = 0
    used_cycles: int = 0
    used_cost_usd: float = 0.0  # informational only — Doc07 §3.2
    limits_tokens: int
    limits_cycles: int
    quota_exhausted: bool = False
    breaches: list[OverrideRecord] = Field(default_factory=list)
    reservations_total: int = 0
    reservations_expired: int = 0

    @property
    def tokens_pct(self) -> float:
        if self.limits_tokens == 0:
            return 0.0
        return self.used_tokens / self.limits_tokens

    @property
    def cycles_pct(self) -> float:
        if self.limits_cycles == 0:
            return 0.0
        return self.used_cycles / self.limits_cycles
