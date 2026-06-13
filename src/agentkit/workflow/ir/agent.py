"""Agent-level IR data models.

See ``Doc03_AgentRuntime.md §2.2`` and ``Doc04_WorkflowGraph.md §2``.
"""

from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from agentkit.llm.models import LLMBinding, RateLimit
from agentkit.models.enums import Role


class Subscription(BaseModel):
    """One ``(topic_pattern, tag_filter)`` declared by an Agent."""

    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1, description="Pattern; supports * and #")
    tag_filter: dict[str, str] = Field(default_factory=dict)


class PublishSpec(BaseModel):
    """One topic an Agent is allowed to publish to."""

    model_config = ConfigDict(extra="forbid")

    topic: str = Field(min_length=1)
    schema_ref: str | None = None


class ReplicaSpec(BaseModel):
    """Min/max replica counts per Template — drives Autoscaler bounds."""

    model_config = ConfigDict(extra="forbid")

    min: int = Field(default=1, ge=0)
    max: int = Field(default=1, ge=1)

    def model_post_init(self, __context: Any) -> None:
        # Pydantic v2 uses ``model_post_init`` for cross-field validation.
        if self.max < self.min:
            raise ValueError(f"replicas.max ({self.max}) < min ({self.min})")


class FallbackSpec(BaseModel):
    """Per-Template fallback policy. See ``Doc03 §6.2``."""

    model_config = ConfigDict(extra="forbid")

    on: list[str] = Field(
        default_factory=lambda: ["recoverable_error", "guardrail_exceeded"],
    )
    strategy: Literal["retry", "alt_instance", "alt_template"] = "retry"
    alt_template: str | None = None  # required when strategy == "alt_template"
    retry: dict[str, Any] = Field(
        default_factory=lambda: {
            "max": 3,
            "backoff": "exp",
            "base_ms": 500,
            "jitter": True,
        },
    )

    def model_post_init(self, __context: Any) -> None:
        if self.strategy == "alt_template" and not self.alt_template:
            raise ValueError(
                "fallback.alt_template is required when strategy=alt_template",
            )


class AgentGuardrail(BaseModel):
    """Per-Agent guardrail caps. See ``Doc07 §2.1``."""

    model_config = ConfigDict(extra="forbid")

    max_tokens_per_call: int = Field(default=8_000, ge=1)
    max_cycles: int = Field(default=5, ge=1)


class AggregateSpec(BaseModel):
    """Fan-in gating: wait for multiple upstream events before firing.

    Default behavior (no AggregateSpec set) is event-by-event dispatch
    — every arriving envelope triggers one handler invocation.

    With AggregateSpec set, the runtime buffers events per-run-id,
    keeping the latest envelope on each subscribe topic. The handler
    fires when:
      ``len(received) >= threshold`` AND ``required ⊆ received``.

    Then a synthetic merged event is built — its payload is the union
    of all buffered payloads, plus a ``_inputs`` map keyed by topic
    for explicit per-source access.

    Once fired, the per-run buffer is cleared so subsequent rounds
    (rare; usually a Run hits an aggregator at most once) can refill.
    """

    model_config = ConfigDict(extra="forbid")

    threshold: int = Field(
        default=0,
        ge=0,
        description=(
            "Minimum number of distinct subscribe topics that must "
            "arrive before firing. 0 = use ``len(subscribe)`` (all)."
        ),
    )
    required: list[str] = Field(
        default_factory=list,
        description=(
            "Topics that MUST be present in the buffer to fire. "
            "Independent of threshold — a missing required topic "
            "blocks even if threshold is exceeded by other topics."
        ),
    )


class AgentTemplate(BaseModel):
    """Design-time spec for one Agent template.

    The unique identifier of the template is the *map key* in
    :class:`WorkflowIR.agents` (per Doc03 §2.2 — no separate ``id``
    field). Compiler propagates the key into runtime artifacts.
    """

    model_config = ConfigDict(extra="forbid")

    role: Role
    description: str = ""

    # ── prompts / templates / context refs ──
    prompt_ref: str | None = None

    # ── LLM binding ──
    # Optional: nodes like Aggregator/Guard/Tool may not need LLM.
    # When present, the Compiler can populate the LLMGateway's
    # bindings registry via the workflow's lookup table.
    llm: LLMBinding | None = None

    # ── tool bindings (forward-compat: Doc04 §4 + Tool module) ──
    tools: list[dict[str, Any]] = Field(default_factory=list)

    # ── tags ──
    tags: dict[str, str] = Field(default_factory=dict)

    # ── topic prefix override (default: ``agent.<template_key>``) ──
    # Users supply this to escape the framework default namespace —
    # e.g. ``"events.outliner"`` makes subscribe/publish topics like
    # ``events.outliner.in.<suffix>``. Useful when integrating into an
    # existing topic taxonomy or to dodge naming collisions.
    #
    # The framework still appends ``.in.`` / ``.out.`` automatically;
    # the override controls only the *base* portion.
    topic_prefix: str | None = None

    # ── topic routing ──
    subscribe: list[Subscription] = Field(default_factory=list)
    publish: list[PublishSpec] = Field(default_factory=list)

    # ── fan-in aggregation (Doc04 §4 — Aggregator pattern) ──
    aggregate: AggregateSpec | None = None

    # ── scaling / failure ──
    replicas: ReplicaSpec = Field(default_factory=ReplicaSpec)
    fallback: FallbackSpec | None = None

    # ── guardrails ──
    guardrail: AgentGuardrail | None = None

    # ── schema gates ──
    schema_in: dict[str, Any] | None = None
    schema_out: dict[str, Any] | None = None

    # ── rate limit (optional, default off; alternate to LLMBinding.rate_limit) ──
    rate_limit: RateLimit | None = None

    # ── subworkflow forward-compat (Phase 2) ──
    subworkflow: str | None = None
    guardrail_mode: Literal["inherit", "isolated"] = "inherit"
