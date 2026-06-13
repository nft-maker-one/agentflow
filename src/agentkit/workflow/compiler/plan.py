"""Step ⑥ — Plan: derive RuntimePlan + BusTopicPlan from a lowered IR.

The Plan step walks the IR and produces *flat* deployment artifacts:

* one :class:`AgentPlan` per Template — what the Runtime worker
  needs to bring up and configure that Agent;
* one :class:`BusTopicPlan` listing every business topic + its
  auto-derived DLQ topic — what the EventBus must
  ``ensure_topic`` for at deploy time.
"""

from __future__ import annotations

from agentkit.bus.naming import derive_consumer_group
from agentkit.workflow.ir import (
    VIRTUAL_NODES,
    AgentTemplate,
    WorkflowIR,
)
from agentkit.workflow.plan import (
    AgentPlan,
    BusTopicPlan,
    BusTopicSpec,
    RuntimePlan,
    derive_dlq_topics,
)

# Default topic provisioning per Doc02 §3.4.
_DEFAULT_PARTITIONS_FLOOR = 6
_DEFAULT_RETENTION_HOURS = 24

# Default Run-level guardrail caps when the IR didn't declare any.
_DEFAULT_RUN_TOKENS = 200_000
_DEFAULT_RUN_CYCLES = 200
_DEFAULT_AGENT_TOKENS_PER_CALL = 8_000
_DEFAULT_AGENT_CYCLES = 5


def make_plan(ir: WorkflowIR) -> RuntimePlan:
    """Materialize a :class:`RuntimePlan` from a lowered :class:`WorkflowIR`.

    Idempotent: same input → same output (modulo dict insertion
    order, which we keep stable).
    """
    workflow_run_caps = _resolve_run_caps(ir)
    agent_default_caps = _resolve_agent_defaults(ir)

    agents: dict[str, AgentPlan] = {}
    for key, tmpl in ir.agents.items():
        agents[key] = _make_agent_plan(
            ir.id, key, tmpl, defaults=agent_default_caps,
        )

    bus_topics = _make_bus_plan(ir)
    bindings = {key: t.llm for key, t in ir.agents.items() if t.llm is not None}

    return RuntimePlan(
        workflow_id=ir.id,
        workflow_version=ir.version,
        ir_hash=ir.meta.ir_hash,
        agents=agents,
        bus_topics=bus_topics,
        llm_bindings=bindings,
        run_max_tokens=workflow_run_caps[0],
        run_max_cycles=workflow_run_caps[1],
    )


# ----------------------------------------------------------------
# Internals
# ----------------------------------------------------------------


def _resolve_run_caps(ir: WorkflowIR) -> tuple[int, int]:
    """Return (run_max_tokens, run_max_cycles) honouring the IR override."""
    if ir.guardrails and ir.guardrails.per_run:
        return (
            ir.guardrails.per_run.max_total_tokens,
            ir.guardrails.per_run.max_cycles_per_run,
        )
    return _DEFAULT_RUN_TOKENS, _DEFAULT_RUN_CYCLES


def _resolve_agent_defaults(ir: WorkflowIR) -> tuple[int, int]:
    """Return (max_tokens_per_call, max_cycles) for the per-agent default."""
    if ir.guardrails and ir.guardrails.per_agent:
        return (
            ir.guardrails.per_agent.max_tokens_per_call,
            ir.guardrails.per_agent.max_cycles,
        )
    return _DEFAULT_AGENT_TOKENS_PER_CALL, _DEFAULT_AGENT_CYCLES


def _make_agent_plan(
    workflow_id: str,
    template_key: str,
    tmpl: AgentTemplate,
    *,
    defaults: tuple[int, int],
) -> AgentPlan:
    sub_topics = [s.topic for s in tmpl.subscribe]
    sub_filters = [s.tag_filter for s in tmpl.subscribe]
    pub_topics = [p.topic for p in tmpl.publish]

    agent_caps = tmpl.guardrail or None
    max_tokens = agent_caps.max_tokens_per_call if agent_caps else defaults[0]
    max_cycles = agent_caps.max_cycles if agent_caps else defaults[1]

    fallback_dump = tmpl.fallback.model_dump(mode="json") if tmpl.fallback else None
    aggregate_dump = (
        tmpl.aggregate.model_dump(mode="json") if tmpl.aggregate else None
    )

    return AgentPlan(
        template_key=template_key,
        role=str(tmpl.role.value),
        description=tmpl.description,
        subscribe_topics=sub_topics,
        subscribe_tag_filters=sub_filters,
        consumer_group=derive_consumer_group(workflow_id, template_key),
        publish_topics=pub_topics,
        llm=tmpl.llm,
        replica_min=tmpl.replicas.min,
        replica_max=tmpl.replicas.max,
        fallback=fallback_dump,
        max_tokens_per_call=max_tokens,
        max_cycles=max_cycles,
        tags=dict(tmpl.tags),
        aggregate=aggregate_dump,
    )


def _make_bus_plan(ir: WorkflowIR) -> BusTopicPlan:
    business_topics = ir.all_topics
    if not business_topics:
        return BusTopicPlan(topics=[])

    # Per-template max replicas drives per-topic partition count
    # (Doc02 §4.2: partitions ≥ replicas.max for alt_instance).
    replicas_max = max(
        (t.replicas.max for t in ir.agents.values()), default=1,
    )
    partitions = max(_DEFAULT_PARTITIONS_FLOOR, replicas_max)

    overrides: dict[str, dict] = {}
    if ir.bus and ir.bus.topic_overrides:
        overrides = ir.bus.topic_overrides

    business_specs: list[BusTopicSpec] = []
    for topic in business_topics:
        ov = overrides.get(topic, {})
        business_specs.append(
            BusTopicSpec(
                topic=topic,
                partitions=int(ov.get("partitions", partitions)),
                retention_hours=int(
                    ov.get("retention_hours", _DEFAULT_RETENTION_HOURS),
                ),
                is_dlq=False,
            ),
        )

    dlq_specs = derive_dlq_topics(business_topics)

    return BusTopicPlan(topics=business_specs + dlq_specs)


# ----------------------------------------------------------------
# Public re-export so callers don't need to import VIRTUAL_NODES
# ----------------------------------------------------------------

__all__ = ["make_plan", "VIRTUAL_NODES"]
