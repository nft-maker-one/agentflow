"""Compiler step ②.5 — Topic shorthand expansion.

Turns user-friendly short forms into the canonical
``agent.<template_key>.{in,out}.<suffix>`` form expected by all
downstream stages (validate / plan / Bus).

Three accepted shapes:

1. **Bare suffix** (no dots) — auto-prefixed with the agent's own
   namespace and direction::

       outliner.subscribe[].topic = "topic"
            → "agent.outliner.in.topic"

       outliner.publish[].topic = "outline"
            → "agent.outliner.out.outline"

2. **Cross-agent shorthand** (``<key>.<in|out>.<suffix>``) —
   prepended with ``agent.``::

       polisher.subscribe[].topic = "outliner.out.outline"
            → "agent.outliner.out.outline"

3. **Fully-qualified** (any other dotted form, e.g. starts with
   ``agent.`` or uses an unrelated namespace) — used as-is::

       handler.publish[].topic = "events.audit.user_login"
            → "events.audit.user_login"

Per-agent override: if ``AgentTemplate.topic_prefix`` is set, it
replaces the default ``agent.<template_key>`` base for that
agent's local-shorthand expansions. Cross-agent shorthand always
uses the literal ``agent.`` prefix and ignores per-agent overrides
(it's a routing identifier, not the producer's topology).

This step is **purely syntactic** — it never changes routing
semantics. Validate runs immediately afterwards on the expanded
form.
"""

from __future__ import annotations

import re
from typing import Final, Literal

from agentkit.workflow.ir import WorkflowIR
from agentkit.workflow.ir.agent import (
    AgentTemplate,
    PublishSpec,
    Subscription,
)
from agentkit.workflow.ir.edge import EdgeBranch, EdgeSpec
from agentkit.workflow.ir.workflow import END_NODE, ERROR_NODE, START_NODE

Direction = Literal["in", "out"]

# Cross-agent shorthand: ``<key>.<in|out>.<suffix>``.
# Use a strict regex so we don't accidentally rewrite
# user-defined topics that happen to look similar.
_CROSS_AGENT_SHORTHAND: Final[re.Pattern[str]] = re.compile(
    r"^(?P<key>[A-Za-z_][A-Za-z0-9_-]*)\.(?P<dir>in|out)\.(?P<rest>[^.\s][^\s]*)$",
)


def expand_workflow(ir: WorkflowIR) -> WorkflowIR:
    """Expand all topic shorthand forms inside an IR.

    Returns a new IR with expanded subscribe/publish/via fields.
    Idempotent: running it on an already-expanded IR is a no-op.
    """
    new_agents: dict[str, AgentTemplate] = {}
    for key, agent in ir.agents.items():
        new_agents[key] = _expand_agent(agent, agent_key=key)

    new_edges: dict[str, EdgeSpec] = {}
    for edge_id, edge in ir.edges.items():
        new_edges[edge_id] = _expand_edge(edge, agents=new_agents)

    return ir.model_copy(update={"agents": new_agents, "edges": new_edges})


# ----------------------------------------------------------------
# Agent-level expansion
# ----------------------------------------------------------------


def _expand_agent(agent: AgentTemplate, *, agent_key: str) -> AgentTemplate:
    base = agent.topic_prefix or f"agent.{agent_key}"

    new_subs = [
        sub.model_copy(
            update={
                "topic": _expand_topic(sub.topic, base=base, direction="in"),
            },
        )
        for sub in agent.subscribe
    ]
    new_pubs = [
        pub.model_copy(
            update={
                "topic": _expand_topic(pub.topic, base=base, direction="out"),
            },
        )
        for pub in agent.publish
    ]
    return agent.model_copy(
        update={"subscribe": new_subs, "publish": new_pubs},
    )


# ----------------------------------------------------------------
# Edge-level expansion
# ----------------------------------------------------------------


def _expand_edge(
    edge: EdgeSpec, *, agents: dict[str, AgentTemplate],
) -> EdgeSpec:
    """Expand top-level ``via`` and any branch.via inside switch edges."""
    updates: dict[str, object] = {}

    # Top-level via (direct / fanout edges).
    if edge.via is not None:
        updates["via"] = _expand_via(
            edge.via, edge_from=edge.from_, edge_to=edge.to,
            agents=agents,
        )

    # Switch case branches.
    if edge.is_switch and edge.cases:
        new_cases: dict[str, EdgeBranch] = {}
        for case_key, branch in edge.cases.items():
            new_cases[case_key] = _expand_branch(
                branch, edge_from=edge.from_, agents=agents,
            )
        updates["cases"] = new_cases

    if edge.default is not None:
        updates["default"] = _expand_branch(
            edge.default, edge_from=edge.from_, agents=agents,
        )

    if not updates:
        return edge
    return edge.model_copy(update=updates)


def _expand_branch(
    branch: EdgeBranch,
    *,
    edge_from: str,
    agents: dict[str, AgentTemplate],
) -> EdgeBranch:
    if branch.via is None:
        return branch
    expanded = _expand_via(
        branch.via,
        edge_from=edge_from,
        edge_to=branch.to,
        agents=agents,
        is_switch_branch=True,
    )
    if expanded == branch.via:
        return branch
    return branch.model_copy(update={"via": expanded})


def _expand_via(
    via: str,
    *,
    edge_from: str,
    edge_to: object,
    agents: dict[str, AgentTemplate],
    is_switch_branch: bool = False,
) -> str:
    """Determine which agent's namespace owns this via, then expand."""
    # If it's already qualified, short-circuit.
    if "." in via:
        m = _CROSS_AGENT_SHORTHAND.match(via)
        if m:
            return f"agent.{via}"
        return via  # full form

    # Bare suffix — needs a producer's namespace.
    base, direction = _via_namespace(
        edge_from=edge_from,
        edge_to=edge_to,
        agents=agents,
        is_switch_branch=is_switch_branch,
    )
    if base is None or direction is None:
        # Can't resolve — leave as-is so validate flags it cleanly.
        return via
    return f"{base}.{direction}.{via}"


def _via_namespace(
    *,
    edge_from: str,
    edge_to: object,
    agents: dict[str, AgentTemplate],
    is_switch_branch: bool = False,
) -> tuple[str | None, Direction | None]:
    """Return ``(base_prefix, direction)`` for a bare ``via`` shorthand.

    Rules:
    * **Switch case branch** → namespace = target Agent's, direction
      = ``in`` (the Orchestrator — not the from-Agent — is the
      producer for switch routing; the topic must match what the
      target Agent subscribes to).
    * **__start__ → A** → namespace = A's, direction = ``in``
      (Orchestrator publishes to A's input).
    * **A → anything** (direct/fanout) → namespace = A's, direction
      = ``out`` (A is the producer).
    * Anything else (e.g. ``__error__`` as source): unresolvable —
      caller leaves the bare suffix unchanged so validate complains.
    """
    if is_switch_branch:
        if isinstance(edge_to, str) and edge_to in agents:
            target = agents[edge_to]
            return _agent_base(target, edge_to), "in"
        return None, None
    if edge_from == START_NODE:
        # Producer is the Orchestrator; topic lives in to-agent's input.
        if isinstance(edge_to, str) and edge_to in agents:
            target = agents[edge_to]
            return _agent_base(target, edge_to), "in"
        return None, None
    if edge_from in agents:
        producer = agents[edge_from]
        return _agent_base(producer, edge_from), "out"
    if edge_from in (END_NODE, ERROR_NODE):
        return None, None
    return None, None


def _agent_base(agent: AgentTemplate, key: str) -> str:
    return agent.topic_prefix or f"agent.{key}"


# ----------------------------------------------------------------
# Generic topic expansion
# ----------------------------------------------------------------


def _expand_topic(topic: str, *, base: str, direction: Direction) -> str:
    """Expand a single topic according to the three-shape rule."""
    if "." not in topic:
        # Bare suffix — local shorthand.
        return f"{base}.{direction}.{topic}"
    if _CROSS_AGENT_SHORTHAND.match(topic):
        return f"agent.{topic}"
    # Fully-qualified — leave alone.
    return topic
