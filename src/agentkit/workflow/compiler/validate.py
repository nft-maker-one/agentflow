"""Step ④ — Validate: static semantic checks.

Returns a list of violation strings. The Compiler turns a non-empty
list into :class:`IRValidationError`. Each rule lives in its own
small function so they're easy to unit-test in isolation.

Coverage map vs ``Doc04 §3.2``:

* Topology: reachability from ``__start__``, no orphan agents,
  every active node has a path to ``__end__`` or ``__error__``.
* Topic whitelist: ``edge.via`` must be in the from-Agent's
  ``publish`` whitelist *and* match a from-/to-Agent's subscribe
  pattern.
* Aggregator gather correctness — Phase 1: skipped (Aggregator
  spec is in Doc04 §4.1; full implementation is Phase 2).
* Switch completeness: each case branch's target must exist.
* Schema refs: Phase 1 — only basic shape; full JSON Schema load
  is Phase 2.
* Role permissions: Phase 2 (depends on Registry / Tenant).

The current rule set covers the essentials needed for a Phase-1
end-to-end run.
"""

from __future__ import annotations

from collections.abc import Iterable
from collections import deque

from agentkit.bus.naming import is_dlq_topic
from agentkit.workflow.ir import (
    END_NODE,
    ERROR_NODE,
    START_NODE,
    VIRTUAL_NODES,
    EdgeSpec,
    WorkflowIR,
)


# ----------------------------------------------------------------
# Public API
# ----------------------------------------------------------------


def validate_ir(ir: WorkflowIR) -> list[str]:
    """Run all checks; return a (possibly empty) list of violations."""
    violations: list[str] = []
    violations.extend(_check_at_least_one_agent(ir))
    violations.extend(_check_no_dangling_targets(ir))
    violations.extend(_check_no_dlq_topics_published(ir))
    violations.extend(_check_publish_whitelist(ir))
    violations.extend(_check_subscribe_match(ir))
    violations.extend(_check_switch_completeness(ir))
    violations.extend(_check_fallback_alt_template(ir))
    violations.extend(_check_reachable_from_start(ir))
    violations.extend(_check_terminates(ir))
    violations.extend(_check_unique_via_topics_in_fanout(ir))
    return violations


# ----------------------------------------------------------------
# Individual rules — each returns Iterable[str] of violations.
# ----------------------------------------------------------------


def _check_at_least_one_agent(ir: WorkflowIR) -> Iterable[str]:
    if not ir.agents:
        yield "workflow has no agents declared"


def _check_no_dangling_targets(ir: WorkflowIR) -> Iterable[str]:
    """Every edge ``from`` / ``to`` must be a known node or virtual."""
    known = ir.all_node_keys() | VIRTUAL_NODES
    for eid, e in ir.edges.items():
        if e.from_ not in known:
            yield f"edges.{eid}.from references unknown node {e.from_!r}"
        for tgt in e.all_targets():
            if tgt not in known:
                yield f"edges.{eid}.to references unknown node {tgt!r}"


def _check_no_dlq_topics_published(ir: WorkflowIR) -> Iterable[str]:
    """Users may not publish to ``*.dlq`` topics — those are framework-managed."""
    for key, agent in ir.agents.items():
        for ps in agent.publish:
            if is_dlq_topic(ps.topic):
                yield (
                    f"agents.{key}.publish[{ps.topic!r}]: '.dlq' suffix is reserved "
                    f"for framework-generated dead-letter topics"
                )


def _check_publish_whitelist(ir: WorkflowIR) -> Iterable[str]:
    """``edge.via`` must be declared in from-Agent's publish whitelist.

    Virtual nodes (``__start__``) have no publish whitelist — those
    edges are skipped (Orchestrator publishes them).

    **Switch edges are also skipped**: per ``Doc05 §5``, the
    Orchestrator subscribes to the from-Agent's output, evaluates
    the switch expression, and publishes to the case-branch ``via``
    itself. The from-Agent never directly publishes to those topics,
    so checking against its publish whitelist would be incorrect.
    """
    for eid, e in ir.edges.items():
        if e.from_ in VIRTUAL_NODES:
            continue
        if e.is_switch:
            continue
        agent = ir.agents.get(e.from_)
        if agent is None:
            # Already flagged by _check_no_dangling_targets.
            continue
        published = {ps.topic for ps in agent.publish}
        for via in e.all_vias():
            if via not in published:
                yield (
                    f"edges.{eid}.via[{via!r}] is not in agents.{e.from_}.publish whitelist"
                )


def _check_subscribe_match(ir: WorkflowIR) -> Iterable[str]:
    """Each non-virtual edge target must have a Subscription matching ``via``.

    This is *Phase 1* matching: literal equality OR exact ``*``-suffix
    pattern. Full AMQP-style wildcard semantics arrive with
    Doc02 §3.2's runtime topic resolver.
    """
    for eid, e in ir.edges.items():
        # Switch edges have a per-case (target, via) pairing — check
        # each pair independently. Cartesian product would (wrongly)
        # require every target to subscribe to every via.
        if e.is_switch:
            pairs: list[tuple[str, str | None]] = []
            if e.cases:
                pairs.extend((b.to, b.via) for b in e.cases.values())
            if e.default is not None:
                pairs.append((e.default.to, e.default.via))
            for tgt, via in pairs:
                if tgt in VIRTUAL_NODES or via is None:
                    continue
                agent = ir.agents.get(tgt)
                if agent is None:
                    continue
                if not _any_subscription_matches(agent.subscribe, via):
                    yield (
                        f"edges.{eid}.via[{via!r}] has no matching "
                        f"subscription on agents.{tgt}.subscribe"
                    )
            continue

        # Direct / fanout: each target must subscribe to the (single) via.
        for tgt in e.all_targets():
            if tgt in VIRTUAL_NODES:
                continue
            agent = ir.agents.get(tgt)
            if agent is None:
                continue
            for via in e.all_vias():
                if not _any_subscription_matches(agent.subscribe, via):
                    yield (
                        f"edges.{eid}.via[{via!r}] has no matching "
                        f"subscription on agents.{tgt}.subscribe"
                    )


def _any_subscription_matches(subs: list, via: str) -> bool:
    for s in subs:
        if _topic_pattern_matches(s.topic, via):
            return True
    return False


def _topic_pattern_matches(pattern: str, topic: str) -> bool:
    """Phase-1 matcher: exact, ``foo.*`` (one segment), ``foo.#`` (any depth)."""
    if pattern == topic:
        return True
    if pattern.endswith(".*"):
        prefix = pattern[: -len(".*")]
        if topic.startswith(prefix + "."):
            tail = topic[len(prefix) + 1 :]
            return "." not in tail and len(tail) > 0
    if pattern.endswith(".#"):
        prefix = pattern[: -len(".#")]
        if topic == prefix or topic.startswith(prefix + "."):
            return True
    return False


def _check_switch_completeness(ir: WorkflowIR) -> Iterable[str]:
    """Switch edges must define at least one case (already checked by
    Pydantic) AND every branch must specify ``via`` if its target is
    not a virtual node."""
    for eid, e in ir.edges.items():
        if not e.is_switch:
            continue
        branches = list((e.cases or {}).items())
        if e.default is not None:
            branches.append(("__default__", e.default))
        for case_name, branch in branches:
            if branch.to not in VIRTUAL_NODES and branch.via is None:
                yield (
                    f"edges.{eid}.cases.{case_name}: target {branch.to!r} is "
                    f"a real Agent and requires a `via` topic"
                )


def _check_fallback_alt_template(ir: WorkflowIR) -> Iterable[str]:
    """``fallback.alt_template`` must reference an existing Agent."""
    for key, agent in ir.agents.items():
        if agent.fallback and agent.fallback.alt_template:
            target = agent.fallback.alt_template
            if target not in ir.agents:
                yield (
                    f"agents.{key}.fallback.alt_template references unknown agent {target!r}"
                )


def _check_reachable_from_start(ir: WorkflowIR) -> Iterable[str]:
    """Every Agent declared in ``agents`` must be reachable from ``__start__``.

    Unreachable Agents are usually a sign of a typo — emit them as
    *warnings* (still violations) so users see them in the CLI.
    """
    if not ir.agents:
        return
    adj = ir.graph_neighbors()
    if START_NODE not in adj:
        yield "no edge originates from __start__"
        return
    seen: set[str] = set()
    queue = deque([START_NODE])
    while queue:
        node = queue.popleft()
        if node in seen:
            continue
        seen.add(node)
        for nxt in adj.get(node, ()):  # type: ignore[arg-type]
            queue.append(nxt)
    unreachable = ir.all_node_keys() - seen
    for u in sorted(unreachable):
        yield f"agents.{u}: unreachable from __start__"


def _check_terminates(ir: WorkflowIR) -> Iterable[str]:
    """Every reachable Agent must have a path to __end__ or __error__.

    BFS *backwards* from terminals; report Agents not in the
    backward-reach set.
    """
    if not ir.agents:
        return
    # Build reverse adjacency (target → set of from-nodes).
    reverse: dict[str, set[str]] = {}
    for e in ir.edges.values():
        for tgt in e.all_targets():
            reverse.setdefault(tgt, set()).add(e.from_)

    can_terminate: set[str] = set()
    queue: deque[str] = deque([END_NODE, ERROR_NODE])
    while queue:
        node = queue.popleft()
        for src in reverse.get(node, ()):
            if src not in can_terminate:
                can_terminate.add(src)
                queue.append(src)

    for key in ir.agents:
        if key not in can_terminate:
            yield (
                f"agents.{key}: no outgoing path to __end__ or __error__ "
                f"(possible infinite loop)"
            )


def _check_unique_via_topics_in_fanout(ir: WorkflowIR) -> Iterable[str]:
    """Fanout edges target multiple agents through a single ``via`` topic.

    All targets must subscribe to that topic.
    """
    for eid, e in ir.edges.items():
        if not e.is_fanout:
            continue
        if e.via is None:
            yield f"edges.{eid}: fanout edge requires a `via` topic"
        # Per-target subscribe matches already checked by
        # ``_check_subscribe_match``; nothing extra to do here.


# ----------------------------------------------------------------
# Convenience: type-narrow EdgeSpec (used by tests)
# ----------------------------------------------------------------


def is_terminating_edge(edge: EdgeSpec) -> bool:
    """True iff ``edge`` leads (directly) to a terminal virtual node."""
    return any(t in (END_NODE, ERROR_NODE) for t in edge.all_targets())
