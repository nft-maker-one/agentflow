"""Switch routing + terminal detection.

Both routers attach to the same EventBus and observe events flowing
between agents. They:

* :class:`SwitchRouter` — for each switch edge, subscribes to the
  from-Agent's publish topic, evaluates the switch expression, and
  re-publishes to the chosen case-branch's ``via`` topic.
* :class:`TerminalDetector` — for each edge whose target is
  ``__end__`` or ``__error__``, watches the ``via`` topic and
  notifies the Orchestrator so it can finalize the Run.

Both run as background asyncio tasks. The Orchestrator owns their
lifecycle (start / stop / drain).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from dataclasses import dataclass

from agentkit.bus.builder import build_envelope
from agentkit.bus.interface import EventBus, SubscribeSpec, Subscriber
from agentkit.common.logging import get_logger
from agentkit.models.envelope import Envelope
from agentkit.orchestrator.errors import SwitchEvalError
from agentkit.orchestrator.run import BranchEvent
from agentkit.orchestrator.switch_eval import (
    evaluate_switch_expression,
    pick_case,
)
from agentkit.workflow.ir import (
    END_NODE,
    ERROR_NODE,
    EdgeBranch,
    EdgeSpec,
    WorkflowIR,
)

log = get_logger(__name__)


# ============================================================
# Switch routing
# ============================================================


@dataclass(frozen=True)
class _SwitchEntry:
    """Pre-computed routing data for one switch edge."""

    edge_id: str
    from_template: str
    expr: str
    cases: dict[str, EdgeBranch]
    default: EdgeBranch | None


class SwitchRouter:
    """Drives switch-edge routing for a deployed Workflow IR.

    Phase 2.4 optimization: ``replicas`` controls how many concurrent
    asyncio tasks share each trigger_topic's subscriber. With Kafka
    bus + slow downstream publishes, raising replicas to 2-4 lets
    multiple events on the same topic dispatch in parallel. For
    InProcess bus the gain is small (no network IO), but the option
    is harmless. Default 1 preserves prior behavior.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        ir: WorkflowIR,
        on_branch: Callable[[str, BranchEvent], Awaitable[None]] | None = None,
        replicas: int = 1,
    ) -> None:
        self._bus = bus
        self._ir = ir
        self._on_branch = on_branch
        self._replicas = max(1, replicas)
        # ``trigger_topic → list[_SwitchEntry]``. Each Agent typically
        # publishes one topic, but we support multiple cleanly here.
        self._table: dict[str, list[_SwitchEntry]] = self._build_table(ir)
        self._subscribers: list[Subscriber] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._stopped = False

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        for trigger_topic in self._table:
            sub = await self._bus.subscribe(
                SubscribeSpec(
                    topic_pattern=trigger_topic,
                    group=f"grp.orch.switch.{self._ir.id}.{trigger_topic}",
                    starting_position="latest",
                ),
            )
            self._subscribers.append(sub)
            # Spawn ``replicas`` consumer tasks sharing the same subscriber.
            # The bus's group semantics ensure each message is delivered
            # exactly once across the consumer group; the dispatch itself
            # is stateless (pure expression eval + publish), so multiple
            # workers can safely process distinct messages in parallel.
            for _ in range(self._replicas):
                self._tasks.append(
                    asyncio.create_task(self._consume_loop(sub, trigger_topic)),
                )
        log.info(
            "orch.switch.started",
            workflow_id=self._ir.id,
            triggers=sorted(self._table),
            replicas=self._replicas,
        )

    async def stop(self) -> None:
        self._stopped = True
        for sub in self._subscribers:
            try:
                await sub.close()
            except Exception:
                log.exception("orch.switch.close_failed")
        for t in self._tasks:
            t.cancel()
        self._subscribers.clear()
        self._tasks.clear()

    @property
    def trigger_topics(self) -> list[str]:
        return sorted(self._table)

    # ------------------------------------------------------------
    # Loop
    # ------------------------------------------------------------

    async def _consume_loop(self, sub: Subscriber, trigger_topic: str) -> None:
        try:
            async for msg in sub.messages():
                if self._stopped:
                    break
                envelope = msg.envelope
                entries = self._table.get(trigger_topic, [])
                for entry in entries:
                    # Match the actual sender — only fire switch when
                    # the event is from the right Agent.
                    if (
                        envelope.from_.agent_id is None
                        or self._sender_matches(envelope, entry.from_template)
                    ):
                        await self._dispatch_one(entry, envelope)
                try:
                    await self._bus.ack(sub, msg)
                except Exception:
                    log.exception("orch.switch.ack_failed")
        except asyncio.CancelledError:
            pass

    def _sender_matches(self, envelope: Envelope, template_key: str) -> bool:
        """In Phase 1 the bus envelope carries ``from_.agent_id`` (the
        runtime instance id) plus the publishing topic. We don't have
        a template_key field on the envelope yet — we infer match by
        topic alone since the trigger_topic was already filtered to
        topics that this Template publishes to.
        """
        del template_key, envelope  # parametrized for future tightening
        return True

    async def _dispatch_one(
        self, entry: _SwitchEntry, envelope: Envelope,
    ) -> None:
        """Evaluate the switch expression and publish to the chosen branch."""
        try:
            extracted = evaluate_switch_expression(entry.expr, envelope.payload)
        except SwitchEvalError as e:
            log.exception(
                "orch.switch.eval_failed",
                edge_id=entry.edge_id,
                expr=entry.expr,
                run_id=envelope.run_id,
                error=str(e),
            )
            await self._route_to_error(entry, envelope, reason=f"switch_eval_failed: {e}")
            return

        chosen = pick_case(entry.cases, extracted, default_key=None)
        branch = entry.cases.get(chosen) if chosen is not None else entry.default

        if branch is None:
            log.warning(
                "orch.switch.no_match",
                edge_id=entry.edge_id,
                run_id=envelope.run_id,
                extracted=extracted,
            )
            await self._route_to_error(
                entry, envelope, reason=f"switch_no_match (extracted={extracted!r})",
            )
            return

        await self._route_to_branch(entry, envelope, chosen=chosen, branch=branch)

    async def _route_to_branch(
        self,
        entry: _SwitchEntry,
        envelope: Envelope,
        *,
        chosen: str | None,
        branch: EdgeBranch,
    ) -> None:
        if branch.to in (END_NODE, ERROR_NODE):
            # Terminal — no via topic. We still emit a BranchEvent so
            # the Run audit shows the decision.
            await self._notify_branch(
                envelope.run_id,
                BranchEvent(
                    edge_id=entry.edge_id,
                    chosen=branch.to,
                    by="auto",
                    reason=f"extracted={chosen!r}",
                ),
            )
            # If branch.to == __end__/__error__, the TerminalDetector
            # handles status finalization separately. Without a via
            # topic to publish on, we publish a synthetic terminal
            # marker so TerminalDetector can pick it up.
            terminal_topic = (
                f"system.run.{envelope.run_id}.end"
                if branch.to == END_NODE
                else f"system.run.{envelope.run_id}.error"
            )
            await self._bus.publish(
                build_envelope(
                    topic=terminal_topic,
                    payload={"from_edge": entry.edge_id, "chosen": chosen},
                    workflow_id=envelope.workflow_id,
                    run_id=envelope.run_id,
                    trace_id=envelope.trace_id,
                    causation_id=envelope.event_id,
                ),
            )
            return

        # Real Agent target — publish to its via topic.
        if branch.via is None:
            log.error(
                "orch.switch.branch_missing_via",
                edge_id=entry.edge_id,
                run_id=envelope.run_id,
            )
            await self._route_to_error(entry, envelope, reason="branch missing via")
            return

        await self._bus.publish(
            build_envelope(
                topic=branch.via,
                payload=envelope.payload,
                workflow_id=envelope.workflow_id,
                run_id=envelope.run_id,
                trace_id=envelope.trace_id,
                causation_id=envelope.event_id,
            ),
        )
        await self._notify_branch(
            envelope.run_id,
            BranchEvent(
                edge_id=entry.edge_id,
                chosen=chosen or "__default__",
                by="auto",
                reason=f"via={branch.via}",
            ),
        )

    async def _route_to_error(
        self, entry: _SwitchEntry, envelope: Envelope, *, reason: str,
    ) -> None:
        await self._bus.publish(
            build_envelope(
                topic=f"system.run.{envelope.run_id}.error",
                payload={"from_edge": entry.edge_id, "reason": reason},
                workflow_id=envelope.workflow_id,
                run_id=envelope.run_id,
                trace_id=envelope.trace_id,
                causation_id=envelope.event_id,
            ),
        )
        await self._notify_branch(
            envelope.run_id,
            BranchEvent(
                edge_id=entry.edge_id,
                chosen=ERROR_NODE,
                by="system",
                reason=reason,
            ),
        )

    async def _notify_branch(self, run_id: str, ev: BranchEvent) -> None:
        if self._on_branch is None:
            return
        try:
            await self._on_branch(run_id, ev)
        except Exception:
            log.exception("orch.switch.on_branch_listener_failed")

    # ------------------------------------------------------------
    # Build trigger table
    # ------------------------------------------------------------

    @staticmethod
    def _build_table(ir: WorkflowIR) -> dict[str, list[_SwitchEntry]]:
        """Walk every switch edge; group by from-Agent's publish topics."""
        table: dict[str, list[_SwitchEntry]] = {}
        for edge_id, edge in ir.edges.items():
            if not edge.is_switch:
                continue
            from_template = edge.from_
            from_agent = ir.agents.get(from_template)
            if from_agent is None:
                # Should have been caught in compiler validate; defensive only.
                continue
            from_topics = [ps.topic for ps in from_agent.publish]
            switch = edge.to
            assert hasattr(switch, "switch")  # noqa: S101 — we know is_switch
            entry = _SwitchEntry(
                edge_id=edge_id,
                from_template=from_template,
                expr=switch.switch,  # type: ignore[union-attr]
                cases=dict(edge.cases or {}),
                default=edge.default,
            )
            for topic in from_topics:
                table.setdefault(topic, []).append(entry)
        return table


# ============================================================
# Terminal detection
# ============================================================


class TerminalDetector:
    """Watch terminal-edge ``via`` topics and notify on completion.

    Two trigger families:

    * **synthetic terminal markers** — ``system.run.<run_id>.end`` /
      ``system.run.<run_id>.error`` published by :class:`SwitchRouter`
      when a switch resolves to ``__end__`` / ``__error__``;
    * **direct terminal edges** — non-switch edges whose target is
      ``__end__`` / ``__error__``. Subscribe to their ``via`` topics
      so we catch normal end-of-pipeline events too.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        ir: WorkflowIR,
        on_terminal: Callable[
            [str, str, str | None, dict | None], Awaitable[None]
        ],
    ) -> None:
        self._bus = bus
        self._ir = ir
        # callback receives (run_id, terminal_kind, reason, payload)
        # ``payload`` is the terminating envelope's payload — i.e. the
        # output the agent published onto the __end__ edge's via-topic.
        self._on_terminal = on_terminal
        self._end_topics, self._error_topics = self._build_topics(ir)
        self._subscribers: list[Subscriber] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._stopped = False

        # ── End-node fan-in (FlowControl) ──
        # When ``ir.end_join`` is on AND more than one *direct* edge
        # targets __end__, the run is only declared terminal once EVERY
        # end-edge via-topic has fired for that run (instead of the
        # default "first signal wins"). Mirrors the per-agent aggregate
        # join, applied to the terminal node.
        self._end_join: bool = (
            bool(getattr(ir, "end_join", False)) and len(self._end_topics) > 1
        )
        # run_id → set of required end-topics seen so far (insertion-
        # ordered dict gives us cheap oldest-first eviction).
        self._end_seen: dict[str, set[str]] = {}
        self._max_join_runs = 10_000

    @property
    def end_topics(self) -> list[str]:
        return sorted(self._end_topics)

    @property
    def error_topics(self) -> list[str]:
        return sorted(self._error_topics)

    async def start(self) -> None:
        # Always subscribe to the synthetic terminal-marker pattern.
        # In the v1 MockBus we model literal topics; the synthetic
        # markers are per-Run so they're hard to predeclare. Solution:
        # subscribe to a wildcard ``system.run.*.end``-style pattern.
        await self._spawn(
            "system.run-terminal-end-stream",
            self._end_topics | {"system.run.#"},
            terminal_kind=END_NODE,
            wildcard_role="end",
        )
        await self._spawn(
            "system.run-terminal-error-stream",
            self._error_topics | {"system.run.#"},
            terminal_kind=ERROR_NODE,
            wildcard_role="error",
        )
        log.info(
            "orch.terminal.started",
            workflow_id=self._ir.id,
            end_topics=sorted(self._end_topics),
            error_topics=sorted(self._error_topics),
        )

    async def stop(self) -> None:
        self._stopped = True
        for sub in self._subscribers:
            try:
                await sub.close()
            except Exception:
                log.exception("orch.terminal.close_failed")
        for t in self._tasks:
            t.cancel()
        self._subscribers.clear()
        self._tasks.clear()

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    async def _spawn(
        self,
        group: str,
        topics: set[str],
        *,
        terminal_kind: str,
        wildcard_role: str,
    ) -> None:
        for topic in topics:
            # Per-run terminal markers (system.run.<id>.end / .error) live
            # on dynamically-created topics matched by the `system.run.#`
            # wildcard. On Kafka, a `latest` consumer that discovers such a
            # brand-new topic positions at the partition END and SKIPS the
            # single marker at offset 0 — so the run would never finalize.
            # Use `earliest` for wildcard streams so offset 0 is read;
            # manual-commit group tracking means established topics still
            # resume from their committed offset (no replay storm). Stable
            # literal terminal topics keep `latest` (they're long-lived and
            # shared across runs — earliest would replay all history).
            is_wildcard = "#" in topic or "*" in topic
            sub = await self._bus.subscribe(
                SubscribeSpec(
                    topic_pattern=topic,
                    group=f"{group}.{topic}",
                    starting_position="earliest" if is_wildcard else "latest",
                ),
            )
            self._subscribers.append(sub)
            self._tasks.append(
                asyncio.create_task(
                    self._consume_loop(
                        sub, terminal_kind=terminal_kind, wildcard_role=wildcard_role,
                    ),
                ),
            )

    async def _consume_loop(
        self,
        sub: Subscriber,
        *,
        terminal_kind: str,
        wildcard_role: str,
    ) -> None:
        try:
            async for msg in sub.messages():
                if self._stopped:
                    break
                env = msg.envelope
                # Filter wildcard subs to only the role we care about.
                if wildcard_role == "end" and not (
                    env.topic in self._end_topics
                    or env.topic.endswith(f".{env.run_id}.end")
                ):
                    pass  # not ours; ack and move on
                elif wildcard_role == "error" and not (
                    env.topic in self._error_topics
                    or env.topic.endswith(f".{env.run_id}.error")
                ):
                    pass
                else:
                    await self._notify(env, terminal_kind=terminal_kind)
                try:
                    await self._bus.ack(sub, msg)
                except Exception:
                    log.exception("orch.terminal.ack_failed")
        except asyncio.CancelledError:
            pass

    async def _notify(self, env: Envelope, *, terminal_kind: str) -> None:
        # End-node fan-in: hold back the terminal until every direct
        # end-edge has fired for this run (FlowControl). Errors and
        # switch-synthetic markers bypass the join (see _end_join_ready).
        if terminal_kind == END_NODE and self._end_join:
            if not self._end_join_ready(env):
                return
        reason = (env.payload or {}).get("reason") if isinstance(env.payload, dict) else None
        payload = env.payload if isinstance(env.payload, dict) else None
        try:
            await self._on_terminal(env.run_id, terminal_kind, reason, payload)
        except Exception:
            log.exception(
                "orch.terminal.callback_failed", run_id=env.run_id,
            )

    def _end_join_ready(self, env: Envelope) -> bool:
        """Return True once *all* required end-topics have fired for this run.

        A synthetic switch marker (``system.run.<id>.end``) is an explicit
        single terminal decision — it is NOT one of the direct end via-
        topics, so it bypasses the join and fires immediately.
        """
        if env.topic not in self._end_topics:
            return True  # switch-synthetic / non-joined terminal

        run_id = env.run_id
        seen = self._end_seen.get(run_id)
        if seen is None:
            seen = set()
            self._end_seen[run_id] = seen
            # Bound memory: drop oldest in-flight joins (orphaned runs
            # whose remaining end-topics never arrive). Insertion order.
            while len(self._end_seen) > self._max_join_runs:
                self._end_seen.pop(next(iter(self._end_seen)), None)
        seen.add(env.topic)

        if seen >= self._end_topics:           # collected every end-topic
            self._end_seen.pop(run_id, None)
            return True
        log.debug(
            "orch.terminal.join_wait",
            run_id=run_id, have=sorted(seen), need=sorted(self._end_topics),
        )
        return False

    # ------------------------------------------------------------
    # Build terminal topic sets
    # ------------------------------------------------------------

    @staticmethod
    def _build_topics(ir: WorkflowIR) -> tuple[set[str], set[str]]:
        ends: set[str] = set()
        errors: set[str] = set()
        for edge in ir.edges.values():
            # Direct/fanout edges with terminal target — their ``via``
            # is the terminal trigger.
            if not edge.is_switch and edge.via:
                if isinstance(edge.to, str):
                    if edge.to == END_NODE:
                        ends.add(edge.via)
                    elif edge.to == ERROR_NODE:
                        errors.add(edge.via)
                elif isinstance(edge.to, list):
                    if END_NODE in edge.to:
                        ends.add(edge.via)
                    if ERROR_NODE in edge.to:
                        errors.add(edge.via)
        return ends, errors


# ----------------------------------------------------------------
# Helper: validate that an EdgeSpec qualifies for switch routing
# (used by tests to assert IR shape).
# ----------------------------------------------------------------


def is_switch_edge(edge: EdgeSpec) -> bool:
    return edge.is_switch
