"""AgentInstance — single Template's runtime worker.

One ``AgentInstance`` corresponds to one running Agent: it
subscribes to topics, runs incoming events through the 4-gate
pipeline, invokes the user handler, publishes results, and updates
the FSM.

We deliberately keep the main loop *small and linear* — every
step has a clear log line and contributes to one specific FSM
transition. Persistence (state log, audit) plugs in via
:class:`StateChangeListener` callbacks (default: no-op).
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from datetime import datetime
from typing import Any, Final

from agentkit.bus.errors import BusError
from agentkit.bus.builder import build_envelope
from agentkit.bus.interface import (
    DeliveredMessage,
    EventBus,
    SubscribeSpec,
    Subscriber,
)
from agentkit.bus.naming import derive_consumer_group
from agentkit.common.ids import new_agent_id
from agentkit.common.logging import get_logger
from agentkit.observability import metrics
from agentkit.common.time import utcnow
from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.guardrail_iface import GuardrailHandle, NoOpGuardrail, Reservation
from agentkit.models.envelope import Envelope
from agentkit.runtime.config import RuntimeSettings
from agentkit.runtime.context import (
    AgentContext,
    Event,
    PublishPipeline,
)
from agentkit.runtime.dedup import DedupStore
from agentkit.runtime.errors import GuardrailExceeded
from agentkit.runtime.executor import AgentExecutor
from agentkit.runtime.fallback import (
    FailureClass,
    RetryTimingPolicy,
    classify_handler_exception,
    compute_next_retry_ms,
)
from agentkit.runtime.fsm import (
    AgentSnapshot,
    FSMTransition,
    apply_transition,
    initial_snapshot,
    transition_for_failure,
)
from agentkit.workflow.plan import AgentPlan

log = get_logger(__name__)


# Sentinel workflow_id stamped by external_io on standalone (no active
# run) ingestion — see ``ExternalIOManager.make_emit``. Such events are
# global and must bypass the per-workflow isolation guard below, alongside
# the empty string (legacy / untagged envelopes).
_GLOBAL_WORKFLOW_IDS = frozenset({"", "external"})


# ---- Public callback types ----

StateChangeListener = Callable[[AgentSnapshot, AgentSnapshot, FSMTransition], Awaitable[None]]


# A small constant — token estimate used when the agent has no
# explicit max_tokens budget on its calls. Accurate budgeting is
# done by the LLM Gateway / Tokenizer; this value is only used by
# Gate 3 (Guardrail.precheck) before we know what the handler
# will actually invoke. Keeping it conservative avoids early blocks.
_DEFAULT_PRECHECK_TOKENS: Final[int] = 1_000


class AgentInstance:
    """One running Agent worker.

    Lifecycle::

        AgentInstance(...).run()   # async — runs until cancel()
                       .cancel()   # graceful drain + shutdown

    The constructor is synchronous; ``run()`` is the coroutine
    that subscribes and dispatches.
    """

    def __init__(
        self,
        *,
        plan: AgentPlan,
        workflow_id: str,
        bus: EventBus,
        llm: LLMGatewayClient,
        executor: AgentExecutor,
        settings: RuntimeSettings | None = None,
        guardrail: GuardrailHandle | None = None,
        runtime_node: str | None = None,
        agent_id: str | None = None,
        on_state_change: StateChangeListener | None = None,
        retry_policy: RetryTimingPolicy | None = None,
        max_concurrent: int = 1,
    ) -> None:
        self._plan = plan
        self._workflow_id = workflow_id
        self._bus = bus
        self._llm = llm
        self._executor = executor
        self._settings = settings or RuntimeSettings()
        self._guardrail = guardrail or NoOpGuardrail()
        self._runtime_node = runtime_node or self._settings.runtime_id
        self._agent_id = agent_id or new_agent_id()
        self._on_state_change = on_state_change
        self._retry_policy = retry_policy or RetryTimingPolicy(
            base_ms=self._settings.retry_base_ms,
            factor=self._settings.retry_factor,
            max_ms=self._settings.retry_max_ms,
            jitter=self._settings.retry_jitter,
        )

        # Constructed lazily in run()
        self._snapshot = initial_snapshot(
            agent_id=self._agent_id,
            template_key=plan.template_key,
            workflow_id=workflow_id,
            description=plan.description,
            max_retries=self._settings.max_handler_retries,
        )
        self._dedup = DedupStore(
            window_ms=self._settings.dedup_window_ms,
        )
        self._publish_pipeline = PublishPipeline(
            bus=bus,
            agent_id=self._agent_id,
            agent_role=_role_from_str(plan.role),
            runtime_node=self._runtime_node,
            workflow_id=workflow_id,
            publish_whitelist=set(plan.publish_topics),
        )
        self._subscriber: Subscriber | None = None
        self._subscribers: list[Subscriber] = []
        self._subscriber_tasks: list[asyncio.Task[None]] = []
        self._fanin_queue: asyncio.Queue[tuple[Subscriber, "DeliveredMessage"]] = (
            asyncio.Queue(maxsize=self._settings.fanin_queue_size)
        )
        self._cancel_event = asyncio.Event()
        self._running = False

        # Phase 2.5 optimization: in-flight Semaphore.
        # When ``max_concurrent > 1`` the dispatch loop schedules
        # _process_message via asyncio.create_task gated by this
        # semaphore — letting LLM IO overlap across messages on the
        # SAME instance. Default 1 preserves prior strict-serial
        # semantics (one event at a time per instance).
        self._max_concurrent = max(1, max_concurrent)
        self._inflight_sem: asyncio.Semaphore | None = (
            asyncio.Semaphore(self._max_concurrent)
            if self._max_concurrent > 1 else None
        )
        # Track outstanding message tasks so cancel() can drain them.
        self._inflight_tasks: set[asyncio.Task[None]] = set()

        # ── Aggregator (fan-in) state ──
        # When ``plan.aggregate`` is set, dispatch is gated: we buffer
        # incoming envelopes per run_id (latest envelope per topic),
        # and only invoke the handler once threshold + required
        # criteria are satisfied. The buffer is then cleared for that
        # run_id so a subsequent loop (rare) can refill.
        self._aggregate_cfg: dict | None = plan.aggregate
        if self._aggregate_cfg is not None:
            # Threshold of 0 means "all subscribe topics".
            cfg_thresh = int(self._aggregate_cfg.get("threshold", 0))
            self._aggregate_threshold: int = (
                cfg_thresh if cfg_thresh > 0 else len(plan.subscribe_topics)
            )
            self._aggregate_required: set[str] = set(
                self._aggregate_cfg.get("required", [])
            )
        else:
            self._aggregate_threshold = 0
            self._aggregate_required = set()
        # Per-run buffer: run_id -> {topic: latest envelope}. Guarded by
        # ``_aggregate_lock`` so concurrent messages (max_concurrent>1)
        # can't corrupt the nested dict (B18), and bounded by TTL +
        # max-buckets so orphaned runs (a required topic that never
        # arrives) can't leak memory forever (B19).
        self._aggregate_buffer: dict[str, dict[str, Envelope]] = {}
        self._aggregate_seen_at: dict[str, datetime] = {}
        self._aggregate_lock = asyncio.Lock()
        self._aggregate_ttl_ms: int = self._settings.aggregate_buffer_ttl_ms
        self._aggregate_max_buckets: int = self._settings.aggregate_max_buckets

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    @property
    def agent_id(self) -> str:
        return self._agent_id

    @property
    def template_key(self) -> str:
        return self._plan.template_key

    @property
    def state(self) -> AgentSnapshot:
        return self._snapshot

    async def run(self) -> None:
        """Subscribe and dispatch events until ``cancel()`` is called."""
        if self._running:
            raise RuntimeError("AgentInstance already running")
        self._running = True
        try:
            await self._transit(FSMTransition.ENV_CHECK_PASS, reason=None)
            await self._subscribe()
            await self._dispatch_loop()
        finally:
            await self._drain_and_close()
            self._running = False

    async def cancel(self) -> None:
        """Request graceful shutdown. Safe to call from any coroutine.

        Sets the cancel event AND closes the subscriber so the
        ``messages()`` async iterator unblocks immediately — without
        this the dispatch loop would only check ``_cancel_event``
        after receiving the next real message, which during idle
        periods means waiting for the worker drain timeout (~30s).
        """
        self._cancel_event.set()
        if self._subscriber is not None:
            try:
                await self._subscriber.close()
            except Exception:  # noqa: BLE001
                log.exception("agent.subscriber_close_failed", agent_id=self._agent_id)

    # ------------------------------------------------------------
    # Internals — subscription
    # ------------------------------------------------------------

    async def _subscribe(self) -> None:
        # All subscriptions for this template share one consumer
        # group (Doc02 §4.1). For multiple subscription patterns we
        # subscribe to each and merge — but in v1 our IR already
        # collapses into a single consumer group; we just register
        # each topic.
        group = self._plan.consumer_group or derive_consumer_group(
            self._workflow_id, self._plan.template_key,
        )
        # v1 simplification: subscribe to the first pattern only.
        # Multi-pattern subscriptions are tracked for Phase 2 (Doc02 §3.2).
        if not self._plan.subscribe_topics:
            log.warning(
                "agent.no_subscriptions",
                agent_id=self._agent_id,
                template_key=self.template_key,
            )
            return

        # Multi-subscribe: open one Subscriber per topic. A per-instance
        # fan-in queue collects DeliveredMessages from all of them; the
        # dispatch loop consumes from that single queue.
        for topic_pattern in self._plan.subscribe_topics:
            spec = SubscribeSpec(
                topic_pattern=topic_pattern,
                group=group,
                starting_position="committed",
                batch_size=self._settings.max_inflight,
                max_inflight=self._settings.max_inflight,
                visibility_timeout_ms=self._settings.handler_timeout_ms,
            )
            sub = await self._bus.subscribe(spec)
            self._subscribers.append(sub)
        # Back-compat: keep ``self._subscriber`` set to the first one
        # so existing call sites that ack via ``self._subscriber`` (e.g.
        # gate-drop ack) still work — those paths currently only fire
        # in the single-topic case.
        self._subscriber = self._subscribers[0] if self._subscribers else None

    # ------------------------------------------------------------
    # Internals — dispatch loop
    # ------------------------------------------------------------

    async def _dispatch_loop(self) -> None:
        if not self._subscribers:
            await self._wait_for_cancel()
            return

        # Spawn one consumer task per subscriber that pushes onto the
        # fan-in queue.
        async def _drain_one(sub: Subscriber) -> None:
            try:
                async for msg in sub.messages():
                    await self._fanin_queue.put((sub, msg))
            except asyncio.CancelledError:
                pass
            except Exception:  # noqa: BLE001
                log.exception(
                    "agent.subscriber_drain_failed",
                    agent_id=self._agent_id,
                )

        for sub in self._subscribers:
            self._subscriber_tasks.append(asyncio.create_task(_drain_one(sub)))

        # Read from the fan-in queue and dispatch as before.
        while not self._cancel_event.is_set():
            try:
                sub, msg = await asyncio.wait_for(
                    self._fanin_queue.get(), timeout=0.5,
                )
            except asyncio.TimeoutError:
                continue
            # B17: the owning subscriber is threaded *explicitly* through
            # the whole processing chain (not stashed on self._subscriber),
            # so concurrent in-flight tasks under max_concurrent>1 can no
            # longer overwrite each other's ack/nack target.
            if self._inflight_sem is None:
                # Strict-serial path (preserves prior behavior).
                try:
                    await self._process_message(msg, sub)
                except asyncio.CancelledError:
                    raise
                except Exception:  # noqa: BLE001
                    log.exception(
                        "agent.dispatch_loop_error",
                        agent_id=self._agent_id,
                    )
            else:
                async def _wrapped(_msg: DeliveredMessage,
                                   _sub: Subscriber) -> None:
                    async with self._inflight_sem:  # type: ignore[union-attr]
                        try:
                            await self._process_message(_msg, _sub)
                        except asyncio.CancelledError:
                            raise
                        except Exception:  # noqa: BLE001
                            log.exception(
                                "agent.dispatch_loop_error",
                                agent_id=self._agent_id,
                            )
                t = asyncio.create_task(_wrapped(msg, sub))
                self._inflight_tasks.add(t)
                t.add_done_callback(self._inflight_tasks.discard)

    async def _wait_for_cancel(self) -> None:
        try:
            await self._cancel_event.wait()
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------
    # Internals — single-message processing
    # ------------------------------------------------------------

    async def _process_message(
        self, msg: DeliveredMessage, sub: Subscriber,
    ) -> None:
        envelope = msg.envelope

        # 0) Workflow isolation. Two deployed workflows that define the
        # same agent / topic (e.g. both subscribe to ``agent.researcher.in``)
        # each get an independent consumer group on the *same* bus stream,
        # so every message is delivered to both. Drop envelopes stamped with
        # a *different* workflow's id so a sibling workflow's run can't drive
        # — or exhaust the guardrail of — this instance. Empty / "external"
        # ids are global ingestion events and always pass.
        wid = envelope.workflow_id
        if wid not in _GLOBAL_WORKFLOW_IDS and wid != self._workflow_id:
            log.debug(
                "agent.skip.foreign_workflow",
                agent_id=self._agent_id,
                template_key=self._plan.template_key,
                own_workflow_id=self._workflow_id,
                envelope_workflow_id=wid,
                event_id=envelope.event_id,
            )
            await self._bus.ack(sub, msg)
            return

        # 1) FSM: Active → Processing (on event_arrived)
        await self._transit(
            FSMTransition.EVENT_ARRIVED, event_id=envelope.event_id,
        )

        # 2) Run gating (4-gate pipeline). The reservation is carried as a
        # local (not on self) so concurrent messages can't clobber it.
        gating_outcome, reservation = await self._run_gating(envelope)
        if gating_outcome is not None:  # not passed
            verdict, reason = gating_outcome
            metrics.gating_block_total.labels(
                gate=verdict,
                template_key=self._plan.template_key,
                reason=reason[:64] if reason else "",
            ).inc()
            await self._handle_gating_drop(
                msg, sub=sub, verdict=verdict, reason=reason,
            )
            return

        # 2.5) Aggregator gate — buffer until ready, then invoke once.
        if self._aggregate_cfg is not None:
            ready_envelope = await self._aggregate_admit(envelope)
            if ready_envelope is None:
                # Still waiting for more upstream events — ack and
                # return so the message isn't redelivered.
                await self._bus.ack(sub, msg)
                # Snap FSM back to Active (we transitioned to Processing on
                # event_arrived but no handler will run for this event).
                await self._transit(
                    FSMTransition.HANDLER_OK,
                    event_id=envelope.event_id,
                    reason="aggregator buffered",
                )
                return
            # Replace the envelope we hand to the handler with the
            # synthetic merged one (same identity but enriched payload).
            envelope = ready_envelope

        # 3) Build context + invoke handler with retry budget
        try:
            await self._invoke_handler_with_retry(envelope, msg, sub, reservation)
            metrics.agent_event_processed_total.labels(
                template_key=self._plan.template_key, result="ok",
            ).inc()
        except Exception:
            metrics.agent_event_processed_total.labels(
                template_key=self._plan.template_key, result="failed",
            ).inc()
            # _invoke_handler_with_retry handles all classification
            # and FSM transitions internally. If we land here, it
            # already logged + transitioned; we just continue.
            log.debug(
                "agent.handler.exception_already_handled",
                agent_id=self._agent_id,
                event_id=envelope.event_id,
            )

    async def _run_gating(
        self, envelope: Envelope,
    ) -> tuple[tuple[str, str] | None, Reservation | None]:
        """Run the 4-gate pipeline.

        Returns ``(drop_outcome, reservation)``:
        * pass  → ``(None, reservation)`` — reservation may be None.
        * drop  → ``((verdict, reason), None)``.

        The reservation is returned (not stashed on ``self``) so
        concurrent in-flight messages don't clobber each other (B17).
        """
        from agentkit.runtime.gating import run_gating  # noqa: PLC0415

        result = await run_gating(
            envelope,
            agent_tags=self._plan.tags,
            schema_in=None,  # AgentPlan currently doesn't carry schema_in directly
            guardrail=self._guardrail,
            agent_id=self._agent_id,
            est_tokens=_DEFAULT_PRECHECK_TOKENS,
            dedup_store=self._dedup,
            template_key=self.template_key,
        )
        if result.passed:
            return None, result.reservation
        if result.is_guardrail_block:
            # Direct → Failure (no Retry).
            await self._transit(
                FSMTransition.GUARDRAIL_EXCEEDED, reason=result.reason,
            )
        return (result.verdict.value, result.reason), None

    async def _handle_gating_drop(
        self, msg: DeliveredMessage, *, sub: Subscriber, verdict: str, reason: str,
    ) -> None:
        """Ack-or-DLQ + return to Active state."""
        from agentkit.runtime.gating import GatingVerdict  # noqa: PLC0415

        if verdict == GatingVerdict.DLQ_SCHEMA.value:
            await self._bus.send_to_dlq(msg.envelope, reason=reason)
            await self._bus.ack(sub, msg)
        elif verdict == GatingVerdict.GUARDRAIL_BLOCK.value:
            # The run hit a guardrail (e.g. token/cycle quota). Emit a
            # terminal ``system.run.<id>.error`` so the Orchestrator marks
            # the Run Failed with an inspectable reason — otherwise it
            # would hang in "Running" until the visibility timeout.
            await self._emit_run_error(
                msg.envelope,
                klass=FailureClass.GUARDRAIL_EXCEEDED.value,
                reason=reason,
            )
            # FSM is already in Failure; ack so we don't redeliver
            # endlessly (the next event will hit Failure again with
            # the same outcome until quota resets).
            await self._bus.ack(sub, msg)
        else:
            # DROP_TAG / DROP_DEDUP — ack silently.
            await self._bus.ack(sub, msg)

        # If we transitioned to Failure earlier, the next event will
        # need EVENT_ARRIVED from Failure which isn't a legal
        # transition. The Orchestrator handles Failure recovery in
        # full; for v1 we simply rejoin Active so the next event can
        # be handled (this matches "Failure is a *terminal* state for
        # this event but not for the instance").
        if self._snapshot.state.value == "Failure":
            # Manual recovery for v1: clear and go back to Active.
            self._snapshot = self._snapshot.model_copy(
                update={"state": self._snapshot.state.__class__.ACTIVE},
            )

    async def _emit_run_error(
        self, envelope: Envelope, *, klass: str, reason: str,
    ) -> None:
        """Publish a synthetic ``system.run.<id>.error`` terminal marker.

        The Orchestrator's TerminalDetector watches this topic and marks
        the Run ``Failed`` with ``failure_reason=reason``. Without it a
        run that fails (handler error, or a guardrail/quota block at the
        gate) would hang in "Running" until the visibility timeout.

        Best-effort: a publish failure must not shadow the original error.
        """
        try:
            await self._bus.publish(
                build_envelope(
                    topic=f"system.run.{envelope.run_id}.error",
                    payload={
                        "agent_id":     self._agent_id,
                        "template_key": self.template_key,
                        "klass":        klass,
                        "event_id":     envelope.event_id,
                        "reason":       reason,
                    },
                    workflow_id=envelope.workflow_id,
                    run_id=envelope.run_id,
                    trace_id=envelope.trace_id,
                    causation_id=envelope.event_id,
                ),
            )
        except Exception:
            log.exception(
                "agent.terminal_marker_publish_failed",
                agent_id=self._agent_id,
                run_id=envelope.run_id,
            )

    # ------------------------------------------------------------
    # Internals — handler invocation with retry
    # ------------------------------------------------------------

    async def _invoke_handler_with_retry(
        self,
        envelope: Envelope,
        msg: DeliveredMessage,
        sub: Subscriber,
        reservation: Reservation | None,
        attempt: int = 0,
    ) -> None:
        """Run the handler, route exceptions to FSM transitions.

        ``sub`` and ``reservation`` are threaded explicitly (not read
        from ``self``) so concurrent messages stay isolated (B17).

        ``attempt`` is the per-message retry counter, threaded through
        the recursion instead of read from the shared FSM snapshot
        (B4). Under ``max_concurrent>1`` the FSM is instance-wide and
        cannot represent per-message retry progress — so the retry
        decision keys off this local counter, not ``self._snapshot``.
        """
        ctx = AgentContext(
            agent_id=self._agent_id,
            template_key=self.template_key,
            workflow_id=self._workflow_id,
            run_id=envelope.run_id,
            trace_id=envelope.trace_id,
            llm=self._llm,
            llm_binding=self._plan.llm,
            publish_pipeline=self._publish_pipeline,
            causation_id=envelope.event_id,
            agent_tags=self._plan.tags,
        )

        timeout_s = self._settings.handler_timeout_ms / 1000.0

        try:
            results = await asyncio.wait_for(
                self._executor.on_event(ctx, envelope), timeout=timeout_s,
            )
        except asyncio.TimeoutError:
            await self._handle_handler_failure(
                envelope=envelope,
                msg=msg,
                sub=sub,
                reservation=reservation,
                klass=FailureClass.RECOVERABLE,
                reason=f"handler timeout after {timeout_s:.1f}s",
                attempt=attempt,
            )
            return
        except GuardrailExceeded as e:
            await self._handle_handler_failure(
                envelope=envelope,
                msg=msg,
                sub=sub,
                reservation=reservation,
                klass=FailureClass.GUARDRAIL_EXCEEDED,
                reason=str(e),
                attempt=attempt,
            )
            return
        except Exception as e:
            klass = classify_handler_exception(e)
            await self._handle_handler_failure(
                envelope=envelope, msg=msg, sub=sub, reservation=reservation,
                klass=klass, reason=str(e), attempt=attempt,
            )
            return

        # Handler returned normally — publish result events.
        try:
            for ev in results:
                await ctx.publish(ev)
        except Exception as e:
            # A bad publish (whitelist violation, schema error) is
            # treated as fatal — same event will fail again.
            log.exception(
                "agent.publish_failed",
                agent_id=self._agent_id,
                event_id=envelope.event_id,
                error=str(e),
            )
            await self._handle_handler_failure(
                envelope=envelope,
                msg=msg,
                sub=sub,
                reservation=reservation,
                klass=FailureClass.FATAL,
                reason=f"publish failed: {e}",
                attempt=attempt,
            )
            return

        # Success — consume guardrail reservation, ack, transition.
        if reservation is not None:
            await self._guardrail.consume(
                reservation,
                actual_tokens=0,  # Per-call token tracking lives in LLM Gateway;
                                  # publish itself doesn't burn agent tokens.
                actual_cost=0.0,
            )

        try:
            await self._bus.ack(sub, msg)
        except BusError:
            log.exception("agent.ack_failed", agent_id=self._agent_id)
        await self._transit(FSMTransition.HANDLER_OK)

    async def _handle_handler_failure(
        self,
        *,
        envelope: Envelope,
        msg: DeliveredMessage,
        sub: Subscriber,
        reservation: Reservation | None,
        klass: FailureClass,
        reason: str,
        attempt: int,
    ) -> None:
        """Drive the FSM + bus ack/nack based on failure class.

        The retry decision is made from the *per-message-local*
        ``attempt`` counter and the configured retry budget — NOT from
        the shared FSM snapshot (B4). Under ``max_concurrent>1`` the
        FSM is instance-wide and concurrent messages would otherwise
        clobber each other's ``retry_count`` / ``Retry`` state, causing
        recoverable errors to skip retries and go straight to the DLQ.
        FSM transitions below are best-effort observability only.
        """
        # Release the reservation since we're not consuming.
        if reservation is not None:
            await self._guardrail.release(reservation, reason=f"handler_{klass.value}")

        log.warning(
            "agent.handler.failed",
            agent_id=self._agent_id,
            event_id=envelope.event_id,
            klass=klass.value,
            reason=reason,
            attempt=attempt,
        )

        # Local retry decision (B4): recoverable + budget remaining.
        max_retries = self._settings.max_handler_retries
        should_retry = klass is FailureClass.RECOVERABLE and attempt < max_retries
        wait_ms = (
            compute_next_retry_ms(attempt=attempt, policy=self._retry_policy)
            if should_retry else 0
        )

        transition = transition_for_failure(klass)
        await self._transit(
            transition,
            reason=reason,
            next_retry_at=_time_in_future_ms(wait_ms) if should_retry else None,
        )

        # Recoverable with budget left: back off and re-process the same
        # in-flight message (bus is NOT ack'd yet) with attempt+1.
        if should_retry:
            await self._sleep_backoff(wait_ms)
            await self._transit(FSMTransition.RETRY_DUE)
            await self._invoke_handler_with_retry(
                envelope, msg, sub, reservation, attempt=attempt + 1,
            )
            return

        # ── TERMINAL FAILURE for this event ──
        # We're going to Failure (FATAL / GUARDRAIL_EXCEEDED / or
        # RECOVERABLE with retries exhausted). Without a published
        # signal the Orchestrator's TerminalDetector has no way to
        # learn the Run failed and the Run stays "Running" forever
        # in the UI. Emit a synthetic ``system.run.<id>.error`` so
        # the run terminates with a real ``failure_reason`` the user
        # can inspect.
        await self._emit_run_error(envelope, klass=klass.value, reason=reason)

        # Failure or Down: nack to DLQ (recoverable exhausted is treated
        # as fatal-for-this-event), then ack the original.
        try:
            await self._bus.nack(
                sub,
                msg,
                requeue=False,
                reason=reason,
            )
        except BusError:
            log.exception(
                "agent.nack_failed", agent_id=self._agent_id,
                event_id=envelope.event_id,
            )

        # For v1, after a Failure we recover the FSM back to Active so the
        # instance keeps consuming subsequent events. (Doc05 Orchestrator
        # later applies fancier alt_template / role-down policy.)
        if self._snapshot.state.value == "Failure":
            self._snapshot = self._snapshot.model_copy(
                update={"state": self._snapshot.state.__class__.ACTIVE},
            )

    async def _sleep_backoff(self, wait_ms: int) -> None:
        """Sleep ``wait_ms`` before a retry, waking early on cancel.

        Takes the backoff as an explicit local value (B4) rather than
        reading ``next_retry_at`` off the shared FSM snapshot, which is
        unreliable under ``max_concurrent>1``.
        """
        if wait_ms <= 0:
            return
        try:
            await asyncio.wait_for(
                self._cancel_event.wait(), timeout=wait_ms / 1000.0,
            )
        except asyncio.TimeoutError:
            pass

    # ------------------------------------------------------------
    # Internals — drain/close
    # ------------------------------------------------------------

    async def _drain_and_close(self) -> None:
        log.info(
            "agent.draining",
            agent_id=self._agent_id,
            template_key=self.template_key,
        )
        if self._subscriber is not None:
            try:
                await self._subscriber.close()
            except Exception:  # noqa: BLE001
                log.exception("agent.subscriber_close_failed", agent_id=self._agent_id)
        for sub in self._subscribers:
            try:
                await sub.close()
            except Exception:  # noqa: BLE001
                log.exception("agent.subscriber_close_failed", agent_id=self._agent_id)
        self._subscribers = []
        for t in self._subscriber_tasks:
            t.cancel()
        # Drain task cancellations.
        if self._subscriber_tasks:
            await asyncio.gather(
                *self._subscriber_tasks, return_exceptions=True,
            )
        self._subscriber_tasks = []
        self._fanin_queue = asyncio.Queue(maxsize=self._settings.fanin_queue_size)

    # ------------------------------------------------------------
    # Internals — FSM transition with listener notification
    # ------------------------------------------------------------

    async def _transit(self, transition: FSMTransition, **kwargs) -> None:
        prev = self._snapshot
        try:
            self._snapshot = apply_transition(prev, transition, **kwargs)
        except Exception as e:
            # Under ``max_concurrent>1`` the FSM is instance-wide while
            # messages are processed concurrently, so transitions like
            # EVENT_ARRIVED-from-Processing are *expected* and benign
            # (the per-message control flow no longer depends on the
            # snapshot — B3/B4). Downgrade from error+traceback to a
            # debug line so concurrency doesn't spam the logs.
            log.debug(
                "agent.fsm.invalid_transition",
                agent_id=self._agent_id,
                from_state=prev.state.value,
                transition=transition.value,
                error=str(e),
            )
            return

        log.debug(
            "agent.fsm.transit",
            agent_id=self._agent_id,
            from_state=prev.state.value,
            to_state=self._snapshot.state.value,
            reason=self._snapshot.state_meta.reason,
        )
        if self._on_state_change is not None:
            try:
                await self._on_state_change(prev, self._snapshot, transition)
            except Exception:
                log.exception("agent.fsm.listener_failed")


    # ------------------------------------------------------------
    # Internals — aggregator buffer
    # ------------------------------------------------------------

    async def _aggregate_admit(self, envelope: Envelope) -> Envelope | None:
        """Buffer ``envelope`` per run_id. Return a merged envelope if
        threshold + required are satisfied, else ``None``.

        The merged envelope carries the latest envelope's metadata
        (event_id, ts, run_id, trace_id) and a payload that is the
        union of all buffered payloads — plus an ``_inputs`` map
        keyed by topic for explicit per-source access.

        Guarded by ``_aggregate_lock`` (B18) and bounded by TTL +
        max-buckets (B19) so concurrent messages stay consistent and
        orphaned run buffers can't leak.
        """
        async with self._aggregate_lock:
            run_id = envelope.run_id or "_no_run"
            bucket = self._aggregate_buffer.setdefault(run_id, {})
            self._aggregate_seen_at[run_id] = utcnow()
            # Latest envelope on each topic wins (replace).
            bucket[envelope.topic] = envelope

            # Evict stale/overflow buckets — but never the one we just
            # touched (it's the freshest / current work).
            self._evict_stale_buckets(keep=run_id)

            # Check gate.
            if not self._aggregate_required.issubset(bucket.keys()):
                return None
            if len(bucket) < self._aggregate_threshold:
                return None

            # Ready — build merged envelope and clear bucket.
            merged_payload: dict[str, Any] = {}
            inputs_map: dict[str, dict[str, Any]] = {}
            # Iteration order = insertion order (Python 3.7+ dict guarantee).
            # Latest envelope on each topic wins on key conflict — natural,
            # since payloads typically accumulate fields downstream.
            for topic, env in bucket.items():
                merged_payload.update(env.payload)
                inputs_map[topic] = dict(env.payload)
            merged_payload["_inputs"] = inputs_map

            # Free the bucket so a subsequent round on the same run_id
            # can refill (rare, but keep it correct).
            del self._aggregate_buffer[run_id]
            self._aggregate_seen_at.pop(run_id, None)

        # Build a *new* envelope using the latest one as a template.
        return envelope.model_copy(update={"payload": merged_payload})

    def _evict_stale_buckets(self, *, keep: str | None = None) -> None:
        """Drop aggregator buckets that are too old or over the cap (B19).

        Caller must hold ``_aggregate_lock``. Never evicts ``keep`` (the
        run_id just touched). Prevents unbounded growth when a required
        topic never arrives for some run_id.
        """
        buf = self._aggregate_buffer
        if not buf:
            return
        # ① TTL eviction (insertion-order dict; oldest first).
        if self._aggregate_ttl_ms > 0:
            now = utcnow()
            ttl_s = self._aggregate_ttl_ms / 1000.0
            stale = [
                rid for rid, ts in self._aggregate_seen_at.items()
                if rid != keep and (now - ts).total_seconds() > ttl_s
            ]
            for rid in stale:
                buf.pop(rid, None)
                self._aggregate_seen_at.pop(rid, None)
            if stale:
                log.warning(
                    "agent.aggregate.ttl_evicted",
                    agent_id=self._agent_id, evicted=len(stale),
                )
        # ② Hard cap — evict oldest (insertion order) beyond the cap.
        overflow = len(buf) - self._aggregate_max_buckets
        if overflow > 0:
            evictable = [rid for rid in buf if rid != keep]
            for rid in evictable[:overflow]:
                buf.pop(rid, None)
                self._aggregate_seen_at.pop(rid, None)
            log.warning(
                "agent.aggregate.cap_evicted",
                agent_id=self._agent_id, evicted=overflow,
                cap=self._aggregate_max_buckets,
            )


# ---- helpers ----


def _time_in_future_ms(ms: int):
    from datetime import timedelta  # noqa: PLC0415

    return utcnow() + timedelta(milliseconds=ms)


def _role_from_str(role: str):
    """Coerce ``AgentPlan.role`` (a str) back to the Role enum used by
    PublishPipeline. Falls back to THINKING for unknown values to keep
    the runtime forward-compatible if a Role is added in IR before
    propagating here.
    """
    from agentkit.models.enums import Role  # noqa: PLC0415

    try:
        return Role(role)
    except ValueError:
        return Role.THINKING
