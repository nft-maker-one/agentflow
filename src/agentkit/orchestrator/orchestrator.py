"""The :class:`Orchestrator` — control-plane Run lifecycle.

Phase 1 responsibilities:

* **deploy(ir)** — register a Workflow IR; spin up SwitchRouter +
  TerminalDetector to monitor its routing.
* **create_run(workflow_id, input)** — allocate a Run, publish the
  initial event to the start-edge's ``via`` topic, return a handle.
* **wait_for_completion(run_id, timeout)** — block until the Run
  reaches a terminal status (Succeeded / Failed / Cancelled).
* **cancel_run(run_id)** — mark the Run as Cancelled (handlers in
  flight will continue, but no further routing happens).
* **get_run / list_runs** — introspection.

Out of scope for Phase 1 (per design notes elsewhere):

* HumanNode timeouts / Aggregator session timeouts (Doc04 §4)
* Run-Overlay application (Doc04 §8) — needs a separate Intent
  Resolver.
* Autoscaler (Doc05 §9) — needs Bus consumer_lag metric integration.
* Multi-Orchestrator + leader election (Doc05 §11).
* Heartbeat-based liveness detection from Runtime (Doc05 §10).

These come in later iterations; the surface declared here is
forward-compatible with all of them.
"""

from __future__ import annotations

import asyncio
import inspect
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from agentkit.guardrail.inprocess import InProcessGuardrail
    from agentkit.llm.guardrail_iface import GuardrailHandle

from agentkit.bus.builder import build_envelope
from agentkit.bus.interface import EventBus
from agentkit.common.logging import get_logger
from agentkit.common.time import utcnow
from agentkit.guardrail.resolver import resolve_guardrail_context
from agentkit.models.enums import RunStatus
from agentkit.observability import metrics
from agentkit.orchestrator.errors import RunNotFound, UnknownWorkflow
from agentkit.orchestrator.routing import SwitchRouter, TerminalDetector
from agentkit.orchestrator.run import (
    BranchEvent,
    Run,
    new_run,
)
from agentkit.orchestrator.store import InMemoryRunStore, RunStore
from agentkit.workflow.ir import (
    END_NODE,
    ERROR_NODE,
    START_NODE,
    WorkflowIR,
)

log = get_logger(__name__)

# Branch-event batching — Doc05/B16. Buffer per-run BranchEvents in
# memory and flush them to the store on a size-or-time trigger,
# whichever comes first. This collapses N store round-trips
# (get→mutate→update per event) into a single read-modify-write per
# run per flush window, which matters a lot for the Postgres store's
# small connection pool (min=2/max=10) under a high branch rate.
#
# Used as defaults when the orchestrator isn't configured with
# explicit overrides (see ``Orchestrator.__init__``).
BRANCH_FLUSH_INTERVAL_MS = 100
BRANCH_FLUSH_MAX = 100


def _ir_snapshot(ir: WorkflowIR) -> dict:
    """Serialize the compiled IR for storage on a Run, so the run detail
    view can render the workflow topology/agents AS THEY WERE at run time
    (even after the live workflow is edited). Best-effort: never fail run
    creation over a snapshot."""
    try:
        return ir.model_dump(by_alias=True, exclude_none=True, mode="json")
    except Exception:  # noqa: BLE001
        log.exception("orch.ir_snapshot_failed", workflow_id=ir.id)
        return {}


class Orchestrator:
    """Control-plane Run lifecycle manager.

    One instance manages multiple Workflows. Construction is sync;
    ``start()``/``stop()`` boot/drain the routers.
    """

    def __init__(
        self,
        *,
        bus: EventBus,
        store: RunStore | None = None,
        guardrail: "InProcessGuardrail | GuardrailHandle | None" = None,
        settings: Any | None = None,
        branch_flush_interval_ms: int | None = None,
        branch_flush_max: int | None = None,
    ) -> None:
        self._bus = bus
        self._guardrail = guardrail
        # NOTE: explicit ``is None`` check — InMemoryRunStore defines
        # ``__len__``, so an empty store evaluates falsy under ``or``
        # and gets silently replaced by a fresh one. Same trap that
        # bit HandlerRegistry + SDK earlier; keep this pattern explicit.
        self._store: RunStore = (
            store if store is not None else InMemoryRunStore()
        )
        # workflow_id → IR
        self._workflows: dict[str, WorkflowIR] = {}
        # workflow_id → routers (one set per IR)
        self._switch_routers: dict[str, SwitchRouter] = {}
        self._terminal_detectors: dict[str, TerminalDetector] = {}
        # run_id → completion event (set when Run reaches terminal status)
        self._completion: dict[str, asyncio.Event] = {}
        self._running = False

        # ------------------------------------------------------------
        # Branch-event batching (B16) — buffer + size-or-time flush.
        #
        # Resolution order for interval/max: explicit constructor kwarg
        # → ``settings`` object attribute (if the host wires one up) →
        # module-level default. This keeps the feature configurable
        # without forcing every caller to pass a settings object.
        # ------------------------------------------------------------
        self._branch_flush_interval_ms = (
            branch_flush_interval_ms
            if branch_flush_interval_ms is not None
            else getattr(settings, "branch_flush_interval_ms", BRANCH_FLUSH_INTERVAL_MS)
        )
        self._branch_flush_max = (
            branch_flush_max
            if branch_flush_max is not None
            else getattr(settings, "branch_flush_max", BRANCH_FLUSH_MAX)
        )
        # run_id → buffered events awaiting flush (ordering preserved
        # via list append/pop-all-under-lock).
        self._branch_buffers: dict[str, list[BranchEvent]] = {}
        self._branch_lock = asyncio.Lock()
        self._branch_flush_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        """Begin orchestration. Must be called before ``deploy()``."""
        if self._running:
            return
        self._running = True
        self._branch_flush_task = asyncio.create_task(self._branch_flush_loop())
        log.info("orch.start")

    async def stop(self) -> None:
        """Stop all routers and clear completion events."""
        if not self._running:
            return
        for r in list(self._switch_routers.values()):
            await r.stop()
        for d in list(self._terminal_detectors.values()):
            await d.stop()
        self._switch_routers.clear()
        self._terminal_detectors.clear()

        # CRITICAL — drain any buffered branch events before we stop,
        # otherwise the persisted branch log would be missing the most
        # recent decisions for runs that were active at shutdown time.
        await self._flush_all_branch_events()
        await self._stop_branch_flush_task()

        # Wake any pending waiters so they can observe the shutdown.
        for ev in self._completion.values():
            ev.set()
        self._running = False
        log.info("orch.stop")

    async def _stop_branch_flush_task(self) -> None:
        """Cancel and await the background flush task — clean shutdown,
        no "task was destroyed but it is pending" warnings."""
        task, self._branch_flush_task = self._branch_flush_task, None
        if task is None:
            return
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass

    # ------------------------------------------------------------
    # Deployment
    # ------------------------------------------------------------

    async def deploy(self, ir: WorkflowIR) -> None:
        """Register a Workflow IR and start routing tasks for it."""
        if not self._running:
            await self.start()

        if ir.id in self._workflows:
            # Re-deploy: shut down old routers first.
            await self._teardown(ir.id)

        self._workflows[ir.id] = ir

        # Launch SwitchRouter for this IR.
        switch = SwitchRouter(
            bus=self._bus, ir=ir, on_branch=self._record_branch_event,
        )
        await switch.start()
        self._switch_routers[ir.id] = switch

        # Launch TerminalDetector for this IR.
        detector = TerminalDetector(
            bus=self._bus, ir=ir, on_terminal=self._on_terminal,
        )
        await detector.start()
        self._terminal_detectors[ir.id] = detector

        log.info(
            "orch.deploy",
            workflow_id=ir.id,
            ir_hash=ir.meta.ir_hash,
            switch_triggers=switch.trigger_topics,
            terminal_end_topics=detector.end_topics,
            terminal_error_topics=detector.error_topics,
        )

    async def undeploy(self, workflow_id: str) -> None:
        """Stop routers for ``workflow_id`` and drop the IR."""
        await self._teardown(workflow_id)
        self._workflows.pop(workflow_id, None)
        log.info("orch.undeploy", workflow_id=workflow_id)

    # ------------------------------------------------------------
    # Run lifecycle
    # ------------------------------------------------------------

    async def create_run(
        self,
        *,
        workflow_id: str,
        input: dict[str, Any] | None = None,
        trigger: str = "api",
    ) -> Run:
        """Create a Run and publish the initial event.

        ``trigger`` is informational (label on ``run_started_total``).
        """
        ir = self._workflows.get(workflow_id)
        if ir is None:
            raise UnknownWorkflow(
                f"workflow {workflow_id!r} is not deployed",
            )

        run = new_run(
            workflow_id=workflow_id,
            workflow_version=ir.version,
            input=input or {},
        )
        run.workflow_snapshot = _ir_snapshot(ir)
        await self._store.put(run)
        self._completion[run.run_id] = asyncio.Event()

        # Register quota context with the guardrail backend so that
        # every LLM call and agent cycle in this run is tracked and
        # capped against the workflow's declared limits.
        await self._register_run_quota(run, ir)

        # Find the start edge. Phase 1 supports only one ``__start__``
        # edge; if multiple are declared, we fan out to all.
        start_edges = ir.edges_from(START_NODE)
        if not start_edges:
            run.status = RunStatus.FAILED
            run.failure_reason = "no edge originates from __start__"
            run.ended_at = utcnow()
            await self._store.update(run)
            self._completion[run.run_id].set()
            return run

        run.status = RunStatus.RUNNING
        await self._store.update(run)

        # Publish the initial event(s) — one per *distinct* start topic.
        #
        # Multiple start edges may share the same ``via`` topic (e.g.
        # START→a, START→b, START→c all on "agent.start.in"). Publishing
        # once per edge would deliver N identical copies to every
        # subscriber of that topic, so the whole downstream DAG executes
        # N times. That latent bug was masked by serial dispatch (the Run
        # reached terminal before the duplicate copies were processed) but
        # surfaces the moment instances process events concurrently
        # (``max_concurrent>1``, B3). Publish each distinct topic exactly
        # once; still record a branch event per edge so the Run log keeps
        # the full set of start edges taken.
        published_topics: set[str] = set()
        for edge_id, edge in start_edges:
            if not edge.via:
                log.warning(
                    "orch.run.start_edge_no_via",
                    workflow_id=workflow_id,
                    edge_id=edge_id,
                )
                continue
            if edge.via not in published_topics:
                envelope = build_envelope(
                    topic=edge.via,
                    payload=run.input,
                    workflow_id=workflow_id,
                    run_id=run.run_id,
                    trace_id=run.trace_id,
                )
                await self._bus.publish(envelope)
                published_topics.add(edge.via)
            run.add_branch_event(
                BranchEvent(
                    edge_id=edge_id,
                    chosen=edge_id,
                    by="system",
                    reason="run started",
                ),
            )

        await self._store.update(run)
        # Doc10 §4.4 — count run lifecycle starts.
        metrics.run_started_total.labels(
            workflow_id=workflow_id, trigger=trigger,
        ).inc()
        log.info(
            "orch.run.created",
            run_id=run.run_id,
            workflow_id=workflow_id,
            trace_id=run.trace_id,
        )
        return run

    async def create_run_for_external(
        self,
        *,
        workflow_id: str,
        initial_input: dict[str, Any] | None = None,
        source_name: str = "external",
    ) -> Run:
        """Allocate a Run for an externally-triggered execution.

        Same as :meth:`create_run` *minus* the start-edge publish step:
        the external source is publishing its own envelope (already
        carrying the user payload) so we just need a Run object whose
        run_id can be stamped onto that envelope. The TerminalDetector
        and tap loop then track the Run end-to-end identically.
        """
        ir = self._workflows.get(workflow_id)
        if ir is None:
            raise UnknownWorkflow(
                f"workflow {workflow_id!r} is not deployed",
            )
        run = new_run(
            workflow_id=workflow_id,
            workflow_version=ir.version,
            input=initial_input or {},
        )
        run.workflow_snapshot = _ir_snapshot(ir)
        run.status = RunStatus.RUNNING
        await self._store.put(run)
        self._completion[run.run_id] = asyncio.Event()

        await self._register_run_quota(run, ir)

        log.info(
            "orch.run.created_for_external",
            run_id=run.run_id, workflow_id=workflow_id,
            source_name=source_name, trigger="external",
        )
        return run

    async def _register_run_quota(self, run: Run, ir: WorkflowIR) -> None:
        """Seed the guardrail's per-run quota ledger — backend-agnostic.

        Two guardrail backends expose *different* run-registration APIs:

        * :class:`~agentkit.guardrail.inprocess.InProcessGuardrail` —
          synchronous ``register_run(run_id, ctx)``.
        * :class:`~agentkit.guardrail.redis_backend.guardrail.RedisGuardrail`
          — async ``init_run_quota(ctx)`` (does Redis I/O).

        We detect whichever the configured backend offers so the
        Orchestrator stays decoupled from the ``--guardrail`` choice.
        Both consume the same :class:`GuardrailContext` resolved from
        the workflow's declared limits.
        """
        if self._guardrail is None:
            return

        wg = ir.guardrails
        # Per-agent ("this agent only") overrides set in the UI live on
        # each AgentTemplate.guardrail — fold them into the context keyed
        # by template_key so precheck can apply the tighter per-agent cap.
        from agentkit.guardrail.models import AgentGuardrail  # noqa: PLC0415
        agent_overrides = {
            key: AgentGuardrail(**tmpl.guardrail.model_dump())
            for key, tmpl in ir.agents.items()
            if tmpl.guardrail is not None
        }
        ctx = resolve_guardrail_context(
            run_id=run.run_id,
            workflow_id=run.workflow_id,
            workflow_version=ir.version,
            workflow_agent=wg.per_agent if wg and wg.per_agent else None,
            workflow_run=wg.per_run if wg and wg.per_run else None,
            agent_overrides=agent_overrides,
        )

        register_run = getattr(self._guardrail, "register_run", None)
        if callable(register_run):
            result = register_run(run.run_id, ctx)
            # Tolerate an async register_run too (forward-compatible).
            if inspect.isawaitable(result):
                await result
            return

        init_run_quota = getattr(self._guardrail, "init_run_quota", None)
        if callable(init_run_quota):
            await init_run_quota(ctx)
            return

        log.warning(
            "orch.guardrail.no_register_api",
            run_id=run.run_id,
            guardrail=type(self._guardrail).__name__,
        )

    async def mark_run_terminal_external(
        self, run_id: str, *, reason: str = "external_sink_delivered",
    ) -> None:
        """Mark a Run as Succeeded because an external sink received its
        envelope.

        External sinks are outside the IR — there's no ``__end__`` edge
        for the TerminalDetector to fire on, so without this an
        externally-routed Run would stay RUNNING forever. We delegate to
        the existing :meth:`_on_terminal` so the run lifecycle is
        identical to a normal IR-driven termination (idempotent if the
        run is already terminal)."""
        if not run_id:
            return
        await self._on_terminal(run_id, END_NODE, reason)

    async def cancel_run(self, run_id: str, *, reason: str = "user_cancel") -> Run:
        run = await self._store.get(run_id)
        if run.is_terminal:
            return run
        run.status = RunStatus.CANCELLED
        run.failure_reason = reason
        run.ended_at = utcnow()
        await self._store.update(run)
        ev = self._completion.get(run_id)
        if ev is not None:
            ev.set()
        log.info("orch.run.cancelled", run_id=run_id, reason=reason)
        return run

    async def get_run(self, run_id: str) -> Run:
        return await self._store.get(run_id)

    async def list_runs(self) -> list[Run]:
        return await self._store.list_active()

    async def wait_for_completion(
        self, run_id: str, *, timeout: float | None = None,
    ) -> Run:
        """Block until ``run_id`` reaches a terminal status."""
        run = await self._store.get(run_id)
        if run.is_terminal:
            return run
        ev = self._completion.get(run_id)
        if ev is None:
            # Run was created elsewhere; recover lazily.
            ev = asyncio.Event()
            self._completion[run_id] = ev
            if run.is_terminal:
                ev.set()

        try:
            await asyncio.wait_for(ev.wait(), timeout=timeout)
        except asyncio.TimeoutError as e:
            raise TimeoutError(
                f"run {run_id} did not complete within {timeout}s",
            ) from e
        return await self._store.get(run_id)

    # ------------------------------------------------------------
    # Internals — terminal callback
    # ------------------------------------------------------------

    async def _on_terminal(
        self,
        run_id: str,
        terminal_kind: str,
        reason: str | None,
        payload: dict | None = None,
    ) -> None:
        """Called by :class:`TerminalDetector` when a run hits a terminal.

        ``payload`` is the terminating envelope's payload — the output the
        agent published onto the __end__ edge. Captured as ``run.output``
        so the run's result is durable and shown in the UI."""
        # CRITICAL — flush this run's buffered branch events first so
        # the read below (and the branch log we persist) reflects every
        # decision made before the terminal, preserving ordering and
        # avoiding loss of the last buffered events.
        await self._flush_branch_events_for_run(run_id)

        try:
            run = await self._store.get(run_id)
        except RunNotFound:
            log.warning(
                "orch.terminal.unknown_run", run_id=run_id, kind=terminal_kind,
            )
            return

        if run.is_terminal:
            return  # already finalized — idempotent

        if terminal_kind == END_NODE:
            run.status = RunStatus.SUCCEEDED
            # Capture the workflow output (the payload that reached __end__).
            # Only overwrite when we actually have one — an external-sink
            # terminal or a payload-less signal leaves any prior output be.
            if payload is not None and run.output is None:
                run.output = payload
        elif terminal_kind == ERROR_NODE:
            run.status = RunStatus.FAILED
            run.failure_reason = reason or "reached __error__"
        else:  # pragma: no cover — defensive
            log.warning(
                "orch.terminal.unknown_kind",
                run_id=run_id, kind=terminal_kind,
            )
            return

        run.ended_at = utcnow()
        run.add_branch_event(
            BranchEvent(
                edge_id="__terminal__",
                chosen=terminal_kind,
                by="system",
                reason=reason,
            ),
        )
        await self._store.update(run)

        # Publish a synthetic terminal envelope so the tap loop /
        # Inbox subsystem learn about run completion without taking a
        # direct dependency on AppState.  Best-effort.
        try:
            term_topic = f"system.run.{run_id}.completed"
            term_env = build_envelope(
                topic=term_topic,
                payload={
                    "run_id": run_id,
                    "status": run.status.value,
                    "workflow_id": run.workflow_id,
                    "terminal_kind": terminal_kind,
                    "reason": reason or run.failure_reason,
                    "started_at": run.started_at.isoformat() if run.started_at else None,
                    "ended_at": run.ended_at.isoformat() if run.ended_at else None,
                },
                workflow_id=run.workflow_id,
                run_id=run_id,
                trace_id=getattr(run, "trace_id", "") or "",
            )
            await self._bus.publish(term_env)
        except Exception:  # noqa: BLE001
            log.exception("orch.terminal.publish_synthetic_failed", run_id=run_id)

        # Doc10 §4.4 — completion metrics.
        metrics.run_completed_total.labels(
            workflow_id=run.workflow_id, terminal_status=run.status.value,
        ).inc()
        if run.started_at is not None and run.ended_at is not None:
            metrics.run_duration_seconds.labels(
                workflow_id=run.workflow_id,
            ).observe((run.ended_at - run.started_at).total_seconds())

        ev = self._completion.get(run_id)
        if ev is not None:
            ev.set()
            # Phase 2.3 optimization: schedule cleanup so long-running
            # Orchestrators don't accumulate one event per terminated run.
            # We delay slightly so any concurrent wait_for_completion
            # callers awaiting on the event can observe the set() first.
            async def _gc() -> None:
                await asyncio.sleep(0.5)
                self._completion.pop(run_id, None)
            asyncio.create_task(_gc())

        log.info(
            "orch.run.terminal",
            run_id=run_id,
            status=run.status.value,
            reason=reason,
        )

    async def _record_branch_event(
        self, run_id: str, ev: BranchEvent,
    ) -> None:
        """SwitchRouter callback — buffer the BranchEvent for batched flush.

        B16: rather than doing an immediate get→mutate→update round
        trip per event (which exhausts the Postgres pool under a high
        branch rate), we enqueue into an in-memory per-run buffer. The
        background flush loop — or an immediate flush triggered by the
        size threshold here — collapses all pending events for a run
        into a single read-modify-write. The external contract is
        unchanged: callers still invoke this once per event.
        """
        # Doc10 §4.4 — count branch decisions.
        metrics.branch_decision_total.labels(
            edge_id=ev.edge_id, by=ev.by,
        ).inc()

        flush_now = False
        async with self._branch_lock:
            buf = self._branch_buffers.setdefault(run_id, [])
            buf.append(ev)
            if len(buf) >= self._branch_flush_max:
                flush_now = True

        if flush_now:
            await self._flush_branch_events_for_run(run_id)

    # ------------------------------------------------------------
    # Internals — branch-event batching (B16)
    # ------------------------------------------------------------

    async def _branch_flush_loop(self) -> None:
        """Background task: flush buffered branch events on a fixed
        cadence (size-based flushes happen inline in
        :meth:`_record_branch_event`)."""
        interval_s = self._branch_flush_interval_ms / 1000.0
        try:
            while True:
                await asyncio.sleep(interval_s)
                await self._flush_all_branch_events()
        except asyncio.CancelledError:
            pass

    async def _drain_branch_buffer(self, run_id: str) -> list[BranchEvent]:
        """Atomically pop and return all buffered events for ``run_id``.

        Returns an empty list (and leaves the buffer untouched) when
        there's nothing pending — callers use this to skip the
        get→mutate→update round trip entirely for idle runs.
        """
        async with self._branch_lock:
            buf = self._branch_buffers.get(run_id)
            if not buf:
                return []
            self._branch_buffers.pop(run_id, None)
            return buf

    async def _flush_branch_events_for_run(self, run_id: str) -> None:
        """Apply every buffered event for ``run_id`` in one
        read-modify-write, preserving original append order.

        On any failure the drained events are re-queued at the front of
        the buffer so a transient store error doesn't lose them — the
        next flush attempt (background loop, terminal, or shutdown)
        will retry.
        """
        pending = await self._drain_branch_buffer(run_id)
        if not pending:
            return

        try:
            run = await self._store.get(run_id)
        except RunNotFound:
            log.warning(
                "orch.branch.unknown_run", run_id=run_id, count=len(pending),
            )
            return
        except Exception:
            log.exception("orch.branch.flush_get_failed", run_id=run_id)
            await self._requeue_branch_events(run_id, pending)
            return

        for ev in pending:
            run.add_branch_event(ev)

        try:
            await self._store.update(run)
        except Exception:
            log.exception("orch.branch.flush_update_failed", run_id=run_id)
            await self._requeue_branch_events(run_id, pending)
            return

        log.debug(
            "orch.branch.flushed", run_id=run_id, count=len(pending),
        )

    async def _requeue_branch_events(
        self, run_id: str, events: list[BranchEvent],
    ) -> None:
        """Re-prepend ``events`` to ``run_id``'s buffer after a failed
        flush, preserving their original relative order ahead of any
        events appended in the meantime."""
        async with self._branch_lock:
            buf = self._branch_buffers.setdefault(run_id, [])
            self._branch_buffers[run_id] = events + buf

    async def _flush_all_branch_events(self) -> None:
        """Flush every run that currently has buffered branch events."""
        async with self._branch_lock:
            run_ids = list(self._branch_buffers.keys())
        for run_id in run_ids:
            await self._flush_branch_events_for_run(run_id)

    # ------------------------------------------------------------
    # Internals — teardown
    # ------------------------------------------------------------

    async def _teardown(self, workflow_id: str) -> None:
        switch = self._switch_routers.pop(workflow_id, None)
        detector = self._terminal_detectors.pop(workflow_id, None)
        if switch is not None:
            await switch.stop()
        if detector is not None:
            await detector.stop()
