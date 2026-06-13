"""AgentWorker — process-level manager for many :class:`AgentInstance` objects.

Given a :class:`RuntimePlan` (Compiler output) and a
:class:`HandlerRegistry`, the Worker boots one or more
:class:`AgentInstance` per Template (matching ``replica_min``) and
runs them concurrently in this process.

Phase 1 scope:

* All instances run in a single asyncio event loop.
* The worker honors ``replica_min`` only — Autoscaler integration
  (Doc05) handles dynamic ``replica_max`` scaling.
* Filtering by ``RuntimeSettings.role_filter`` lets one process serve
  a subset of Roles.
"""

from __future__ import annotations

import asyncio

from agentkit.bus.interface import EventBus
from agentkit.common.logging import get_logger
from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.guardrail_iface import GuardrailHandle
from agentkit.runtime.config import RuntimeSettings
from agentkit.runtime.executor import AgentExecutor, HandlerRegistry
from agentkit.runtime.instance import AgentInstance
from agentkit.workflow.plan import AgentPlan, RuntimePlan

log = get_logger(__name__)


class AgentWorker:
    """One worker process / coroutine that runs multiple AgentInstances."""

    def __init__(
        self,
        *,
        plan: RuntimePlan,
        bus: EventBus,
        llm: LLMGatewayClient,
        handlers: HandlerRegistry | None = None,
        settings: RuntimeSettings | None = None,
        guardrail: GuardrailHandle | None = None,
        max_concurrent_per_instance: int | None = None,
    ) -> None:
        self._plan = plan
        self._bus = bus
        self._llm = llm
        # Explicit None check — HandlerRegistry has ``__len__`` so an
        # empty registry is falsy under ``or``.
        self._handlers = (
            handlers if handlers is not None else HandlerRegistry.global_default()
        )
        self._settings = settings or RuntimeSettings()
        self._guardrail = guardrail
        self._instances: list[AgentInstance] = []
        self._tasks: list[asyncio.Task[None]] = []
        self._running = False
        # Resolve per-instance concurrency: explicit arg wins, else the
        # runtime default (B3). ``None`` means "use the configured default".
        self._max_concurrent_per_instance = (
            max_concurrent_per_instance
            if max_concurrent_per_instance is not None
            else self._settings.default_max_concurrent
        )

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        """Boot one :class:`AgentInstance` per template in the plan.

        Honors :class:`RuntimeSettings.role_filter`: skips Templates
        whose Role isn't included in the filter.
        """
        if self._running:
            raise RuntimeError("AgentWorker already running")

        role_filter = self._settings.role_filter

        for template_key, agent_plan in self._plan.agents.items():
            if role_filter is not None and agent_plan.role not in role_filter:
                log.debug(
                    "worker.skip_role",
                    template_key=template_key,
                    role=agent_plan.role,
                    role_filter=sorted(role_filter),
                )
                continue

            executor = self._resolve_executor(template_key)
            if executor is None:
                log.warning(
                    "worker.no_handler",
                    template_key=template_key,
                    workflow_id=self._plan.workflow_id,
                )
                continue

            self._spawn_replicas(template_key, agent_plan, executor)

        if not self._instances:
            log.warning(
                "worker.no_instances_started",
                workflow_id=self._plan.workflow_id,
            )

        self._running = True
        log.info(
            "worker.started",
            workflow_id=self._plan.workflow_id,
            instances=len(self._instances),
        )

    async def stop(self) -> None:
        """Gracefully cancel every instance and await drain."""
        if not self._running:
            return

        log.info("worker.stopping", instances=len(self._instances))
        for inst in self._instances:
            try:
                await inst.cancel()
            except Exception:
                log.exception(
                    "worker.cancel_failed", agent_id=inst.agent_id,
                )

        # Wait for tasks to finish; bound by drain timeout.
        if self._tasks:
            timeout_s = self._settings.drain_timeout_ms / 1000.0
            done, pending = await asyncio.wait(
                self._tasks, timeout=timeout_s,
                return_when=asyncio.ALL_COMPLETED,
            )
            for t in pending:
                t.cancel()
            if pending:
                log.warning(
                    "worker.drain_timeout",
                    pending=len(pending),
                    timeout_ms=self._settings.drain_timeout_ms,
                )

        self._running = False
        self._instances.clear()
        self._tasks.clear()

    async def run_forever(self) -> None:
        """Convenience: ``start()`` and await all tasks (until cancelled)."""
        await self.start()
        if not self._tasks:
            return
        try:
            await asyncio.gather(*self._tasks)
        except asyncio.CancelledError:
            await self.stop()
            raise

    # ------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------

    @property
    def instances(self) -> list[AgentInstance]:
        return list(self._instances)

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    def _resolve_executor(self, template_key: str) -> AgentExecutor | None:
        return self._handlers.get(
            workflow_id=self._plan.workflow_id, template_key=template_key,
        )

    def _spawn_replicas(
        self,
        template_key: str,
        agent_plan: AgentPlan,
        executor: AgentExecutor,
    ) -> None:
        replicas = max(1, agent_plan.replica_min)
        for _ in range(replicas):
            inst = AgentInstance(
                plan=agent_plan,
                workflow_id=self._plan.workflow_id,
                bus=self._bus,
                llm=self._llm,
                executor=executor,
                settings=self._settings,
                guardrail=self._guardrail,
                max_concurrent=self._max_concurrent_per_instance,
            )
            self._instances.append(inst)
            self._tasks.append(asyncio.create_task(inst.run()))
        log.info(
            "worker.template_started",
            template_key=template_key,
            replicas=replicas,
            max_concurrent=self._max_concurrent_per_instance,
        )
