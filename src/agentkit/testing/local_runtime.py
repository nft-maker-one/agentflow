"""``LocalRuntime`` — single-process Workflow runner for tests / demos.

Wires the in-process Bus, Worker, Orchestrator (+ optional Notifier
+ Guardrail) together so a user can write::

    async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
        run = await rt.run(input={"q": "hello"})
        assert run.status is RunStatus.SUCCEEDED

…instead of having to know about every internal class.

Doc09 §9.2.
"""

from __future__ import annotations

import asyncio
from collections.abc import Awaitable, Callable
from pathlib import Path
from typing import Any

from agentkit.bus.inprocess import InProcessEventBus
from agentkit.common.logging import get_logger
from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.guardrail_iface import GuardrailHandle
from agentkit.models.envelope import Envelope
from agentkit.orchestrator import InMemoryRunStore, Orchestrator, Run
from agentkit.runtime import AgentWorker, Event, HandlerRegistry
from agentkit.runtime.config import RuntimeSettings
from agentkit.runtime.context import AgentContext, PublishPipeline
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.sdk.decorators import HandlerFn, get_agent_meta
from agentkit.sdk.workflow import WorkflowDef
from agentkit.testing.mock_llm import MockLLMGateway
from agentkit.workflow import compile_from_dict, load_workflow_yaml
from agentkit.workflow.ir import WorkflowIR
from agentkit.workflow.plan import RuntimePlan

log = get_logger(__name__)


# ============================================================
# LocalRuntime
# ============================================================


class LocalRuntime:
    """In-process Workflow runner.

    Two construction modes:

    * Direct:    ``LocalRuntime(wf=workflow_def, llm=...)``
    * From YAML: ``LocalRuntime.from_yaml("path.yaml", handlers={"k": fn}, llm=...)``

    Use as an async context manager for clean shutdown::

        async with LocalRuntime(wf) as rt:
            run = await rt.run(input={"q": "hi"})
    """

    def __init__(
        self,
        wf: WorkflowDef | None = None,
        *,
        ir: WorkflowIR | None = None,
        plan: RuntimePlan | None = None,
        handlers: dict[str, HandlerFn] | None = None,
        llm: LLMGatewayClient | None = None,
        guardrail: GuardrailHandle | None = None,
        runtime_settings: RuntimeSettings | None = None,
    ) -> None:
        if wf is None and ir is None:
            raise ValueError("LocalRuntime requires either wf= or ir=")

        # Compile if we got a WorkflowDef.
        self._registry = HandlerRegistry()
        if wf is not None:
            ir, plan = wf.compile(registry=self._registry)
        elif plan is None:
            # Caller provided IR but no plan — recompile.
            ir, plan = compile_from_dict(ir.model_dump(by_alias=True, exclude_none=True))
            assert ir is not None and plan is not None

        # Otherwise (ir + plan + handlers passed directly) the user is
        # bringing their own compilation; register their handlers.
        if handlers:
            for k, fn in handlers.items():
                self._registry.register(
                    workflow_id=ir.id, template_key=k,
                    executor=_FunctionExecutor(fn), replace=True,
                )

        self._ir = ir
        self._plan = plan
        self._llm = llm or MockLLMGateway()
        self._guardrail = guardrail
        # Wire the same guardrail into the LLM gateway so token/cost
        # consumption is tracked against the run's quota (mirrors the
        # serve path). Without this the gateway consumes into a NoOp and
        # the run-level token cap never accumulates.
        if self._guardrail is not None and hasattr(self._llm, "_guardrail"):
            self._llm._guardrail = self._guardrail  # type: ignore[attr-defined]
        self._settings = runtime_settings or RuntimeSettings(
            handler_timeout_ms=10_000,
            retry_base_ms=10,
            retry_max_ms=50,
            retry_jitter=False,
            max_handler_retries=2,
            drain_timeout_ms=2_000,
        )

        # Wired in start().
        self._bus = InProcessEventBus()
        # Wire the guardrail into the Orchestrator too — without it the
        # per-run quota is never registered, so token/cycle guardrails
        # silently never enforce under LocalRuntime.
        self._orch = Orchestrator(
            bus=self._bus, store=InMemoryRunStore(),
            guardrail=self._guardrail,
        )
        self._worker = AgentWorker(
            plan=plan, bus=self._bus, llm=self._llm,
            handlers=self._registry, settings=self._settings,
            guardrail=self._guardrail,
        )
        self._started = False

    # ------------------------------------------------------------
    # Construction shortcuts
    # ------------------------------------------------------------

    @classmethod
    def from_yaml(
        cls,
        path: str | Path,
        *,
        handlers: dict[str, HandlerFn],
        llm: LLMGatewayClient | None = None,
        guardrail: GuardrailHandle | None = None,
        runtime_settings: RuntimeSettings | None = None,
    ) -> LocalRuntime:
        """Compile a YAML workflow + register the supplied handlers."""
        raw = load_workflow_yaml(Path(path))
        ir, plan = compile_from_dict(raw)
        return cls(
            ir=ir, plan=plan, handlers=handlers,
            llm=llm, guardrail=guardrail, runtime_settings=runtime_settings,
        )

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        if self._started:
            return
        await self._bus.start()
        await self._orch.start()
        await self._orch.deploy(self._ir)
        await self._worker.start()
        # AgentInstance.run() schedules its subscription inside the
        # task loop; give it one tick before publishing anything.
        await asyncio.sleep(0.05)
        self._started = True
        log.info(
            "local_runtime.started",
            workflow_id=self._ir.id,
            agents=list(self._plan.agents),
        )

    async def stop(self) -> None:
        if not self._started:
            return
        await self._worker.stop()
        await self._orch.stop()
        await self._bus.stop()
        self._started = False
        log.info("local_runtime.stopped", workflow_id=self._ir.id)

    async def __aenter__(self) -> LocalRuntime:
        await self.start()
        return self

    async def __aexit__(self, *exc_info) -> None:
        await self.stop()

    # ------------------------------------------------------------
    # Run a workflow
    # ------------------------------------------------------------

    async def run(
        self,
        *,
        input: dict[str, Any] | None = None,
        timeout: float = 30.0,
    ) -> Run:
        """Create a Run and wait for it to terminate (or timeout)."""
        if not self._started:
            await self.start()
        run = await self._orch.create_run(
            workflow_id=self._ir.id, input=input or {},
        )
        return await self._orch.wait_for_completion(run.run_id, timeout=timeout)

    # ------------------------------------------------------------
    # Inspection
    # ------------------------------------------------------------

    @property
    def bus(self) -> InProcessEventBus:
        return self._bus

    @property
    def llm(self) -> LLMGatewayClient:
        return self._llm

    @property
    def orchestrator(self) -> Orchestrator:
        return self._orch

    @property
    def ir(self) -> WorkflowIR:
        return self._ir


# ============================================================
# run_agent_locally — single-handler quick test
# ============================================================


async def run_agent_locally(
    handler: HandlerFn,
    *,
    event: Envelope | Event | None = None,
    input_payload: dict[str, Any] | None = None,
    input_topic: str | None = None,
    llm: LLMGatewayClient | None = None,
    template_key: str | None = None,
    workflow_id: str = "wf_local_test",
    run_id: str = "run_local_test",
    trace_id: str = "trc_local_test",
) -> list[Event]:
    """Invoke an ``@agent``-decorated handler directly and return its emit list.

    Bypasses the Bus, Orchestrator, and Worker entirely — only the
    handler's body is exercised. ``ctx.publish`` calls go to a
    captured list returned to the caller alongside the handler's
    explicit return value.

    This is the fastest unit-testing path for a single handler::

        @agent(role="thinking", subscribe=["agent.x.in.q"], publish=["agent.x.out.r"])
        async def my_handler(ctx, event):
            return [Event(topic="agent.x.out.r", payload={"ok": True})]

        events = await run_agent_locally(
            my_handler,
            input_topic="agent.x.in.q",
            input_payload={"q": "test"},
        )
        assert events[0].payload == {"ok": True}
    """
    from agentkit.bus.builder import build_envelope  # noqa: PLC0415

    meta = get_agent_meta(handler)
    if meta is None:
        raise TypeError(
            f"{handler!r} is not @agent-decorated; "
            f"use @agent(...) before run_agent_locally()",
        )

    key = template_key or meta.template_key
    template = meta.template

    # Build the input envelope.
    if event is None:
        topic = input_topic or (
            template.subscribe[0].topic if template.subscribe else "agent.test.in"
        )
        envelope = build_envelope(
            topic=topic,
            payload=input_payload or {},
            workflow_id=workflow_id,
            run_id=run_id,
            trace_id=trace_id,
        )
    elif isinstance(event, Event):
        envelope = build_envelope(
            topic=event.topic,
            payload=event.payload,
            workflow_id=workflow_id,
            run_id=run_id,
            trace_id=trace_id,
        )
    else:
        envelope = event

    # Capture-only Bus — never publishes anywhere.
    captured: list[Envelope] = []
    capture_bus = _CaptureBus(captured)

    publish_pipeline = PublishPipeline(
        bus=capture_bus,
        agent_id=f"agt_local_{key}",
        agent_role=template.role,
        runtime_node="local",
        workflow_id=workflow_id,
        publish_whitelist={p.topic for p in template.publish},
    )
    ctx = AgentContext(
        agent_id=f"agt_local_{key}",
        template_key=key,
        workflow_id=workflow_id,
        run_id=envelope.run_id,
        trace_id=envelope.trace_id,
        llm=llm or MockLLMGateway(),
        llm_binding=template.llm,
        publish_pipeline=publish_pipeline,
    )

    # Run the handler. The handler may return events directly,
    # or call ctx.publish (which lands in `captured`).
    direct_returns = await handler(ctx, envelope)
    if direct_returns is None:
        direct_returns = []

    # If the handler explicitly returned events, publish them so they
    # also land in the captured list — keeping the contract uniform.
    for ev in direct_returns:
        await ctx.publish(ev)

    # Convert captured envelopes back to user-facing Event objects.
    return [
        Event(
            topic=env.topic,
            payload=env.payload or {},
            user_headers={
                k: v
                for k, v in (env.headers.user_headers or {}).items()
            },
        )
        for env in captured
    ]


# ============================================================
# Internal — capture-only bus used by run_agent_locally
# ============================================================


class _CaptureBus:
    """Minimal Bus impl that just collects published envelopes.

    Doesn't satisfy the EventBus Protocol completely — only
    ``publish`` is implemented since that's all PublishPipeline
    needs. Used in process by run_agent_locally to sidestep
    spinning up a real bus.
    """

    def __init__(self, sink: list[Envelope]) -> None:
        self._sink = sink

    async def publish(self, env: Envelope):  # type: ignore[override]
        self._sink.append(env)
        from agentkit.bus.interface import PublishReceipt  # noqa: PLC0415
        return PublishReceipt(
            event_id=env.event_id, topic=env.topic, partition=0, offset=len(self._sink),
        )
