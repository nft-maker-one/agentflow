"""End-to-end Workflow-Run perf benchmarks.

The user-facing question we're answering:

    "How fast can AgentKit run an N-agent workflow on the InProcess bus
     with no LLM in the loop (just my logic)?"

We benchmark three shapes:

1. ``test_perf_run_single_agent``    — minimal echo (1 agent, ~3 events)
2. ``test_perf_run_two_agent_chain`` — fetcher → summarizer (2 agents)
3. ``test_perf_run_five_agent_chain``— linear 5-agent pipeline

For each shape we report **mean wall-clock per Run**, including:

* IR compile (cached after first call inside the benchmark loop)
* Worker subscribe + Orchestrator deploy (per-iteration teardown/setup)
* Orchestrator.create_run → wait_for_completion full cycle
* Bus publish + consume + ack on every hop

Numbers from these benchmarks set the floor for "fast enough" — if a
real LLM call adds 800ms-3s on top, the framework overhead should be
sub-100ms for typical small workflows.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from agentkit import END, START, Agent, Event, workflow
from agentkit.bus.inprocess import InProcessEventBus
from agentkit.orchestrator import InMemoryRunStore, Orchestrator
from agentkit.runtime import AgentWorker, HandlerRegistry
from agentkit.testing import MockLLMGateway


# ----------------------------------------------------------------
# Shared agent factory — pure-Python pass-through, no LLM
# ----------------------------------------------------------------


def _make_passthrough_chain(n: int) -> list[Agent]:
    """Build N pass-through agents wired in a linear chain.

    Each agent subscribes to the previous agent's publish topic and
    emits to its own publish topic — pure router.
    """
    agents: list[Agent] = []
    for i in range(n):
        in_topic = f"agent.step{i}.in" if i == 0 else f"agent.step{i-1}.out"
        out_topic = f"agent.step{i}.out"

        class _Step(Agent):
            subscribe = [in_topic]
            publish = [out_topic]
            template_key = f"step_{i}"

            async def handle(self, ctx, event):
                # Pass-through with a tiny tag — any compute would dominate.
                p = dict(event.payload)
                p[f"hop_{type(self).__name__}"] = True
                return [Event(self.publish_topics[0], p)]

        # Make each subclass uniquely named so introspection / repr is sane.
        _Step.__name__ = f"Step{i}"
        agents.append(_Step())
    return agents


async def _build_runtime(agents: list[Agent], wf_id: str):
    """Compile + start everything; return (bus, orch, worker, ir_id)."""
    wf = workflow(wf_id)
    for a in agents:
        wf.add(a)
    wf.connect(START, agents[0])
    for a, b in zip(agents, agents[1:], strict=False):
        wf.connect(a, b)
    wf.connect(agents[-1], END)

    ir, plan = wf.compile()
    bus = InProcessEventBus()
    await bus.start()

    # Per-test isolated registry to avoid cross-pollution.
    registry = HandlerRegistry()
    for a in agents:
        from agentkit.runtime.executor import _FunctionExecutor  # noqa: PLC0415
        registry.register(
            workflow_id=ir.id,
            template_key=a.key,
            executor=_FunctionExecutor(a.handler),
            replace=True,
        )

    orch = Orchestrator(bus=bus, store=InMemoryRunStore())
    await orch.start()
    await orch.deploy(ir)
    worker = AgentWorker(
        plan=plan, bus=bus, llm=MockLLMGateway(),
        handlers=registry,
    )
    await worker.start()
    # Subscribers are scheduled on the loop — give them a tick.
    await asyncio.sleep(0.02)
    return bus, orch, worker, ir.id


async def _shutdown(bus, orch, worker) -> None:
    await worker.stop()
    await orch.stop()
    await bus.stop()


# ----------------------------------------------------------------
# Benchmarks — async loop wrapped in a sync benchmark callable
# ----------------------------------------------------------------


def _bench_run(benchmark, *, n_agents: int) -> None:
    """Time a single Run, end-to-end, on a chain of ``n_agents``.

    Setup (compile + worker start) happens once outside the timed
    region; only the Run itself is measured.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        agents = _make_passthrough_chain(n_agents)
        bus, orch, worker, wf_id = loop.run_until_complete(
            _build_runtime(agents, f"wf_perf_{n_agents}"),
        )
        try:
            counter = {"n": 0}

            def one_run() -> None:
                counter["n"] += 1

                async def go() -> None:
                    run = await orch.create_run(
                        workflow_id=wf_id,
                        input={"i": counter["n"]},
                    )
                    await orch.wait_for_completion(run.run_id, timeout=5.0)

                loop.run_until_complete(go())

            benchmark(one_run)
        finally:
            loop.run_until_complete(_shutdown(bus, orch, worker))
    finally:
        loop.close()


@pytest.mark.perf
def test_perf_run_single_agent(benchmark) -> None:
    """Single-agent echo — what's the lowest floor?"""
    _bench_run(benchmark, n_agents=1)


@pytest.mark.perf
def test_perf_run_two_agent_chain(benchmark) -> None:
    """Fetch → summarize style: 2 hops on the bus."""
    _bench_run(benchmark, n_agents=2)


@pytest.mark.perf
def test_perf_run_five_agent_chain(benchmark) -> None:
    """Realistic small pipeline: 5 hops."""
    _bench_run(benchmark, n_agents=5)


# ----------------------------------------------------------------
# Throughput — concurrent Runs
# ----------------------------------------------------------------


@pytest.mark.perf
def test_perf_throughput_concurrent_runs(benchmark) -> None:
    """How many Runs/sec when we fire 10 concurrently on a 2-agent chain?

    benchmark.pedantic with iterations=1 + rounds=N to amortize setup.
    """
    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    BATCH = 10
    try:
        agents = _make_passthrough_chain(2)
        bus, orch, worker, wf_id = loop.run_until_complete(
            _build_runtime(agents, "wf_perf_throughput"),
        )
        try:
            counter = {"n": 0}

            def fire_batch() -> None:
                async def batch() -> None:
                    runs = await asyncio.gather(*[
                        orch.create_run(
                            workflow_id=wf_id,
                            input={"i": counter["n"] * BATCH + j},
                        )
                        for j in range(BATCH)
                    ])
                    counter["n"] += 1
                    await asyncio.gather(*[
                        orch.wait_for_completion(r.run_id, timeout=10.0)
                        for r in runs
                    ])

                loop.run_until_complete(batch())

            benchmark.pedantic(fire_batch, iterations=1, rounds=5, warmup_rounds=1)
        finally:
            loop.run_until_complete(_shutdown(bus, orch, worker))
    finally:
        loop.close()


# ----------------------------------------------------------------
# Bus latency — single publish + consume on InProcess bus
# ----------------------------------------------------------------


@pytest.mark.perf
def test_perf_inprocess_bus_publish_consume(benchmark) -> None:
    """How fast is one publish→consume hop on the in-process bus?"""
    from agentkit.bus.builder import build_envelope        # noqa: PLC0415
    from agentkit.bus.interface import SubscribeSpec       # noqa: PLC0415

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        bus = InProcessEventBus()
        loop.run_until_complete(bus.start())

        sub = loop.run_until_complete(bus.subscribe(SubscribeSpec(
            topic_pattern="bench.t",
            group="g.bench",
            starting_position="latest",
        )))
        sub_iter = sub.messages()

        async def one_hop() -> None:
            env = build_envelope(topic="bench.t", payload={"k": "v"})
            await bus.publish(env)
            # Pull exactly one message off the iterator.
            msg = await sub_iter.__anext__()
            await bus.ack(sub, msg)

        def call() -> None:
            loop.run_until_complete(one_hop())

        try:
            benchmark(call)
        finally:
            loop.run_until_complete(sub.close())
            loop.run_until_complete(bus.stop())
    finally:
        loop.close()
