"""Concurrency & stress tests for AgentKit's multi-workflow runtime.

Three orthogonal axes:

1. Multi-workflow scale-out    — N independent workflows on one Orchestrator
2. Single-workflow concurrency — M concurrent runs against ONE workflow
3. Combined stress             — N × M (workflows × runs each)

Uses InProcessEventBus + MockLLMGateway → measures pure framework overhead.
"""

from __future__ import annotations

import asyncio
import statistics
import time

import pytest

from agentkit import END, START, Agent, Event, workflow
from agentkit.bus.inprocess import InProcessEventBus
from agentkit.orchestrator import InMemoryRunStore, Orchestrator
from agentkit.runtime import AgentWorker, HandlerRegistry
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.testing import MockLLMGateway


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------


def _make_one_agent_workflow(wf_id: str):
    """Single pass-through agent workflow."""
    in_topic = f"agent.{wf_id}_step.in"
    out_topic = f"agent.{wf_id}_step.out"

    class _Step(Agent):
        subscribe = [in_topic]
        publish = [out_topic]
        template_key = f"{wf_id}_step"

        async def handle(self, ctx, event):
            return [Event(self.publish_topics[0], dict(event.payload))]

    _Step.__name__ = f"Step_{wf_id}"
    a = _Step()
    wf = workflow(wf_id).add(a).connect(START, a).connect(a, END)
    return wf, a


async def _build_isolated_runtime(wfs):
    """One Bus + one Orchestrator, but one Worker per workflow (production-like)."""
    bus = InProcessEventBus()
    await bus.start()
    orch = Orchestrator(bus=bus, store=InMemoryRunStore())
    await orch.start()

    workers: list[AgentWorker] = []
    for wf in wfs:
        ir, plan = wf.compile()

        # Per-workflow isolated handler registry.
        registry = HandlerRegistry()
        for a in wf._agents_by_key.values():
            registry.register(
                workflow_id=ir.id,
                template_key=a.key,
                executor=_FunctionExecutor(a.handler),
                replace=True,
            )
        await orch.deploy(ir)
        w = AgentWorker(
            plan=plan, bus=bus,
            llm=MockLLMGateway(reply="ok"),
            handlers=registry,
        )
        await w.start()
        workers.append(w)
    await asyncio.sleep(0.05)
    return bus, orch, workers


async def _stop(bus, orch, workers):
    for w in workers:
        await w.stop()
    await orch.stop()
    await bus.stop()


# ----------------------------------------------------------------
# Test 1 — Multi-workflow scale-out (deploy N workflows, fire 1 run each)
# ----------------------------------------------------------------


@pytest.mark.perf
@pytest.mark.parametrize("n_workflows", [1, 5, 10, 25, 50])
@pytest.mark.asyncio
async def test_concurrent_workflows_one_run_each(n_workflows: int):
    wfs = [_make_one_agent_workflow(f"wf{i}")[0] for i in range(n_workflows)]
    bus, orch, workers = await _build_isolated_runtime(wfs)

    t0 = time.perf_counter()
    runs = await asyncio.gather(*[
        orch.create_run(workflow_id=wf.id, input={"i": i})
        for i, wf in enumerate(wfs)
    ])
    finals = await asyncio.gather(*[
        orch.wait_for_completion(r.run_id, timeout=15) for r in runs
    ])
    elapsed = time.perf_counter() - t0

    succeeded = sum(1 for f in finals if f.status.value == "Succeeded")
    print(
        f"\n[multi-wf]   N={n_workflows:>3}  "
        f"elapsed={elapsed*1000:>7.2f}ms  "
        f"per-wf={elapsed/n_workflows*1000:>6.2f}ms  "
        f"ok={succeeded}/{n_workflows}"
    )
    assert succeeded == n_workflows
    await _stop(bus, orch, workers)


# ----------------------------------------------------------------
# Test 2 — Single-workflow concurrent runs (M parallel runs)
# ----------------------------------------------------------------


@pytest.mark.perf
@pytest.mark.parametrize("n_runs", [1, 10, 50, 100, 200])
@pytest.mark.asyncio
async def test_single_workflow_concurrent_runs(n_runs: int):
    wf, _ = _make_one_agent_workflow("wf_load")
    bus, orch, workers = await _build_isolated_runtime([wf])

    t0 = time.perf_counter()
    runs = await asyncio.gather(*[
        orch.create_run(workflow_id="wf_load", input={"i": i})
        for i in range(n_runs)
    ])
    finals = await asyncio.gather(*[
        orch.wait_for_completion(r.run_id, timeout=30) for r in runs
    ])
    elapsed = time.perf_counter() - t0

    latencies = [
        (f.ended_at - f.started_at).total_seconds() * 1000
        for f in finals if f.ended_at and f.started_at
    ]
    succeeded = sum(1 for f in finals if f.status.value == "Succeeded")
    p50 = statistics.median(latencies) if latencies else 0
    p99 = statistics.quantiles(latencies, n=100)[-1] if len(latencies) > 1 else 0
    throughput = n_runs / elapsed
    print(
        f"\n[single-wf]  M={n_runs:>3}  "
        f"elapsed={elapsed*1000:>7.2f}ms  "
        f"throughput={throughput:>7.1f} runs/s  "
        f"p50={p50:>5.2f}ms  p99={p99:>5.2f}ms  "
        f"ok={succeeded}/{n_runs}"
    )
    assert succeeded == n_runs
    await _stop(bus, orch, workers)


# ----------------------------------------------------------------
# Test 3 — Combined stress
# ----------------------------------------------------------------


@pytest.mark.perf
@pytest.mark.parametrize(
    ("n_workflows", "n_runs_per_wf"),
    [(5, 20), (10, 20), (20, 10)],
)
@pytest.mark.asyncio
async def test_combined_stress(n_workflows: int, n_runs_per_wf: int):
    wfs = [_make_one_agent_workflow(f"sw{i}")[0] for i in range(n_workflows)]
    bus, orch, workers = await _build_isolated_runtime(wfs)

    t0 = time.perf_counter()
    creates = []
    for wf in wfs:
        for k in range(n_runs_per_wf):
            creates.append(orch.create_run(workflow_id=wf.id, input={"k": k}))
    runs = await asyncio.gather(*creates)
    finals = await asyncio.gather(*[
        orch.wait_for_completion(r.run_id, timeout=60) for r in runs
    ])
    elapsed = time.perf_counter() - t0

    total = n_workflows * n_runs_per_wf
    succeeded = sum(1 for f in finals if f.status.value == "Succeeded")
    throughput = total / elapsed
    print(
        f"\n[combined]   N×M={n_workflows}×{n_runs_per_wf}={total:>4}  "
        f"elapsed={elapsed*1000:>7.2f}ms  "
        f"throughput={throughput:>7.1f} runs/s  "
        f"ok={succeeded}/{total}"
    )
    assert succeeded == total
    await _stop(bus, orch, workers)
