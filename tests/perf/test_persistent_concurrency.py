"""持久化模式下的并发压测对比 (Phase 3)。

三个对比维度:

A. **baseline-inmem**       — 原始 InMemoryRunStore + max_concurrent=1
B. **persistent-pg**        — PostgresRunStore + max_concurrent=1
C. **persistent-pg-opt**    — PostgresRunStore + max_concurrent=4 + switch_replicas=2

每个维度跑同一组拓扑 (M concurrent runs / single workflow)，
直接打印对比表。Postgres 不可达时 skip。
"""

from __future__ import annotations

import asyncio
import os
import socket
import statistics
import time

import pytest

from agentkit import END, START, Agent, Event, workflow
from agentkit.bus.inprocess import InProcessEventBus
from agentkit.orchestrator import (
    InMemoryRunStore,
    Orchestrator,
    PostgresRunStore,
)
from agentkit.runtime import AgentWorker, HandlerRegistry
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.testing import MockLLMGateway
from agentkit.workflow.compiler.pipeline import (
    clear_compile_cache,
    get_compile_cache_stats,
)


PG_HOST = os.getenv("AGENTKIT_PG_HOST", "localhost")
PG_PORT = int(os.getenv("AGENTKIT_PG_PORT", "5432"))


def _pg_reachable() -> bool:
    try:
        with socket.create_connection((PG_HOST, PG_PORT), timeout=0.5):
            return True
    except OSError:
        return False


def _make_one_agent_workflow(wf_id: str, *, simulated_io_ms: float = 0.0):
    """Single pass-through agent workflow.

    ``simulated_io_ms`` adds an asyncio.sleep inside the handler to
    simulate LLM call latency — this is where in-flight Semaphore
    (max_concurrent>1) helps.
    """
    in_topic = f"agent.{wf_id}_step.in"
    out_topic = f"agent.{wf_id}_step.out"

    class _Step(Agent):
        subscribe = [in_topic]
        publish = [out_topic]
        template_key = f"{wf_id}_step"

        async def handle(self, ctx, event):
            if simulated_io_ms > 0:
                await asyncio.sleep(simulated_io_ms / 1000.0)
            return [Event(self.publish_topics[0], dict(event.payload))]

    _Step.__name__ = f"Step_{wf_id}"
    a = _Step()
    wf = workflow(wf_id).add(a).connect(START, a).connect(a, END)
    return wf, a


async def _build_runtime(
    wfs,
    *,
    store,
    max_concurrent: int = 1,
):
    """Build bus + orch(store) + per-wf worker with given concurrency."""
    bus = InProcessEventBus()
    await bus.start()
    orch = Orchestrator(bus=bus, store=store)
    await orch.start()

    workers = []
    for wf in wfs:
        ir, plan = wf.compile()
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
            max_concurrent_per_instance=max_concurrent,
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


async def _run_concurrent(
    *,
    n_runs: int,
    store,
    max_concurrent: int,
    label: str,
    simulated_io_ms: float = 0.0,
):
    """Fire M concurrent runs against ONE workflow, return metrics dict."""
    wf, _ = _make_one_agent_workflow(
        f"wf_{label}", simulated_io_ms=simulated_io_ms,
    )
    bus, orch, workers = await _build_runtime(
        [wf], store=store, max_concurrent=max_concurrent,
    )

    t0 = time.perf_counter()
    runs = await asyncio.gather(*[
        orch.create_run(workflow_id=wf.id, input={"i": i})
        for i in range(n_runs)
    ])
    finals = await asyncio.gather(*[
        orch.wait_for_completion(r.run_id, timeout=60) for r in runs
    ])
    elapsed = time.perf_counter() - t0

    succeeded = sum(1 for f in finals if f.status.value == "Succeeded")
    latencies = [
        (f.ended_at - f.started_at).total_seconds() * 1000
        for f in finals if f.ended_at and f.started_at
    ]
    p50 = statistics.median(latencies) if latencies else 0.0
    p99 = (statistics.quantiles(latencies, n=100)[-1]
           if len(latencies) > 1 else 0.0)
    throughput = n_runs / elapsed if elapsed > 0 else 0.0

    await _stop(bus, orch, workers)

    return {
        "label": label,
        "n_runs": n_runs,
        "elapsed_ms": elapsed * 1000,
        "throughput_rps": throughput,
        "p50_ms": p50,
        "p99_ms": p99,
        "ok": succeeded,
    }


@pytest.mark.perf
@pytest.mark.asyncio
async def test_phase3_baseline_vs_persistent_vs_optimized():
    """A/B/C/D 四组对比:

    A. baseline-inmem        InMemory store + sync passthrough
    B. persistent-pg         Postgres store + sync passthrough
    C. pg-no-iobound         Postgres store + sync passthrough (max_conc=1)
                             — 验证 PG persistence 单纯开销
    D. pg-iobound-conc       Postgres + 50ms simulated IO + max_conc=4
                             — 体现 in-flight Semaphore 优势
    D-baseline. pg-iobound-seq Postgres + 50ms simulated IO + max_conc=1
                             — D 的对照组
    """
    if not _pg_reachable():
        pytest.skip(f"Postgres unreachable at {PG_HOST}:{PG_PORT}")

    # Warm up compile cache so all three runs benefit equally.
    clear_compile_cache()

    n_runs = 100
    rows: list[dict] = []

    # ── A. baseline-inmem ──────────────────────────────────────
    rows.append(await _run_concurrent(
        n_runs=n_runs,
        store=InMemoryRunStore(),
        max_concurrent=1,
        label="A_inmem",
    ))

    # ── B. persistent-pg (sequential, no simulated IO) ─────────
    pg_b = PostgresRunStore(
        dsn=f"postgresql://agentkit:agentkit@{PG_HOST}:{PG_PORT}/agentkit",
        schema="perf_b",
    )
    await pg_b.start()
    try:
        rows.append(await _run_concurrent(
            n_runs=n_runs,
            store=pg_b,
            max_concurrent=1,
            label="B_pg",
        ))
    finally:
        await pg_b.stop()

    # ── D-baseline. PG + 50ms simulated LLM, max_conc=1 ────────
    pg_d1 = PostgresRunStore(
        dsn=f"postgresql://agentkit:agentkit@{PG_HOST}:{PG_PORT}/agentkit",
        schema="perf_d1",
    )
    await pg_d1.start()
    try:
        rows.append(await _run_concurrent(
            n_runs=n_runs,
            store=pg_d1,
            max_concurrent=1,
            simulated_io_ms=50.0,
            label="D1_io_seq",
        ))
    finally:
        await pg_d1.stop()

    # ── D. PG + 50ms simulated LLM, max_conc=4 ─────────────────
    pg_d2 = PostgresRunStore(
        dsn=f"postgresql://agentkit:agentkit@{PG_HOST}:{PG_PORT}/agentkit",
        schema="perf_d2",
    )
    await pg_d2.start()
    try:
        rows.append(await _run_concurrent(
            n_runs=n_runs,
            store=pg_d2,
            max_concurrent=4,
            simulated_io_ms=50.0,
            label="D2_io_conc",
        ))
    finally:
        await pg_d2.stop()

    # ── 输出对比表 ────────────────────────────────────────────
    cache_stats = get_compile_cache_stats()
    print(f"\n=== Phase 3 持久化压测对比  (M={n_runs} concurrent runs) ===")
    print(f"{'label':<14} | {'elapsed':>10} | {'rps':>10} | "
          f"{'p50':>8} | {'p99':>8} | ok")
    print("-" * 70)
    for r in rows:
        print(
            f"{r['label']:<14} | "
            f"{r['elapsed_ms']:>8.1f}ms | "
            f"{r['throughput_rps']:>8.1f}/s | "
            f"{r['p50_ms']:>6.2f}ms | "
            f"{r['p99_ms']:>6.2f}ms | "
            f"{r['ok']}/{r['n_runs']}"
        )
    print(f"\n[compile cache] hits={cache_stats['hits']} "
          f"misses={cache_stats['misses']} size={cache_stats['size']}")

    A, B, D1, D2 = rows
    pg_overhead = (B["elapsed_ms"] - A["elapsed_ms"]) / A["elapsed_ms"] * 100
    semaphore_speedup = D1["elapsed_ms"] / D2["elapsed_ms"]
    print(f"\nPostgres 引入开销 (passthrough):  {pg_overhead:+.1f}%")
    print(f"In-flight Semaphore 加速 (50ms IO):  {semaphore_speedup:.2f}x")

    # 全部都应 succeed
    for r in rows:
        assert r["ok"] == r["n_runs"], f"{r['label']} had failures"
