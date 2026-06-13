"""End-to-end demo: real Kafka + Prometheus scrape + Grafana visualization.

Showcases the **class-based** SDK API (Agent subclass + auto-via).

Prereq::

    make up                 # bring up docker stack (Redpanda+Redis+PG+Prom+Grafana+OTel)

Run::

    PYTHONPATH=src python examples/full_stack_demo.py

What it does:

1. Defines two Agent subclasses (Fetcher → Summarizer).
2. Starts a :class:`ProbeServer` on :9100 → Prometheus scrapes it.
3. Compiles + spins up a real Kafka-backed Worker + Orchestrator.
4. Runs N workflow Runs in a loop, each ~50ms.
5. Sleeps until Ctrl+C — open Grafana http://localhost:3000 (anon viewer)
   and load the **AgentKit Overview** dashboard to watch live metrics.

Stop:

* foreground (no ``&``)         → Ctrl+C
* backgrounded with ``&``       → ``kill -INT %1``  (Ctrl+C in a parent
                                  shell does NOT reach background jobs)
"""

from __future__ import annotations

import asyncio
import os
import signal
import uuid

from agentkit import (
    END,
    START,
    Agent,
    Event,
    workflow,
)
from agentkit.bus.kafka import KafkaEventBus, KafkaSettings
from agentkit.observability import ProbeServer, configure_tracing
from agentkit.orchestrator import InMemoryRunStore, Orchestrator
from agentkit.runtime import AgentWorker, HandlerRegistry
from agentkit.testing import MockLLMGateway

KAFKA = os.getenv("AGENTKIT_BUS_BROKERS", "localhost:9092")
OTEL = os.getenv("AGENTKIT_OTEL_ENDPOINT", "http://localhost:4317")
PROBE_PORT = int(os.getenv("AGENTKIT_PROM_PORT", "9100"))
RUNS_PER_MINUTE = int(os.getenv("RUNS_PER_MINUTE", "30"))


# -----------------------------------------------------------------
# Agent classes — everything is configured as class attributes.
# -----------------------------------------------------------------


class Fetcher(Agent):
    """Step 1: 'fetches' the topic and emits an enriched payload."""

    role = "thinking"
    description = "Synthesize a richer record from a query."
    subscribe = ["agent.fetcher.in.q"]
    publish = ["agent.fetcher.out.r"]

    async def handle(self, ctx, event):
        q = event.payload.get("q", "")
        return [Event(
            "agent.fetcher.out.r",
            {"q": q, "fetched": f"data-for-{q}"},
        )]


class Summarizer(Agent):
    """Step 2: turns the fetched record into a short summary via LLM."""

    role = "thinking"
    description = "Summarize the fetched record."
    subscribe = ["agent.fetcher.out.r"]
    publish = ["agent.summarizer.out.r"]

    async def handle(self, ctx, event):
        text = await ctx.llm.chat(
            f"Summarize: {event.payload.get('fetched', '')}",
        )
        return [Event(
            "agent.summarizer.out.r",
            {"summary": text, "input_q": event.payload.get("q", "")},
        )]


# -----------------------------------------------------------------
# Wire everything up
# -----------------------------------------------------------------


async def main() -> None:
    # ---- 1) Probe server (Prometheus scrapes :9100/metrics) ----
    probe = ProbeServer(host="0.0.0.0", port=PROBE_PORT)
    probe.start()
    print(f"✓ Probe server at http://localhost:{probe.actual_port}/metrics")

    # ---- 2) Configure tracing → OTel collector (optional) ----
    try:
        configure_tracing(
            endpoint=OTEL, service_name="agentkit-demo", sample_rate=1.0,
        )
        print(f"✓ OTLP traces → {OTEL}")
    except Exception as e:  # noqa: BLE001
        print(f"(skipping OTLP — {e})")

    # ---- 3) Build & compile the workflow with Agent objects ----
    fetcher = Fetcher()
    summarizer = Summarizer()

    wf_id = "wf_demo_pipeline"
    wf = workflow(wf_id, description="Demo: fetch → summarize")
    wf.add(fetcher)
    wf.add(summarizer)
    # No string template-keys, no `via` topic strings — both are
    # auto-derived from the agents' subscribe/publish declarations.
    wf.connect(START, fetcher)
    wf.connect(fetcher, summarizer)
    wf.connect(summarizer, END)

    ir, plan = wf.compile()
    print(f"✓ Workflow {wf_id!r} compiled (ir_hash={ir.meta.ir_hash})")

    # ---- 4) Wire real Kafka + worker ----
    bus = KafkaEventBus(KafkaSettings(brokers=KAFKA))
    await bus.start()
    print(f"✓ Kafka bus connected to {KAFKA}")

    store = InMemoryRunStore()
    orch = Orchestrator(bus=bus, store=store)
    await orch.start()
    await orch.deploy(ir)

    worker = AgentWorker(
        plan=plan,
        bus=bus,
        llm=MockLLMGateway(reply="MOCKED-SUMMARY"),
        handlers=HandlerRegistry.global_default(),
    )
    await worker.start()
    print("✓ Worker + Orchestrator ready")
    await asyncio.sleep(2)  # let Kafka rebalance

    # ---- 5) Loop forever, firing runs ----
    pid = os.getpid()
    print(
        f"\nLoop: ~{RUNS_PER_MINUTE} runs / minute. "
        f"Open http://localhost:3000 (admin/admin) → AgentKit Overview\n",
    )
    print(f"  Process PID = {pid}")
    print( "  To stop:")
    print( "    - foreground (no `&`):  Ctrl+C")
    print(f"    - backgrounded:         kill -INT {pid}    (or `kill %1`)")
    print( "    - tear down stack:      docker compose down\n")

    interval_s = 60.0 / RUNS_PER_MINUTE
    stop = asyncio.Event()

    def _signal_handler() -> None:
        print("\n(stopping…)")
        stop.set()

    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            loop.add_signal_handler(sig, _signal_handler)
        except NotImplementedError:
            pass

    n = 0
    try:
        while not stop.is_set():
            run = await orch.create_run(
                workflow_id=wf_id,
                input={"q": f"topic-{uuid.uuid4().hex[:6]}"},
            )
            try:
                final = await orch.wait_for_completion(run.run_id, timeout=5)
                n += 1
                if n % 10 == 0:
                    print(f"  run #{n}: {final.status.value}")
            except TimeoutError:
                print(f"  run #{n+1}: TIMEOUT (run_id={run.run_id})")
            try:
                await asyncio.wait_for(stop.wait(), timeout=interval_s)
            except asyncio.TimeoutError:
                pass
    finally:
        print("Stopping worker / orchestrator / bus…")
        await worker.stop()
        await orch.stop()
        await bus.stop()
        probe.stop()
        print(f"Done. Total runs: {n}")


if __name__ == "__main__":
    asyncio.run(main())
