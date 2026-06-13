"""End-to-end full-stack integration test.

Verifies that an AgentKit process running against the dev stack
actually:

1. Publishes envelopes through real **Redpanda** (not in-process bus).
2. Exposes ``/metrics`` via :class:`ProbeServer`.
3. Has its metrics actually scraped + queryable via **Prometheus**.

(Real-Redis Guardrail is covered separately in
``tests/integration/test_guardrail_redis_real.py``; here the focus
is the Bus + Observability link.)

Marked ``@pytest.mark.e2e``. Auto-skipped if any of the two
external services (Kafka / Prometheus) isn't reachable — so CI
without docker compose stays green.

Run with::

    make up        # start the full stack
    make test-e2e
"""

from __future__ import annotations

import asyncio
import os
import socket
import uuid

import httpx
import pytest

from agentkit import Event, agent, workflow
from agentkit.bus.kafka import KafkaEventBus, KafkaSettings
from agentkit.models.enums import RunStatus
from agentkit.observability import ProbeServer
from agentkit.orchestrator import InMemoryRunStore, Orchestrator
from agentkit.runtime import AgentWorker, HandlerRegistry
from agentkit.testing import MockLLMGateway


# ----------------------------------------------------------------
# Reachability gates — auto-skip if any service is missing
# ----------------------------------------------------------------


def _tcp_reachable(host: str, port: int, timeout: float = 0.5) -> bool:
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


KAFKA_BROKERS = os.getenv("AGENTKIT_BUS_BROKERS", "localhost:9092")
PROM_URL = os.getenv("AGENTKIT_PROMETHEUS_URL", "http://localhost:9090")
PROBE_PORT = int(os.getenv("AGENTKIT_PROM_PORT", "9100"))


@pytest.fixture(scope="module")
def kafka_brokers() -> str:
    host, _, port = KAFKA_BROKERS.split(",")[0].partition(":")
    if not _tcp_reachable(host, int(port or "9092")):
        pytest.skip(f"Kafka {KAFKA_BROKERS} unreachable — run `make up`")
    return KAFKA_BROKERS


@pytest.fixture(scope="module")
def prometheus_url() -> str:
    # Parse "http://host:port"
    rest = PROM_URL.split("//", 1)[1]
    host, _, port_s = rest.partition(":")
    port = int((port_s or "80").split("/", 1)[0])
    if not _tcp_reachable(host, port):
        pytest.skip(f"Prometheus {PROM_URL} unreachable — run `make up`")
    return PROM_URL


# ----------------------------------------------------------------
# Probe server fixture
# ----------------------------------------------------------------


@pytest.fixture(scope="module")
def probe_server() -> ProbeServer:
    """Start a ProbeServer on the configured port for Prometheus to scrape."""
    server = ProbeServer(host="0.0.0.0", port=PROBE_PORT)
    server.start()
    try:
        yield server
    finally:
        server.stop()


# ----------------------------------------------------------------
# Handlers (defined at module scope so @agent meta is captured once)
# ----------------------------------------------------------------


@agent(role="thinking",
       subscribe=["agent.fetcher.in.q"],
       publish=["agent.fetcher.out.r"])
async def fetcher(ctx, event):
    """Step 1: 'fetches' the topic; emits an enriched payload."""
    q = event.payload.get("q", "")
    return [Event(
        topic="agent.fetcher.out.r",
        payload={"q": q, "fetched": f"data-for-{q}"},
    )]


@agent(role="thinking",
       subscribe=["agent.fetcher.out.r"],
       publish=["agent.summarizer.out.r"])
async def summarizer(ctx, event):
    """Step 2: turns enriched data into a 'summary' via mock LLM."""
    text = await ctx.llm.chat(
        f"Summarize: {event.payload.get('fetched', '')}",
    )
    return [Event(
        topic="agent.summarizer.out.r",
        payload={"summary": text, "input_q": event.payload.get("q", "")},
    )]


# ----------------------------------------------------------------
# The actual e2e test
# ----------------------------------------------------------------


@pytest.mark.e2e
async def test_full_stack_run_succeeds_and_metrics_appear(
    kafka_brokers, prometheus_url, probe_server,
) -> None:
    # ---- 1) Build the workflow + compile ----
    wf_id = f"wf_e2e_{uuid.uuid4().hex[:8]}"
    wf = workflow(wf_id, description="E2E: fetch → summarize")
    wf.add(fetcher)
    wf.add(summarizer)
    wf.connect("__start__", "fetcher", via="agent.fetcher.in.q")
    wf.connect("fetcher", "summarizer", via="agent.fetcher.out.r")
    wf.connect("summarizer", "__end__", via="agent.summarizer.out.r")
    ir, plan = wf.compile()

    # ---- 2) Wire real Kafka + in-memory orchestrator + worker ----
    bus = KafkaEventBus(KafkaSettings(brokers=kafka_brokers))
    await bus.start()

    store = InMemoryRunStore()
    orch = Orchestrator(bus=bus, store=store)
    await orch.start()
    await orch.deploy(ir)

    worker = AgentWorker(
        plan=plan,
        bus=bus,
        llm=MockLLMGateway(reply="REAL-STACK-SUMMARY"),
        handlers=HandlerRegistry.global_default(),
    )
    await worker.start()
    # Kafka subscriber needs a few seconds to settle (group rebalance).
    await asyncio.sleep(2)

    try:
        # ---- 3) Run + verify lifecycle ----
        run = await orch.create_run(
            workflow_id=wf_id, input={"q": "what-is-agentkit"},
        )
        final = await orch.wait_for_completion(run.run_id, timeout=30.0)
        assert final.status is RunStatus.SUCCEEDED, (
            f"Run failed: {final.failure_reason}"
        )

        # ---- 4) Sleep long enough for one Prometheus scrape (5s interval) ----
        await asyncio.sleep(8)

        # ---- 5) Query Prometheus for our metrics ----
        async with httpx.AsyncClient(timeout=5.0) as client:
            # 5a) compile_total{result=ok} should be >0.
            samples = await _query(client, prometheus_url, 'compile_total{result="ok"}')
            assert samples, (
                "Prometheus has no compile_total samples — verify "
                f"the probe endpoint at :{PROBE_PORT}/metrics is reachable "
                "from inside docker (host.docker.internal mapping)"
            )
            assert float(samples[0]["value"][1]) >= 1.0

            # 5b) run_started_total{workflow_id=<our wf>} should be exactly 1.
            samples = await _query(
                client, prometheus_url,
                f'run_started_total{{workflow_id="{wf_id}"}}',
            )
            assert samples, f"no run_started_total for {wf_id}"
            assert float(samples[0]["value"][1]) == 1.0

            # 5c) run_completed_total{terminal_status=Succeeded} should be 1.
            samples = await _query(
                client, prometheus_url,
                f'run_completed_total{{workflow_id="{wf_id}",terminal_status="Succeeded"}}',
            )
            assert samples, f"no run_completed_total for {wf_id}"
            assert float(samples[0]["value"][1]) == 1.0

            # 5d) Each agent processed at least one event.
            for tk in ("fetcher", "summarizer"):
                samples = await _query(
                    client, prometheus_url,
                    f'agent_event_processed_total{{template_key="{tk}",result="ok"}}',
                )
                assert samples, f"no agent_event_processed_total for {tk}"
                assert float(samples[0]["value"][1]) >= 1.0

            # 5e) Probe server itself reachable (sanity).
            resp = await client.get(
                f"http://localhost:{probe_server.actual_port}/metrics",
            )
            assert resp.status_code == 200
            assert "compile_total" in resp.text

    finally:
        await worker.stop()
        await orch.stop()
        await bus.stop()


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------


async def _query(client: httpx.AsyncClient, prom_url: str, expr: str) -> list:
    """Run a Prometheus instant query, return result samples."""
    resp = await client.get(
        f"{prom_url}/api/v1/query", params={"query": expr},
    )
    data = resp.json()
    assert data.get("status") == "success", f"query failed: {data}"
    return data["data"]["result"]
