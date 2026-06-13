"""End-to-end Kafka throughput / latency benchmarks.

Run with::

    make up
    make test-perf

These are NOT in the default test loop (see pytest markers in
``conftest.py``). They are intended as smoke benchmarks — exact
numbers depend heavily on host hardware. Failing thresholds below
are loose; tighten in CI when you have a baseline.
"""

from __future__ import annotations

import asyncio
import statistics
import time
import uuid

import pytest

from agentkit.bus import SubscribeSpec, build_envelope
from agentkit.bus.kafka import KafkaEventBus


def _topic() -> str:
    return f"agentkit.perf.{uuid.uuid4().hex[:10]}"


# ----------------------------------------------------------------
# Throughput: how many envelopes/second can a single producer push?
# ----------------------------------------------------------------


@pytest.mark.perf
async def test_publish_throughput_single_producer(kafka_bus: KafkaEventBus) -> None:
    topic = _topic()
    n = 2_000

    envelopes = [
        build_envelope(
            topic=topic,
            payload={"i": i, "data": "x" * 200},
            workflow_id="wf_perf",
            run_id="run_perf",
        )
        for i in range(n)
    ]

    t0 = time.perf_counter()
    # Use ``publish_batch`` (which fans out via gather)
    await kafka_bus.publish_batch(envelopes)
    dt = time.perf_counter() - t0

    msgs_per_sec = n / dt
    print(f"\n[publish_throughput] {n} msgs in {dt:.3f}s = {msgs_per_sec:,.0f} msg/s")

    # Loose floor: should easily clear 500 msg/s on dev hardware.
    # Tighten in CI once you have a stable baseline.
    assert msgs_per_sec > 500


# ----------------------------------------------------------------
# Latency: end-to-end publish -> consume p50 / p95 / p99
# ----------------------------------------------------------------


@pytest.mark.perf
async def test_end_to_end_latency(kafka_bus: KafkaEventBus) -> None:
    topic = _topic()
    group = f"grp.perf.lat.{uuid.uuid4().hex[:6]}"

    sub = await kafka_bus.subscribe(
        SubscribeSpec(topic_pattern=topic, group=group, starting_position="earliest"),
    )

    n = 200
    latencies_ms: list[float] = []

    async def consumer_loop() -> None:
        i = 0
        async for msg in sub.messages():
            recv_ns = time.perf_counter_ns()
            sent_ns = msg.envelope.payload["sent_ns"]
            latencies_ms.append((recv_ns - sent_ns) / 1_000_000)
            await kafka_bus.ack(sub, msg)
            i += 1
            if i >= n:
                break

    consumer_task = asyncio.create_task(consumer_loop())

    # Stagger sends so we measure steady-state, not burst-flush latency.
    for i in range(n):
        env = build_envelope(
            topic=topic,
            payload={"i": i, "sent_ns": time.perf_counter_ns()},
            run_id="run_perf",
        )
        await kafka_bus.publish(env)
        await asyncio.sleep(0.005)  # ~200 msg/s

    await asyncio.wait_for(consumer_task, timeout=60)
    await sub.close()

    p50 = statistics.median(latencies_ms)
    p95 = statistics.quantiles(latencies_ms, n=20)[18]
    p99 = statistics.quantiles(latencies_ms, n=100)[98]
    print(
        f"\n[latency] n={len(latencies_ms)} "
        f"p50={p50:.1f}ms p95={p95:.1f}ms p99={p99:.1f}ms",
    )

    # Loose ceilings for a single-node Redpanda on dev hardware.
    # See Doc02 / Doc11 §4.2.3 — collab target is < 200ms end-to-end;
    # the bus alone should be a fraction of that.
    assert p50 < 200, f"p50 {p50:.1f}ms exceeds 200ms"
    assert p95 < 500, f"p95 {p95:.1f}ms exceeds 500ms"
