"""Microbenchmarks for envelope encode / decode.

These are CPU-only and run without Kafka. They use ``pytest-benchmark``
which reports stats; CI can fail-fast on regressions if needed.
"""

from __future__ import annotations

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.bus.kafka.serde import decode, encode


@pytest.fixture
def small_envelope():
    return build_envelope(
        topic="agent.research.in.q1",
        payload={"q": "hello"},
        workflow_id="wf_x",
        run_id="run_y",
    )


@pytest.fixture
def large_envelope():
    big_payload = {
        "summary": "x" * 8_000,
        "items": [{"id": i, "score": float(i) / 100} for i in range(100)],
    }
    return build_envelope(
        topic="agent.research.out.summary",
        payload=big_payload,
        workflow_id="wf_x",
        run_id="run_y",
    )


@pytest.mark.perf
def test_perf_encode_small(benchmark, small_envelope) -> None:
    benchmark(encode, small_envelope)


@pytest.mark.perf
def test_perf_encode_large(benchmark, large_envelope) -> None:
    benchmark(encode, large_envelope)


@pytest.mark.perf
def test_perf_decode_small(benchmark, small_envelope) -> None:
    raw = encode(small_envelope)
    benchmark(decode, raw)


@pytest.mark.perf
def test_perf_decode_large(benchmark, large_envelope) -> None:
    raw = encode(large_envelope)
    benchmark(decode, raw)


@pytest.mark.perf
def test_perf_round_trip_small(benchmark, small_envelope) -> None:
    def round_trip() -> None:
        decode(encode(small_envelope))

    benchmark(round_trip)
