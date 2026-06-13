"""Microbenchmark for Guardrail Lua roundtrip throughput.

Measures the cost of one ``precheck`` + ``consume`` pair. Provides
a baseline for comparing against real Redis later (fakeredis is
purely in-process so numbers are optimistic).
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit.guardrail import RedisGuardrail
from tests.helpers.fakeredis_fixture import (  # noqa: F401 — fixtures
    fake_redis,
    guardrail,
    make_context,
)


@pytest.mark.perf
def test_perf_precheck_consume(benchmark, fake_redis) -> None:
    """End-to-end precheck + consume timing."""
    from agentkit.guardrail import GuardrailSettings  # noqa: PLC0415

    g = RedisGuardrail(
        settings=GuardrailSettings(fail_mode="permissive"),
        redis=fake_redis,
    )

    async def setup() -> None:
        await g.start()
        ctx = make_context(
            run_max_tokens=1_000_000_000,
            run_max_cycles=1_000_000_000,
            agent_max_tokens=1_000_000_000,
            agent_max_cycles=1_000_000_000,
        )
        await g.init_run_quota(ctx)

    asyncio.run(setup())

    counter = {"n": 0}

    def cycle() -> None:
        counter["n"] += 1

        async def _run() -> None:
            rsv = await g.precheck(
                run_id="run_test_001",
                agent_id=f"agt_{counter['n']}",
                est_tokens=100,
            )
            await g.consume(rsv, actual_tokens=120)

        asyncio.run(_run())

    benchmark(cycle)
