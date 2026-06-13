"""Fakeredis fixture for Guardrail unit tests.

We use ``fakeredis.aioredis.FakeRedis`` which speaks the same async
``redis.asyncio.Redis`` interface AgentKit code expects, so we can
inject it transparently into ``RedisGuardrail`` for unit tests.

This is a *test helper* — never imported from ``src/``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator

import fakeredis.aioredis
import pytest

from agentkit.guardrail import GuardrailContext, GuardrailSettings, RedisGuardrail
from agentkit.guardrail.models import AgentGuardrail, RunGuardrail


@pytest.fixture
async def fake_redis() -> AsyncIterator[fakeredis.aioredis.FakeRedis]:
    """A fresh FakeRedis instance per test.

    Returns an async Redis client backed by an in-memory simulator.
    Lua eval is supported in fakeredis ≥2.21 which is what we pin.
    """
    client = fakeredis.aioredis.FakeRedis(decode_responses=True)
    try:
        yield client
    finally:
        await client.aclose()


@pytest.fixture
async def guardrail(fake_redis) -> AsyncIterator[RedisGuardrail]:
    """A ready-to-use ``RedisGuardrail`` over fakeredis.

    Settings default to permissive fail_mode + a tight TTL so any
    forgotten reservation expires quickly during the test.
    """
    settings = GuardrailSettings(
        fail_mode="permissive",
        default_reservation_ttl_ms=2_000,
        run_hash_ttl_seconds=60,
    )
    g = RedisGuardrail(settings=settings, redis=fake_redis)
    await g.start()
    yield g
    await g.stop()


def make_context(
    *,
    run_id: str = "run_test_001",
    workflow_id: str = "wf_test",
    agent_max_tokens: int = 1_000,
    agent_max_cycles: int = 3,
    run_max_tokens: int = 5_000,
    run_max_cycles: int = 10,
) -> GuardrailContext:
    """Tiny helper for building tight test contexts."""
    return GuardrailContext(
        run_id=run_id,
        workflow_id=workflow_id,
        agent=AgentGuardrail(
            max_tokens_per_call=agent_max_tokens,
            max_cycles=agent_max_cycles,
        ),
        run=RunGuardrail(
            max_total_tokens=run_max_tokens,
            max_cycles_per_run=run_max_cycles,
        ),
    )
