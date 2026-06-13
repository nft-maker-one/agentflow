"""Integration test against a real Redis.

Skipped if ``REDIS_URL`` env var isn't set or the server is
unreachable. To run::

    docker run --rm -p 6379:6379 redis:7
    REDIS_URL=redis://localhost:6379/0 pytest -m integration tests/integration/test_guardrail_redis_real.py

The Lua scripts are identical to the ones exercised in unit tests
via fakeredis — this run validates that the wire protocol and
``EVAL`` semantics also match real Redis.
"""

from __future__ import annotations

import os

import pytest

from agentkit.guardrail import (
    GuardrailSettings,
    QuotaExceeded,
    RedisGuardrail,
)
from agentkit.guardrail.errors import GuardrailUnavailable
from agentkit.guardrail.models import AgentGuardrail, GuardrailContext, RunGuardrail

pytestmark = pytest.mark.integration


REDIS_URL = os.environ.get("REDIS_URL", "redis://localhost:6379/0")


def _make_ctx(run_id: str) -> GuardrailContext:
    return GuardrailContext(
        run_id=run_id, workflow_id="wf_int_test",
        agent=AgentGuardrail(max_tokens_per_call=2_000, max_cycles=10),
        run=RunGuardrail(max_total_tokens=5_000, max_cycles_per_run=20),
    )


@pytest.fixture
async def real_guardrail():
    settings = GuardrailSettings(
        redis_url=REDIS_URL,
        fail_mode="strict",
        default_reservation_ttl_ms=5_000,
    )
    g = RedisGuardrail(settings=settings)
    try:
        await g.start()
    except GuardrailUnavailable:
        pytest.skip(f"Redis not reachable at {REDIS_URL}")

    # Wipe leftover guardrail state from previous runs so per-agent
    # cumulative counters don't leak across tests sharing an agent_id.
    await _flush_guardrail_keys(g, settings.namespace_prefix)

    yield g

    # Be a good citizen on the way out as well.
    await _flush_guardrail_keys(g, settings.namespace_prefix)
    await g.stop()


async def _flush_guardrail_keys(g: RedisGuardrail, prefix: str) -> None:
    redis = g._redis  # type: ignore[attr-defined]
    if redis is None:
        return
    cursor = 0
    while True:
        cursor, batch = await redis.scan(
            cursor=cursor, match=f"{prefix}:*", count=200,
        )
        if batch:
            await redis.delete(*batch)
        if cursor == 0:
            break


class TestRealRedisRoundTrip:
    async def test_precheck_consume_finalize(self, real_guardrail) -> None:
        ctx = _make_ctx("run_int_001")
        await real_guardrail.init_run_quota(ctx)

        rsv = await real_guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=300,
        )
        await real_guardrail.consume(rsv, actual_tokens=270, actual_cost=0.01)
        usage = await real_guardrail.finalize_run(ctx.run_id)

        assert usage.used_tokens == 270
        assert usage.used_cycles == 1
        assert abs(usage.used_cost_usd - 0.01) < 1e-6
        assert not usage.quota_exhausted

    async def test_real_redis_caps_enforce(self, real_guardrail) -> None:
        """Run cap enforcement — drive consumption up to Run cap with
        calls that each respect the per-call agent cap (Doc07 §2.1).
        """
        ctx = _make_ctx("run_int_002")
        await real_guardrail.init_run_quota(ctx)

        # Run cap = 5_000, agent per-call cap = 2_000 (from _make_ctx).
        # Each precheck stays under per-call (1_500 < 2_000), and three
        # calls cumulatively use 4_500 — still under Run cap.
        for _ in range(3):
            rsv = await real_guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=1_500,
            )
            await real_guardrail.consume(rsv, actual_tokens=1_500)

        # The 4th call would push Run usage to 6_000 > 5_000 — Run cap
        # trips before per-call cap (each call is still 1_500 < 2_000).
        with pytest.raises(QuotaExceeded) as exc:
            await real_guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=1_500,
            )
        assert exc.value.layer == "run"
        assert exc.value.dim == "tokens"

    async def test_per_call_agent_cap_blocks_oversize(self, real_guardrail) -> None:
        """Single oversize call (> agent_max_tokens_per_call) is rejected
        even when Run budget would allow it.
        """
        ctx = _make_ctx("run_int_003")
        await real_guardrail.init_run_quota(ctx)

        # 3_000 > agent per-call cap of 2_000, even though run cap (5_000) has room.
        with pytest.raises(QuotaExceeded) as exc:
            await real_guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=3_000,
            )
        assert exc.value.layer == "agent"
        assert exc.value.dim == "tokens"
