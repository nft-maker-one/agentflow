"""Unit tests for ``RedisGuardrail`` over fakeredis.

Exercises the Lua scripts end-to-end without spinning up a real
Redis. fakeredis ≥ 2.21 supports ``EVAL`` so all our atomic paths
work the same as production.
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit.guardrail import QuotaExceeded, Reservation
from tests.helpers.fakeredis_fixture import (  # noqa: F401 — fixtures
    fake_redis,
    guardrail,
    make_context,
)


# ----------------------------------------------------------------
# init_run_quota
# ----------------------------------------------------------------


class TestInitRunQuota:
    async def test_seeds_run_hash(self, guardrail, fake_redis) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        raw = await fake_redis.hgetall("guardrail:run:run_test_001")
        meta = {
            (k.decode() if isinstance(k, bytes) else k):
            (v.decode() if isinstance(v, bytes) else v)
            for k, v in (raw or {}).items()
        }
        assert meta["workflow_id"] == "wf_test"
        assert int(meta["limit_tokens"]) == 10_000
        assert int(meta["limit_cycles"]) == 10  # default in helper

    async def test_idempotent(self, guardrail) -> None:
        ctx = make_context()
        await guardrail.init_run_quota(ctx)
        # Second init must not raise.
        await guardrail.init_run_quota(ctx)


# ----------------------------------------------------------------
# precheck — happy path
# ----------------------------------------------------------------


class TestPrecheckHappyPath:
    async def test_first_call_allocates(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000, agent_max_tokens=2_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id,
            agent_id="agt_x",
            est_tokens=500,
        )
        assert isinstance(rsv, Reservation)
        assert rsv.est_tokens == 500
        assert rsv.workflow_id == "wf_test"

    async def test_usage_increments(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=100,
        )
        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.used_tokens == 100
        assert usage.used_cycles == 1


# ----------------------------------------------------------------
# precheck — limit enforcement
# ----------------------------------------------------------------


class TestPrecheckLimits:
    async def test_run_token_cap(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=1_000, agent_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        with pytest.raises(QuotaExceeded) as exc:
            await guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=2_000,
            )
        assert exc.value.layer == "run"
        assert exc.value.dim == "tokens"

    async def test_agent_token_cap(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=100_000, agent_max_tokens=500)
        await guardrail.init_run_quota(ctx)

        with pytest.raises(QuotaExceeded) as exc:
            await guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=600,
            )
        assert exc.value.layer == "agent"

    async def test_run_cycle_cap(self, guardrail) -> None:
        ctx = make_context(
            run_max_tokens=1_000_000,
            run_max_cycles=2,
            agent_max_tokens=1_000_000,
            agent_max_cycles=100,
        )
        await guardrail.init_run_quota(ctx)

        await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=1,
        )
        await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=1,
        )
        with pytest.raises(QuotaExceeded) as exc:
            await guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=1,
            )
        assert exc.value.dim == "cycles"
        assert exc.value.layer == "run"

    async def test_rejected_request_does_not_charge(
        self, guardrail, fake_redis,
    ) -> None:
        ctx = make_context(run_max_tokens=1_000)
        await guardrail.init_run_quota(ctx)

        with pytest.raises(QuotaExceeded):
            await guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=2_000,
            )
        # Read defensively — fakeredis may return bytes regardless of
        # decode_responses=True.
        used_raw = await fake_redis.hget(
            "guardrail:run:run_test_001", "tokens_used",
        )
        used = used_raw.decode() if isinstance(used_raw, bytes) else used_raw
        assert int(used or 0) == 0


# ----------------------------------------------------------------
# consume — adjust by delta
# ----------------------------------------------------------------


class TestConsume:
    async def test_actual_lt_est_refunds_tokens(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=500,
        )
        await guardrail.consume(rsv, actual_tokens=300)

        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.used_tokens == 300

    async def test_actual_gt_est_charges_more(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=200,
        )
        await guardrail.consume(rsv, actual_tokens=350)

        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.used_tokens == 350

    async def test_consume_overshoot_marks_exhausted(self, guardrail) -> None:
        # Set a tight cap; precheck just under it; LLM actually
        # returned more than expected → consume overshoots.
        ctx = make_context(run_max_tokens=1_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=900,
        )
        await guardrail.consume(rsv, actual_tokens=1_500)

        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.quota_exhausted
        # Subsequent precheck must be denied.
        with pytest.raises(QuotaExceeded) as exc:
            await guardrail.precheck(
                run_id=ctx.run_id, agent_id="agt", est_tokens=10,
            )
        assert exc.value.layer == "run"

    async def test_cost_is_informational(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=100,
        )
        await guardrail.consume(rsv, actual_tokens=100, actual_cost=0.0042)

        usage = await guardrail.get_run_usage(ctx.run_id)
        # 0.0042 USD ± rounding (we store cost*10000 as int).
        assert abs(usage.used_cost_usd - 0.0042) < 1e-6


# ----------------------------------------------------------------
# release — rollback path
# ----------------------------------------------------------------


class TestRelease:
    async def test_release_rolls_back_charge(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=500,
        )
        usage_before = await guardrail.get_run_usage(ctx.run_id)
        assert usage_before.used_tokens == 500

        await guardrail.release(rsv, reason="test")
        usage_after = await guardrail.get_run_usage(ctx.run_id)
        assert usage_after.used_tokens == 0
        assert usage_after.used_cycles == 0

    async def test_release_after_consume_is_noop(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)
        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=200,
        )
        await guardrail.consume(rsv, actual_tokens=200)

        # Idempotent — release after consume must NOT undo the consume.
        await guardrail.release(rsv, reason="late")

        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.used_tokens == 200


# ----------------------------------------------------------------
# Concurrent precheck — Lua atomicity
# ----------------------------------------------------------------


class TestConcurrency:
    async def test_concurrent_prechecks_dont_oversubscribe(
        self, guardrail,
    ) -> None:
        # Cap the run at 1000 tokens; give agent layer plenty of headroom
        # so it can't trip first (we want to exercise the run-token cap).
        ctx = make_context(
            run_max_tokens=1_000,
            run_max_cycles=1_000_000,
            agent_max_tokens=1_000_000,
            agent_max_cycles=1_000_000,
        )
        await guardrail.init_run_quota(ctx)

        async def attempt(i: int) -> bool:
            try:
                await guardrail.precheck(
                    run_id=ctx.run_id,
                    agent_id="agt",
                    est_tokens=100,
                )
                return True
            except QuotaExceeded:
                return False

        # 20 concurrent attempts, but cap is 1000 → only 10 should win.
        results = await asyncio.gather(*(attempt(i) for i in range(20)))
        wins = sum(1 for r in results if r)
        assert wins == 10  # exactly 10 × 100 = 1000

        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.used_tokens == 1_000


# ----------------------------------------------------------------
# Replay (dry_charge)
# ----------------------------------------------------------------


class TestReplay:
    async def test_dry_charge_skips_redis(
        self, guardrail, fake_redis,
    ) -> None:
        ctx = make_context(run_max_tokens=1_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt",
            est_tokens=500, dry_charge=True,
        )
        # Reservation returned but Redis untouched:
        used_raw = await fake_redis.hget(
            "guardrail:run:run_test_001", "tokens_used",
        )
        used = used_raw.decode() if isinstance(used_raw, bytes) else used_raw
        assert int(used or 0) == 0
        assert rsv.dry_charge is True

    async def test_dry_consume_is_noop(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=1_000)
        await guardrail.init_run_quota(ctx)
        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt",
            est_tokens=500, dry_charge=True,
        )
        await guardrail.consume(rsv, actual_tokens=300)

        usage = await guardrail.get_run_usage(ctx.run_id)
        assert usage.used_tokens == 0


# ----------------------------------------------------------------
# Lifecycle / RunUsage
# ----------------------------------------------------------------


class TestFinalize:
    async def test_finalize_returns_usage(self, guardrail) -> None:
        ctx = make_context(run_max_tokens=10_000)
        await guardrail.init_run_quota(ctx)

        rsv = await guardrail.precheck(
            run_id=ctx.run_id, agent_id="agt", est_tokens=200,
        )
        await guardrail.consume(rsv, actual_tokens=180, actual_cost=0.001)

        usage = await guardrail.finalize_run(ctx.run_id)
        assert usage.run_id == ctx.run_id
        assert usage.used_tokens == 180
        assert usage.used_cycles == 1
        assert usage.limits_tokens == 10_000


# ----------------------------------------------------------------
# Pre-check before init_run_quota → unavailable
# ----------------------------------------------------------------


class TestInvariants:
    async def test_precheck_before_init_raises(self, guardrail) -> None:
        from agentkit.guardrail import GuardrailUnavailable

        with pytest.raises(GuardrailUnavailable):
            await guardrail.precheck(
                run_id="never_inited", agent_id="agt", est_tokens=1,
            )
