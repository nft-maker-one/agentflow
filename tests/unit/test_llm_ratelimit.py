"""Unit tests for the in-process token-bucket RateLimiter."""

from __future__ import annotations

import time

import pytest

from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.models import RateLimit
from agentkit.llm.ratelimit import RateLimiter


class TestNoOp:
    async def test_none_rate_limit_is_no_op(self) -> None:
        rl = RateLimiter()
        # Should return immediately with no exception.
        await rl.acquire(
            None,
            provider="p",
            model="m",
            agent_tag=None,
            tokens_estimated=100,
            budget_ms=1_000,
        )

    async def test_zero_rpm_and_zero_tpm_is_no_op(self) -> None:
        rl = RateLimiter()
        await rl.acquire(
            RateLimit(rpm=0, tpm=0),
            provider="p",
            model="m",
            agent_tag=None,
            tokens_estimated=10_000,
            budget_ms=1_000,
        )


class TestRpmFailFast:
    async def test_first_request_passes_then_fails_fast(self) -> None:
        rl = RateLimiter()
        rate = RateLimit(rpm=1, tpm=0, strategy="fail_fast")

        await rl.acquire(
            rate,
            provider="p",
            model="m",
            agent_tag=None,
            tokens_estimated=0,
            budget_ms=1_000,
        )
        with pytest.raises(LLMError) as ei:
            await rl.acquire(
                rate,
                provider="p",
                model="m",
                agent_tag=None,
                tokens_estimated=0,
                budget_ms=1_000,
            )
        assert ei.value.klass is LLMErrorClass.RATE_LIMIT_429


class TestRpmWaitStrategy:
    async def test_wait_within_budget_succeeds(self) -> None:
        # capacity=60 per minute => one token per second; 2 acquisitions
        # within ~1.1s should succeed when strategy is wait.
        rl = RateLimiter()
        rate = RateLimit(rpm=60, tpm=0, strategy="wait")

        t0 = time.monotonic()
        for _ in range(2):
            await rl.acquire(
                rate,
                provider="p",
                model="m",
                agent_tag=None,
                tokens_estimated=0,
                budget_ms=2_000,
            )
        elapsed = time.monotonic() - t0
        # Both should complete; second may briefly wait, but well under
        # the 2s budget on any reasonable hardware.
        assert elapsed < 2.0

    async def test_wait_exceeding_budget_raises(self) -> None:
        rl = RateLimiter()
        rate = RateLimit(rpm=1, tpm=0, strategy="wait")

        # Drain the bucket
        await rl.acquire(
            rate,
            provider="p",
            model="m",
            agent_tag=None,
            tokens_estimated=0,
            budget_ms=1_000,
        )
        # 2nd call would need ~60s; budget 200ms forces a fail.
        with pytest.raises(LLMError) as ei:
            await rl.acquire(
                rate,
                provider="p",
                model="m",
                agent_tag=None,
                tokens_estimated=0,
                budget_ms=200,
            )
        assert ei.value.klass is LLMErrorClass.RATE_LIMIT_429


class TestTpmIndependence:
    async def test_tpm_drained_blocks_even_when_rpm_open(self) -> None:
        rl = RateLimiter()
        rate = RateLimit(rpm=1_000, tpm=100, strategy="fail_fast")

        await rl.acquire(
            rate,
            provider="p",
            model="m",
            agent_tag=None,
            tokens_estimated=100,
            budget_ms=1_000,
        )
        # tpm now empty; rpm still has plenty left, but acquire blocks anyway.
        with pytest.raises(LLMError):
            await rl.acquire(
                rate,
                provider="p",
                model="m",
                agent_tag=None,
                tokens_estimated=1,
                budget_ms=1_000,
            )


class TestKeyIsolation:
    async def test_different_keys_have_independent_buckets(self) -> None:
        rl = RateLimiter()
        rate = RateLimit(rpm=1, tpm=0, strategy="fail_fast")

        await rl.acquire(
            rate,
            provider="a",
            model="m",
            agent_tag=None,
            tokens_estimated=0,
            budget_ms=1_000,
        )
        # Different provider — fresh bucket.
        await rl.acquire(
            rate,
            provider="b",
            model="m",
            agent_tag=None,
            tokens_estimated=0,
            budget_ms=1_000,
        )
        # Different agent_tag — also fresh bucket.
        await rl.acquire(
            rate,
            provider="a",
            model="m",
            agent_tag="zh",
            tokens_estimated=0,
            budget_ms=1_000,
        )
