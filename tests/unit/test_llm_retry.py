"""Unit tests for the retry decision policy."""

from __future__ import annotations

import pytest

from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.retry import RetryPolicy, decide_retry


@pytest.fixture
def policy() -> RetryPolicy:
    """Deterministic policy: small budget, no jitter."""
    return RetryPolicy(
        max_attempts_5xx=3,
        max_attempts_429=3,
        max_attempts_timeout=1,
        max_attempts_unknown=2,
        base_backoff_ms_5xx=10,
        base_backoff_ms_429=20,
        base_backoff_ms_unknown=10,
        max_backoff_ms=1_000,
        backoff_factor=2.0,
        jitter=False,
    )


class TestNonRetryable:
    @pytest.mark.parametrize(
        "klass",
        [
            LLMErrorClass.AUTH,
            LLMErrorClass.INVALID_REQUEST,
            LLMErrorClass.CONTENT_FILTER,
            LLMErrorClass.QUOTA_EXCEEDED,
            LLMErrorClass.PROVIDER_DOWN,
        ],
    )
    def test_non_retryable_classes(self, policy: RetryPolicy, klass) -> None:
        d = decide_retry(LLMError(klass, "x"), attempt=1, policy=policy)
        assert d.should_retry is False
        assert d.wait_ms == 0


class TestRetryableBudget:
    def test_5xx_within_budget(self, policy: RetryPolicy) -> None:
        d = decide_retry(
            LLMError(LLMErrorClass.TRANSIENT_5XX, "x"),
            attempt=1,
            policy=policy,
        )
        assert d.should_retry is True
        assert d.wait_ms == 10  # base, attempt 1 -> base_ms * factor^0

    def test_5xx_backoff_grows(self, policy: RetryPolicy) -> None:
        d2 = decide_retry(
            LLMError(LLMErrorClass.TRANSIENT_5XX, "x"),
            attempt=2,
            policy=policy,
        )
        assert d2.wait_ms == 20  # 10 * 2

    def test_5xx_budget_exhausted(self, policy: RetryPolicy) -> None:
        d = decide_retry(
            LLMError(LLMErrorClass.TRANSIENT_5XX, "x"),
            attempt=3,  # >= max_attempts_5xx
            policy=policy,
        )
        assert d.should_retry is False

    def test_timeout_one_retry_only(self, policy: RetryPolicy) -> None:
        d1 = decide_retry(
            LLMError(LLMErrorClass.TIMEOUT, "x"),
            attempt=0,
            policy=policy,
        )
        assert d1.should_retry is True
        assert d1.wait_ms == 0

        d2 = decide_retry(
            LLMError(LLMErrorClass.TIMEOUT, "x"),
            attempt=1,
            policy=policy,
        )
        assert d2.should_retry is False


class TestRateLimitRetryAfter:
    def test_uses_provider_retry_after_when_present(
        self, policy: RetryPolicy,
    ) -> None:
        err = LLMError(
            LLMErrorClass.RATE_LIMIT_429,
            "x",
            retry_after_ms=5_000,
        )
        d = decide_retry(err, attempt=1, policy=policy)
        assert d.should_retry is True
        # base_ms is overridden by retry_after_ms; first attempt uses base.
        assert d.wait_ms == 5_000

    def test_falls_back_to_default_backoff(self, policy: RetryPolicy) -> None:
        err = LLMError(LLMErrorClass.RATE_LIMIT_429, "x")
        d = decide_retry(err, attempt=1, policy=policy)
        assert d.should_retry is True
        assert d.wait_ms == 20  # policy default


class TestBackoffCap:
    def test_cap_applies(self, policy: RetryPolicy) -> None:
        # With base 10ms and factor 2, attempt 30 would be huge —
        # cap should clamp it to max_backoff_ms.
        err = LLMError(LLMErrorClass.TRANSIENT_5XX, "x")
        # This attempt is past max_attempts; force a fresh policy with
        # a huge budget so we still see the cap check.
        big = RetryPolicy(
            max_attempts_5xx=100,
            base_backoff_ms_5xx=10,
            max_backoff_ms=500,
            backoff_factor=2.0,
            jitter=False,
        )
        d = decide_retry(err, attempt=20, policy=big)
        assert d.should_retry is True
        assert d.wait_ms <= big.max_backoff_ms


def test_jitter_keeps_wait_within_bounds() -> None:
    """With jitter enabled wait_ms should be in [base/2, base]."""
    policy = RetryPolicy(base_backoff_ms_5xx=100, jitter=True)
    waits = [
        decide_retry(
            LLMError(LLMErrorClass.TRANSIENT_5XX, "x"),
            attempt=1,
            policy=policy,
        ).wait_ms
        for _ in range(50)
    ]
    assert all(50 <= w <= 100 for w in waits)
    # Saw at least 2 distinct values — proves jitter actually fires.
    assert len(set(waits)) > 1
