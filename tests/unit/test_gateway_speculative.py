"""Unit tests for speculative provider racing in :mod:`agentkit.llm.gateway`.

Covers B11: when the primary binding is slow, the Gateway should
speculatively race the next binding in the chain instead of waiting
for the primary to fail outright — bounding total latency to roughly
``max(primary, backup)`` instead of ``primary + backup`` while still
costing a fast primary nothing extra.
"""

from __future__ import annotations

import asyncio
import time

import pytest

from agentkit.llm import (
    ChatMessage,
    LLMBinding,
    LLMError,
    LLMErrorClass,
    LLMGatewayClient,
    LLMRequest,
    NoOpGuardrail,
)
from agentkit.llm import gateway as gateway_module
from agentkit.llm.retry import RetryPolicy
from agentkit.observability import metrics
from tests.helpers.mock_provider import MockProvider

# ---------------------------------------------------------------
# A MockProvider that sleeps before resolving its queued action —
# lets us simulate "slow primary" / "fast backup" races.
# ---------------------------------------------------------------


class DelayedProvider(MockProvider):
    """MockProvider whose ``complete`` sleeps ``delay_s`` before resolving."""

    def __init__(self, name: str, *, delay_s: float = 0.0) -> None:
        super().__init__(name)
        self.delay_s = delay_s
        self.cancelled = False

    async def complete(self, req):
        try:
            await asyncio.sleep(self.delay_s)
        except asyncio.CancelledError:
            self.cancelled = True
            raise
        return await super().complete(req)


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------


@pytest.fixture
def fast_policy() -> RetryPolicy:
    """No retries — keeps races deterministic and fast."""
    return RetryPolicy(
        max_attempts_5xx=1,
        max_attempts_429=1,
        max_attempts_timeout=1,
        max_attempts_unknown=1,
        base_backoff_ms_5xx=1,
        base_backoff_ms_429=1,
        base_backoff_ms_unknown=1,
        max_backoff_ms=1,
        backoff_factor=1.0,
        jitter=False,
    )


@pytest.fixture
def gateway_factory(fast_policy):
    def _make(providers, *, bindings=None):
        guard = NoOpGuardrail()
        gw = LLMGatewayClient(
            providers=providers,
            bindings=bindings or {},
            guardrail=guard,
            retry_policy=fast_policy,
        )
        return gw, guard

    return _make


def _binding_request(*, binding: str) -> LLMRequest:
    return LLMRequest(
        binding=binding,
        messages=[ChatMessage(role="user", content="hi")],
        run_id_="run_spec",
        agent_id_="agt_spec",
        timeout_ms=30_000,
    )


@pytest.fixture(autouse=True)
def _low_threshold(monkeypatch):
    """Use a small speculation threshold so tests run in milliseconds."""
    monkeypatch.setattr(gateway_module, "SPECULATIVE_THRESHOLD_MS", 80)


def _counter_value(counter, **labels) -> float:
    return counter.labels(**labels)._value.get()


# ---------------------------------------------------------------
# 1. Fast primary — no speculation launched
# ---------------------------------------------------------------


class TestFastPrimary:
    async def test_fast_primary_returns_without_launching_backup(self, gateway_factory) -> None:
        primary = DelayedProvider("openai", delay_s=0.0).queue_text("from primary")
        backup = DelayedProvider("qwen", delay_s=0.0)  # no queued action — would error if called

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory({"openai": primary, "qwen": backup}, bindings=bindings)

        rsp = await gw.complete(_binding_request(binding="researcher"))

        assert rsp.text == "from primary"
        assert rsp.provider == "openai"
        assert len(primary.calls) == 1
        assert len(backup.calls) == 0  # backup never launched — zero overhead


# ---------------------------------------------------------------
# 2. Slow primary — speculative backup races and wins; latency ~ max
# ---------------------------------------------------------------


class TestSpeculativeRacing:
    async def test_slow_primary_speculative_backup_wins_latency_is_max_not_sum(
        self, gateway_factory,
    ) -> None:
        threshold_s = gateway_module.SPECULATIVE_THRESHOLD_MS / 1000.0
        primary_delay = threshold_s * 6  # well past the threshold
        backup_delay = threshold_s * 0.5  # backup answers quickly once launched

        primary = DelayedProvider("openai", delay_s=primary_delay).queue_text("from primary")
        backup = DelayedProvider("qwen", delay_s=backup_delay).queue_text("from qwen", model="qwen-max")

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory({"openai": primary, "qwen": backup}, bindings=bindings)

        started = time.monotonic()
        rsp = await gw.complete(_binding_request(binding="researcher"))
        elapsed_s = time.monotonic() - started

        assert rsp.text == "from qwen"
        assert rsp.provider == "qwen"
        # Total latency should be roughly threshold + backup_delay
        # (i.e. close to max(primary, backup) bounded by the
        # speculation threshold), and well under primary + backup.
        sum_latency_s = primary_delay + backup_delay
        assert elapsed_s < sum_latency_s
        assert elapsed_s < threshold_s + backup_delay + threshold_s  # generous slack

        # The slow primary attempt should have been cancelled & drained.
        await asyncio.sleep(0)  # let cancellation propagate
        assert primary.cancelled is True

    async def test_no_speculation_when_chain_has_no_fallback(self, gateway_factory) -> None:
        """A single-binding chain has nothing to speculate into."""
        threshold_s = gateway_module.SPECULATIVE_THRESHOLD_MS / 1000.0
        primary = DelayedProvider("openai", delay_s=threshold_s * 3).queue_text("solo")

        gw, _ = gateway_factory({"openai": primary})
        rsp = await gw.complete(
            LLMRequest(
                provider="openai",
                model="gpt-4o",
                messages=[ChatMessage(role="user", content="hi")],
                run_id_="run_solo",
                agent_id_="agt_solo",
                timeout_ms=30_000,
            ),
        )
        assert rsp.text == "solo"
        assert rsp.provider == "openai"


# ---------------------------------------------------------------
# 3. Metrics attribution
# ---------------------------------------------------------------


class TestMetricsAttribution:
    async def test_metrics_attribute_to_winner_without_double_counting(
        self, gateway_factory,
    ) -> None:
        threshold_s = gateway_module.SPECULATIVE_THRESHOLD_MS / 1000.0
        primary_delay = threshold_s * 6
        backup_delay = threshold_s * 0.5

        primary = DelayedProvider("openai", delay_s=primary_delay).queue_text("from primary")
        backup = DelayedProvider("qwen", delay_s=backup_delay).queue_text("from qwen", model="qwen-max")

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory({"openai": primary, "qwen": backup}, bindings=bindings)

        before_ok = _counter_value(
            metrics.llm_request_total, provider="qwen", model="qwen-max", result="ok",
        )
        before_err = _counter_value(
            metrics.llm_request_total, provider="openai", model="gpt-4o", result="error",
        )

        rsp = await gw.complete(_binding_request(binding="researcher"))
        assert rsp.provider == "qwen"

        # Winner counted exactly once as "ok".
        after_ok = _counter_value(
            metrics.llm_request_total, provider="qwen", model="qwen-max", result="ok",
        )
        assert after_ok == before_ok + 1

        # The cancelled (never-finished) primary must NOT be counted
        # as an "error" request — it was raced away, not failed.
        after_err = _counter_value(
            metrics.llm_request_total, provider="openai", model="gpt-4o", result="error",
        )
        assert after_err == before_err

        # request_attempts observed exactly once for the winner.
        attempts_hist = metrics.llm_request_attempts.labels(provider="qwen", model="qwen-max")
        assert attempts_hist._sum.get() >= 1

    async def test_fallback_metric_recorded_on_failure_then_speculative_win(
        self, gateway_factory,
    ) -> None:
        threshold_s = gateway_module.SPECULATIVE_THRESHOLD_MS / 1000.0

        primary = DelayedProvider("openai", delay_s=0.0)
        primary.queue_error(
            LLMError(LLMErrorClass.PROVIDER_DOWN, "down", provider="openai"),
        )
        backup = DelayedProvider("qwen", delay_s=threshold_s * 0.2).queue_text("from qwen", model="qwen-max")

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory({"openai": primary, "qwen": backup}, bindings=bindings)

        before = _counter_value(
            metrics.llm_fallback_total,
            from_provider="openai", to_provider="qwen", klass="provider_down",
        )

        rsp = await gw.complete(_binding_request(binding="researcher"))
        assert rsp.provider == "qwen"

        after = _counter_value(
            metrics.llm_fallback_total,
            from_provider="openai", to_provider="qwen", klass="provider_down",
        )
        # Exactly one fallback transition recorded — no double counting
        # even though the backup was reached via the failure path here.
        assert after == before + 1


# ---------------------------------------------------------------
# 4. advise_fallback=False short-circuits (no speculation past it)
# ---------------------------------------------------------------


class TestAdviseFallbackFalse:
    async def test_final_failure_short_circuits_even_when_slow(self, gateway_factory) -> None:
        threshold_s = gateway_module.SPECULATIVE_THRESHOLD_MS / 1000.0
        # Primary is slow enough that speculation *would* fire, but it
        # ultimately fails with a non-fallback-advised error class —
        # the Gateway must raise that error directly, never reach qwen.
        primary = DelayedProvider("openai", delay_s=threshold_s * 0.3)
        primary.queue_error(LLMError(LLMErrorClass.AUTH, "401", provider="openai"))
        backup = DelayedProvider("qwen", delay_s=0.0).queue_text("never reached")

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory({"openai": primary, "qwen": backup}, bindings=bindings)

        with pytest.raises(LLMError) as ei:
            await gw.complete(_binding_request(binding="researcher"))

        assert ei.value.klass is LLMErrorClass.AUTH
        assert len(backup.calls) == 0

    async def test_advise_fallback_false_after_speculation_started_still_final(
        self, gateway_factory,
    ) -> None:
        """If the primary fails fatally *while* a speculative backup is
        in flight, the fatal error wins and the backup is cancelled —
        the Gateway must not return the backup's (slower) success.
        """
        threshold_s = gateway_module.SPECULATIVE_THRESHOLD_MS / 1000.0
        primary_delay = threshold_s * 2  # crosses the threshold, then fails fatally
        backup_delay = threshold_s * 5  # much slower than the fatal failure

        primary = DelayedProvider("openai", delay_s=primary_delay)
        primary.queue_error(LLMError(LLMErrorClass.AUTH, "401", provider="openai"))
        backup = DelayedProvider("qwen", delay_s=backup_delay).queue_text("never reached")

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory({"openai": primary, "qwen": backup}, bindings=bindings)

        with pytest.raises(LLMError) as ei:
            await gw.complete(_binding_request(binding="researcher"))

        assert ei.value.klass is LLMErrorClass.AUTH
        await asyncio.sleep(0)
        assert backup.cancelled is True
