"""Unit tests for ``LLMGatewayClient`` using the MockProvider."""

from __future__ import annotations

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
from agentkit.llm.retry import RetryPolicy
from tests.helpers.mock_provider import MockProvider


# ---------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------


@pytest.fixture
def deterministic_policy() -> RetryPolicy:
    """Tiny backoffs so tests run fast."""
    return RetryPolicy(
        max_attempts_5xx=3,
        max_attempts_429=3,
        max_attempts_timeout=1,
        max_attempts_unknown=2,
        base_backoff_ms_5xx=1,
        base_backoff_ms_429=1,
        base_backoff_ms_unknown=1,
        max_backoff_ms=5,
        backoff_factor=2.0,
        jitter=False,
    )


@pytest.fixture
def gateway_factory(deterministic_policy):
    """Helper to build a Gateway with a custom set of providers/bindings."""

    def _make(
        providers: dict[str, MockProvider],
        *,
        bindings: dict[str, LLMBinding] | None = None,
    ) -> tuple[LLMGatewayClient, NoOpGuardrail]:
        guard = NoOpGuardrail()
        gw = LLMGatewayClient(
            providers=providers,  # type: ignore[arg-type]
            bindings=bindings or {},
            guardrail=guard,
            retry_policy=deterministic_policy,
        )
        return gw, guard

    return _make


def _user_request(*, provider: str = "openai", model: str = "gpt-4o") -> LLMRequest:
    return LLMRequest(
        provider=provider,
        model=model,
        messages=[ChatMessage(role="user", content="hello")],
        run_id_="run_test",
        agent_id_="agt_test",
        timeout_ms=30_000,
    )


# ---------------------------------------------------------------
# Bind resolve
# ---------------------------------------------------------------


class TestBindResolve:
    async def test_explicit_provider_and_model(self, gateway_factory) -> None:
        prov = MockProvider("openai").queue_text("ok")
        gw, _ = gateway_factory({"openai": prov})

        rsp = await gw.complete(_user_request())
        assert rsp.text == "ok"
        assert prov.calls[0].model == "gpt-4o"

    async def test_unknown_provider_rejected(self, gateway_factory) -> None:
        prov = MockProvider("openai")
        gw, _ = gateway_factory({"openai": prov})

        with pytest.raises(LLMError) as ei:
            await gw.complete(
                LLMRequest(
                    provider="ghost",
                    model="x",
                    messages=[ChatMessage(role="user", content="hi")],
                ),
            )
        assert ei.value.klass is LLMErrorClass.INVALID_REQUEST

    async def test_binding_with_fallback_chain(self, gateway_factory) -> None:
        primary = MockProvider("openai")
        primary.queue_error(LLMError(LLMErrorClass.PROVIDER_DOWN, "down", provider="openai"))
        backup = MockProvider("qwen").queue_text("from qwen")

        bindings = {
            "researcher": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory(
            {"openai": primary, "qwen": backup}, bindings=bindings,
        )

        rsp = await gw.complete(
            LLMRequest(
                binding="researcher",
                messages=[ChatMessage(role="user", content="hi")],
                run_id_="run_t",
                agent_id_="agt_t",
            ),
        )
        assert rsp.text == "from qwen"
        assert rsp.provider == "qwen"

    async def test_binding_unknown(self, gateway_factory) -> None:
        gw, _ = gateway_factory({"openai": MockProvider("openai")})
        with pytest.raises(LLMError) as ei:
            await gw.complete(
                LLMRequest(
                    binding="missing",
                    messages=[ChatMessage(role="user", content="hi")],
                ),
            )
        assert ei.value.klass is LLMErrorClass.INVALID_REQUEST

    async def test_request_without_provider_or_binding_rejected(
        self, gateway_factory,
    ) -> None:
        gw, _ = gateway_factory({"openai": MockProvider("openai")})
        with pytest.raises(LLMError):
            await gw.complete(
                LLMRequest(messages=[ChatMessage(role="user", content="hi")]),
            )


# ---------------------------------------------------------------
# Retry behaviour
# ---------------------------------------------------------------


class TestRetry:
    async def test_5xx_retries_then_succeeds(self, gateway_factory) -> None:
        prov = MockProvider("openai")
        prov.queue_error(
            LLMError(LLMErrorClass.TRANSIENT_5XX, "boom 1", provider="openai"),
        )
        prov.queue_error(
            LLMError(LLMErrorClass.TRANSIENT_5XX, "boom 2", provider="openai"),
        )
        prov.queue_text("eventually ok")

        gw, _ = gateway_factory({"openai": prov})
        rsp = await gw.complete(_user_request())
        assert rsp.text == "eventually ok"
        assert rsp.attempts == 3
        assert len(prov.calls) == 3

    async def test_5xx_exhausts_retries_then_fails(self, gateway_factory) -> None:
        prov = MockProvider("openai")
        for i in range(5):
            prov.queue_error(
                LLMError(
                    LLMErrorClass.TRANSIENT_5XX,
                    f"boom {i}",
                    provider="openai",
                ),
            )
        gw, _ = gateway_factory({"openai": prov})

        with pytest.raises(LLMError) as ei:
            await gw.complete(_user_request())
        assert ei.value.klass is LLMErrorClass.TRANSIENT_5XX

    async def test_non_retryable_error_propagates_immediately(
        self, gateway_factory,
    ) -> None:
        prov = MockProvider("openai")
        prov.queue_error(LLMError(LLMErrorClass.AUTH, "401", provider="openai"))
        prov.queue_text("never reached")

        gw, _ = gateway_factory({"openai": prov})
        with pytest.raises(LLMError) as ei:
            await gw.complete(_user_request())
        assert ei.value.klass is LLMErrorClass.AUTH
        assert len(prov.calls) == 1


# ---------------------------------------------------------------
# Provider fallback
# ---------------------------------------------------------------


class TestProviderFallback:
    async def test_quota_exceeded_advances_to_fallback_provider(
        self, gateway_factory,
    ) -> None:
        primary = MockProvider("openai")
        primary.queue_error(
            LLMError(LLMErrorClass.QUOTA_EXCEEDED, "out of quota", provider="openai"),
        )
        backup = MockProvider("qwen").queue_text("via fallback")

        bindings = {
            "b": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory(
            {"openai": primary, "qwen": backup}, bindings=bindings,
        )
        rsp = await gw.complete(
            LLMRequest(
                binding="b",
                messages=[ChatMessage(role="user", content="hi")],
                run_id_="r",
                agent_id_="a",
            ),
        )
        assert rsp.text == "via fallback"
        assert rsp.provider == "qwen"

    async def test_all_providers_fail_returns_last_error(
        self, gateway_factory,
    ) -> None:
        primary = MockProvider("openai")
        primary.queue_error(LLMError(LLMErrorClass.PROVIDER_DOWN, "down", provider="openai"))
        backup = MockProvider("qwen")
        backup.queue_error(
            LLMError(LLMErrorClass.PROVIDER_DOWN, "down", provider="qwen"),
        )

        bindings = {
            "b": LLMBinding(
                provider="openai",
                model="gpt-4o",
                fallback=[LLMBinding(provider="qwen", model="qwen-max")],
            ),
        }
        gw, _ = gateway_factory(
            {"openai": primary, "qwen": backup}, bindings=bindings,
        )

        with pytest.raises(LLMError) as ei:
            await gw.complete(
                LLMRequest(
                    binding="b",
                    messages=[ChatMessage(role="user", content="hi")],
                    run_id_="r",
                    agent_id_="a",
                ),
            )
        assert ei.value.klass is LLMErrorClass.PROVIDER_DOWN


# ---------------------------------------------------------------
# Guardrail integration (via NoOpGuardrail)
# ---------------------------------------------------------------


class TestGuardrailIntegration:
    async def test_consume_records_actual_tokens_and_cost(
        self, gateway_factory,
    ) -> None:
        prov = MockProvider("openai").queue_text(
            "ok",
            prompt_tokens=120,
            completion_tokens=15,
            cost=0.003,
        )
        gw, guard = gateway_factory({"openai": prov})

        await gw.complete(_user_request())
        # NoOpGuardrail tracks totals so we can assert.
        assert guard.used_tokens("run_test") == 135
        assert guard.used_cost("run_test") == pytest.approx(0.003)
        assert guard.used_cycles("run_test") == 1

    async def test_failure_does_not_consume_tokens(
        self, gateway_factory,
    ) -> None:
        prov = MockProvider("openai")
        prov.queue_error(LLMError(LLMErrorClass.AUTH, "401", provider="openai"))
        gw, guard = gateway_factory({"openai": prov})

        with pytest.raises(LLMError):
            await gw.complete(_user_request())
        assert guard.used_tokens("run_test") == 0


# ---------------------------------------------------------------
# Convenience entry points
# ---------------------------------------------------------------


class TestChatShortcut:
    async def test_chat_returns_text_only(self, gateway_factory) -> None:
        prov = MockProvider("openai").queue_text("hello back")
        gw, _ = gateway_factory({"openai": prov})

        text = await gw.chat("hi", provider="openai", model="gpt-4o")
        assert text == "hello back"

    async def test_count_tokens_offline(self, gateway_factory) -> None:
        prov = MockProvider("openai")
        gw, _ = gateway_factory({"openai": prov})
        n = gw.count_tokens("some text", provider="openai", model="gpt-4o")
        assert n > 0
