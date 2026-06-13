"""Integration tests for the OpenAI provider against a real (or compatible) endpoint.

Skipped unless ``AGENTKIT_LLM_OPENAI_API_KEY`` (or the override
variant for a custom base_url) is set in the environment.

By default we hit ``https://api.openai.com/v1`` with ``gpt-4o-mini``
which is the cheapest + fastest OpenAI model suitable for tests as
of 2026 ($0.15 / 1M prompt tokens, $0.60 / 1M completion tokens —
roughly 100x cheaper than gpt-4o for non-quality-critical tests).

Override with::

    AGENTKIT_LLM_OPENAI_API_KEY=...
    AGENTKIT_LLM_OPENAI_BASE_URL=...     # e.g. for an OpenAI-compatible local
    AGENTKIT_TEST_LLM_MODEL=...          # e.g. ``deepseek-chat`` / ``qwen-turbo``

Recommended cheap-and-fast models for each compat preset:

* OpenAI:    ``gpt-4o-mini``          (default)
* DeepSeek:  ``deepseek-chat``        (very cheap, very fast)
* Qwen:      ``qwen-turbo``           (cheapest in the Qwen family)
* Local:     whatever your vLLM/Ollama runs
"""

from __future__ import annotations

import os

import pytest

from agentkit.llm import (
    ChatMessage,
    LLMGatewayClient,
    LLMRequest,
)
from agentkit.llm.providers import OpenAIProvider, OpenAIProviderConfig

API_KEY_ENV = "AGENTKIT_LLM_OPENAI_API_KEY"
BASE_URL_ENV = "AGENTKIT_LLM_OPENAI_BASE_URL"
MODEL_ENV = "AGENTKIT_TEST_LLM_MODEL"


def _has_credentials() -> bool:
    return bool(os.environ.get(API_KEY_ENV))


@pytest.fixture
def model() -> str:
    return os.environ.get(MODEL_ENV, "gpt-4o-mini")


@pytest.fixture
def gateway() -> LLMGatewayClient:
    if not _has_credentials():
        pytest.skip(f"{API_KEY_ENV} not set — set it to run LLM integration tests")
    base_url = os.environ.get(BASE_URL_ENV)
    config = OpenAIProviderConfig(
        instance_name="openai",
        compat="openai",
        base_url=base_url,
    )
    provider = OpenAIProvider(config)
    return LLMGatewayClient(providers={"openai": provider})


# ----------------------------------------------------------------
# Smoke tests
# ----------------------------------------------------------------


async def test_health_reports_ok(gateway: LLMGatewayClient) -> None:
    # Pull the underlying provider — we exposed it through the
    # gateway's internal map; a small adapter for tests is fine.
    prov: OpenAIProvider = gateway._providers["openai"]  # type: ignore[assignment, attr-defined]
    health = await prov.health()
    assert health.healthy, health.detail


async def test_simple_chat_completion(gateway: LLMGatewayClient, model: str) -> None:
    text = await gateway.chat(
        "Reply with exactly the word 'pong' (no punctuation).",
        provider="openai",
        model=model,
        max_tokens=4,
        temperature=0.0,
    )
    assert "pong" in text.lower()


async def test_usage_and_cost_present(gateway: LLMGatewayClient, model: str) -> None:
    rsp = await gateway.complete(
        LLMRequest(
            provider="openai",
            model=model,
            messages=[ChatMessage(role="user", content="Hi")],
            max_tokens=10,
            temperature=0.0,
            run_id_="run_int",
            agent_id_="agt_int",
        ),
    )
    assert rsp.usage.prompt_tokens > 0
    assert rsp.usage.completion_tokens > 0
    assert rsp.usage.total_tokens >= rsp.usage.prompt_tokens
    # Cost may be 0 for unknown models — assert non-negative only.
    assert rsp.cost_usd >= 0
