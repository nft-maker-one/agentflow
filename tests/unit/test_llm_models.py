"""Unit tests for the LLM data models."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from agentkit.llm.errors import (
    FALLBACK_ADVISED_CLASSES,
    RETRYABLE_CLASSES,
    LLMError,
    LLMErrorClass,
)
from agentkit.llm.models import (
    ChatMessage,
    LLMBinding,
    LLMRequest,
    RateLimit,
    TokenUsage,
)
from agentkit.llm.tools import ToolCall, ToolChoice, ToolSchema


class TestErrorTaxonomy:
    @pytest.mark.parametrize(
        ("klass", "expected_retryable"),
        [
            (LLMErrorClass.TRANSIENT_5XX, True),
            (LLMErrorClass.RATE_LIMIT_429, True),
            (LLMErrorClass.TIMEOUT, True),
            (LLMErrorClass.UNKNOWN, True),
            (LLMErrorClass.AUTH, False),
            (LLMErrorClass.INVALID_REQUEST, False),
            (LLMErrorClass.CONTENT_FILTER, False),
            (LLMErrorClass.QUOTA_EXCEEDED, False),
            (LLMErrorClass.PROVIDER_DOWN, False),
        ],
    )
    def test_retryable_property(self, klass, expected_retryable: bool) -> None:
        err = LLMError(klass, "x")
        assert err.retryable is expected_retryable
        assert (klass in RETRYABLE_CLASSES) is expected_retryable

    @pytest.mark.parametrize(
        ("klass", "expected_fallback"),
        [
            (LLMErrorClass.QUOTA_EXCEEDED, True),
            (LLMErrorClass.PROVIDER_DOWN, True),
            (LLMErrorClass.TRANSIENT_5XX, True),
            (LLMErrorClass.AUTH, False),
            (LLMErrorClass.INVALID_REQUEST, False),
            (LLMErrorClass.CONTENT_FILTER, False),
        ],
    )
    def test_advise_fallback_property(self, klass, expected_fallback: bool) -> None:
        err = LLMError(klass, "x")
        assert err.advise_fallback is expected_fallback
        assert (klass in FALLBACK_ADVISED_CLASSES) is expected_fallback

    def test_repr_includes_class_and_message(self) -> None:
        err = LLMError(
            LLMErrorClass.AUTH,
            "bad key",
            provider="openai",
            model="gpt-4o",
            http_status=401,
        )
        text = repr(err)
        assert "auth" in text
        assert "openai" in text
        assert "gpt-4o" in text


class TestChatMessage:
    def test_minimal(self) -> None:
        m = ChatMessage(role="user", content="hi")
        assert m.role == "user"
        assert m.content == "hi"

    def test_rejects_unknown_role(self) -> None:
        with pytest.raises(ValidationError):
            ChatMessage(role="boss", content="x")  # type: ignore[arg-type]

    def test_tool_call_attached(self) -> None:
        m = ChatMessage(
            role="assistant",
            content="",
            tool_calls=[ToolCall(id="t1", name="search", arguments={"q": "x"})],
        )
        assert m.tool_calls[0].name == "search"


class TestLLMRequest:
    def test_round_trip_json(self) -> None:
        req = LLMRequest(
            provider="openai",
            model="gpt-4o",
            messages=[ChatMessage(role="user", content="hi")],
            tools=[
                ToolSchema(
                    name="search",
                    description="...",
                    parameters={"type": "object", "properties": {}},
                ),
            ],
        )
        raw = req.model_dump(mode="json")
        round_tripped = LLMRequest.model_validate(raw)
        assert round_tripped.provider == "openai"
        assert round_tripped.tools[0].name == "search"

    def test_extra_field_rejected(self) -> None:
        with pytest.raises(ValidationError):
            LLMRequest(
                model="gpt-4o",
                messages=[],
                bogus_field=1,  # type: ignore[call-arg]
            )

    def test_default_tool_choice_is_auto(self) -> None:
        req = LLMRequest(
            provider="openai",
            model="gpt-4o",
            messages=[ChatMessage(role="user", content="hi")],
        )
        assert req.tool_choice.root == "auto"


class TestToolChoice:
    def test_keyword(self) -> None:
        tc = ToolChoice("required")
        assert tc.is_named is False
        assert tc.name is None

    def test_named(self) -> None:
        tc = ToolChoice({"name": "search_web"})
        assert tc.is_named is True
        assert tc.name == "search_web"

    def test_invalid_keyword_rejected(self) -> None:
        with pytest.raises(ValidationError):
            ToolChoice("nope")

    def test_named_round_trip_json(self) -> None:
        tc = ToolChoice({"name": "x"})
        s = tc.model_dump_json()
        rebuilt = ToolChoice.model_validate(json.loads(s))
        assert rebuilt.name == "x"


class TestLLMBinding:
    def test_recursive_fallback(self) -> None:
        primary = LLMBinding(
            provider="openai",
            model="gpt-4o",
            fallback=[LLMBinding(provider="qwen", model="qwen-max")],
        )
        assert primary.fallback[0].provider == "qwen"

    def test_rate_limit_default_off(self) -> None:
        b = LLMBinding(provider="openai", model="gpt-4o")
        assert b.rate_limit is None

    def test_rate_limit_explicit(self) -> None:
        b = LLMBinding(
            provider="openai",
            model="gpt-4o",
            rate_limit=RateLimit(rpm=600, tpm=200_000),
        )
        assert b.rate_limit is not None
        assert b.rate_limit.rpm == 600


class TestTokenUsage:
    def test_defaults_zero(self) -> None:
        u = TokenUsage()
        assert u.prompt_tokens == 0
        assert u.completion_tokens == 0
        assert u.total_tokens == 0
