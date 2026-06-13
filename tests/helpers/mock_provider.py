"""Reusable MockProvider for LLM unit tests.

Lives under ``tests/helpers/`` (not in ``src/``) — these are testing
utilities, not framework code shipped to users.

Behavior is fully scriptable: the test pre-loads a queue of
"actions" (responses or exceptions to raise) and the mock pops one
per :meth:`complete`/:meth:`stream` call.
"""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator
from typing import Any, ClassVar

from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.models import (
    ChatMessage,
    FinishReason,
    LLMChunk,
    LLMRequest,
    LLMResponse,
    TokenUsage,
)
from agentkit.llm.provider import (
    LLMProvider,
    ProviderCapabilities,
    ProviderHealth,
)
from agentkit.llm.tools import ToolSchema


class MockProvider:
    """Scriptable provider that satisfies :class:`LLMProvider`."""

    name: ClassVar[str] = "mock"
    capabilities: ClassVar[ProviderCapabilities] = ProviderCapabilities(
        streaming=True,
        tool_calling=True,
        json_mode=False,
        max_context=32_000,
    )

    def __init__(self, name: str = "mock") -> None:
        self._instance_name = name
        # Each item is either an LLMResponse OR an LLMError to raise.
        self._actions: deque[LLMResponse | LLMError] = deque()
        # Stream chunks: list of pre-canned chunks per call, with an
        # optional terminating LLMError to simulate stream interrupts.
        self._stream_actions: deque[list[LLMChunk] | LLMError] = deque()
        self.calls: list[LLMRequest] = []
        self.token_count_calls: list[tuple[Any, str]] = []

    # ------------------------------------------------------------
    # Test scripting API
    # ------------------------------------------------------------

    def queue_response(self, rsp: LLMResponse) -> MockProvider:
        self._actions.append(rsp)
        return self

    def queue_error(self, err: LLMError) -> MockProvider:
        self._actions.append(err)
        return self

    def queue_text(
        self,
        text: str,
        *,
        prompt_tokens: int = 100,
        completion_tokens: int = 20,
        cost: float = 0.0,
        latency_ms: int = 1,
        model: str = "mock-model",
    ) -> MockProvider:
        rsp = LLMResponse(
            text=text,
            tool_calls=[],
            finish_reason=FinishReason.STOP,
            usage=TokenUsage(
                prompt_tokens=prompt_tokens,
                completion_tokens=completion_tokens,
                total_tokens=prompt_tokens + completion_tokens,
            ),
            cost_usd=cost,
            provider=self._instance_name,
            model=model,
            latency_ms=latency_ms,
            attempts=1,
        )
        return self.queue_response(rsp)

    def queue_stream(self, chunks: list[LLMChunk]) -> MockProvider:
        self._stream_actions.append(chunks)
        return self

    def queue_stream_error(self, err: LLMError) -> MockProvider:
        self._stream_actions.append(err)
        return self

    # ------------------------------------------------------------
    # LLMProvider Protocol
    # ------------------------------------------------------------

    async def complete(self, req: LLMRequest) -> LLMResponse:
        self.calls.append(req)
        if not self._actions:
            raise LLMError(
                LLMErrorClass.UNKNOWN,
                "MockProvider.complete called without queued action",
                provider=self._instance_name,
                model=req.model or "?",
            )
        action = self._actions.popleft()
        if isinstance(action, LLMError):
            raise action
        return action

    def stream(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        return self._stream_impl(req)

    async def _stream_impl(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        self.calls.append(req)
        if not self._stream_actions:
            raise LLMError(
                LLMErrorClass.UNKNOWN,
                "MockProvider.stream called without queued action",
                provider=self._instance_name,
                model=req.model or "?",
            )
        action = self._stream_actions.popleft()
        if isinstance(action, LLMError):
            raise action
        for chunk in action:
            yield chunk

    async def health(self) -> ProviderHealth:
        return ProviderHealth(healthy=True)

    def count_tokens(
        self,
        text_or_messages: str | list[ChatMessage],
        model: str,
        *,
        tools: list[ToolSchema] | None = None,
    ) -> int:
        del tools
        self.token_count_calls.append((text_or_messages, model))
        if isinstance(text_or_messages, str):
            return max(1, len(text_or_messages) // 4)
        return sum(max(1, len(m.content) // 4) for m in text_or_messages)

    def price(self, model: str, usage: TokenUsage) -> float:
        del model
        # Fixed mock pricing: $0.001 per 1k tokens, no cache discount.
        return usage.total_tokens / 1000.0 * 0.001

    async def close(self) -> None:
        return None


# Static check — fail at import if drift creeps in.
_: type[LLMProvider] = MockProvider  # type: ignore[assignment, misc]
