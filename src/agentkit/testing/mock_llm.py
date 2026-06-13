"""User-facing mock LLM Provider + Gateway shortcut.

Lifted from ``tests/helpers/mock_provider.py`` and trimmed to a
clean public surface — users can:

* Pre-load deterministic replies::

    mock = MockLLMProvider()
    mock.queue_response("hello world")
    mock.queue_response("done", finish_reason="stop")

* Or use a simple "always reply X" helper::

    gateway = MockLLMGateway(reply="test summary")

The :class:`MockLLMProvider` satisfies the
:class:`agentkit.llm.LLMProvider` Protocol, and
:class:`MockLLMGateway` builds an :class:`LLMGatewayClient` wired
to that mock — drop-in replacement for the real Gateway in tests.
"""

from __future__ import annotations

from collections import deque
from collections.abc import AsyncIterator
from typing import Any, ClassVar

from agentkit.llm.errors import LLMError
from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.models import (
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


class MockLLMProvider:
    """Scriptable provider satisfying :class:`LLMProvider` Protocol."""

    name: ClassVar[str] = "mock"
    capabilities: ClassVar[ProviderCapabilities] = ProviderCapabilities(
        streaming=True,
        tool_calling=True,
        json_mode=False,
        max_context=32_000,
    )

    def __init__(self, instance_name: str = "mock") -> None:
        self._instance_name = instance_name
        # Each action is either an LLMResponse to return or an
        # LLMError to raise.
        self._actions: deque[LLMResponse | LLMError] = deque()
        # Optional default reply when the queue is empty — useful for
        # simple smoke tests that don't care about content variety.
        self._default_reply: str | None = None

    # ---- Configuration ----

    def queue_response(
        self,
        text: str = "ok",
        *,
        finish_reason: FinishReason = "stop",
        prompt_tokens: int = 10,
        completion_tokens: int = 5,
    ) -> MockLLMProvider:
        """Push a response onto the action queue (returned next call)."""
        self._actions.append(
            LLMResponse(
                text=text,
                finish_reason=finish_reason,
                usage=TokenUsage(
                    prompt_tokens=prompt_tokens,
                    completion_tokens=completion_tokens,
                    total_tokens=prompt_tokens + completion_tokens,
                ),
                provider=self._instance_name,
                model="mock",
            ),
        )
        return self

    def queue_error(self, err: LLMError) -> MockLLMProvider:
        self._actions.append(err)
        return self

    def set_default_reply(self, text: str) -> MockLLMProvider:
        """Reply with this string on every call once the queue is empty."""
        self._default_reply = text
        return self

    # ---- LLMProvider Protocol ----

    @property
    def instance_name(self) -> str:
        return self._instance_name

    async def complete(self, req: LLMRequest) -> LLMResponse:
        if self._actions:
            action = self._actions.popleft()
            if isinstance(action, LLMError):
                raise action
            return action
        if self._default_reply is None:
            # Synthesize a tiny default rather than blowing up — most
            # tests that don't pre-queue just want SOMETHING back.
            return LLMResponse(
                text=f"[mock reply for: {(req.messages[-1].content if req.messages else '')[:60]}]",
                finish_reason="stop",
                usage=TokenUsage(
                    prompt_tokens=10, completion_tokens=5, total_tokens=15,
                ),
                provider=self._instance_name,
                model=req.model or "mock",
            )
        return LLMResponse(
            text=self._default_reply,
            finish_reason="stop",
            usage=TokenUsage(prompt_tokens=10, completion_tokens=5, total_tokens=15),
            provider=self._instance_name,
            model=req.model or "mock",
        )

    async def stream(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        # Phase 1: stream by chunking the complete() output one char
        # at a time — sufficient for testing stream consumption.
        rsp = await self.complete(req)
        for c in rsp.text:
            yield LLMChunk(text=c, provider=self._instance_name, model=rsp.model)
        yield LLMChunk(
            text="",
            finish_reason=rsp.finish_reason,
            usage=rsp.usage,
            provider=self._instance_name,
            model=rsp.model,
        )

    def count_tokens(self, content: object, model: str) -> int:  # noqa: ARG002
        # Naive word count — fine for tests.
        if isinstance(content, str):
            return max(1, len(content.split()))
        return 1

    def price(self, model: str, usage: TokenUsage) -> float:  # noqa: ARG002
        # Mock has no cost.
        return 0.0

    async def health(self) -> ProviderHealth:
        return ProviderHealth(healthy=True, latency_ms=0)

    def supported_tools(self) -> list[ToolSchema]:
        return []

    # ---- Inspection ----

    @property
    def remaining_actions(self) -> int:
        return len(self._actions)


# ============================================================
# Gateway shortcut
# ============================================================


def MockLLMGateway(
    *,
    reply: str = "ok",
    provider_name: str = "mock",
    default_model: str = "mock",
) -> LLMGatewayClient:
    """Build an :class:`LLMGatewayClient` backed by a single ``MockLLMProvider``.

    All calls return ``reply`` regardless of prompt — the simplest
    drop-in replacement for the real Gateway in unit tests::

        gateway = MockLLMGateway(reply="test summary")
        async with LocalRuntime(wf, llm=gateway) as rt:
            ...
    """
    provider = MockLLMProvider(instance_name=provider_name)
    provider.set_default_reply(reply)
    return LLMGatewayClient(
        providers={provider_name: provider},
        default_provider=provider_name,
        default_model=default_model,
    )
