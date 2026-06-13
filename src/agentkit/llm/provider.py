"""LLMProvider Protocol and capability descriptor.

Every concrete provider adapter (OpenAI, Qwen, Gemini, Anthropic,
local vLLM, ...) implements :class:`LLMProvider`. The Gateway
interacts ONLY with this interface.

See ``Doc06 §3.1``.
"""

from __future__ import annotations

from collections.abc import AsyncIterator
from typing import ClassVar, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict

from agentkit.llm.models import (
    ChatMessage,
    LLMChunk,
    LLMRequest,
    LLMResponse,
    TokenUsage,
)
from agentkit.llm.tools import ToolSchema


class ProviderCapabilities(BaseModel):
    """Static description of what a provider supports.

    Used by the Gateway / Compiler to pre-validate a request (e.g.
    rejecting ``response_format=json_schema`` on a model that
    doesn't support it, before incurring an API call).
    """

    model_config = ConfigDict(frozen=True, extra="forbid")

    streaming: bool = True
    tool_calling: bool = True
    json_mode: bool = False
    json_schema_mode: bool = False
    vision: bool = False
    prompt_cache: bool = False
    reasoning_models: bool = False
    max_context: int = 8_192


class ProviderHealth(BaseModel):
    """Returned by :meth:`LLMProvider.health`."""

    model_config = ConfigDict(frozen=True)

    healthy: bool
    detail: str = ""


@runtime_checkable
class LLMProvider(Protocol):
    """The interface every provider adapter implements.

    Method contract:

    * Methods raise :class:`agentkit.llm.LLMError` (with a
      :class:`LLMErrorClass`) on failure — NEVER raw provider SDK
      exceptions, NEVER ``Exception`` subclasses outside that
      hierarchy.
    * ``stream`` returns an async iterator of :class:`LLMChunk`;
      the final chunk MUST carry ``usage``.
    * ``count_tokens`` is a *synchronous* offline call — used by
      the Gateway for pre-flight Guardrail estimation.
    * ``price`` returns USD cost for the given usage; informational
      only (per Doc07 v0.2).
    """

    # Class-level identification
    name: ClassVar[str]
    capabilities: ClassVar[ProviderCapabilities]

    async def complete(self, req: LLMRequest) -> LLMResponse:
        """Run a non-streaming chat completion."""
        ...

    def stream(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        """Stream a chat completion as :class:`LLMChunk`s.

        Returns an async iterator (not a coroutine) so callers do
        ``async for chunk in provider.stream(req): ...``.
        """
        ...

    async def health(self) -> ProviderHealth:
        """Cheap liveness probe; called periodically by the Gateway."""
        ...

    def count_tokens(
        self,
        text_or_messages: str | list[ChatMessage],
        model: str,
        *,
        tools: list[ToolSchema] | None = None,
    ) -> int:
        """Offline token count for a piece of text / messages."""
        ...

    def price(self, model: str, usage: TokenUsage) -> float:
        """Return USD cost for ``usage`` on ``model`` (informational)."""
        ...

    async def close(self) -> None:
        """Release any underlying HTTP / SDK resources."""
        ...
