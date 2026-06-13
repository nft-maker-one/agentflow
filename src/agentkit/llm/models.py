"""LLM data models — request / response / streaming chunk / binding.

Pure Pydantic data — no I/O, no SDK calls.

See ``Doc06 §2``.
"""

from __future__ import annotations

from enum import StrEnum
from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field

from agentkit.llm.tools import ToolCall, ToolCallDelta, ToolChoice, ToolSchema


# ============================================================
# Chat messages and finish reasons
# ============================================================


class ChatMessage(BaseModel):
    """OpenAI-style chat message.

    ``content`` is plain text in v0.1; vision / multimodal is on
    the Phase 2 roadmap (Doc06 §16.2).
    """

    model_config = ConfigDict(extra="forbid")

    role: Literal["system", "user", "assistant", "tool"]
    content: str = ""
    name: str | None = None
    # Set on assistant messages that triggered tool calls. Set on
    # tool messages to indicate which call this is replying to.
    tool_call_id: str | None = None
    tool_calls: list[ToolCall] = Field(default_factory=list)


class FinishReason(StrEnum):
    STOP = "stop"
    LENGTH = "length"
    TOOL_CALL = "tool_call"
    CONTENT_FILTER = "content_filter"
    ERROR = "error"


# ============================================================
# Response format / sampling controls
# ============================================================


class ResponseFormat(BaseModel):
    """Optional structured output hint."""

    model_config = ConfigDict(extra="forbid")

    kind: Literal["text", "json_object", "json_schema"] = "text"
    schema_: dict[str, Any] | None = Field(default=None, alias="schema")


class TokenUsage(BaseModel):
    """Token accounting reported by the provider."""

    model_config = ConfigDict(extra="forbid")

    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    cached_prompt_tokens: int = 0
    reasoning_tokens: int = 0


# ============================================================
# Request / Response
# ============================================================


class LLMRequest(BaseModel):
    """A normalized request to the Gateway.

    Either ``binding`` (preferred — references a Workflow IR
    LLMBinding) OR explicit ``provider`` + ``model`` must be set.
    Direct mode is what we exercise in v0.1 since the Workflow
    module isn't built yet; binding registry plugs in later.
    """

    model_config = ConfigDict(extra="forbid")

    # ---- routing ----
    provider: str | None = None
    model: str | None = None
    binding: str | None = None

    # ---- content ----
    messages: list[ChatMessage]
    system: str | None = None
    tools: list[ToolSchema] = Field(default_factory=list)
    tool_choice: ToolChoice = Field(default_factory=lambda: ToolChoice("auto"))
    response_format: ResponseFormat | None = None

    # ---- sampling ----
    temperature: float = 0.7
    top_p: float | None = None
    max_tokens: int | None = None
    stop: list[str] = Field(default_factory=list)
    seed: int | None = None

    # ---- control ----
    timeout_ms: int = Field(default=60_000, ge=1_000)
    stream: bool = False
    extra: dict[str, Any] = Field(default_factory=dict)

    # ---- correlation (Pipeline / Runtime fills these; user shouldn't set) ----
    trace_id_: str | None = None
    agent_id_: str | None = None
    run_id_: str | None = None
    template_key_: str | None = None   # for per-agent guardrail overrides


class LLMResponse(BaseModel):
    """A non-streaming response from the Gateway."""

    model_config = ConfigDict(extra="allow")

    text: str = ""
    tool_calls: list[ToolCall] = Field(default_factory=list)
    finish_reason: FinishReason = FinishReason.STOP
    usage: TokenUsage = Field(default_factory=TokenUsage)
    cost_usd: float = 0.0

    # bookkeeping
    provider: str
    model: str
    latency_ms: int = 0
    attempts: int = 1
    raw: dict[str, Any] | None = None


class LLMChunk(BaseModel):
    """A streaming chunk in the Gateway's normalized form."""

    model_config = ConfigDict(extra="forbid")

    delta_text: str = ""
    delta_tool_call: ToolCallDelta | None = None
    finish_reason: FinishReason | None = None
    # Set only on the final chunk so callers can ``async for`` and
    # still capture usage at the end.
    usage: TokenUsage | None = None
    cost_usd: float = 0.0


# ============================================================
# Binding + RateLimit (referenced in Workflow IR)
# ============================================================


class RateLimit(BaseModel):
    """Per-(provider, model) rate-limit knobs.

    Default off: ``rpm == 0`` and ``tpm == 0``. Setting either to a
    positive int activates the limiter.
    """

    model_config = ConfigDict(extra="forbid")

    rpm: int = Field(default=0, ge=0, description="Requests per minute (0 = off)")
    tpm: int = Field(default=0, ge=0, description="Tokens per minute (0 = off)")
    strategy: Literal["wait", "fail_fast"] = "wait"


class LLMBinding(BaseModel):
    """A reusable, named LLM provider/model selection.

    The same binding may be referenced from multiple agents in a
    Workflow IR — see ``Doc06 §2.2``.
    """

    model_config = ConfigDict(extra="forbid")

    provider: str = Field(min_length=1)
    model: str = Field(min_length=1)
    params: dict[str, Any] = Field(default_factory=dict)
    fallback: list["LLMBinding"] = Field(default_factory=list)
    rate_limit: RateLimit | None = None
    timeout_ms: int = Field(default=60_000, ge=1_000)
