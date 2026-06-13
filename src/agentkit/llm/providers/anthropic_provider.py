"""Anthropic Claude adapter.

Implements :class:`agentkit.llm.provider.LLMProvider` for the
Anthropic Messages API. Unlike the OpenAI-compat path used by
DeepSeek/Qwen/Gemini, Anthropic's wire format diverges on:

* ``system`` is a **top-level** field, not a ``role: system`` message
* ``max_tokens`` is **required** (we default to 4096 if caller omits)
* assistant content is a **list of blocks**, not a flat string —
  we concat the ``text`` blocks for ``LLMResponse.text``
* finish reasons map ``end_turn → STOP``, ``max_tokens → LENGTH``,
  ``tool_use → TOOL_CALL``, ``stop_sequence → STOP``
* errors are typed exceptions from the ``anthropic`` SDK that we
  classify to :class:`LLMErrorClass` so the Gateway's retry /
  fallback logic stays provider-agnostic.

This adapter intentionally does NOT implement tool calling or
streaming tool deltas yet — Phase 3 work. It does support text
streaming via the SDK's async iterator.
"""

from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, ClassVar

try:
    import anthropic
    from anthropic import AsyncAnthropic
except ImportError as e:  # pragma: no cover - import guard
    raise ImportError(
        "anthropic SDK not installed — install with `uv pip install anthropic`",
    ) from e

from agentkit.common.logging import get_logger
from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.models import (
    ChatMessage,
    FinishReason,
    LLMChunk,
    LLMRequest,
    LLMResponse,
    TokenUsage,
)
from agentkit.llm.provider import ProviderCapabilities, ProviderHealth
from agentkit.llm.tools import ToolSchema

log = get_logger(__name__)


# Anthropic finish_reason → AgentKit FinishReason
_FINISH_REASON_MAP: dict[str, FinishReason] = {
    "end_turn":      FinishReason.STOP,
    "stop_sequence": FinishReason.STOP,
    "max_tokens":    FinishReason.LENGTH,
    "tool_use":      FinishReason.TOOL_CALL,
}


@dataclass(frozen=True)
class AnthropicProviderConfig:
    """Connection parameters for the Anthropic adapter."""

    instance_name: str = "anthropic"
    api_key: str | None = None      # None ⇒ resolve from env
    base_url: str | None = None     # None ⇒ SDK default
    # 180 s default — Claude Opus / Sonnet "extended thinking" mode
    # can take 60+ s for hard problems. Operators override via
    # ``LLMInstanceConfig.timeout_s``.
    timeout_s: float = 180.0
    # Anthropic requires max_tokens; this is the fallback when an
    # LLMRequest comes in without it.
    default_max_tokens: int = 4096


class AnthropicProvider:
    """Concrete :class:`LLMProvider` for the Anthropic Messages API."""

    name: ClassVar[str] = "anthropic"
    capabilities: ClassVar[ProviderCapabilities] = ProviderCapabilities(
        streaming=True,
        tool_calling=False,   # not wired yet — Phase 3
        json_mode=False,
        json_schema_mode=False,
        vision=False,
        prompt_cache=True,    # Anthropic's prompt-caching is opt-in via headers
        reasoning_models=True,
        max_context=200_000,  # Claude 4+ family
    )

    def __init__(self, config: AnthropicProviderConfig | None = None) -> None:
        cfg = config or AnthropicProviderConfig()
        api_key = (
            cfg.api_key
            or os.environ.get("AGENTKIT_LLM_ANTHROPIC_API_KEY")
            or os.environ.get("ANTHROPIC_API_KEY")
        )
        if not api_key:
            api_key = "agentkit-missing-key-placeholder"
        self._cfg = cfg
        self._has_credentials = api_key != "agentkit-missing-key-placeholder"
        kwargs: dict[str, Any] = {
            "api_key": api_key,
            "timeout": cfg.timeout_s,
            "max_retries": 0,  # we retry at the Gateway level
        }
        if cfg.base_url:
            kwargs["base_url"] = cfg.base_url
        self._client = AsyncAnthropic(**kwargs)
        self._instance_name = cfg.instance_name

    @property
    def instance_name(self) -> str:
        return self._instance_name

    @property
    def compat(self) -> str:
        return "anthropic"

    @property
    def base_url(self) -> str | None:
        return self._cfg.base_url

    @property
    def api_key(self) -> str | None:
        # Hide the actual value; just signal presence to the API layer.
        return "***" if self._has_credentials else None

    # ------------------------------------------------------------
    # complete
    # ------------------------------------------------------------

    async def complete(self, req: LLMRequest) -> LLMResponse:
        if not req.model:
            raise LLMError(
                LLMErrorClass.INVALID_REQUEST,
                "model must be set on LLMRequest",
                provider=self._instance_name,
            )

        kwargs = self._build_kwargs(req, stream=False)
        t0 = time.perf_counter()
        try:
            msg = await self._client.messages.create(**kwargs)
        except Exception as e:
            raise self._classify_error(e, model=req.model) from e
        latency_ms = int((time.perf_counter() - t0) * 1000)

        # Concat text blocks; ignore tool_use blocks for v0.1.
        text_parts: list[str] = []
        for block in (msg.content or []):
            if getattr(block, "type", None) == "text":
                text_parts.append(getattr(block, "text", "") or "")
        text = "".join(text_parts)

        usage = TokenUsage(
            prompt_tokens=getattr(msg.usage, "input_tokens", 0) or 0,
            completion_tokens=getattr(msg.usage, "output_tokens", 0) or 0,
            total_tokens=(
                (getattr(msg.usage, "input_tokens", 0) or 0)
                + (getattr(msg.usage, "output_tokens", 0) or 0)
            ),
        )

        finish_reason = _FINISH_REASON_MAP.get(
            getattr(msg, "stop_reason", "") or "", FinishReason.STOP,
        )

        return LLMResponse(
            text=text,
            finish_reason=finish_reason,
            usage=usage,
            cost_usd=0.0,  # pricing handled in price() if needed
            provider=self._instance_name,
            model=req.model,
            latency_ms=latency_ms,
        )

    # ------------------------------------------------------------
    # stream
    # ------------------------------------------------------------

    def stream(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        return self._stream_impl(req)

    async def _stream_impl(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        if not req.model:
            raise LLMError(
                LLMErrorClass.INVALID_REQUEST,
                "model must be set on LLMRequest",
                provider=self._instance_name,
            )
        kwargs = self._build_kwargs(req, stream=True)
        try:
            async with self._client.messages.stream(**kwargs) as stream:
                async for delta in stream.text_stream:
                    yield LLMChunk(delta_text=delta)
                # Emit a final chunk carrying usage + finish_reason.
                final = await stream.get_final_message()
                usage = TokenUsage(
                    prompt_tokens=getattr(final.usage, "input_tokens", 0) or 0,
                    completion_tokens=getattr(final.usage, "output_tokens", 0) or 0,
                    total_tokens=(
                        (getattr(final.usage, "input_tokens", 0) or 0)
                        + (getattr(final.usage, "output_tokens", 0) or 0)
                    ),
                )
                yield LLMChunk(
                    delta_text="",
                    finish_reason=_FINISH_REASON_MAP.get(
                        getattr(final, "stop_reason", "") or "",
                        FinishReason.STOP,
                    ),
                    usage=usage,
                )
        except Exception as e:
            raise self._classify_error(e, model=req.model) from e

    # ------------------------------------------------------------
    # health / count_tokens / price / close
    # ------------------------------------------------------------

    async def health(self) -> ProviderHealth:
        if not self._has_credentials:
            return ProviderHealth(
                healthy=False, detail="ANTHROPIC_API_KEY missing",
            )
        # Cheap call: list models. Anthropic's SDK exposes this.
        try:
            await self._client.models.list(limit=1)
            return ProviderHealth(healthy=True, detail="ok")
        except Exception as e:
            return ProviderHealth(
                healthy=False,
                detail=f"{type(e).__name__}: {e}",
            )

    def count_tokens(
        self,
        text_or_messages: str | list[ChatMessage],
        model: str,
        *,
        tools: list[ToolSchema] | None = None,
    ) -> int:
        """Approximate token count.

        Anthropic exposes a ``count_tokens`` endpoint (sync) in newer
        SDK versions; we use a simple char-based heuristic here to
        keep this method synchronous + offline (matching the
        provider Protocol). The Gateway only uses this for guardrail
        pre-flight estimates so a 30% miss is fine.
        """
        if isinstance(text_or_messages, str):
            return max(1, len(text_or_messages) // 4)
        total = 0
        for m in text_or_messages:
            total += len(m.content) // 4
        return max(1, total)

    def price(self, model: str, usage: TokenUsage) -> float:
        # Anthropic pricing varies per model family — return 0 for
        # now. The metric pipeline already records per-token usage so
        # we can compute cost out-of-band if needed.
        return 0.0

    async def close(self) -> None:
        if hasattr(self._client, "close"):
            try:
                await self._client.close()
            except Exception:
                pass

    # ------------------------------------------------------------
    # internals
    # ------------------------------------------------------------

    def _build_kwargs(self, req: LLMRequest, *, stream: bool) -> dict[str, Any]:
        # Translate ChatMessage[] → Anthropic shape:
        # - system messages get extracted into top-level `system`
        # - user/assistant messages stay as-is
        system_text = req.system or ""
        msgs: list[dict[str, Any]] = []
        for m in req.messages:
            role = m.role
            if role == "system":
                # Concat into top-level system; preserve order if multiple.
                if system_text:
                    system_text += "\n\n" + (m.content or "")
                else:
                    system_text = m.content or ""
                continue
            if role not in ("user", "assistant"):
                # Anthropic doesn't have "tool" role yet — fold into user.
                role = "user"
            msgs.append({"role": role, "content": m.content or ""})

        # Anthropic requires at least one user message.
        if not msgs or msgs[0]["role"] != "user":
            msgs.insert(0, {"role": "user", "content": ""})

        kwargs: dict[str, Any] = {
            "model": req.model,
            "messages": msgs,
            "max_tokens": req.max_tokens or self._cfg.default_max_tokens,
            "temperature": req.temperature,
        }
        if system_text:
            kwargs["system"] = system_text
        if req.top_p is not None:
            kwargs["top_p"] = req.top_p
        if req.stop:
            kwargs["stop_sequences"] = list(req.stop)
        # Note: stream is NOT passed via kwargs here because we use
        # the explicit ``messages.stream(...)`` async-context manager.
        # ``stream`` parameter is used only to differentiate the
        # caller's intent.
        _ = stream
        return kwargs

    def _classify_error(self, exc: Exception, model: str) -> LLMError:
        """Map Anthropic SDK exceptions → LLMError."""
        klass = LLMErrorClass.UNKNOWN
        http_status: int | None = None
        msg = str(exc)

        if isinstance(exc, anthropic.AuthenticationError):
            klass = LLMErrorClass.AUTH
            http_status = 401
        elif isinstance(exc, anthropic.PermissionDeniedError):
            klass = LLMErrorClass.AUTH
            http_status = 403
        elif isinstance(exc, anthropic.RateLimitError):
            klass = LLMErrorClass.RATE_LIMIT_429
            http_status = 429
        elif isinstance(exc, anthropic.BadRequestError):
            # Anthropic uses 400 for both invalid params AND
            # "credit_balance_too_low" — sniff the message.
            if "credit" in msg.lower() or "quota" in msg.lower() or "billing" in msg.lower():
                klass = LLMErrorClass.QUOTA_EXCEEDED
            else:
                klass = LLMErrorClass.INVALID_REQUEST
            http_status = 400
        elif isinstance(exc, anthropic.APITimeoutError):
            klass = LLMErrorClass.TIMEOUT
        elif isinstance(exc, anthropic.APIConnectionError):
            klass = LLMErrorClass.PROVIDER_DOWN
        elif isinstance(exc, anthropic.InternalServerError):
            klass = LLMErrorClass.TRANSIENT_5XX
            http_status = 500
        elif isinstance(exc, anthropic.APIStatusError):
            http_status = getattr(exc, "status_code", None)
            if http_status and 500 <= http_status < 600:
                klass = LLMErrorClass.TRANSIENT_5XX
            elif http_status == 429:
                klass = LLMErrorClass.RATE_LIMIT_429
            elif http_status in (401, 403):
                klass = LLMErrorClass.AUTH
            elif http_status == 400:
                klass = LLMErrorClass.INVALID_REQUEST

        return LLMError(
            klass,
            f"{type(exc).__name__}: {msg}",
            provider=self._instance_name,
            model=model,
            http_status=http_status,
        )
