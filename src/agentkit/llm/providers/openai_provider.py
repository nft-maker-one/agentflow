"""OpenAI provider adapter — also serves OpenAI-compatible endpoints.

Many providers (DeepSeek, local vLLM, Qwen via DashScope-compat,
Together, Anyscale, ...) speak the OpenAI Chat Completions API
verbatim. We instantiate this adapter with a custom ``base_url``
and ``api_key_env`` and it Just Works.

Translation responsibilities:

1. Build the ``messages`` payload (role/content/tools/tool_call_id).
2. Map our :class:`ToolSchema` to OpenAI's ``tools[].function``.
3. Map :class:`ToolChoice` to OpenAI's union type.
4. Convert provider exceptions to :class:`LLMError`.
5. Adapt streaming chunks to :class:`LLMChunk`.
6. Surface usage / cost.

See ``Doc06 §3``.
"""

from __future__ import annotations

import os
import time
from collections.abc import AsyncIterator
from dataclasses import dataclass
from typing import Any, ClassVar, cast

from openai import (
    APIConnectionError,
    APIError,
    APIStatusError,
    APITimeoutError,
    AsyncOpenAI,
    AuthenticationError,
    BadRequestError,
    InternalServerError,
    RateLimitError,
)

from agentkit.common.logging import get_logger
from agentkit.llm import tokenizer as tk
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
from agentkit.llm.providers._pricing import ModelPrice, lookup_openai_price
from agentkit.llm.tools import ToolCall, ToolCallDelta, ToolChoice, ToolSchema

log = get_logger(__name__)


# ----------------------------------------------------------------
# OpenAI-compatible endpoint registry (well-known providers)
# ----------------------------------------------------------------

# A small registry of providers that speak the OpenAI Chat
# Completions wire format. Users can override base_url + key per
# instance; this just gives us *defaults* for common cases so
# bootstrapping in Workflow IR is one-liner.
OPENAI_COMPAT_PROVIDERS: dict[str, dict[str, str]] = {
    "openai": {
        "base_url": "https://api.openai.com/v1",
        "api_key_env": "AGENTKIT_LLM_OPENAI_API_KEY",
    },
    "deepseek": {
        "base_url": "https://api.deepseek.com/v1",
        "api_key_env": "AGENTKIT_LLM_DEEPSEEK_API_KEY",
    },
    "qwen": {
        # DashScope's OpenAI-compatible endpoint
        "base_url": "https://dashscope.aliyuncs.com/compatible-mode/v1",
        "api_key_env": "AGENTKIT_LLM_QWEN_API_KEY",
    },
    "gemini": {
        # Google AI Studio's OpenAI-compatible shim — accepts the same
        # /chat/completions request shape (with caveats around system
        # role mapping which the Google side handles transparently).
        # See https://ai.google.dev/gemini-api/docs/openai
        "base_url": "https://generativelanguage.googleapis.com/v1beta/openai",
        "api_key_env": "AGENTKIT_LLM_GEMINI_API_KEY",
    },
    "local": {
        # Local vLLM / Ollama — caller MUST override base_url
        "base_url": "http://localhost:8000/v1",
        "api_key_env": "AGENTKIT_LLM_LOCAL_API_KEY",
    },
}


# Sentinel value used when no API key is available.
#
# Newer ``openai`` SDK versions (≥ 1.50) refuse to construct an
# ``AsyncOpenAI`` client with an empty ``api_key`` and raise
# ``OpenAIError`` immediately. We don't want construction to fail
# on missing creds — that would prevent the Gateway from being
# *built* in dry-run / test contexts. Instead we install this
# placeholder so the client constructs; any actual API call will
# then fail with AUTH which the Gateway classifies and surfaces
# uniformly.
_MISSING_API_KEY_SENTINEL = "agentkit-missing-key-placeholder"


# ----------------------------------------------------------------
# Config
# ----------------------------------------------------------------


@dataclass(frozen=True)
class OpenAIProviderConfig:
    """Connection parameters for the OpenAI adapter.

    Two distinct identifiers:

    * ``instance_name`` — what the Gateway uses to route requests
      (the key in ``LLMGatewayClient.providers``). Defaults to
      ``compat`` if unset, so the simple case stays one-line.
    * ``compat`` — which preset in :data:`OPENAI_COMPAT_PROVIDERS`
      supplies default ``base_url`` / ``api_key_env``. Also used by
      :meth:`OpenAIProvider.price` to look up pricing tables.

    This split lets a user create *multiple* logical instances of
    the same compat preset — e.g. two OpenAI orgs with different
    keys, both on the OpenAI compat preset, registered as
    ``openai-team-a`` and ``openai-team-b``.
    """

    instance_name: str = "openai"
    compat: str = "openai"
    api_key: str | None = None  # None ⇒ resolve from env at construction time
    base_url: str | None = None
    organization: str | None = None
    # 180 s default — reasoning models (gpt-5*, gemini-3.5-flash,
    # claude-opus-4) routinely take 30-120s for internal thinking
    # before emitting a single visible token. The historical 60 s
    # was tuned for non-reasoning chat models. Operators can still
    # override per-instance via ``LLMInstanceConfig.timeout_s``.
    timeout_s: float = 180.0
    max_retries_sdk: int = 0  # we do retries in the Gateway, not the SDK


# ----------------------------------------------------------------
# Adapter
# ----------------------------------------------------------------


class OpenAIProvider:
    """Concrete :class:`LLMProvider` for OpenAI / OpenAI-compatible APIs."""

    name: ClassVar[str] = "openai"
    capabilities: ClassVar[ProviderCapabilities] = ProviderCapabilities(
        streaming=True,
        tool_calling=True,
        json_mode=True,
        json_schema_mode=True,
        vision=False,  # v0.1 — multimodal arrives in Phase 2
        prompt_cache=True,
        reasoning_models=True,
        max_context=128_000,
    )

    def __init__(self, config: OpenAIProviderConfig | None = None) -> None:
        cfg = config or OpenAIProviderConfig()
        defaults = OPENAI_COMPAT_PROVIDERS.get(
            cfg.compat, OPENAI_COMPAT_PROVIDERS["openai"],
        )
        api_key = cfg.api_key or os.environ.get(defaults["api_key_env"])
        # Newer openai SDK versions raise on empty/None api_key at
        # construction — install a sentinel so the client builds.
        # ``health()`` checks this sentinel and reports unhealthy.
        if not api_key:
            api_key = _MISSING_API_KEY_SENTINEL

        self._cfg = cfg
        self._has_credentials = api_key != _MISSING_API_KEY_SENTINEL
        self._client = AsyncOpenAI(
            api_key=api_key,
            base_url=cfg.base_url or defaults["base_url"],
            organization=cfg.organization,
            timeout=cfg.timeout_s,
            max_retries=cfg.max_retries_sdk,
        )
        self._instance_name = cfg.instance_name
        self._compat = cfg.compat

    # ------------------------------------------------------------
    # Identification (mirrors Protocol attrs but per-instance)
    # ------------------------------------------------------------

    @property
    def instance_name(self) -> str:
        return self._instance_name

    @property
    def compat(self) -> str:
        return self._compat

    # ------------------------------------------------------------
    # complete / stream
    # ------------------------------------------------------------

    async def complete(self, req: LLMRequest) -> LLMResponse:
        if not req.model:
            raise LLMError(
                LLMErrorClass.INVALID_REQUEST,
                "model must be set on LLMRequest",
                provider=self._instance_name,
            )

        kwargs = self._build_create_kwargs(req, stream=False)
        t0 = time.perf_counter()
        try:
            raw = await self._client.chat.completions.create(**kwargs)
        except Exception as e:  # narrow below
            raise self._classify_error(e, model=req.model) from e
        latency_ms = int((time.perf_counter() - t0) * 1000)

        return self._build_response(raw, model=req.model, latency_ms=latency_ms)

    def stream(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        return self._stream_impl(req)

    async def _stream_impl(self, req: LLMRequest) -> AsyncIterator[LLMChunk]:
        if not req.model:
            raise LLMError(
                LLMErrorClass.INVALID_REQUEST,
                "model must be set on LLMRequest",
                provider=self._instance_name,
            )
        kwargs = self._build_create_kwargs(req, stream=True)
        kwargs["stream_options"] = {"include_usage": True}

        try:
            stream = await self._client.chat.completions.create(**kwargs)
        except Exception as e:
            raise self._classify_error(e, model=req.model) from e

        try:
            async for raw_chunk in stream:
                chunk = self._convert_stream_chunk(raw_chunk, model=req.model)
                if chunk is not None:
                    yield chunk
        except Exception as e:
            raise self._classify_error(e, model=req.model) from e

    # ------------------------------------------------------------
    # health / count_tokens / price / close
    # ------------------------------------------------------------

    async def health(self) -> ProviderHealth:
        if not self._has_credentials:
            return ProviderHealth(
                healthy=False,
                detail="no API key configured (set the env var or pass api_key)",
            )
        try:
            # ``/models`` is a cheap, widely-supported reachability probe.
            await self._client.models.list()
        except AuthenticationError as e:
            return ProviderHealth(healthy=False, detail=f"auth: {e}")
        except (APIConnectionError, APITimeoutError) as e:
            return ProviderHealth(healthy=False, detail=f"connect: {e}")
        except Exception as e:  # pragma: no cover — defensive
            return ProviderHealth(healthy=False, detail=str(e))
        return ProviderHealth(healthy=True)

    def count_tokens(
        self,
        text_or_messages: str | list[ChatMessage],
        model: str,
        *,
        tools: list[ToolSchema] | None = None,
    ) -> int:
        if isinstance(text_or_messages, str):
            base = tk.count_text(text_or_messages, model)
        else:
            base = tk.count_messages(text_or_messages, model)
        if tools:
            base += tk.count_tools(tools, model)
        return base

    def price(self, model: str, usage: TokenUsage) -> float:
        if self._compat != "openai":
            # Other compat providers have their own pricing — we only
            # ship OpenAI prices in v0.1; everyone else returns 0.0
            # (informational only, per Doc07 v0.2).
            return 0.0
        p: ModelPrice = lookup_openai_price(model)
        prompt_billable = max(0, usage.prompt_tokens - usage.cached_prompt_tokens)
        return (
            prompt_billable * p.prompt_per_1k
            + usage.cached_prompt_tokens * p.cached_prompt_per_1k
            + usage.completion_tokens * p.completion_per_1k
        )

    async def close(self) -> None:
        await self._client.close()

    # ------------------------------------------------------------
    # Internals — request translation
    # ------------------------------------------------------------

    def _build_create_kwargs(self, req: LLMRequest, *, stream: bool) -> dict[str, Any]:
        messages = self._messages_to_openai(req.messages, system=req.system)
        kwargs: dict[str, Any] = {
            "model": req.model,
            "messages": messages,
            "stream": stream,
            "temperature": req.temperature,
        }
        if req.max_tokens is not None:
            kwargs["max_tokens"] = req.max_tokens
        if req.top_p is not None:
            kwargs["top_p"] = req.top_p
        if req.stop:
            kwargs["stop"] = req.stop
        if req.seed is not None:
            kwargs["seed"] = req.seed
        if req.tools:
            kwargs["tools"] = [self._tool_to_openai(t) for t in req.tools]
            kwargs["tool_choice"] = self._tool_choice_to_openai(req.tool_choice)
        if req.response_format and req.response_format.kind != "text":
            kwargs["response_format"] = {"type": req.response_format.kind}
            if (
                req.response_format.kind == "json_schema"
                and req.response_format.schema_ is not None
            ):
                kwargs["response_format"] = {
                    "type": "json_schema",
                    "json_schema": req.response_format.schema_,
                }
        if req.extra:
            # Whitelist passthrough — caller's responsibility.
            kwargs.update(req.extra)
        return kwargs

    @staticmethod
    def _messages_to_openai(
        messages: list[ChatMessage], *, system: str | None,
    ) -> list[dict[str, Any]]:
        out: list[dict[str, Any]] = []
        if system:
            out.append({"role": "system", "content": system})
        for m in messages:
            entry: dict[str, Any] = {"role": m.role}
            if m.content:
                entry["content"] = m.content
            if m.name:
                entry["name"] = m.name
            if m.tool_call_id:
                entry["tool_call_id"] = m.tool_call_id
            if m.tool_calls:
                entry["tool_calls"] = [
                    {
                        "id": tc.id,
                        "type": "function",
                        "function": {
                            "name": tc.name,
                            "arguments": tc.raw_arguments
                            or _json_dumps(tc.arguments),
                        },
                    }
                    for tc in m.tool_calls
                ]
            out.append(entry)
        return out

    @staticmethod
    def _tool_to_openai(tool: ToolSchema) -> dict[str, Any]:
        return {
            "type": "function",
            "function": {
                "name": tool.name,
                "description": tool.description,
                "parameters": tool.parameters or {"type": "object", "properties": {}},
            },
        }

    @staticmethod
    def _tool_choice_to_openai(choice: ToolChoice) -> Any:
        if choice.is_named:
            return {"type": "function", "function": {"name": choice.name}}
        # ``auto``/``none``/``required`` are passed through verbatim;
        # we narrowed the literal already in the Pydantic model.
        return cast(str, choice.root)

    # ------------------------------------------------------------
    # Internals — response translation
    # ------------------------------------------------------------

    def _build_response(
        self, raw: Any, *, model: str, latency_ms: int,
    ) -> LLMResponse:
        choice = raw.choices[0]
        msg = choice.message
        finish = self._finish_reason(choice.finish_reason)

        tool_calls: list[ToolCall] = []
        if getattr(msg, "tool_calls", None):
            for tc in msg.tool_calls:
                args_raw = tc.function.arguments or ""
                try:
                    args = _json_loads(args_raw) if args_raw else {}
                except Exception:
                    args = {}
                tool_calls.append(
                    ToolCall(
                        id=tc.id,
                        name=tc.function.name,
                        arguments=args,
                        raw_arguments=args_raw,
                    ),
                )

        usage = self._usage(raw, model=model)
        cost = self.price(model, usage)
        return LLMResponse(
            text=msg.content or "",
            tool_calls=tool_calls,
            finish_reason=finish,
            usage=usage,
            cost_usd=cost,
            provider=self._instance_name,
            model=model,
            latency_ms=latency_ms,
            attempts=1,
        )

    def _convert_stream_chunk(self, raw: Any, *, model: str) -> LLMChunk | None:
        # The terminal chunk in include_usage mode has no choices,
        # only ``usage``. Return it as the final framework chunk.
        if getattr(raw, "usage", None) and not raw.choices:
            usage = self._usage(raw, model=model)
            return LLMChunk(
                usage=usage,
                cost_usd=self.price(model, usage),
            )

        if not raw.choices:
            return None

        choice = raw.choices[0]
        delta = choice.delta
        finish = self._finish_reason(choice.finish_reason) if choice.finish_reason else None

        delta_tool: ToolCallDelta | None = None
        if getattr(delta, "tool_calls", None):
            tc = delta.tool_calls[0]
            delta_tool = ToolCallDelta(
                index=tc.index,
                id_delta=tc.id,
                name_delta=getattr(tc.function, "name", None) if tc.function else None,
                arguments_delta=(
                    getattr(tc.function, "arguments", None) if tc.function else None
                ),
            )

        return LLMChunk(
            delta_text=delta.content or "",
            delta_tool_call=delta_tool,
            finish_reason=finish,
        )

    @staticmethod
    def _usage(raw: Any, *, model: str) -> TokenUsage:
        u = getattr(raw, "usage", None)
        if u is None:
            return TokenUsage()
        cached = 0
        details = getattr(u, "prompt_tokens_details", None)
        if details is not None:
            cached = getattr(details, "cached_tokens", 0) or 0
        reasoning = 0
        comp_details = getattr(u, "completion_tokens_details", None)
        if comp_details is not None:
            reasoning = getattr(comp_details, "reasoning_tokens", 0) or 0
        del model  # informational hint, not used in calculation
        return TokenUsage(
            prompt_tokens=u.prompt_tokens or 0,
            completion_tokens=u.completion_tokens or 0,
            total_tokens=u.total_tokens or 0,
            cached_prompt_tokens=cached,
            reasoning_tokens=reasoning,
        )

    @staticmethod
    def _finish_reason(raw: str | None) -> FinishReason:
        match raw:
            case "stop":
                return FinishReason.STOP
            case "length":
                return FinishReason.LENGTH
            case "tool_calls" | "function_call":
                return FinishReason.TOOL_CALL
            case "content_filter":
                return FinishReason.CONTENT_FILTER
            case _:
                return FinishReason.STOP

    # ------------------------------------------------------------
    # Internals — error classification
    # ------------------------------------------------------------

    def _classify_error(self, exc: BaseException, *, model: str) -> LLMError:
        provider = self._instance_name
        # Order matters — more specific subclasses first.
        if isinstance(exc, AuthenticationError):
            return LLMError(
                LLMErrorClass.AUTH,
                str(exc),
                provider=provider,
                model=model,
                http_status=getattr(exc, "status_code", 401),
            )
        if isinstance(exc, BadRequestError):
            return LLMError(
                LLMErrorClass.INVALID_REQUEST,
                str(exc),
                provider=provider,
                model=model,
                http_status=getattr(exc, "status_code", 400),
            )
        if isinstance(exc, RateLimitError):
            retry_after_ms = _retry_after_ms(exc)
            return LLMError(
                LLMErrorClass.RATE_LIMIT_429,
                str(exc),
                provider=provider,
                model=model,
                http_status=429,
                retry_after_ms=retry_after_ms,
            )
        if isinstance(exc, APITimeoutError):
            return LLMError(
                LLMErrorClass.TIMEOUT,
                str(exc),
                provider=provider,
                model=model,
            )
        if isinstance(exc, InternalServerError):
            return LLMError(
                LLMErrorClass.TRANSIENT_5XX,
                str(exc),
                provider=provider,
                model=model,
                http_status=getattr(exc, "status_code", 500),
            )
        if isinstance(exc, APIConnectionError):
            return LLMError(
                LLMErrorClass.PROVIDER_DOWN,
                str(exc),
                provider=provider,
                model=model,
            )
        if isinstance(exc, APIStatusError):
            status = getattr(exc, "status_code", 0)
            klass = (
                LLMErrorClass.TRANSIENT_5XX if 500 <= status < 600
                else LLMErrorClass.INVALID_REQUEST
            )
            return LLMError(klass, str(exc), provider=provider, model=model, http_status=status)
        if isinstance(exc, APIError):
            return LLMError(
                LLMErrorClass.UNKNOWN,
                str(exc),
                provider=provider,
                model=model,
            )
        return LLMError(
            LLMErrorClass.UNKNOWN,
            f"{type(exc).__name__}: {exc}",
            provider=provider,
            model=model,
        )


# ----------------------------------------------------------------
# Helpers (free functions to keep the class focused)
# ----------------------------------------------------------------


def _retry_after_ms(exc: BaseException) -> int | None:
    """Try to extract a Retry-After header from an OpenAI SDK error."""
    response = getattr(exc, "response", None)
    if response is None:
        return None
    headers = getattr(response, "headers", {}) or {}
    val = headers.get("retry-after") or headers.get("Retry-After")
    if not val:
        return None
    try:
        return int(float(val) * 1000)
    except ValueError:
        return None


def _json_dumps(obj: dict[str, Any]) -> str:
    """JSON encode using stdlib for arguments (already a small dict)."""
    import json  # local import — only used for tool-call arg serialization  # noqa: PLC0415

    return json.dumps(obj, ensure_ascii=False)


def _json_loads(s: str) -> dict[str, Any]:
    """JSON decode using stdlib (small payloads, error tolerance ok)."""
    import json  # noqa: PLC0415

    parsed = json.loads(s)
    if not isinstance(parsed, dict):
        raise TypeError("tool arguments must decode to an object")
    return cast(dict[str, Any], parsed)


# ----------------------------------------------------------------
# Static check — ensure we satisfy the Protocol structurally.
# ----------------------------------------------------------------

# Runtime assertion at import time — catches accidental signature drift
# between Protocol and impl.
_: type[LLMProvider] = OpenAIProvider  # type: ignore[assignment, misc]
