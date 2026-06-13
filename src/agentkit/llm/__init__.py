"""LLM Gateway — multi-provider LLM client with retry / fallback / rate limit.

Public surface::

    from agentkit.llm import (
        # main client
        LLMGatewayClient,
        # data models
        LLMRequest, LLMResponse, LLMChunk, ChatMessage, TokenUsage,
        ToolSchema, ToolCall, ToolChoice,
        LLMBinding, RateLimit,
        # errors
        LLMError, LLMErrorClass,
        # provider Protocol (for custom providers)
        LLMProvider, ProviderCapabilities,
        # guardrail Protocol stub (replaced by real Guardrail when Doc07 lands)
        GuardrailHandle, NoOpGuardrail, Reservation,
    )

See ``Doc06_LLMGateway.md`` for the design.
"""

from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.gateway import LLMGatewayClient
from agentkit.llm.guardrail_iface import (
    GuardrailHandle,
    NoOpGuardrail,
    Reservation,
)
from agentkit.llm.models import (
    ChatMessage,
    FinishReason,
    LLMBinding,
    LLMChunk,
    LLMRequest,
    LLMResponse,
    RateLimit,
    TokenUsage,
)
from agentkit.llm.provider import LLMProvider, ProviderCapabilities
from agentkit.llm.registry import (
    LLMInstanceConfig,
    build_llm_gateway,
    build_provider,
    list_adapters,
    register_adapter,
)
from agentkit.llm.tools import ToolCall, ToolCallDelta, ToolChoice, ToolSchema

__all__ = [
    "ChatMessage",
    "FinishReason",
    "GuardrailHandle",
    "LLMBinding",
    "LLMChunk",
    "LLMError",
    "LLMErrorClass",
    "LLMGatewayClient",
    "LLMInstanceConfig",
    "LLMProvider",
    "LLMRequest",
    "LLMResponse",
    "NoOpGuardrail",
    "ProviderCapabilities",
    "RateLimit",
    "Reservation",
    "TokenUsage",
    "ToolCall",
    "ToolCallDelta",
    "ToolChoice",
    "ToolSchema",
    "build_llm_gateway",
    "build_provider",
    "list_adapters",
    "register_adapter",
]
