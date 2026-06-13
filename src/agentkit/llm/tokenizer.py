"""Tokenizer Facade — model-aware offline token counting.

Why centralize here:

* Guardrail wants to *pre-check* token usage before the API call,
  but the provider hasn't been picked yet (binding may have a fallback
  chain) — so the count must be local.
* Different providers use different tokenizers; we don't want each
  provider adapter to re-implement the same dispatch logic.

v0.1 strategy: tiktoken for OpenAI-encoding-compatible models
(OpenAI, Azure OpenAI, DeepSeek, most Qwen-OpenAI-compatible
endpoints, local vLLM with most chat models). When more providers
land we add cases here without changing the interface.

See ``Doc06 §7``.
"""

from __future__ import annotations

import threading
from typing import Protocol

import tiktoken

from agentkit.llm.models import ChatMessage
from agentkit.llm.tools import ToolSchema

# ----------------------------------------------------------------
# Encoding selection
# ----------------------------------------------------------------

# Models we map to specific tiktoken encodings. Anything not in
# this map falls back to ``cl100k_base`` (GPT-3.5/4-era encoding),
# which is good enough for *estimation* — Guardrail uses the
# provider's authoritative count for actual billing.
_MODEL_ENCODING: dict[str, str] = {
    # GPT-4o / o1 family
    "gpt-4o": "o200k_base",
    "gpt-4o-mini": "o200k_base",
    "o1": "o200k_base",
    "o1-mini": "o200k_base",
    "o1-preview": "o200k_base",
    # GPT-4 / 3.5 family
    "gpt-4": "cl100k_base",
    "gpt-4-turbo": "cl100k_base",
    "gpt-3.5-turbo": "cl100k_base",
}

# Per-message overhead in OpenAI chat completions (system tokens
# OpenAI's docs document for prompt accounting).
# These are heuristic: 3 tokens per message + 3 priming tokens.
_PER_MESSAGE_OVERHEAD = 3
_PRIMING_OVERHEAD = 3
_FALLBACK_ENCODING = "cl100k_base"


class TokenizerImpl(Protocol):
    """The shape every tokenizer impl must satisfy.

    Currently only the tiktoken-based one exists; defining the
    Protocol up front means we can plug Anthropic / Gemini SDKs
    later without touching call sites.
    """

    def encode(self, text: str) -> list[int]: ...


# ----------------------------------------------------------------
# Lazy-cached tiktoken encodings (thread-safe)
# ----------------------------------------------------------------

_encoding_cache: dict[str, tiktoken.Encoding] = {}
_cache_lock = threading.Lock()


def _get_encoding(model: str) -> tiktoken.Encoding:
    """Resolve the right tiktoken encoding for ``model``.

    Caches the result — encoding init is expensive on first call.
    """
    enc_name = _MODEL_ENCODING.get(model, _FALLBACK_ENCODING)
    cached = _encoding_cache.get(enc_name)
    if cached is not None:
        return cached
    with _cache_lock:
        if enc_name not in _encoding_cache:
            _encoding_cache[enc_name] = tiktoken.get_encoding(enc_name)
        return _encoding_cache[enc_name]


# ----------------------------------------------------------------
# Public API
# ----------------------------------------------------------------


def count_text(text: str, model: str) -> int:
    """Count tokens in a single string for ``model``."""
    enc = _get_encoding(model)
    return len(enc.encode(text))


def count_messages(messages: list[ChatMessage], model: str) -> int:
    """Approximate prompt tokens for an OpenAI-style chat list.

    Formula matches the heuristic in the OpenAI cookbook: each
    message has fixed overhead, the role + content + name fields
    are encoded, and the whole list ends with priming tokens.
    """
    enc = _get_encoding(model)
    total = 0
    for m in messages:
        total += _PER_MESSAGE_OVERHEAD
        total += len(enc.encode(m.role))
        if m.content:
            total += len(enc.encode(m.content))
        if m.name:
            total += len(enc.encode(m.name))
    total += _PRIMING_OVERHEAD
    return total


def count_tools(tools: list[ToolSchema], model: str) -> int:
    """Approximate tokens consumed by tool schemas.

    Different providers serialize tools differently; we count the
    JSON form which is a reasonable upper bound for OpenAI-style
    function-calling. Dual-purposed: we use this for Guardrail
    estimates only (the real wire format is whatever the adapter
    sends).
    """
    if not tools:
        return 0
    import orjson  # local import: keep top-level deps explicit  # noqa: PLC0415

    enc = _get_encoding(model)
    total = 0
    for t in tools:
        total += len(enc.encode(t.name))
        if t.description:
            total += len(enc.encode(t.description))
        total += len(enc.encode(orjson.dumps(t.parameters).decode("utf-8")))
    return total


def estimate_request_tokens(
    *,
    messages: list[ChatMessage],
    model: str,
    tools: list[ToolSchema] | None = None,
    system: str | None = None,
) -> int:
    """One-shot estimator combining messages + tools + system prompt.

    Returns total prompt-side tokens — Guardrail multiplies this by
    the request's ``max_tokens`` (or a safe default) for the
    pre-reservation amount.
    """
    msgs = list(messages)
    if system:
        msgs = [ChatMessage(role="system", content=system), *msgs]
    n = count_messages(msgs, model)
    if tools:
        n += count_tools(tools, model)
    return n
