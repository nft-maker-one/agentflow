"""Performance benchmarks for LLM-module utilities.

These run without any external services — purely local CPU work.
"""

from __future__ import annotations

import pytest

from agentkit.llm import tokenizer as tk
from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.llm.models import ChatMessage
from agentkit.llm.retry import RetryPolicy, decide_retry


# ----------------------------------------------------------------
# Tokenizer
# ----------------------------------------------------------------


SHORT_TEXT = "hello world, how are you doing today?"
LONG_TEXT = (
    "The quick brown fox jumps over the lazy dog. " * 200
)  # ~2000 tokens


@pytest.mark.perf
def test_perf_count_text_short(benchmark) -> None:
    benchmark(tk.count_text, SHORT_TEXT, "gpt-4o")


@pytest.mark.perf
def test_perf_count_text_long(benchmark) -> None:
    benchmark(tk.count_text, LONG_TEXT, "gpt-4o")


@pytest.mark.perf
def test_perf_count_messages(benchmark) -> None:
    msgs = [
        ChatMessage(role="user", content=SHORT_TEXT),
        ChatMessage(role="assistant", content=LONG_TEXT[:500]),
        ChatMessage(role="user", content=SHORT_TEXT),
    ]
    benchmark(tk.count_messages, msgs, "gpt-4o")


# ----------------------------------------------------------------
# Retry policy is a pure function — should be sub-microsecond.
# ----------------------------------------------------------------


@pytest.mark.perf
def test_perf_decide_retry(benchmark) -> None:
    err = LLMError(LLMErrorClass.TRANSIENT_5XX, "boom")
    policy = RetryPolicy(jitter=False)

    def call() -> None:
        decide_retry(err, attempt=2, policy=policy)

    benchmark(call)
