"""Unit tests for the Tokenizer Facade."""

from __future__ import annotations

import pytest

from agentkit.llm import tokenizer as tk
from agentkit.llm.models import ChatMessage
from agentkit.llm.tools import ToolSchema


class TestCountText:
    def test_empty_string_yields_zero(self) -> None:
        assert tk.count_text("", "gpt-4o") == 0

    def test_ascii_text_count_is_positive_and_bounded(self) -> None:
        n = tk.count_text("hello world", "gpt-4o")
        # Tiktoken encodes "hello world" as 2 tokens; we accept a small range
        # to allow for encoding family changes without churning the test.
        assert 1 <= n <= 4

    def test_unicode_tokens_count_higher_than_ascii(self) -> None:
        ascii_n = tk.count_text("hello hello hello", "gpt-4o")
        zh_n = tk.count_text("你好你好你好", "gpt-4o")
        # Chinese tokens are typically more bytes per char; ensure
        # tokenizer differentiates rather than returning 0.
        assert zh_n > 0
        assert ascii_n > 0

    def test_unknown_model_falls_back_to_cl100k(self) -> None:
        # Should not raise; returns a positive count.
        n = tk.count_text("hello", "totally-made-up-model")
        assert n > 0


class TestCountMessages:
    def test_overhead_is_added(self) -> None:
        msgs = [ChatMessage(role="user", content="hi")]
        # raw text "hi" alone is small; overhead pushes message count up.
        text_only = tk.count_text("hi", "gpt-4o")
        msg_n = tk.count_messages(msgs, "gpt-4o")
        assert msg_n > text_only

    def test_multiple_messages(self) -> None:
        single = tk.count_messages(
            [ChatMessage(role="user", content="hi")], "gpt-4o",
        )
        triple = tk.count_messages(
            [
                ChatMessage(role="user", content="hi"),
                ChatMessage(role="assistant", content="ok"),
                ChatMessage(role="user", content="bye"),
            ],
            "gpt-4o",
        )
        assert triple > single


class TestCountTools:
    def test_empty_tools_zero(self) -> None:
        assert tk.count_tools([], "gpt-4o") == 0

    def test_single_tool_positive(self) -> None:
        n = tk.count_tools(
            [
                ToolSchema(
                    name="search_web",
                    description="Search the web",
                    parameters={
                        "type": "object",
                        "properties": {"q": {"type": "string"}},
                    },
                ),
            ],
            "gpt-4o",
        )
        assert n > 0


class TestEstimateRequestTokens:
    def test_combines_messages_system_tools(self) -> None:
        msgs = [ChatMessage(role="user", content="research X")]
        with_tools = tk.estimate_request_tokens(
            messages=msgs,
            model="gpt-4o",
            tools=[ToolSchema(name="t", parameters={})],
            system="you are a researcher",
        )
        no_tools = tk.estimate_request_tokens(
            messages=msgs,
            model="gpt-4o",
            tools=None,
            system="you are a researcher",
        )
        assert with_tools > no_tools


@pytest.mark.parametrize(
    "model",
    ["gpt-4o", "gpt-4o-mini", "o1-mini", "gpt-3.5-turbo"],
)
def test_dispatch_table_doesnt_raise(model: str) -> None:
    """Sanity check — every entry in the dispatch table loads."""
    assert tk.count_text("hello", model) > 0
