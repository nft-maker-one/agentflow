"""Unit tests for the executor / handler registry / decorator."""

from __future__ import annotations

import pytest

from agentkit.common.errors import AgentKitError
from agentkit.runtime.context import AgentContext, Event
from agentkit.runtime.executor import (
    HandlerRegistry,
    _FunctionExecutor,
    agent_handler,
)


class TestRegistry:
    def test_register_and_lookup(self) -> None:
        reg = HandlerRegistry()

        async def fn(ctx, event):
            return []

        reg.register(
            workflow_id="wf",
            template_key="t",
            executor=_FunctionExecutor(fn),
        )
        assert reg.get(workflow_id="wf", template_key="t") is not None
        assert ("wf", "t") in reg
        assert len(reg) == 1

    def test_double_register_rejected(self) -> None:
        reg = HandlerRegistry()

        async def a(ctx, event):
            return []

        async def b(ctx, event):
            return []

        reg.register(workflow_id="wf", template_key="t", executor=_FunctionExecutor(a))
        with pytest.raises(AgentKitError, match="already registered"):
            reg.register(
                workflow_id="wf", template_key="t", executor=_FunctionExecutor(b),
            )

    def test_replace_flag_overrides(self) -> None:
        reg = HandlerRegistry()

        async def a(ctx, event):
            return []

        async def b(ctx, event):
            return []

        reg.register(workflow_id="wf", template_key="t", executor=_FunctionExecutor(a))
        reg.register(
            workflow_id="wf",
            template_key="t",
            executor=_FunctionExecutor(b),
            replace=True,
        )
        assert reg.get(workflow_id="wf", template_key="t") is not None

    def test_require_raises_when_missing(self) -> None:
        reg = HandlerRegistry()
        with pytest.raises(AgentKitError, match="no handler registered"):
            reg.require(workflow_id="wf", template_key="ghost")


class TestDecorator:
    def test_decorator_registers_into_explicit_registry(self) -> None:
        reg = HandlerRegistry()

        @agent_handler(
            workflow_id="wf_x", template_key="researcher", registry=reg,
        )
        async def researcher(ctx, event):
            return []

        assert ("wf_x", "researcher") in reg
        # Decorator returns the function unchanged.
        assert callable(researcher)

    def test_decorator_falls_back_to_global(self) -> None:
        # Use a fresh global default to avoid cross-test pollution.
        global_reg = HandlerRegistry.global_default()
        global_reg.clear()
        try:
            @agent_handler(workflow_id="wf_global", template_key="t")
            async def handler(ctx, event):
                return []

            assert ("wf_global", "t") in global_reg
        finally:
            global_reg.clear()


class TestFunctionExecutor:
    async def test_returns_list_of_events(self) -> None:
        async def fn(ctx, event):
            return [Event(topic="agent.t.out.x", payload={"a": 1})]

        ex = _FunctionExecutor(fn)
        result = await ex.on_event(None, None)  # type: ignore[arg-type]
        assert len(result) == 1
        assert result[0].topic == "agent.t.out.x"

    async def test_none_normalized_to_empty(self) -> None:
        async def fn(ctx, event):
            return None

        ex = _FunctionExecutor(fn)
        result = await ex.on_event(None, None)  # type: ignore[arg-type]
        assert result == []

    async def test_non_list_return_raises(self) -> None:
        async def fn(ctx, event):
            return "oops"  # not a list

        ex = _FunctionExecutor(fn)
        with pytest.raises(AgentKitError, match="must return list"):
            await ex.on_event(None, None)  # type: ignore[arg-type]
