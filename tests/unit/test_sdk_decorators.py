"""Tests for the SDK decorators + IRBuilder."""

from __future__ import annotations

import pytest

from agentkit import Event, IRBuilder, agent, judge, workflow
from agentkit.llm.models import LLMBinding
from agentkit.models.enums import Role
from agentkit.sdk.decorators import get_agent_meta


class TestAgentDecorator:
    def test_attaches_metadata(self) -> None:
        @agent(role="thinking", subscribe=["agent.x.in.q"], publish=["agent.x.out.r"])
        async def my_handler(ctx, event):
            return []

        meta = get_agent_meta(my_handler)
        assert meta is not None
        assert meta.template_key == "my_handler"
        assert meta.template.role is Role.THINKING
        assert meta.template.subscribe[0].topic == "agent.x.in.q"
        assert meta.template.publish[0].topic == "agent.x.out.r"

    def test_function_unchanged(self) -> None:
        # The decorator returns the function as-is (still callable directly).
        @agent(role="thinking")
        async def my_handler(ctx, event):
            return [Event(topic="agent.test.out.x", payload={"k": 1})]

        # Raw call works without going through ctx.publish:
        # (here we just verify it remains async-callable)
        import asyncio
        result = asyncio.run(my_handler(None, None))  # type: ignore[arg-type]
        assert result[0].topic == "agent.test.out.x"

    def test_llm_string_shorthand(self) -> None:
        @agent(role="thinking", llm="openai/gpt-4o-mini")
        async def h(ctx, event):
            return []

        meta = get_agent_meta(h)
        assert meta.template.llm == LLMBinding(provider="openai", model="gpt-4o-mini")

    def test_explicit_template_key(self) -> None:
        @agent(role="thinking", template_key="custom_name")
        async def whatever(ctx, event):
            return []

        meta = get_agent_meta(whatever)
        assert meta.template_key == "custom_name"

    def test_subscribe_with_tag_filter(self) -> None:
        @agent(role="thinking",
               subscribe=[("agent.x.in.q", {"language": "zh"})],
               publish=["agent.x.out.r"])
        async def h(ctx, event):
            return []

        meta = get_agent_meta(h)
        assert meta.template.subscribe[0].tag_filter == {"language": "zh"}

    def test_judge_role(self) -> None:
        @judge(subscribe=["agent.j.in.q"], publish=["agent.j.out.choice"])
        async def my_judge(ctx, event):
            return []

        meta = get_agent_meta(my_judge)
        assert meta.template.role is Role.JUDGE


class TestIRBuilder:
    def test_minimal_build(self) -> None:
        b = IRBuilder(id="wf_test", description="x")
        b.add_agent(
            "a", role="thinking",
            subscribe=["agent.a.in.q"],
            publish=["agent.a.out.r"],
        )
        b.add_edge("e_in", from_="__start__", to="a", via="agent.a.in.q")
        b.add_edge("e_out", from_="a", to="__end__", via="agent.a.out.r")

        ir = b.build()
        assert ir.id == "wf_test"
        assert "a" in ir.agents
        assert len(ir.edges) == 2

    def test_duplicate_agent_rejected(self) -> None:
        b = IRBuilder(id="wf")
        b.add_agent("a", role="thinking")
        with pytest.raises(ValueError, match="already added"):
            b.add_agent("a", role="thinking")

    def test_switch_edge(self) -> None:
        b = IRBuilder(id="wf")
        b.add_agent("judge", role="judge")
        b.add_agent("writer", role="thinking")
        b.add_switch(
            "e_branch",
            from_="judge",
            expr="$.choice",
            cases={
                "writer": {"to": "writer", "via": "agent.writer.in.draft"},
                "end": {"to": "__end__"},
            },
        )
        ir = b.build()
        assert ir.edges["e_branch"].is_switch

    def test_with_guardrails(self) -> None:
        b = IRBuilder(id="wf")
        b.add_agent("a", role="thinking")
        b.set_guardrails(
            per_agent={"max_tokens_per_call": 100, "max_cycles": 2},
            per_run={"max_total_tokens": 1000, "max_cycles_per_run": 5},
        )
        ir = b.build()
        assert ir.guardrails is not None
        assert ir.guardrails.per_run.max_total_tokens == 1000


class TestWorkflowDef:
    def test_compile_round_trip(self) -> None:
        @agent(role="thinking", subscribe=["q"], publish=["r"])
        async def echo(ctx, event):
            return [Event(topic="r", payload=event.payload)]

        wf = workflow("wf_test", description="echo")
        wf.add(echo)
        wf.connect("__start__", "echo", via="q")
        wf.connect("echo", "__end__", via="r")

        ir, plan = wf.compile()
        assert ir.id == "wf_test"
        assert "echo" in plan.agents
        assert ir.meta.ir_hash != ""

    def test_add_non_agent_rejected(self) -> None:
        async def not_decorated(ctx, event):
            return []

        wf = workflow("wf_x")
        with pytest.raises(TypeError, match="not an @agent-decorated"):
            wf.add(not_decorated)

    def test_yaml_round_trip(self, tmp_path) -> None:
        @agent(role="thinking", subscribe=["q"], publish=["r"])
        async def echo(ctx, event):
            return []

        wf = workflow("wf_x", description="round trip")
        wf.add(echo)
        wf.connect("__start__", "echo", via="q")
        wf.connect("echo", "__end__", via="r")

        out = tmp_path / "wf.yaml"
        wf.dump_yaml(out)
        text = out.read_text(encoding="utf-8")
        assert "wf_x" in text
        assert "echo" in text
