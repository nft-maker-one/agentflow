"""Tests for agentkit.testing helpers (LocalRuntime + run_agent_locally + MockLLMGateway)."""

from __future__ import annotations

import pytest

from agentkit import Event, agent, workflow
from agentkit.models.enums import RunStatus
from agentkit.testing import (
    LocalRuntime,
    MockLLMGateway,
    MockLLMProvider,
    run_agent_locally,
)


# ----------------------------------------------------------------
# MockLLMProvider
# ----------------------------------------------------------------


class TestMockLLMProvider:
    async def test_default_reply(self) -> None:
        from agentkit.llm.models import ChatMessage, LLMRequest

        prov = MockLLMProvider()
        prov.set_default_reply("hi there")
        rsp = await prov.complete(LLMRequest(
            provider="mock",
            model="mock",
            messages=[ChatMessage(role="user", content="hello")],
        ))
        assert rsp.text == "hi there"

    async def test_queued_responses_in_order(self) -> None:
        from agentkit.llm.models import ChatMessage, LLMRequest

        prov = MockLLMProvider()
        prov.queue_response("first").queue_response("second")
        req = LLMRequest(
            provider="mock", model="mock",
            messages=[ChatMessage(role="user", content="x")],
        )
        a = await prov.complete(req)
        b = await prov.complete(req)
        assert a.text == "first"
        assert b.text == "second"


# ----------------------------------------------------------------
# MockLLMGateway
# ----------------------------------------------------------------


class TestMockLLMGateway:
    async def test_chat_returns_default_reply(self) -> None:
        gateway = MockLLMGateway(reply="hello world")
        text = await gateway.chat("ping")
        assert text == "hello world"


# ----------------------------------------------------------------
# run_agent_locally — single handler tests
# ----------------------------------------------------------------


class TestRunAgentLocally:
    async def test_handler_returns_published_events(self) -> None:
        @agent(role="thinking",
               subscribe=["agent.echo.in.q"],
               publish=["agent.echo.out.r"])
        async def echo_handler(ctx, event):
            return [Event(
                topic="agent.echo.out.r",
                payload={"text": event.payload.get("q", "")},
            )]

        events = await run_agent_locally(
            echo_handler,
            input_topic="agent.echo.in.q",
            input_payload={"q": "hello"},
        )
        assert len(events) == 1
        assert events[0].payload["text"] == "hello"

    async def test_handler_can_call_llm(self) -> None:
        gateway = MockLLMGateway(reply="from-llm")

        @agent(role="thinking",
               subscribe=["agent.echo.in.q"],
               publish=["agent.echo.out.r"])
        async def llm_handler(ctx, event):
            text = await ctx.llm.chat("test prompt")
            return [Event(topic="agent.echo.out.r", payload={"reply": text})]

        events = await run_agent_locally(
            llm_handler,
            input_topic="agent.echo.in.q",
            input_payload={"q": "x"},
            llm=gateway,
        )
        assert events[0].payload["reply"] == "from-llm"

    async def test_handler_publish_via_ctx(self) -> None:
        @agent(role="thinking",
               subscribe=["agent.x.in.q"],
               publish=["agent.x.out.r"])
        async def via_ctx_handler(ctx, event):
            await ctx.publish(Event(
                topic="agent.x.out.r", payload={"k": "via_ctx"},
            ))
            return None  # no direct return

        events = await run_agent_locally(
            via_ctx_handler,
            input_topic="agent.x.in.q",
            input_payload={},
        )
        assert events[0].payload == {"k": "via_ctx"}

    async def test_non_agent_function_rejected(self) -> None:
        async def plain(ctx, event):
            return []

        with pytest.raises(TypeError, match="not @agent-decorated"):
            await run_agent_locally(
                plain,
                input_topic="t",
                input_payload={},
            )


# ----------------------------------------------------------------
# LocalRuntime — full workflow execution
# ----------------------------------------------------------------


class TestLocalRuntime:
    async def test_single_agent_workflow_succeeds(self) -> None:
        @agent(role="thinking",
               subscribe=["agent.echo.in.q"],
               publish=["agent.echo.out.r"])
        async def echo(ctx, event):
            return [Event(
                topic="agent.echo.out.r",
                payload={"text": event.payload.get("q", "")},
            )]

        wf = workflow("wf_local_e2e", description="echo demo")
        wf.add(echo)
        wf.connect("__start__", "echo", via="agent.echo.in.q")
        wf.connect("echo", "__end__", via="agent.echo.out.r")

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"q": "hello world"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        # Inspect the bus to see the final event.
        out = rt.bus.published_for_topic("agent.echo.out.r")
        assert out[-1].payload["text"] == "hello world"

    async def test_multi_agent_pipeline(self) -> None:
        @agent(role="thinking",
               subscribe=["agent.first.in.q"],
               publish=["agent.first.out.r"])
        async def first(ctx, event):
            return [Event(
                topic="agent.first.out.r",
                payload={"step1": event.payload.get("q") + "→1"},
            )]

        @agent(role="thinking",
               subscribe=["agent.first.out.r"],
               publish=["agent.second.out.r"])
        async def second(ctx, event):
            return [Event(
                topic="agent.second.out.r",
                payload={"step2": event.payload.get("step1") + "→2"},
            )]

        wf = workflow("wf_multi", description="2-step")
        wf.add(first).add(second)
        wf.connect("__start__", "first", via="agent.first.in.q")
        wf.connect("first", "second", via="agent.first.out.r")
        wf.connect("second", "__end__", via="agent.second.out.r")

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"q": "x"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        final = rt.bus.published_for_topic("agent.second.out.r")[-1]
        assert final.payload["step2"] == "x→1→2"

    async def test_handler_uses_mock_llm(self) -> None:
        @agent(role="thinking",
               subscribe=["agent.s.in.q"],
               publish=["agent.s.out.r"])
        async def summarizer(ctx, event):
            text = await ctx.llm.chat(
                f"Summarize: {event.payload.get('text','')}",
            )
            return [Event(
                topic="agent.s.out.r", payload={"summary": text},
            )]

        wf = workflow("wf_llm_demo")
        wf.add(summarizer)
        wf.connect("__start__", "summarizer", via="agent.s.in.q")
        wf.connect("summarizer", "__end__", via="agent.s.out.r")

        async with LocalRuntime(
            wf, llm=MockLLMGateway(reply="MOCKED-SUMMARY"),
        ) as rt:
            run = await rt.run(input={"text": "long text"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.s.out.r")[-1]
        assert out.payload["summary"] == "MOCKED-SUMMARY"
