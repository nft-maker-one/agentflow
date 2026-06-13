"""Tests for the class-based Agent API."""

from __future__ import annotations

import pytest

from agentkit import END, ERROR, START, Agent, Event, workflow
from agentkit.llm.errors import LLMError, LLMErrorClass
from agentkit.models.enums import Role, RunStatus
from agentkit.testing import LocalRuntime, MockLLMGateway, MockLLMProvider
from agentkit.testing.mock_llm import MockLLMProvider as _MockProv  # noqa: F401
from agentkit.llm.gateway import LLMGatewayClient


# ----------------------------------------------------------------
# Construction + class-attr → meta
# ----------------------------------------------------------------


class TestAgentConstruction:
    def test_subclass_class_attrs_propagate(self) -> None:
        class Researcher(Agent):
            role = "thinking"
            subscribe = ["agent.x.in.q"]
            publish = ["agent.x.out.r"]
            tags = {"language": "zh"}

            async def handle(self, ctx, event):
                return []

        r = Researcher()
        assert r.key == "researcher"  # auto from class name
        assert r.meta.template.role is Role.THINKING
        assert r.subscribe_topics == ["agent.x.in.q"]
        assert r.publish_topics == ["agent.x.out.r"]
        assert r.meta.template.tags == {"language": "zh"}

    def test_camel_case_to_snake_case_key(self) -> None:
        class URLFetcher(Agent):
            subscribe = ["q"]
            publish = ["r"]
            async def handle(self, ctx, event):
                return []

        class HTTPClient(Agent):
            subscribe = ["q"]
            publish = ["r"]
            async def handle(self, ctx, event):
                return []

        assert URLFetcher().key == "url_fetcher"
        assert HTTPClient().key == "http_client"

    def test_explicit_template_key_overrides_default(self) -> None:
        class Worker(Agent):
            template_key = "custom_worker_key"
            subscribe = ["q"]
            publish = ["r"]
            async def handle(self, ctx, event):
                return []

        # Class-level explicit
        assert Worker().key == "custom_worker_key"
        # __init__ override
        assert Worker(template_key="other").key == "other"

    def test_init_kwargs_override_class_attrs(self) -> None:
        class Researcher(Agent):
            role = "thinking"
            subscribe = ["agent.x.in.q"]
            publish = ["agent.x.out.r"]
            tags = {"language": "zh"}
            async def handle(self, ctx, event):
                return []

        zh = Researcher(template_key="zh_research")
        en = Researcher(template_key="en_research", tags={"language": "en"})

        assert zh.meta.template.tags == {"language": "zh"}
        assert en.meta.template.tags == {"language": "en"}
        # Class default unchanged.
        assert Researcher.tags == {"language": "zh"}

    def test_unknown_kwarg_rejected(self) -> None:
        class W(Agent):
            subscribe = ["q"]
            publish = ["r"]
            async def handle(self, ctx, event):
                return []

        with pytest.raises(TypeError, match="unknown init kwargs"):
            W(weird_kwarg=1)  # type: ignore[call-arg]

    def test_handle_default_pass_through(self) -> None:
        """When the user doesn't override handle(), default handler runs.

        Pass-through mode: just forwards event.payload to publish[0].
        """
        class Forwarder(Agent):
            subscribe = ["q"]
            publish = ["r"]
            # NOTE: no override of handle() — the default handler should
            # do pass-through.

        import asyncio
        from agentkit.testing import run_agent_locally

        events = asyncio.run(run_agent_locally(
            Forwarder().handler,
            input_topic="q",
            input_payload={"k": "v", "n": 7},
        ))
        assert len(events) == 1
        assert events[0].topic == "r"
        assert events[0].payload == {"k": "v", "n": 7}


# ----------------------------------------------------------------
# WorkflowDef integration with Agent objects
# ----------------------------------------------------------------


class TestWorkflowConnect:
    def _make_two_step(self):
        class Fetcher(Agent):
            role = "thinking"
            subscribe = ["agent.fetcher.in.q"]
            publish = ["agent.fetcher.out.r"]
            async def handle(self, ctx, event):
                return [Event("agent.fetcher.out.r", {"data": event.payload["q"]})]

        class Summarizer(Agent):
            role = "thinking"
            subscribe = ["agent.fetcher.out.r"]
            publish = ["agent.summarizer.out.r"]
            async def handle(self, ctx, event):
                return [Event("agent.summarizer.out.r", {"summary": "x"})]

        return Fetcher(), Summarizer()

    def test_connect_with_agent_objects_auto_via(self) -> None:
        fetcher, summarizer = self._make_two_step()

        wf = workflow("wf_obj_connect")
        wf.add(fetcher)
        wf.add(summarizer)
        wf.connect(START, fetcher)         # via auto from fetcher.subscribe[0]
        wf.connect(fetcher, summarizer)    # via auto from intersection
        wf.connect(summarizer, END)        # via auto from summarizer.publish[0]

        ir, plan = wf.compile()
        edges = {(e.from_, str(e.to)): e.via for e in ir.edges.values()}
        # __error__ edge is auto-injected by the Compiler.
        assert edges[("__start__", "fetcher")] == "agent.fetcher.in.q"
        assert edges[("fetcher", "summarizer")] == "agent.fetcher.out.r"
        assert edges[("summarizer", "__end__")] == "agent.summarizer.out.r"

    def test_connect_unregistered_agent_raises(self) -> None:
        class A(Agent):
            subscribe = ["q"]
            publish = ["r"]
            async def handle(self, ctx, event):
                return []

        a = A()
        wf = workflow("wf_x")
        # Note: we did NOT call wf.add(a)
        with pytest.raises(ValueError, match="not added to this workflow"):
            wf.connect(START, a)

    def test_connect_explicit_via_overrides_auto(self) -> None:
        fetcher, summarizer = self._make_two_step()
        wf = workflow("wf_explicit_via")
        wf.add(fetcher).add(summarizer)
        wf.connect(START, fetcher, via="agent.fetcher.in.q")
        wf.connect(fetcher, summarizer, via="agent.fetcher.out.r")
        wf.connect(summarizer, END, via="agent.summarizer.out.r")
        ir, _ = wf.compile()
        assert any(
            e.via == "agent.fetcher.out.r" for e in ir.edges.values()
        )

    def test_via_required_when_ambiguous(self) -> None:
        class MultiPub(Agent):
            subscribe = ["agent.mp.in.q"]
            publish = ["agent.mp.out.a", "agent.mp.out.b"]
            async def handle(self, ctx, event):
                return []

        m = MultiPub()
        wf = workflow("wf_amb")
        wf.add(m)
        # publishes 2 topics, no `to` agent → cannot auto-derive
        with pytest.raises(ValueError, match="Cannot auto-derive"):
            wf.connect(m, END)

    def test_string_keys_still_work_back_compat(self) -> None:
        # The legacy decorator API + string-based connect should still work
        # alongside the new class API.
        from agentkit import agent

        @agent(role="thinking", subscribe=["q"], publish=["r"])
        async def echo(ctx, event):
            return [Event("r", event.payload)]

        wf = workflow("wf_legacy")
        wf.add(echo)
        wf.connect("__start__", "echo", via="q")
        wf.connect("echo", "__end__", via="r")
        ir, _ = wf.compile()
        assert "echo" in ir.agents


# ----------------------------------------------------------------
# Class-based agent runs end-to-end via LocalRuntime
# ----------------------------------------------------------------


class TestClassAgentLocalRuntime:
    async def test_two_agent_class_pipeline_succeeds(self) -> None:
        class Fetcher(Agent):
            role = "thinking"
            subscribe = ["agent.fetcher.in.q"]
            publish = ["agent.fetcher.out.r"]
            async def handle(self, ctx, event):
                return [Event(
                    "agent.fetcher.out.r",
                    {"data": f"fetched-{event.payload.get('q')}"},
                )]

        class Summarizer(Agent):
            role = "thinking"
            subscribe = ["agent.fetcher.out.r"]
            publish = ["agent.summarizer.out.r"]
            async def handle(self, ctx, event):
                text = await ctx.llm.chat(
                    f"Summarize: {event.payload.get('data')}",
                )
                return [Event(
                    "agent.summarizer.out.r",
                    {"summary": text},
                )]

        wf = workflow("wf_class_e2e")
        wf.add(Fetcher())
        wf.add(Summarizer())
        f, s = next(iter(wf._agents_by_key.values())), list(wf._agents_by_key.values())[1]
        wf.connect(START, f)
        wf.connect(f, s)
        wf.connect(s, END)

        async with LocalRuntime(
            wf, llm=MockLLMGateway(reply="MOCK-SUMMARY"),
        ) as rt:
            run = await rt.run(input={"q": "hello"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.summarizer.out.r")[-1]
        assert out.payload["summary"] == "MOCK-SUMMARY"

    async def test_two_instances_of_same_class(self) -> None:
        """Two instances of the same class with different template_keys
        co-exist (parameterized agents)."""

        class Tagger(Agent):
            role = "thinking"
            # subscribe/publish supplied per-instance via __init__.
            async def handle(self, ctx, event):
                return [Event(
                    self.publish_topics[0],
                    {"tagged": event.payload.get("text", ""), "by": self.key},
                )]

        zh = Tagger(
            template_key="zh_tagger",
            subscribe=["agent.tag.in.zh"],
            publish=["agent.tag.out.zh"],
        )
        en = Tagger(
            template_key="en_tagger",
            subscribe=["agent.tag.in.en"],
            publish=["agent.tag.out.en"],
        )
        # Sanity: the two instances are independent.
        assert zh.key != en.key
        assert zh.subscribe_topics != en.subscribe_topics

        wf = workflow("wf_two_inst")
        # Only `zh` is wired into the graph in this test — `en` would
        # need its own __start__/__end__ edges to pass IR validation.
        wf.add(zh)
        wf.connect(START, zh)
        wf.connect(zh, END)

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"text": "你好"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.tag.out.zh")[-1]
        assert out.payload == {"tagged": "你好", "by": "zh_tagger"}


# ================================================================
# Direct instantiation API (no subclass needed)
# ================================================================


class TestAgentDirect:
    """Verify ``Agent(role=..., subscribe=..., publish=..., ...)`` works
    without any subclass — the default handler powers the pipeline."""

    async def test_pass_through_via_local_runtime(self) -> None:
        forwarder = Agent(
            template_key="forwarder",
            role="thinking",
            description="Forward the input verbatim",
            subscribe=["agent.fwd.in"],
            publish=["agent.fwd.out"],
        )
        wf = workflow("wf_direct_passthrough")
        wf.add(forwarder)
        wf.connect(START, forwarder)
        wf.connect(forwarder, END)

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"q": "hello"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.fwd.out")[-1]
        assert out.payload == {"q": "hello"}

    async def test_llm_mode_with_prompt_template(self) -> None:
        """Prompt + llm → default handler renders Jinja2, calls LLM."""
        summarizer = Agent(
            template_key="summarizer",
            role="thinking",
            description="Summarize the input",
            subscribe=["agent.sum.in"],
            publish=["agent.sum.out"],
            llm="mock/mock",
            prompt="Summarize: {{ payload.text }}",
            output_field="summary",
        )
        wf = workflow("wf_direct_llm")
        wf.add(summarizer)
        wf.connect(START, summarizer)
        wf.connect(summarizer, END)

        async with LocalRuntime(
            wf, llm=MockLLMGateway(reply="MOCK-SUMMARY"),
        ) as rt:
            run = await rt.run(
                input={"text": "long text here"}, timeout=5.0,
            )

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.sum.out")[-1]
        # output_field is "summary"; preserve_input=True keeps the
        # original "text" too.
        assert out.payload["summary"] == "MOCK-SUMMARY"
        assert out.payload["text"] == "long text here"

    def test_unknown_kwarg_rejected(self) -> None:
        with pytest.raises(TypeError, match="unknown init kwargs"):
            Agent(
                template_key="x", subscribe=["q"], publish=["r"],
                this_kwarg_is_bogus="oops",  # type: ignore[call-arg]
            )

    async def test_fallback_response_on_terminal_failure(self) -> None:
        """When LLM keeps failing past max_retries, fallback_response fires."""
        # Provider that always raises a retryable error.
        always_fail = MockLLMProvider("mock")
        for _ in range(20):
            always_fail.queue_error(
                LLMError(LLMErrorClass.PROVIDER_DOWN, "boom"),
            )
        gateway = LLMGatewayClient(
            providers={"mock": always_fail},
            default_provider="mock",
            default_model="mock",
        )

        a = Agent(
            template_key="failer",
            subscribe=["agent.f.in"],
            publish=["agent.f.out"],
            llm="mock/mock",
            prompt="anything",
            max_retries=2,
            retry_backoff_s=0.01,  # keep test fast
            fallback_response={"summary": "(LLM unavailable — fallback)"},
        )
        wf = workflow("wf_fallback")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={"text": "x"}, timeout=10.0)

        # Fallback emits a normal event → run reaches __end__ successfully.
        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.f.out")[-1]
        assert out.payload["summary"] == "(LLM unavailable — fallback)"
        assert "_fallback_reason" in out.payload
        # The Gateway has its own RetryPolicy that may consume multiple
        # queued errors per default-handler attempt — we just verify
        # fallback fired (the run still succeeded with the canned payload).

    async def test_max_retries_succeeds_after_transient_error(self) -> None:
        """max_retries lets the default handler ride out a transient error."""
        prov = MockLLMProvider("mock")
        # First call fails (retryable) → retry → succeeds.
        prov.queue_error(LLMError(
            LLMErrorClass.PROVIDER_DOWN, "transient",
        ))
        prov.queue_response("OK-AFTER-RETRY")
        gateway = LLMGatewayClient(
            providers={"mock": prov},
            default_provider="mock", default_model="mock",
        )

        a = Agent(
            template_key="flaky",
            subscribe=["agent.flaky.in"],
            publish=["agent.flaky.out"],
            llm="mock/mock",
            prompt="hello",
            max_retries=3,
            retry_backoff_s=0.01,
            output_field="answer",
        )
        wf = workflow("wf_retry")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={"x": 1}, timeout=10.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.flaky.out")[-1]
        assert out.payload["answer"] == "OK-AFTER-RETRY"
        # Exactly 2 actions consumed (1 fail + 1 success).
        assert prov.remaining_actions == 0

    def test_default_template_key_for_plain_Agent(self) -> None:
        """Direct ``Agent(...)`` without template_key defaults to 'agent'."""
        a = Agent(subscribe=["q"], publish=["r"])
        assert a.key == "agent"

    async def test_subclass_can_override_handle_to_skip_default(self) -> None:
        """Subclass that overrides handle() bypasses the default body."""
        class Custom(Agent):
            subscribe = ["agent.c.in"]
            publish = ["agent.c.out"]

            async def handle(self, ctx, event):
                # Custom logic — ignore prompt/llm completely.
                return [Event("agent.c.out", {"custom": True, "in": event.payload})]

        wf = workflow("wf_custom")
        wf.add(Custom())
        wf.connect(START, "custom")
        wf.connect("custom", END)

        async with LocalRuntime(wf, llm=MockLLMGateway()) as rt:
            run = await rt.run(input={"a": 1}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.c.out")[-1]
        assert out.payload == {"custom": True, "in": {"a": 1}}


# ================================================================
# json_output / json_schema / json_unwrap
# ================================================================


class TestAgentJsonOutput:
    """When ``json_output=True`` the default handler:

    * asks the provider for ``response_format=json_object``
    * parses the LLM text as JSON (tolerating ```json``` fences)
    * publishes a *dict* (not a string) downstream
    """

    @staticmethod
    def _build_gateway_with_replies(*replies: str):
        """Helper: a Gateway whose mock provider plays the replies in order."""
        prov = MockLLMProvider("mock")
        for r in replies:
            prov.queue_response(r)
        return LLMGatewayClient(
            providers={"mock": prov},
            default_provider="mock", default_model="mock",
        ), prov

    async def test_json_output_parses_into_dict(self) -> None:
        gateway, _ = self._build_gateway_with_replies(
            '{"name": "Alice", "age": 30}',
        )
        a = Agent(
            template_key="extractor",
            subscribe=["agent.x.in"],
            publish=["agent.x.out"],
            llm="mock/mock",
            prompt="Extract person from: {{ payload.text }}",
            json_output=True,
            output_field="person",
        )
        wf = workflow("wf_jsonout")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(
                input={"text": "Alice is 30 years old"}, timeout=5.0,
            )

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.x.out")[-1]
        # The LLM reply is parsed into a dict and tucked under output_field.
        assert out.payload["person"] == {"name": "Alice", "age": 30}
        # Input is preserved (preserve_input default = True).
        assert out.payload["text"] == "Alice is 30 years old"

    async def test_json_output_with_markdown_fences_still_parses(self) -> None:
        """Some providers wrap JSON in ```json``` despite our prompt."""
        gateway, _ = self._build_gateway_with_replies(
            '```json\n{"verdict": "approve"}\n```',
        )
        a = Agent(
            template_key="judge",
            subscribe=["agent.j.in"],
            publish=["agent.j.out"],
            llm="mock/mock",
            prompt="Judge this",
            json_output=True,
            output_field="result",
        )
        wf = workflow("wf_fence")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={"q": "x"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.j.out")[-1]
        assert out.payload["result"] == {"verdict": "approve"}

    async def test_json_unwrap_merges_at_top_level(self) -> None:
        gateway, _ = self._build_gateway_with_replies(
            '{"choice": "publish", "score": 0.9}',
        )
        a = Agent(
            template_key="critic",
            subscribe=["agent.c.in"],
            publish=["agent.c.out"],
            llm="mock/mock",
            prompt="Decide",
            json_output=True,
            json_unwrap=True,    # spread parsed dict at top level
        )
        wf = workflow("wf_unwrap")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={"q": "test"}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.c.out")[-1]
        # Keys spread directly (no nesting under output_field).
        assert out.payload["choice"] == "publish"
        assert out.payload["score"] == 0.9
        assert out.payload["q"] == "test"

    async def test_malformed_json_triggers_retry(self) -> None:
        """First reply is garbage, second is valid → retries succeed."""
        gateway, prov = self._build_gateway_with_replies(
            "this is not JSON at all",   # parse fails
            '{"ok": true}',               # second attempt parses
        )
        a = Agent(
            template_key="strict",
            subscribe=["agent.s.in"],
            publish=["agent.s.out"],
            llm="mock/mock",
            prompt="Reply with JSON",
            json_output=True,
            max_retries=2,
            retry_backoff_s=0.01,
            output_field="data",
        )
        wf = workflow("wf_jsonretry")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.s.out")[-1]
        assert out.payload["data"] == {"ok": True}
        # Both replies consumed: 1 garbage + 1 valid.
        assert prov.remaining_actions == 0

    async def test_malformed_json_falls_back_after_retries(self) -> None:
        """All replies invalid → fallback_response fires."""
        gateway, _ = self._build_gateway_with_replies(*["bogus"] * 10)
        a = Agent(
            template_key="brittle",
            subscribe=["agent.b.in"],
            publish=["agent.b.out"],
            llm="mock/mock",
            prompt="Reply with JSON",
            json_output=True,
            max_retries=1,
            retry_backoff_s=0.01,
            output_field="data",
            fallback_response={"data": {"_error": "could not parse JSON"}},
        )
        wf = workflow("wf_jsonfallback")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.b.out")[-1]
        # Fallback path: data was overwritten by canned response.
        assert out.payload["data"] == {"_error": "could not parse JSON"}
        assert "_fallback_reason" in out.payload

    async def test_json_schema_validation_passes(self) -> None:
        gateway, _ = self._build_gateway_with_replies(
            '{"name": "Bob", "age": 25}',
        )
        a = Agent(
            template_key="typed",
            subscribe=["agent.t.in"],
            publish=["agent.t.out"],
            llm="mock/mock",
            prompt="extract",
            json_output=True,
            json_schema={
                "type": "object",
                "required": ["name", "age"],
                "properties": {
                    "name": {"type": "string"},
                    "age":  {"type": "integer"},
                },
            },
            output_field="person",
        )
        wf = workflow("wf_jsonschema_ok")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.t.out")[-1]
        assert out.payload["person"]["name"] == "Bob"

    async def test_json_schema_violation_triggers_retry(self) -> None:
        """First reply fails schema; second satisfies it → retries succeed."""
        gateway, prov = self._build_gateway_with_replies(
            '{"name": "Charlie"}',           # missing required `age`
            '{"name": "Charlie", "age": 5}', # complete
        )
        a = Agent(
            template_key="typed_retry",
            subscribe=["agent.tr.in"],
            publish=["agent.tr.out"],
            llm="mock/mock",
            prompt="extract",
            json_output=True,
            json_schema={
                "type": "object",
                "required": ["name", "age"],
            },
            max_retries=2,
            retry_backoff_s=0.01,
        )
        wf = workflow("wf_schema_retry")
        wf.add(a)
        wf.connect(START, a)
        wf.connect(a, END)

        async with LocalRuntime(wf, llm=gateway) as rt:
            run = await rt.run(input={}, timeout=5.0)

        assert run.status is RunStatus.SUCCEEDED
        out = rt.bus.published_for_topic("agent.tr.out")[-1]
        assert out.payload["result"]["age"] == 5
        assert prov.remaining_actions == 0
