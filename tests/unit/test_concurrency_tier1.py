"""Tests for the tier-1 concurrency optimizations (docs/CONCURRENCY.md §10).

Covers:
* ``common.offload.maybe_offload`` — inline vs thread by size_hint (B2/B5/B7).
* ``common.jsonschema_cache`` — validator caching + correctness (B5).
* Agent Jinja prompt **pre-compilation** + StrictUndefined behavior (B6).
* Agent ``python_script`` offloaded to a thread, sync + async (B2).
* Agent ``json_output`` parsing via orjson + cached schema validation (B7/B5).
* ``RuntimeSettings.fanin_queue_size`` wired into the AgentInstance queue.
* InProcessGuardrail no longer carries the unused ``_global_lock`` (B10).
"""

from __future__ import annotations

import asyncio
import threading

import jsonschema
import pytest

from agentkit import Agent
from agentkit.common import jsonschema_cache as jsc
from agentkit.common.offload import DEFAULT_OFFLOAD_THRESHOLD, maybe_offload
from agentkit.guardrail.inprocess import InProcessGuardrail
from agentkit.runtime.config import RuntimeSettings
from agentkit.runtime.context import Event
from agentkit.testing.local_runtime import run_agent_locally


# ============================================================
# offload.maybe_offload
# ============================================================


class TestMaybeOffload:
    async def test_inline_below_threshold_runs_on_event_loop(self) -> None:
        loop_thread = threading.get_ident()
        seen: dict[str, int] = {}

        def fn(x: str) -> int:
            seen["thread"] = threading.get_ident()
            return len(x)

        out = await maybe_offload(fn, "abc", size_hint=3, threshold=10)
        assert out == 3
        # Inline → ran on the event-loop thread (no hand-off).
        assert seen["thread"] == loop_thread

    async def test_offloads_to_worker_thread_above_threshold(self) -> None:
        loop_thread = threading.get_ident()
        seen: dict[str, int] = {}

        def fn(x: str) -> int:
            seen["thread"] = threading.get_ident()
            return len(x)

        out = await maybe_offload(fn, "x" * 100, size_hint=100, threshold=10)
        assert out == 100
        # Offloaded → ran on a different (worker pool) thread.
        assert seen["thread"] != loop_thread

    async def test_none_size_hint_always_offloads(self) -> None:
        out = await maybe_offload(str.upper, "hi")
        assert out == "HI"

    def test_default_threshold_is_positive(self) -> None:
        assert DEFAULT_OFFLOAD_THRESHOLD > 0


# ============================================================
# jsonschema_cache
# ============================================================


class TestJsonSchemaCache:
    def setup_method(self) -> None:
        jsc.clear_cache()

    def test_validator_is_cached_per_schema_identity(self) -> None:
        schema = {"type": "object"}
        assert jsc.get_validator(schema) is jsc.get_validator(schema)

    def test_distinct_schemas_get_distinct_validators(self) -> None:
        a = {"type": "object"}
        b = {"type": "array"}
        assert jsc.get_validator(a) is not jsc.get_validator(b)

    def test_valid_payload_passes(self) -> None:
        schema = {"type": "object", "required": ["x"]}
        jsc.validate_cached({"x": 1}, schema)  # no raise

    def test_invalid_payload_raises_validation_error(self) -> None:
        schema = {"type": "object", "required": ["x"]}
        with pytest.raises(jsonschema.ValidationError):
            jsc.validate_cached({"y": 1}, schema)

    def test_clear_cache_drops_validators(self) -> None:
        schema = {"type": "object"}
        v1 = jsc.get_validator(schema)
        jsc.clear_cache()
        assert jsc.get_validator(schema) is not v1


# ============================================================
# Agent Jinja prompt pre-compilation (B6)
# ============================================================


def _prompt_agent(prompt: str) -> Agent:
    return Agent(
        template_key="t", subscribe=["agent.t.in"], publish=["agent.t.out"],
        llm="deepseek/deepseek-chat", prompt=prompt, output_field="out",
    )


class TestPromptPrecompile:
    def test_prompt_is_precompiled_at_construction(self) -> None:
        a = _prompt_agent("Hello {{ name }}")
        assert a._compiled_prompt is not None

    def test_no_prompt_means_no_compiled_template(self) -> None:
        a = Agent(template_key="t", subscribe=["agent.t.in"],
                  publish=["agent.t.out"])
        assert a._compiled_prompt is None

    def test_render_uses_payload_event_and_topic(self) -> None:
        a = _prompt_agent("Hi {{ name }} via {{ topic }} (k={{ k }})")
        ev = Event("agent.t.in", {"name": "Jerry", "k": 7})
        assert a._render_compiled(ev) == "Hi Jerry via agent.t.in (k=7)"

    def test_render_payload_key_colliding_with_reserved_name(self) -> None:
        # A start-input field named `topic` must NOT crash the render
        # (regression: `render(topic=…, **payload)` raised "got multiple
        # values for keyword argument 'topic'"). Payload key wins; the
        # envelope topic stays reachable via `event.topic`.
        a = _prompt_agent("subject={{ topic }} env={{ event.topic }}")
        ev = Event("agent.t.in", {"topic": "羽扇纶巾"})
        assert a._render_compiled(ev) == "subject=羽扇纶巾 env=agent.t.in"

    def test_strict_undefined_still_raises_on_missing_var(self) -> None:
        # A missing variable still fails loudly — now as an actionable
        # PermanentError (which classifies FATAL: no pointless retries),
        # wrapping the underlying Jinja UndefinedError as its cause.
        from agentkit.common.errors import PermanentError
        from jinja2 import UndefinedError
        a = _prompt_agent("Hello {{ missing }}")
        with pytest.raises(PermanentError) as ei:
            a._render_compiled(Event("agent.t.in", {}))
        assert isinstance(ei.value.__cause__, UndefinedError)
        assert "Available payload fields" in str(ei.value)

    def test_bad_template_fails_fast_at_construction(self) -> None:
        from jinja2 import TemplateSyntaxError
        with pytest.raises(TemplateSyntaxError):
            _prompt_agent("Hello {{ unclosed ")


# ============================================================
# python_script offloaded to a thread (B2)
# ============================================================


class TestPythonScriptOffload:
    async def test_sync_script_runs_in_worker_thread(self) -> None:
        # The script records the thread it executed on; assert it's not
        # the event-loop thread (i.e. it was offloaded via to_thread).
        script = (
            "import threading\n"
            "def handle(payload):\n"
            "    return {'tid': threading.get_ident(), 'n': len(payload.get('s',''))}\n"
        )
        a = Agent(template_key="s", subscribe=["agent.s.in"],
                  publish=["agent.s.out"], python_script=script)
        loop_tid = threading.get_ident()
        evs = await run_agent_locally(
            a.handler, input_topic="agent.s.in", input_payload={"s": "abcd"},
        )
        assert evs[0].payload["n"] == 4
        assert evs[0].payload["tid"] != loop_tid  # ran off the loop thread

    async def test_async_script_still_supported(self) -> None:
        script = (
            "import asyncio\n"
            "async def handle(payload):\n"
            "    await asyncio.sleep(0)\n"
            "    return {'doubled': payload.get('n', 0) * 2}\n"
        )
        a = Agent(template_key="d", subscribe=["agent.d.in"],
                  publish=["agent.d.out"], python_script=script)
        evs = await run_agent_locally(
            a.handler, input_topic="agent.d.in", input_payload={"n": 21},
        )
        assert evs[0].payload["doubled"] == 42

    async def test_two_arg_signature_still_supported(self) -> None:
        script = (
            "def handle(payload, event):\n"
            "    return {'topic': event.topic}\n"
        )
        a = Agent(template_key="e", subscribe=["agent.e.in"],
                  publish=["agent.e.out"], python_script=script)
        evs = await run_agent_locally(
            a.handler, input_topic="agent.e.in", input_payload={},
        )
        assert evs[0].payload["topic"] == "agent.e.in"


# ============================================================
# json_output: orjson parse + cached schema validation (B7/B5)
# ============================================================


class TestJsonOutputPath:
    async def test_json_output_parsed_and_schema_validated(self) -> None:
        from agentkit.testing.mock_llm import MockLLMGateway

        schema = {
            "type": "object",
            "required": ["sentiment"],
            "properties": {"sentiment": {"type": "string"}},
        }
        a = Agent(
            template_key="tag", subscribe=["agent.tag.in"], publish=["agent.tag.out"],
            llm="mock/mock", prompt="classify {{ text }}",
            json_output=True, json_schema=schema, output_field="result",
        )
        llm = MockLLMGateway(reply='{"sentiment": "positive"}')
        evs = await run_agent_locally(
            a.handler, input_topic="agent.tag.in",
            input_payload={"text": "great"}, llm=llm,
        )
        assert evs[0].payload["result"] == {"sentiment": "positive"}

    async def test_json_output_invalid_json_falls_back(self) -> None:
        from agentkit.testing.mock_llm import MockLLMGateway

        a = Agent(
            template_key="tag2", subscribe=["agent.tag2.in"], publish=["agent.tag2.out"],
            llm="mock/mock", prompt="x {{ text }}",
            json_output=True, output_field="result",
            max_retries=0, fallback_response={"result": "unknown"},
        )
        llm = MockLLMGateway(reply="not json at all")
        evs = await run_agent_locally(
            a.handler, input_topic="agent.tag2.in",
            input_payload={"text": "y"}, llm=llm,
        )
        # Parse failure → retries exhausted → static fallback emitted.
        assert evs[0].payload["result"] == "unknown"


# ============================================================
# Config knobs + dead-code removal
# ============================================================


class TestConfigAndCleanup:
    def test_fanin_queue_size_default(self) -> None:
        assert RuntimeSettings().fanin_queue_size == 512

    def test_fanin_queue_size_wired_into_instance(self) -> None:
        from agentkit.runtime.instance import AgentInstance
        from agentkit.bus.inprocess import InProcessEventBus
        from agentkit.testing.mock_llm import MockLLMGateway
        from agentkit.workflow.plan import AgentPlan

        plan = AgentPlan(
            template_key="x", role="thinking", description="",
            subscribe_topics=["agent.x.in"], publish_topics=["agent.x.out"],
        )

        async def _build_and_check() -> None:
            inst = AgentInstance(
                plan=plan, workflow_id="wf", bus=InProcessEventBus(),
                llm=MockLLMGateway(),
                executor=type("E", (), {"on_event": None})(),
                settings=RuntimeSettings(fanin_queue_size=99),
            )
            assert inst._fanin_queue.maxsize == 99

        asyncio.run(_build_and_check())

    def test_inprocess_guardrail_has_no_global_lock(self) -> None:
        g = InProcessGuardrail()
        assert not hasattr(g, "_global_lock")
