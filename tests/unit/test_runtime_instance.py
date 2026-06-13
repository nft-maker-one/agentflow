"""End-to-end Runtime tests with MockBus + MockProvider.

These exercise the full ``subscribe → gate → handler → publish → ack``
loop without touching Kafka or any LLM endpoint.
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.llm import LLMGatewayClient, NoOpGuardrail
from agentkit.models.enums import AgentState
from agentkit.runtime import (
    AgentInstance,
    AgentWorker,
    Event,
    HandlerRegistry,
    agent_handler,
)
from agentkit.runtime.config import RuntimeSettings
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.workflow.plan import AgentPlan, BusTopicPlan, RuntimePlan
from tests.helpers.mock_bus import MockEventBus
from tests.helpers.mock_provider import MockProvider


# ----------------------------------------------------------------
# Fixtures
# ----------------------------------------------------------------


@pytest.fixture
async def bus() -> MockEventBus:
    b = MockEventBus()
    await b.start()
    return b


@pytest.fixture
def llm_gateway():
    prov = MockProvider("openai")
    return LLMGatewayClient(providers={"openai": prov})


@pytest.fixture
def echo_plan() -> AgentPlan:
    return AgentPlan(
        template_key="echo",
        role="thinking",
        description="echo agent",
        subscribe_topics=["agent.echo.in.q1"],
        subscribe_tag_filters=[{}],
        consumer_group="grp.wf_test.echo",
        publish_topics=["agent.echo.out.summary"],
        replica_min=1,
        replica_max=1,
    )


@pytest.fixture
def fast_settings() -> RuntimeSettings:
    """Tight timeouts so tests run quickly."""
    return RuntimeSettings(
        runtime_id="test-node",
        handler_timeout_ms=2_000,
        retry_base_ms=10,
        retry_max_ms=50,
        retry_jitter=False,
        max_handler_retries=2,
        dedup_window_ms=10_000,
    )


# ----------------------------------------------------------------
# Helpers
# ----------------------------------------------------------------


async def _run_for(coro, *, seconds: float):
    """Run a coro briefly, then cancel — used to exercise the dispatch loop."""
    task = asyncio.create_task(coro)
    await asyncio.sleep(seconds)
    return task


async def _stop_instance(instance: AgentInstance, task: asyncio.Task) -> None:
    await instance.cancel()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()


# ----------------------------------------------------------------
# Successful path
# ----------------------------------------------------------------


class TestSuccessfulDispatch:
    async def test_handler_runs_and_publishes(self, bus, llm_gateway, echo_plan, fast_settings) -> None:
        async def handler(ctx, event):
            return [Event(
                topic="agent.echo.out.summary",
                payload={"echoed": event.payload.get("text")},
            )]

        instance = AgentInstance(
            plan=echo_plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(handler),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
        )

        task = asyncio.create_task(instance.run())
        # Give the subscription a beat to register.
        await asyncio.sleep(0.1)

        await bus.publish(build_envelope(
            topic="agent.echo.in.q1",
            payload={"text": "hello"},
            workflow_id="wf_test",
            run_id="run_t1",
        ))

        # Wait for handler to run + publish.
        for _ in range(40):
            await asyncio.sleep(0.05)
            if bus.published_for_topic("agent.echo.out.summary"):
                break

        published = bus.published_for_topic("agent.echo.out.summary")
        assert len(published) == 1
        assert published[0].payload == {"echoed": "hello"}
        # Sender is properly stamped.
        assert published[0].from_.agent_id == instance.agent_id

        await _stop_instance(instance, task)


# ----------------------------------------------------------------
# Whitelist enforcement (Pipeline rejects out-of-whitelist publish)
# ----------------------------------------------------------------


class TestPublishWhitelistEnforced:
    async def test_publish_to_unallowed_topic_is_fatal(
        self, bus, llm_gateway, echo_plan, fast_settings,
    ) -> None:
        async def naughty(ctx, event):
            return [Event(
                topic="agent.evil.out.spam",  # NOT in whitelist
                payload={"x": 1},
            )]

        instance = AgentInstance(
            plan=echo_plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(naughty),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
        )
        task = asyncio.create_task(instance.run())
        await asyncio.sleep(0.1)

        await bus.publish(build_envelope(
            topic="agent.echo.in.q1",
            payload={"text": "hi"},
            workflow_id="wf_test",
            run_id="run_t1",
        ))
        # Wait for handler to attempt + fail.
        await asyncio.sleep(0.3)

        # Nothing published on out topic.
        assert bus.published_for_topic("agent.echo.out.summary") == []
        # The original event was nack'd to DLQ.
        assert any(
            e.topic.endswith(".dlq") for _, e, _ in bus.dlq
        )

        await _stop_instance(instance, task)


# ----------------------------------------------------------------
# Retry on recoverable errors
# ----------------------------------------------------------------


class TestRetryOnRecoverable:
    async def test_recoverable_error_retries_then_succeeds(
        self, bus, llm_gateway, echo_plan, fast_settings,
    ) -> None:
        attempts = []

        async def flaky(ctx, event):
            attempts.append(1)
            if len(attempts) < 2:
                # First call: simulate a transient blip
                raise ConnectionError("transient")
            return [Event(
                topic="agent.echo.out.summary",
                payload={"attempts": len(attempts)},
            )]

        instance = AgentInstance(
            plan=echo_plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(flaky),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
        )
        task = asyncio.create_task(instance.run())
        await asyncio.sleep(0.1)

        await bus.publish(build_envelope(
            topic="agent.echo.in.q1",
            payload={},
            workflow_id="wf_test",
            run_id="run_retry",
        ))

        # Allow time for retry + success.
        for _ in range(40):
            await asyncio.sleep(0.05)
            if bus.published_for_topic("agent.echo.out.summary"):
                break

        published = bus.published_for_topic("agent.echo.out.summary")
        assert len(published) == 1
        assert published[0].payload == {"attempts": 2}

        await _stop_instance(instance, task)


# ----------------------------------------------------------------
# Dedup gating
# ----------------------------------------------------------------


class TestDedupGate:
    async def test_duplicate_event_id_dropped(
        self, bus, llm_gateway, echo_plan, fast_settings,
    ) -> None:
        run_count = 0

        async def handler(ctx, event):
            nonlocal run_count
            run_count += 1
            return []

        instance = AgentInstance(
            plan=echo_plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(handler),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
        )
        task = asyncio.create_task(instance.run())
        await asyncio.sleep(0.1)

        env = build_envelope(
            topic="agent.echo.in.q1",
            payload={},
            workflow_id="wf_test",
            run_id="run_dedup",
        )
        # Publish the same envelope twice — duplicates by event_id.
        await bus.publish(env)
        await bus.publish(env)
        await asyncio.sleep(0.3)

        assert run_count == 1
        await _stop_instance(instance, task)


# ----------------------------------------------------------------
# Worker boots from RuntimePlan
# ----------------------------------------------------------------


class TestAgentWorker:
    async def test_worker_starts_handler_for_each_template(
        self, bus, llm_gateway, fast_settings,
    ) -> None:
        # Build a 2-template plan
        plan_a = AgentPlan(
            template_key="a",
            role="thinking",
            description="a",
            subscribe_topics=["agent.a.in.t"],
            subscribe_tag_filters=[{}],
            consumer_group="grp.wf_w.a",
            publish_topics=["agent.a.out.t"],
        )
        plan_b = AgentPlan(
            template_key="b",
            role="thinking",
            description="b",
            subscribe_topics=["agent.b.in.t"],
            subscribe_tag_filters=[{}],
            consumer_group="grp.wf_w.b",
            publish_topics=["agent.b.out.t"],
        )
        runtime_plan = RuntimePlan(
            workflow_id="wf_w",
            workflow_version=1,
            ir_hash="abc",
            agents={"a": plan_a, "b": plan_b},
            bus_topics=BusTopicPlan(),
        )

        # Register handlers in a fresh registry.
        reg = HandlerRegistry()

        @agent_handler(workflow_id="wf_w", template_key="a", registry=reg)
        async def a_handler(ctx, event):
            return [Event(topic="agent.a.out.t", payload={"src": "a"})]

        @agent_handler(workflow_id="wf_w", template_key="b", registry=reg)
        async def b_handler(ctx, event):
            return [Event(topic="agent.b.out.t", payload={"src": "b"})]

        worker = AgentWorker(
            plan=runtime_plan,
            bus=bus,
            llm=llm_gateway,
            handlers=reg,
            settings=fast_settings,
        )
        await worker.start()
        await asyncio.sleep(0.1)

        await bus.publish(build_envelope(
            topic="agent.a.in.t", payload={}, run_id="r",
        ))
        await bus.publish(build_envelope(
            topic="agent.b.in.t", payload={}, run_id="r",
        ))

        for _ in range(40):
            await asyncio.sleep(0.05)
            if (bus.published_for_topic("agent.a.out.t")
                    and bus.published_for_topic("agent.b.out.t")):
                break

        assert len(bus.published_for_topic("agent.a.out.t")) == 1
        assert len(bus.published_for_topic("agent.b.out.t")) == 1
        assert len(worker.instances) == 2

        await worker.stop()


# ----------------------------------------------------------------
# Tag filter at the gate
# ----------------------------------------------------------------


class TestTagGate:
    async def test_tag_mismatched_event_dropped(
        self, bus, llm_gateway, fast_settings,
    ) -> None:
        plan = AgentPlan(
            template_key="zh_only",
            role="thinking",
            description="agent only handles zh",
            subscribe_topics=["agent.zh.in.t"],
            subscribe_tag_filters=[{}],
            consumer_group="grp.wf.zh_only",
            publish_topics=["agent.zh.out.t"],
            tags={"language": "zh"},
        )

        run_count = 0

        async def handler(ctx, event):
            nonlocal run_count
            run_count += 1
            return []

        instance = AgentInstance(
            plan=plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(handler),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
        )
        task = asyncio.create_task(instance.run())
        await asyncio.sleep(0.1)

        # English-tagged event → drop_tag.
        from agentkit.models.envelope import ToFilter
        await bus.publish(build_envelope(
            topic="agent.zh.in.t",
            payload={},
            run_id="r",
            to_filter=ToFilter(tags={"language": "en"}),
        ))
        # Matching zh-tagged event → handler runs.
        await bus.publish(build_envelope(
            topic="agent.zh.in.t",
            payload={},
            run_id="r",
            to_filter=ToFilter(tags={"language": "zh"}),
        ))
        await asyncio.sleep(0.3)

        assert run_count == 1
        await _stop_instance(instance, task)
