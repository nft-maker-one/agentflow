"""Tier-3 B4 + B3 — per-message-local retry under instance concurrency.

These guard the B3/B4 coupling documented in ``docs/CONCURRENCY.md``:

* **B4** — the retry decision must key off a *per-message-local*
  ``attempt`` counter, NOT the shared FSM snapshot. Before this fix,
  raising ``max_concurrent`` would let concurrent messages clobber each
  other's ``retry_count`` / ``Retry`` state, so a recoverable error on
  one message could skip its retries and land in the DLQ — a silent
  regression.
* **B3** — the runtime now defaults per-instance concurrency to
  ``default_max_concurrent`` (>1), which is only safe *because* of B4.
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.llm import LLMGatewayClient, NoOpGuardrail
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
def llm_gateway() -> LLMGatewayClient:
    return LLMGatewayClient(providers={"openai": MockProvider("openai")})


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
    return RuntimeSettings(
        runtime_id="test-node",
        handler_timeout_ms=2_000,
        retry_base_ms=10,
        retry_max_ms=50,
        retry_jitter=False,
        max_handler_retries=2,
        dedup_window_ms=10_000,
    )


async def _stop_instance(instance: AgentInstance, task: asyncio.Task) -> None:
    await instance.cancel()
    try:
        await asyncio.wait_for(task, timeout=2.0)
    except asyncio.TimeoutError:
        task.cancel()


# ----------------------------------------------------------------
# B4 — per-message retry isolation under concurrency
# ----------------------------------------------------------------


class TestPerMessageRetryUnderConcurrency:
    async def test_concurrent_messages_each_get_full_retry_budget(
        self, bus, llm_gateway, echo_plan, fast_settings,
    ) -> None:
        """With ``max_concurrent>1`` several messages fail-then-succeed
        concurrently. Each must retry independently and ALL must succeed
        — none may skip retries into the DLQ because of shared FSM state.
        """
        n = 6
        attempts: dict[str, int] = {}

        async def flaky(ctx, event):
            key = event.payload["id"]
            attempts[key] = attempts.get(key, 0) + 1
            if attempts[key] < 2:
                # First touch of THIS event: transient blip.
                raise ConnectionError(f"transient-{key}")
            return [Event(
                topic="agent.echo.out.summary",
                payload={"id": key, "tries": attempts[key]},
            )]

        instance = AgentInstance(
            plan=echo_plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(flaky),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
            max_concurrent=4,
        )
        task = asyncio.create_task(instance.run())
        await asyncio.sleep(0.1)

        for i in range(n):
            await bus.publish(build_envelope(
                topic="agent.echo.in.q1",
                payload={"id": f"e{i}"},
                workflow_id="wf_test",
                run_id=f"run_{i}",
            ))

        for _ in range(80):
            await asyncio.sleep(0.05)
            if len(bus.published_for_topic("agent.echo.out.summary")) >= n:
                break

        published = bus.published_for_topic("agent.echo.out.summary")
        await _stop_instance(instance, task)

        # All N succeeded after exactly one retry each, none went to DLQ.
        assert len(published) == n
        assert {p.payload["id"] for p in published} == {f"e{i}" for i in range(n)}
        assert all(p.payload["tries"] == 2 for p in published)
        assert all(attempts[f"e{i}"] == 2 for i in range(n))
        assert bus.dlq == []

    async def test_retry_budget_exhausts_then_dlq(
        self, bus, llm_gateway, echo_plan, fast_settings,
    ) -> None:
        """A message that always fails (RECOVERABLE) is retried exactly
        ``max_handler_retries`` times — i.e. invoked ``max+1`` times — and
        then nack'd to the DLQ. The count comes from the local attempt
        counter, independent of any concurrent traffic.
        """
        calls: list[int] = []

        async def always_fail(ctx, event):
            calls.append(1)
            raise ConnectionError("permanently transient")

        instance = AgentInstance(
            plan=echo_plan,
            workflow_id="wf_test",
            bus=bus,
            llm=llm_gateway,
            executor=_FunctionExecutor(always_fail),
            settings=fast_settings,
            guardrail=NoOpGuardrail(),
            max_concurrent=2,
        )
        task = asyncio.create_task(instance.run())
        await asyncio.sleep(0.1)

        await bus.publish(build_envelope(
            topic="agent.echo.in.q1",
            payload={"id": "boom"},
            workflow_id="wf_test",
            run_id="run_boom",
        ))

        for _ in range(80):
            await asyncio.sleep(0.05)
            if bus.dlq:
                break

        await _stop_instance(instance, task)

        # initial attempt + max_handler_retries retries.
        assert len(calls) == fast_settings.max_handler_retries + 1
        assert any(e.topic.endswith(".dlq") for _, e, _ in bus.dlq)


# ----------------------------------------------------------------
# B3 — concurrency default wiring
# ----------------------------------------------------------------


class TestConcurrencyDefaultWiring:
    def test_settings_default_is_concurrent(self) -> None:
        assert RuntimeSettings().default_max_concurrent == 4

    def test_worker_uses_settings_default_when_unspecified(
        self, bus, llm_gateway,
    ) -> None:
        settings = RuntimeSettings(default_max_concurrent=7)
        plan_a = AgentPlan(
            template_key="a",
            role="thinking",
            description="a",
            subscribe_topics=["agent.a.in.t"],
            subscribe_tag_filters=[{}],
            consumer_group="grp.wf_w.a",
            publish_topics=["agent.a.out.t"],
        )
        runtime_plan = RuntimePlan(
            workflow_id="wf_w",
            workflow_version=1,
            ir_hash="abc",
            agents={"a": plan_a},
            bus_topics=BusTopicPlan(),
        )
        worker = AgentWorker(
            plan=runtime_plan,
            bus=bus,
            llm=llm_gateway,
            settings=settings,
        )
        assert worker._max_concurrent_per_instance == 7

    def test_worker_explicit_arg_overrides_settings(
        self, bus, llm_gateway,
    ) -> None:
        settings = RuntimeSettings(default_max_concurrent=7)
        plan_a = AgentPlan(
            template_key="a",
            role="thinking",
            description="a",
            subscribe_topics=["agent.a.in.t"],
            subscribe_tag_filters=[{}],
            consumer_group="grp.wf_w.a",
            publish_topics=["agent.a.out.t"],
        )
        runtime_plan = RuntimePlan(
            workflow_id="wf_w",
            workflow_version=1,
            ir_hash="abc",
            agents={"a": plan_a},
            bus_topics=BusTopicPlan(),
        )
        worker = AgentWorker(
            plan=runtime_plan,
            bus=bus,
            llm=llm_gateway,
            settings=settings,
            max_concurrent_per_instance=1,
        )
        assert worker._max_concurrent_per_instance == 1
