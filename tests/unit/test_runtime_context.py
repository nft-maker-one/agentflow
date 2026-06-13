"""Unit tests for ``AgentContext`` + ``PublishPipeline``.

The contract these tests verify:

* topics outside the publish whitelist are rejected;
* user-supplied reserved header keys (trace_id etc.) are stripped,
  not honored;
* publish injects framework-controlled fields (event_id, trace_id,
  from_, ts) regardless of what the user did.
"""

from __future__ import annotations

import pytest

from agentkit.models.enums import Role
from agentkit.runtime.context import (
    AgentContext,
    Event,
    PublishPipeline,
)
from agentkit.runtime.errors import PermissionDenied
from tests.helpers.mock_bus import MockEventBus


@pytest.fixture
async def bus() -> MockEventBus:
    b = MockEventBus()
    await b.start()
    return b


def _pipeline(bus: MockEventBus, *, whitelist: set[str], schema_out=None) -> PublishPipeline:
    return PublishPipeline(
        bus=bus,
        agent_id="agt_test",
        agent_role=Role.THINKING,
        runtime_node="node-1",
        workflow_id="wf_x",
        publish_whitelist=whitelist,
        schema_out=schema_out,
    )


def _ctx(pipeline: PublishPipeline, *, llm=None) -> AgentContext:
    # Use a sentinel for llm — these tests don't exercise it.
    return AgentContext(
        agent_id="agt_test",
        template_key="echo",
        workflow_id="wf_x",
        run_id="run_1",
        trace_id="trc_1",
        llm=llm,  # type: ignore[arg-type]
        publish_pipeline=pipeline,
        causation_id="evt_in",
    )


# ----------------------------------------------------------------
# Whitelist enforcement
# ----------------------------------------------------------------


class TestPublishWhitelist:
    async def test_publish_allowed_topic_succeeds(self, bus) -> None:
        pipe = _pipeline(bus, whitelist={"agent.test.out.t1"})
        ctx = _ctx(pipe)
        receipt = await ctx.publish(Event(topic="agent.test.out.t1", payload={}))
        assert receipt.topic == "agent.test.out.t1"
        assert len(bus.published) == 1

    async def test_publish_to_non_whitelisted_topic_rejected(self, bus) -> None:
        pipe = _pipeline(bus, whitelist={"agent.test.out.t1"})
        ctx = _ctx(pipe)
        with pytest.raises(PermissionDenied):
            await ctx.publish(Event(topic="ghost.out", payload={}))


# ----------------------------------------------------------------
# Header sanitization
# ----------------------------------------------------------------


class TestHeaderSanitization:
    async def test_reserved_header_keys_are_stripped(self, bus) -> None:
        pipe = _pipeline(bus, whitelist={"agent.test.out.t1"})
        ctx = _ctx(pipe)
        await ctx.publish(
            Event(
                topic="agent.test.out.t1",
                payload={"x": 1},
                user_headers={
                    # Try to spoof framework-controlled fields:
                    "trace_id": "EVIL_TRACE",
                    "run_id": "EVIL_RUN",
                    "event_id": "EVIL_EVENT",
                    "runtime_token": "EVIL_TOKEN",
                    # Keep one legitimate user header:
                    "x-source": "researcher",
                },
            ),
        )
        env = bus.published[0]
        # Reserved keys must NOT make it into user_headers.
        assert "trace_id" not in env.headers.user_headers
        assert "run_id" not in env.headers.user_headers
        assert "event_id" not in env.headers.user_headers
        assert "runtime_token" not in env.headers.user_headers
        # Legitimate header survives.
        assert env.headers.user_headers["x-source"] == "researcher"

    async def test_framework_fields_use_context_values_not_user_input(
        self, bus,
    ) -> None:
        pipe = _pipeline(bus, whitelist={"agent.test.out.t1"})
        ctx = _ctx(pipe)
        await ctx.publish(
            Event(
                topic="agent.test.out.t1",
                payload={},
                user_headers={"trace_id": "EVIL"},
            ),
        )
        env = bus.published[0]
        assert env.trace_id == "trc_1"        # taken from context, not user
        assert env.run_id == "run_1"
        assert env.from_.agent_id == "agt_test"
        assert env.from_.role is Role.THINKING


# ----------------------------------------------------------------
# Schema enforcement on outbound payloads
# ----------------------------------------------------------------


class TestSchemaOut:
    async def test_publish_validates_schema_out(self, bus) -> None:
        schema = {
            "type": "object",
            "properties": {"score": {"type": "number"}},
            "required": ["score"],
        }
        pipe = _pipeline(
            bus, whitelist={"agent.test.out.t1"}, schema_out=schema,
        )
        ctx = _ctx(pipe)

        # Valid payload passes:
        await ctx.publish(
            Event(topic="agent.test.out.t1", payload={"score": 0.9}),
        )
        # Invalid payload rejected:
        with pytest.raises(PermissionDenied):
            await ctx.publish(
                Event(topic="agent.test.out.t1", payload={}),
            )


# ----------------------------------------------------------------
# Event model itself
# ----------------------------------------------------------------


class TestEventModel:
    def test_topic_required(self) -> None:
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            Event(topic="", payload={})

    def test_unknown_field_rejected(self) -> None:
        from pydantic import ValidationError as PydanticValidationError

        with pytest.raises(PydanticValidationError):
            Event(topic="t", payload={}, mystery_field=1)  # type: ignore[call-arg]
