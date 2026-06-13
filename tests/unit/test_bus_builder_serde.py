"""Unit tests for envelope builder + Kafka serde."""

from __future__ import annotations

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.bus.kafka.serde import decode, encode, partition_key
from agentkit.common.errors import ValidationError
from agentkit.models.envelope import AgentRef
from agentkit.models.enums import Role


def test_build_envelope_populates_required_fields() -> None:
    env = build_envelope(
        topic="agent.research.in.q1",
        payload={"q": "hello"},
        workflow_id="wf_1",
        run_id="run_1",
    )
    assert env.event_id.startswith("evt_")
    assert env.trace_id.startswith("trc_")
    assert env.workflow_id == "wf_1"
    assert env.run_id == "run_1"
    assert env.payload == {"q": "hello"}
    assert env.ts is not None


def test_build_envelope_propagates_existing_trace_id() -> None:
    env = build_envelope(
        topic="t",
        payload={},
        trace_id="trc_existing",
    )
    assert env.trace_id == "trc_existing"


def test_build_envelope_with_from_and_filters() -> None:
    env = build_envelope(
        topic="t",
        payload={},
        from_=AgentRef(role=Role.THINKING, agent_id="agt_1"),
    )
    assert env.from_.role is Role.THINKING


def test_serde_round_trip() -> None:
    env = build_envelope(
        topic="agent.research.in.q1",
        payload={"q": "hello", "n": 42, "nested": {"k": [1, 2]}},
        workflow_id="wf_1",
        run_id="run_1",
        from_=AgentRef(role=Role.THINKING, agent_id="agt_1"),
    )
    raw = encode(env)
    decoded = decode(raw)
    assert decoded.event_id == env.event_id
    assert decoded.trace_id == env.trace_id
    assert decoded.payload == env.payload
    assert decoded.from_.role is Role.THINKING


def test_decode_raises_on_corrupt_bytes() -> None:
    with pytest.raises(ValidationError):
        decode(b"not json")


def test_decode_raises_on_schema_violation() -> None:
    with pytest.raises(ValidationError):
        decode(b'{"topic": ""}')  # empty topic violates min_length


def test_partition_key_uses_run_id_when_present() -> None:
    env = build_envelope(topic="t", payload={}, run_id="run_x")
    assert partition_key(env) == b"run_x"


def test_partition_key_falls_back_to_event_id() -> None:
    env = build_envelope(topic="t", payload={})
    # No run_id => key is event_id
    assert partition_key(env) == env.event_id.encode()
