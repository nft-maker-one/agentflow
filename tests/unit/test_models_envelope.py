"""Unit tests for the :class:`agentkit.models.Envelope`."""

from __future__ import annotations

import json

import pytest
from pydantic import ValidationError

from agentkit.models.envelope import (
    AgentRef,
    Envelope,
    EnvelopeHeaders,
    ToFilter,
)
from agentkit.models.enums import Role


def test_envelope_defaults_populate_event_and_trace_ids() -> None:
    env = Envelope(topic="agent.test.in.q1")
    assert env.event_id.startswith("evt_")
    assert env.trace_id.startswith("trc_")
    assert env.ts.tzinfo is not None


def test_envelope_rejects_empty_topic() -> None:
    with pytest.raises(ValidationError):
        Envelope(topic="")


def test_envelope_rejects_unknown_fields() -> None:
    with pytest.raises(ValidationError):
        Envelope(topic="t", unknown_field=1)  # type: ignore[call-arg]


def test_envelope_alias_round_trip_via_json() -> None:
    """``from_`` <-> ``from`` aliasing should round-trip cleanly."""
    env = Envelope(
        topic="agent.research.in.q1",
        from_=AgentRef(role=Role.THINKING, agent_id="agt_1"),
    )
    raw = env.model_dump(mode="json", by_alias=True)
    assert "from" in raw
    assert "from_" not in raw
    assert raw["from"]["role"] == "thinking"

    # And reconstruct from the dumped form
    dumped = json.loads(env.model_dump_json(by_alias=True))
    rebuilt = Envelope.model_validate(dumped)
    assert rebuilt.from_.role is Role.THINKING
    assert rebuilt.event_id == env.event_id


def test_to_filter_accepts_arbitrary_string_tags() -> None:
    tf = ToFilter(role=Role.THINKING, tags={"language": "zh", "region": "cn"})
    assert tf.tags["language"] == "zh"


def test_envelope_headers_priority_bounds() -> None:
    EnvelopeHeaders(priority=0)
    EnvelopeHeaders(priority=9)
    with pytest.raises(ValidationError):
        EnvelopeHeaders(priority=10)
    with pytest.raises(ValidationError):
        EnvelopeHeaders(priority=-1)


def test_envelope_payload_can_be_arbitrary_json() -> None:
    env = Envelope(
        topic="t",
        payload={"a": 1, "nested": {"b": [1, 2, 3]}, "s": "x"},
    )
    j = env.model_dump_json(by_alias=True)
    assert json.loads(j)["payload"]["nested"]["b"] == [1, 2, 3]
