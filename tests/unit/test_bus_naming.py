"""Unit tests for ``agentkit.bus.naming``."""

from __future__ import annotations

import pytest

from agentkit.bus.naming import (
    DLQ_SUFFIX,
    derive_broadcast_group,
    derive_consumer_group,
    dlq_topic_for,
    is_dlq_topic,
)


def test_dlq_topic_for_appends_suffix() -> None:
    assert dlq_topic_for("agent.research.in.q1") == "agent.research.in.q1" + DLQ_SUFFIX


def test_dlq_topic_for_is_idempotent() -> None:
    once = dlq_topic_for("foo")
    twice = dlq_topic_for(once)
    assert once == twice


def test_dlq_topic_for_rejects_empty() -> None:
    with pytest.raises(ValueError):
        dlq_topic_for("")


def test_is_dlq_topic() -> None:
    assert is_dlq_topic("foo.dlq") is True
    assert is_dlq_topic("foo") is False


def test_derive_consumer_group_format() -> None:
    assert derive_consumer_group("wf_001", "researcher") == "grp.wf_001.researcher"


def test_derive_consumer_group_rejects_empty() -> None:
    with pytest.raises(ValueError):
        derive_consumer_group("", "x")
    with pytest.raises(ValueError):
        derive_consumer_group("x", "")


def test_derive_broadcast_group_format() -> None:
    assert derive_broadcast_group("node_a") == "grp.broadcast.node_a"
