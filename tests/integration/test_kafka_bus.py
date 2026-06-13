"""Integration tests for :class:`KafkaEventBus` against a real Redpanda.

Run with::

    make up                # start Redpanda
    make test-integration  # run these tests

These tests are auto-marked ``integration`` by ``conftest.py`` and
skipped if the broker is unreachable.
"""

from __future__ import annotations

import asyncio
import uuid

import pytest

from agentkit.bus import (
    DeliveredMessage,
    SubscribeSpec,
    build_envelope,
    derive_consumer_group,
    dlq_topic_for,
)
from agentkit.bus.kafka import KafkaEventBus

# A short helper that gives every test its own topic to avoid cross-talk.


def _unique_topic(prefix: str = "agentkit.test") -> str:
    return f"{prefix}.{uuid.uuid4().hex[:10]}"


# ----------------------------------------------------------------
# publish + subscribe round trip
# ----------------------------------------------------------------


async def test_publish_and_consume_single_envelope(kafka_bus: KafkaEventBus) -> None:
    topic = _unique_topic()
    group = derive_consumer_group("wf_test", f"role_{uuid.uuid4().hex[:6]}")

    sub = await kafka_bus.subscribe(
        SubscribeSpec(
            topic_pattern=topic,
            group=group,
            starting_position="earliest",
        ),
    )

    sent = build_envelope(
        topic=topic,
        payload={"q": "hello world"},
        workflow_id="wf_test",
        run_id="run_t1",
    )
    await kafka_bus.publish(sent)

    msg = await _next_message(sub, timeout_s=10)
    assert msg.envelope.event_id == sent.event_id
    assert msg.envelope.payload == {"q": "hello world"}
    assert msg.envelope.run_id == "run_t1"

    await kafka_bus.ack(sub, msg)
    await sub.close()


# ----------------------------------------------------------------
# fan-out: two distinct groups both receive every message
# ----------------------------------------------------------------


async def test_two_groups_each_receive_all_messages(
    kafka_bus: KafkaEventBus,
) -> None:
    topic = _unique_topic()
    group_a = f"grp.test_a.{uuid.uuid4().hex[:6]}"
    group_b = f"grp.test_b.{uuid.uuid4().hex[:6]}"

    sub_a = await kafka_bus.subscribe(
        SubscribeSpec(topic_pattern=topic, group=group_a, starting_position="earliest"),
    )
    sub_b = await kafka_bus.subscribe(
        SubscribeSpec(topic_pattern=topic, group=group_b, starting_position="earliest"),
    )

    expected = 5
    for i in range(expected):
        await kafka_bus.publish(
            build_envelope(topic=topic, payload={"n": i}, run_id="run_t2"),
        )

    msgs_a = await _collect_n_messages(sub_a, expected, timeout_s=15)
    msgs_b = await _collect_n_messages(sub_b, expected, timeout_s=15)

    assert sorted(m.envelope.payload["n"] for m in msgs_a) == list(range(expected))
    assert sorted(m.envelope.payload["n"] for m in msgs_b) == list(range(expected))

    await sub_a.close()
    await sub_b.close()


# ----------------------------------------------------------------
# DLQ: nack(requeue=False) routes to <topic>.dlq
# ----------------------------------------------------------------


async def test_nack_no_requeue_sends_to_dlq(kafka_bus: KafkaEventBus) -> None:
    topic = _unique_topic()
    dlq = dlq_topic_for(topic)
    group = f"grp.dlq.test.{uuid.uuid4().hex[:6]}"
    dlq_group = f"grp.dlq.watch.{uuid.uuid4().hex[:6]}"

    main_sub = await kafka_bus.subscribe(
        SubscribeSpec(topic_pattern=topic, group=group, starting_position="earliest"),
    )
    dlq_sub = await kafka_bus.subscribe(
        SubscribeSpec(topic_pattern=dlq, group=dlq_group, starting_position="earliest"),
    )

    await kafka_bus.publish(
        build_envelope(topic=topic, payload={"bad": True}, run_id="run_tdlq"),
    )

    msg = await _next_message(main_sub, timeout_s=10)
    await kafka_bus.nack(main_sub, msg, requeue=False, reason="schema_violation")

    dlq_msg = await _next_message(dlq_sub, timeout_s=10)
    assert dlq_msg.envelope.topic == dlq
    headers = dlq_msg.envelope.headers.user_headers
    assert headers["dlq_reason"] == "schema_violation"
    assert headers["dlq_original_topic"] == topic

    await main_sub.close()
    await dlq_sub.close()


# ----------------------------------------------------------------
# health probe
# ----------------------------------------------------------------


async def test_health_reports_healthy_when_started(kafka_bus: KafkaEventBus) -> None:
    health = await kafka_bus.health()
    assert health.healthy is True


# ----------------------------------------------------------------
# helpers
# ----------------------------------------------------------------


async def _next_message(sub, *, timeout_s: float) -> DeliveredMessage:
    """Read exactly one message from ``sub`` or fail the test."""
    aiter_ = sub.messages().__aiter__()
    try:
        return await asyncio.wait_for(aiter_.__anext__(), timeout=timeout_s)
    except asyncio.TimeoutError as e:
        pytest.fail(f"timed out waiting for message after {timeout_s}s: {e}")
        raise  # unreachable, satisfies the type-checker


async def _collect_n_messages(sub, n: int, *, timeout_s: float) -> list[DeliveredMessage]:
    msgs: list[DeliveredMessage] = []
    aiter_ = sub.messages().__aiter__()
    deadline = asyncio.get_event_loop().time() + timeout_s
    while len(msgs) < n:
        remaining = deadline - asyncio.get_event_loop().time()
        if remaining <= 0:
            pytest.fail(f"only collected {len(msgs)} of {n} messages within {timeout_s}s")
        try:
            msg = await asyncio.wait_for(aiter_.__anext__(), timeout=remaining)
        except asyncio.TimeoutError:
            pytest.fail(f"only collected {len(msgs)} of {n} messages within {timeout_s}s")
        msgs.append(msg)
    return msgs
