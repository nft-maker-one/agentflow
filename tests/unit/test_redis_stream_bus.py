"""RedisStreamBus contract tests (backed by fakeredis).

Covers literal + wildcard delivery, the Bug-B regression (a terminal
marker on a brand-new per-run stream must NOT be missed), ack, and DLQ.
"""

from __future__ import annotations

import asyncio

import fakeredis

from agentkit.bus.builder import build_envelope
from agentkit.bus.interface import SubscribeSpec
from agentkit.bus.redis_stream import RedisStreamBus, RedisStreamSettings


def _bus() -> RedisStreamBus:
    return RedisStreamBus(
        RedisStreamSettings(block_ms=80, scan_interval_ms=80, key_prefix="ak"),
        client=fakeredis.FakeAsyncRedis(),
    )


async def _collect(sub, n: int, *, timeout: float = 3.0) -> list:
    out: list = []

    async def run() -> None:
        async for msg in sub.messages():
            out.append(msg)
            if len(out) >= n:
                return

    try:
        await asyncio.wait_for(run(), timeout)
    except asyncio.TimeoutError:
        pass
    return out


def _env(topic: str, **payload):
    return build_envelope(topic=topic, payload=payload, workflow_id="w", run_id="r1")


class TestRedisStreamBus:
    async def test_literal_publish_subscribe(self) -> None:
        bus = _bus()
        await bus.start()
        sub = await bus.subscribe(SubscribeSpec(
            topic_pattern="agent.x.in", group="g1", starting_position="earliest"))
        task = asyncio.create_task(_collect(sub, 1))
        await asyncio.sleep(0.2)
        await bus.publish(_env("agent.x.in", q="hi"))
        got = await task
        assert [m.envelope.payload for m in got] == [{"q": "hi"}]
        await sub.close()
        await bus.stop()

    async def test_wildcard_picks_up_brand_new_stream(self) -> None:
        # Bug-B regression: a wildcard sub created BEFORE the per-run topic
        # exists must still receive the first (offset-0) message on it.
        bus = _bus()
        await bus.start()
        sub = await bus.subscribe(SubscribeSpec(
            topic_pattern="system.run.#", group="gw", starting_position="earliest"))
        task = asyncio.create_task(_collect(sub, 1))
        await asyncio.sleep(0.2)
        await bus.publish(_env("system.run.r1.error", reason="boom"))
        got = await task
        assert len(got) == 1
        assert got[0].envelope.topic == "system.run.r1.error"
        assert got[0].envelope.payload == {"reason": "boom"}
        await sub.close()
        await bus.stop()

    async def test_latest_wildcard_catches_brand_new_topic(self) -> None:
        # Regression: the event-timeline tap is `#` + starting_position
        # "latest". A message published to a topic that did NOT exist when
        # the tap subscribed must still be delivered — the group has to be
        # created at the stream end BEFORE the XADD, not after (else the
        # `$` position skips that first message and the timeline goes empty).
        bus = _bus()
        await bus.start()
        sub = await bus.subscribe(SubscribeSpec(
            topic_pattern="#", group="tap", starting_position="latest"))
        task = asyncio.create_task(_collect(sub, 1))
        await asyncio.sleep(0.2)
        await bus.publish(_env("agent.writer.out", result="story"))  # brand-new topic
        got = await task
        assert len(got) == 1
        assert got[0].envelope.topic == "agent.writer.out"
        assert got[0].envelope.payload == {"result": "story"}
        await sub.close()
        await bus.stop()

    async def test_hash_wildcard_receives_all_topics(self) -> None:
        bus = _bus()
        await bus.start()
        sub = await bus.subscribe(SubscribeSpec(
            topic_pattern="#", group="tap", starting_position="earliest"))
        task = asyncio.create_task(_collect(sub, 2))
        await asyncio.sleep(0.2)
        await bus.publish(_env("a.one"))
        await bus.publish(_env("b.two"))
        got = await task
        topics = {m.envelope.topic for m in got}
        assert topics == {"a.one", "b.two"}
        await sub.close()
        await bus.stop()

    async def test_ack_removes_from_pending(self) -> None:
        bus = _bus()
        await bus.start()
        sub = await bus.subscribe(SubscribeSpec(
            topic_pattern="agent.y.in", group="g2", starting_position="earliest"))
        task = asyncio.create_task(_collect(sub, 1))
        await asyncio.sleep(0.2)
        await bus.publish(_env("agent.y.in", n=1))
        got = await task
        await bus.ack(sub, got[0])
        # nothing pending for the group after ack
        pend = await bus._redis.xpending(bus._skey("agent.y.in"), "g2")  # noqa: SLF001
        assert pend["pending"] == 0
        await sub.close()
        await bus.stop()

    async def test_send_to_dlq(self) -> None:
        bus = _bus()
        await bus.start()
        await bus.send_to_dlq(_env("agent.z.in", bad=1), reason="exhausted")
        assert [t for t, _, _ in bus.dlq] == ["agent.z.in.dlq"]
        # the DLQ stream got the envelope
        ln = await bus._redis.xlen(bus._skey("agent.z.in.dlq"))  # noqa: SLF001
        assert ln == 1
        await bus.stop()

    async def test_health_ok(self) -> None:
        bus = _bus()
        await bus.start()
        h = await bus.health()
        assert h.healthy is True
        await bus.stop()

    async def test_latest_skips_preexisting(self) -> None:
        # `latest` semantics: a fresh group started at `$` does not replay
        # messages that predate the subscription.
        bus = _bus()
        await bus.start()
        await bus.publish(_env("agent.q.in", old=1))   # before any subscriber
        sub = await bus.subscribe(SubscribeSpec(
            topic_pattern="agent.q.in", group="g3", starting_position="latest"))
        task = asyncio.create_task(_collect(sub, 1, timeout=1.0))
        await asyncio.sleep(0.2)
        await bus.publish(_env("agent.q.in", new=1))
        got = await task
        assert [m.envelope.payload for m in got] == [{"new": 1}]
        await sub.close()
        await bus.stop()
