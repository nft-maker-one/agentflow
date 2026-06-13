"""Redis Streams implementation of the :class:`EventBus` Protocol.

Model
-----
* One **stream** per topic: ``{prefix}:s:{topic}``.
* A registry **set** ``{prefix}:topics`` records every topic seen, so a
  wildcard subscriber can discover streams created by other processes.
* **Literal** subscription → one consumer group on the topic's stream
  (``XGROUP CREATE`` is O(1) — no rebalance), read with ``XREADGROUP``.
* **Wildcard** subscription (``*`` / ``#``) → a dynamic set of streams.
  Same-process publishes are added to the subscriber instantly (in-proc
  registry notification); a periodic ``SMEMBERS`` scan picks up streams
  created by other processes.

This sidesteps Kafka's per-consumer JoinGroup/SyncGroup/rebalance cost,
which dominated AgentKit deploy latency, and its ``latest``-skips-offset-0
gotcha (here the group is created at the requested start id).
"""

from __future__ import annotations

import asyncio
from collections.abc import AsyncIterator

from redis.asyncio import Redis
from redis.exceptions import RedisError, ResponseError

from agentkit.bus.interface import (
    BrokerHealth,
    DeliveredMessage,
    PublishReceipt,
    SubscribeSpec,
)
from agentkit.bus.naming import dlq_topic_for
from agentkit.bus.redis_stream.config import RedisStreamSettings
from agentkit.bus.redis_stream.serde import decode, encode
from agentkit.common.logging import get_logger
from agentkit.models.envelope import Envelope

log = get_logger(__name__)

_START_ID = {"earliest": b"0", "latest": b"$", "committed": b"$"}


def _is_pattern(s: str) -> bool:
    return "*" in s or "#" in s


def _match(pattern: str, topic: str) -> bool:
    """AMQP-style topic match — identical semantics to InProcess/Kafka."""
    if pattern == topic or pattern == "#":
        return True
    if pattern.endswith(".*"):
        return topic.startswith(pattern[:-2] + ".")
    if pattern.endswith(".#"):
        prefix = pattern[:-2]
        return topic == prefix or topic.startswith(prefix + ".")
    if pattern.startswith("*."):
        suffix = pattern[2:]
        return topic.endswith("." + suffix) or topic == suffix
    return False


def _b2s(b: bytes | str) -> str:
    return b.decode() if isinstance(b, bytes) else b


def _id_to_offset(msg_id: bytes | str) -> int:
    """Map a Redis stream id ``ms-seq`` to a monotonic int (the ms part)."""
    try:
        return int(_b2s(msg_id).split("-", 1)[0])
    except (ValueError, IndexError):
        return 0


class RedisStreamSubscriber:
    """:class:`Subscriber` backed by ``XREADGROUP`` over one or more
    streams (one for literal subs, a dynamic set for wildcard subs)."""

    def __init__(self, bus: "RedisStreamBus", spec: SubscribeSpec) -> None:
        self.spec = spec
        self._bus = bus
        self._closed = False
        self._is_pattern = _is_pattern(spec.topic_pattern)
        self._start_id = _START_ID[spec.starting_position]
        self._consumer = f"{spec.group}.c0"
        self._topics: set[str] = set()        # topics this sub reads
        self._grouped: set[str] = set()       # topics whose group exists
        self._wake = asyncio.Event()
        self._scan_task: asyncio.Task[None] | None = None

    # -- wildcard discovery ------------------------------------------
    def _note_topic(self, topic: str) -> None:
        """Called by the bus when a same-process publish creates a topic
        matching this subscriber's pattern."""
        if topic not in self._topics:
            self._topics.add(topic)
            self._wake.set()

    async def _scan_loop(self) -> None:
        """Pick up streams created by OTHER processes."""
        interval = self._bus._settings.scan_interval_ms / 1000.0
        try:
            while not self._closed:
                await asyncio.sleep(interval)
                try:
                    members = await self._bus._redis.smembers(self._bus._topics_key)
                except RedisError:
                    continue
                for m in members:
                    t = _b2s(m)
                    if t not in self._topics and _match(self.spec.topic_pattern, t):
                        self._topics.add(t)
                        self._wake.set()
        except asyncio.CancelledError:
            pass

    def _start_scan(self) -> None:
        self._scan_task = asyncio.create_task(self._scan_loop())

    async def _ensure_group_for(self, topic: str) -> None:
        """Create this subscriber's consumer group on ``topic``'s stream.

        Called by the bus from ``publish`` BEFORE the ``XADD`` for a
        brand-new topic, so a ``latest`` ($) wildcard sub (e.g. the event
        tap) is positioned at the stream end *before* the message lands —
        otherwise the group would be created after the message and ``>``
        would skip it (the offset-0 miss)."""
        if topic in self._grouped:
            return
        try:
            await self._bus._redis.xgroup_create(
                self._bus._skey(topic), self.spec.group,
                id=self._start_id, mkstream=True,
            )
        except ResponseError as e:
            if "BUSYGROUP" not in str(e):
                raise
        self._grouped.add(topic)

    async def _ensure_groups(self) -> None:
        for t in list(self._topics):
            if t in self._grouped:
                continue
            await self._ensure_group_for(t)

    # -- consume -----------------------------------------------------
    async def messages(self) -> AsyncIterator[DeliveredMessage]:
        r = self._bus._redis
        block = self._bus._settings.block_ms
        batch = self._bus._settings.batch
        idle = self._bus._settings.scan_interval_ms / 1000.0
        while not self._closed:
            try:
                await self._ensure_groups()
            except RedisError:
                log.exception("bus.redis.ensure_group_failed")
                await asyncio.sleep(0.2)
                continue
            streams = {self._bus._skey(t): b">" for t in self._topics}
            if not streams:
                try:
                    await asyncio.wait_for(self._wake.wait(), timeout=idle)
                except asyncio.TimeoutError:
                    pass
                self._wake.clear()
                continue
            try:
                resp = await r.xreadgroup(
                    self.spec.group, self._consumer, streams,
                    count=batch, block=block,
                )
            except ResponseError as e:
                if "NOGROUP" in str(e):
                    self._grouped.clear()   # recreate next loop
                    continue
                raise
            except RedisError:
                if self._closed:
                    break
                log.exception("bus.redis.xreadgroup_failed")
                await asyncio.sleep(0.2)
                continue
            if not resp:
                continue
            for skey, entries in resp:
                topic = self._bus._topic_from_skey(skey)
                for msg_id, fields in entries:
                    data = fields.get(b"d")
                    if data is None:
                        continue
                    try:
                        env = decode(data)
                    except Exception:  # noqa: BLE001
                        log.exception("bus.redis.decode_failed", topic=topic)
                        continue
                    yield DeliveredMessage(
                        envelope=env, partition=0,
                        offset=_id_to_offset(msg_id),
                        raw_key=f"{_b2s(skey)}|{_b2s(msg_id)}",
                    )

    async def close(self) -> None:
        self._closed = True
        self._wake.set()
        if self._scan_task is not None:
            self._scan_task.cancel()


class RedisStreamBus:
    """Durable EventBus on Redis Streams."""

    def __init__(
        self,
        settings: RedisStreamSettings | None = None,
        *,
        client: Redis | None = None,
    ) -> None:
        self._settings = settings or RedisStreamSettings()
        # ``client`` injection point (tests pass a fakeredis instance).
        self._redis: Redis = client or Redis.from_url(self._settings.redis_url)
        self._prefix = self._settings.key_prefix
        self._topics_key = f"{self._prefix}:topics"
        self._stream_prefix = f"{self._prefix}:s:"
        self._known_topics: set[str] = set()
        self._pattern_subs: list[RedisStreamSubscriber] = []
        self._subscribers: list[RedisStreamSubscriber] = []
        self._dlq: list[tuple[str, Envelope, str]] = []
        self._started = False

    # -- key helpers --
    def _skey(self, topic: str) -> str:
        return f"{self._stream_prefix}{topic}"

    def _topic_from_skey(self, skey: bytes | str) -> str:
        return _b2s(skey)[len(self._stream_prefix):]

    # -- lifecycle --
    async def start(self) -> None:
        await self._redis.ping()    # fail fast if Redis is unreachable
        self._started = True

    async def stop(self) -> None:
        self._started = False
        for sub in list(self._subscribers):
            try:
                await sub.close()
            except Exception:  # noqa: BLE001
                log.exception("bus.redis.sub_close_failed")
        self._subscribers.clear()
        self._pattern_subs.clear()
        try:
            await self._redis.aclose()
        except Exception:  # noqa: BLE001
            pass

    # -- publish --
    async def _register_topic(self, topic: str) -> None:
        if topic in self._known_topics:
            return
        self._known_topics.add(topic)
        try:
            await self._redis.sadd(self._topics_key, topic)
        except RedisError:
            log.exception("bus.redis.register_topic_failed", topic=topic)
        # Same-process wildcard subscribers pick it up instantly. Create
        # their group on the (brand-new) stream NOW — before the caller's
        # XADD — so a `latest` ($) sub like the event tap is positioned at
        # the stream end before the message lands and won't skip it.
        for psub in self._pattern_subs:
            if _match(psub.spec.topic_pattern, topic):
                try:
                    await psub._ensure_group_for(topic)
                except RedisError:
                    log.exception("bus.redis.pattern_group_create_failed", topic=topic)
                psub._note_topic(topic)

    async def publish(self, env: Envelope) -> PublishReceipt:
        await self._register_topic(env.topic)
        msg_id = await self._redis.xadd(
            self._skey(env.topic), {b"d": encode(env)},
            maxlen=self._settings.maxlen, approximate=True,
        )
        return PublishReceipt(
            event_id=env.event_id, topic=env.topic,
            partition=0, offset=_id_to_offset(msg_id),
        )

    async def publish_batch(self, envs: list[Envelope]) -> list[PublishReceipt]:
        receipts: list[PublishReceipt] = []
        for e in envs:
            receipts.append(await self.publish(e))
        return receipts

    # -- subscribe --
    async def subscribe(self, spec: SubscribeSpec) -> RedisStreamSubscriber:
        sub = RedisStreamSubscriber(self, spec)
        if sub._is_pattern:
            self._pattern_subs.append(sub)
            seed = set(self._known_topics)
            try:
                seed |= {_b2s(m) for m in await self._redis.smembers(self._topics_key)}
            except RedisError:
                pass
            for t in seed:
                if _match(spec.topic_pattern, t):
                    sub._topics.add(t)
            sub._start_scan()
        else:
            sub._topics.add(spec.topic_pattern)
        self._subscribers.append(sub)
        return sub

    # -- ack / nack / dlq --
    async def ack(self, sub: RedisStreamSubscriber, msg: DeliveredMessage) -> None:
        if not msg.raw_key:
            return
        skey, _, mid = msg.raw_key.partition("|")
        try:
            await self._redis.xack(skey, sub.spec.group, mid)
        except RedisError:
            log.exception("bus.redis.ack_failed", raw_key=msg.raw_key)

    async def nack(
        self, sub: RedisStreamSubscriber, msg: DeliveredMessage,
        *, requeue: bool = True, reason: str = "",
    ) -> None:
        # Match InProcess semantics: requeue is best-effort (no broker-side
        # redelivery — the worker owns retry). Always remove from the PEL so
        # it doesn't accumulate; DLQ when not requeuing.
        if not requeue:
            await self.send_to_dlq(msg.envelope, reason=reason)
        await self.ack(sub, msg)

    async def send_to_dlq(self, env: Envelope, *, reason: str) -> None:
        dlq = dlq_topic_for(env.topic)
        dlq_env = env.model_copy(update={"topic": dlq})
        self._dlq.append((dlq, dlq_env, reason))
        await self.publish(dlq_env)

    # -- introspection --
    async def health(self) -> BrokerHealth:
        try:
            await self._redis.ping()
            return BrokerHealth(healthy=True)
        except RedisError as e:
            return BrokerHealth(healthy=False, detail=str(e))

    async def consumer_lag(self, group: str, topic: str) -> int:
        try:
            groups = await self._redis.xinfo_groups(self._skey(topic))
        except RedisError:
            return 0
        for g in groups:
            if _b2s(g.get(b"name", b"")) == group:
                return int(g.get(b"lag", 0) or 0)
        return 0

    # inspection helpers (parity with InProcessEventBus, handy in tests)
    @property
    def dlq(self) -> list[tuple[str, Envelope, str]]:
        return list(self._dlq)
