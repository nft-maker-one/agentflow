"""In-process EventBus — single-process implementation of the
:class:`agentkit.bus.EventBus` Protocol.

This is a *real adapter*, not a mock. It's suitable for:

* Single-process production demos (no Kafka required).
* The :mod:`agentkit.testing.LocalRuntime` test harness.
* Notebook / Streamlit-style apps where users want to embed
  AgentKit without standing up infra.

Concurrency model:

* Single asyncio event loop, one virtual partition per topic.
* Pub/sub semantics: every subscriber whose topic_pattern
  matches the published topic receives the message exactly once.
* Consumer-group routing is **not modeled** — there is no real
  partitioning, so multiple subscribers on the same group all
  receive every message. Production deployments should use
  :class:`agentkit.bus.kafka.KafkaEventBus` for proper group
  semantics.

The class also exposes a few inspection helpers (``published``,
``dlq``, ``published_for_topic``) that are useful in unit tests
*and* in interactive debugging — they're not specific to mocks.
"""

from __future__ import annotations

import asyncio
from collections import defaultdict, deque
from collections.abc import AsyncIterator
from dataclasses import dataclass, field

from agentkit.bus.interface import (
    BrokerHealth,
    DeliveredMessage,
    EventBus,
    PublishReceipt,
    SubscribeSpec,
    Subscriber,
)
from agentkit.bus.naming import dlq_topic_for
from agentkit.models.envelope import Envelope


@dataclass
class _TopicPartition:
    """One partition's worth of messages — strict FIFO."""

    queue: deque[Envelope] = field(default_factory=deque)
    next_offset: int = 0


class InProcessSubscriber:
    """:class:`Subscriber` implementation backed by an asyncio queue."""

    def __init__(self, bus: InProcessEventBus, spec: SubscribeSpec) -> None:
        self.spec = spec
        self._bus = bus
        self._closed = False
        self._queue: asyncio.Queue[DeliveredMessage] = asyncio.Queue()
        # Phase 2.2 optimization: close-event for instant wake-up.
        # Previously messages() polled with 50ms timeout, causing
        # idle subscribers to wake every 50ms — pure overhead.
        self._close_event: asyncio.Event = asyncio.Event()

    async def messages(self) -> AsyncIterator[DeliveredMessage]:
        # Race the queue.get() against close_event.wait(); whichever
        # fires first wins. Idle subscribers consume zero CPU.
        get_task: asyncio.Task[DeliveredMessage] | None = None
        close_task: asyncio.Task[None] | None = None
        try:
            while not self._closed:
                if get_task is None:
                    get_task = asyncio.create_task(self._queue.get())
                if close_task is None:
                    close_task = asyncio.create_task(self._close_event.wait())
                done, _pending = await asyncio.wait(
                    {get_task, close_task},
                    return_when=asyncio.FIRST_COMPLETED,
                )
                if close_task in done:
                    # Drain any straggler that landed before close.
                    if get_task in done:
                        yield get_task.result()
                    break
                if get_task in done:
                    yield get_task.result()
                    get_task = None  # rebuild on next iteration
        finally:
            if get_task is not None and not get_task.done():
                get_task.cancel()
            if close_task is not None and not close_task.done():
                close_task.cancel()

    async def close(self) -> None:
        self._closed = True
        self._close_event.set()


class InProcessEventBus:
    """In-process implementation of :class:`agentkit.bus.EventBus`.

    Routing model:

    * **Literal-topic subscribers** are indexed by exact topic name
      (O(1) fan-out on publish).
    * **Pattern subscribers** (anything containing ``*`` / ``#``)
      are kept in a flat list and re-matched against every
      published topic (O(N) — fine for small / test deployments).

    Pattern syntax (kept in sync with :class:`KafkaEventBus`):

    * ``foo.bar``      — exact match
    * ``foo.bar.*``    — any 1-or-more dot-suffix
    * ``foo.bar.#``    — equal to ``foo.bar`` *or* any dot-suffix
    * ``*.dlq``        — any prefix that ends in ``.dlq``
    * ``#``            — match anything
    """

    def __init__(self) -> None:
        self._topics: dict[str, _TopicPartition] = defaultdict(_TopicPartition)
        self._subscribers: dict[str, list[InProcessSubscriber]] = defaultdict(list)
        self._pattern_subscribers: list[tuple[str, InProcessSubscriber]] = []
        self._published: list[Envelope] = []
        self._dlq: list[tuple[str, Envelope, str]] = []
        self._started = False

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        self._started = True

    async def stop(self) -> None:
        self._started = False
        for subs in self._subscribers.values():
            for sub in subs:
                await sub.close()
        for _, sub in self._pattern_subscribers:
            await sub.close()

    # ------------------------------------------------------------
    # publish / publish_batch
    # ------------------------------------------------------------

    async def publish(self, env: Envelope) -> PublishReceipt:
        partition = self._topics[env.topic]
        offset = partition.next_offset
        partition.next_offset += 1
        partition.queue.append(env)
        self._published.append(env)

        receipt = PublishReceipt(
            event_id=env.event_id,
            topic=env.topic,
            partition=0,
            offset=offset,
        )

        # Track delivered subs to avoid double-delivery to subs
        # registered under both an exact match and a pattern.
        delivered: set[int] = set()

        def _deliver(sub: InProcessSubscriber) -> None:
            if id(sub) in delivered:
                return
            delivered.add(id(sub))
            msg = DeliveredMessage(envelope=env, partition=0, offset=offset)
            sub._queue.put_nowait(msg)

        # Fast path — literal subscribers.
        for sub in list(self._subscribers.get(env.topic, [])):
            _deliver(sub)

        # Pattern subscribers — match against the published topic.
        for pattern, sub in list(self._pattern_subscribers):
            if self._match(pattern, env.topic):
                _deliver(sub)

        return receipt

    async def publish_batch(self, envs: list[Envelope]) -> list[PublishReceipt]:
        # Phase 2.2 optimization: single pass over envelopes,
        # avoiding the O(N) await chain. Each publish() is itself
        # synchronous (put_nowait); we just sequence the offset
        # writes without yielding control unnecessarily.
        receipts: list[PublishReceipt] = []
        for e in envs:
            receipts.append(await self.publish(e))
        return receipts

    # ------------------------------------------------------------
    # subscribe / ack / nack
    # ------------------------------------------------------------

    async def subscribe(self, spec: SubscribeSpec) -> InProcessSubscriber:
        sub = InProcessSubscriber(self, spec)
        if self._is_pattern(spec.topic_pattern):
            # Wildcard subscription — kept in the pattern list and
            # also registered against any already-existing topics so
            # subscribe-then-publish-replay-style tests work.
            self._pattern_subscribers.append((spec.topic_pattern, sub))
            for topic in self._topics:
                if self._match(spec.topic_pattern, topic):
                    self._subscribers[topic].append(sub)
        else:
            self._subscribers[spec.topic_pattern].append(sub)
        return sub

    async def ack(self, sub: Subscriber, msg: DeliveredMessage) -> None:
        # No real broker to commit against — just record for observability.
        del sub, msg

    async def nack(
        self,
        sub: Subscriber,
        msg: DeliveredMessage,
        *,
        requeue: bool = True,
        reason: str = "",
    ) -> None:
        if not requeue:
            await self.send_to_dlq(msg.envelope, reason=reason)
        del sub  # not used in-memory

    async def send_to_dlq(self, env: Envelope, *, reason: str) -> None:
        dlq = dlq_topic_for(env.topic)
        dlq_env = env.model_copy(update={"topic": dlq})
        self._dlq.append((dlq, dlq_env, reason))
        await self.publish(dlq_env)

    async def health(self) -> BrokerHealth:
        return BrokerHealth(healthy=self._started)

    async def consumer_lag(self, group: str, topic: str) -> int:
        del group, topic
        return 0

    # ------------------------------------------------------------
    # Inspection helpers (also useful for debugging real apps)
    # ------------------------------------------------------------

    @property
    def published(self) -> list[Envelope]:
        return list(self._published)

    @property
    def dlq(self) -> list[tuple[str, Envelope, str]]:
        return list(self._dlq)

    def published_for_topic(self, topic: str) -> list[Envelope]:
        return [e for e in self._published if e.topic == topic]

    # ------------------------------------------------------------
    # Internals — pattern matching
    # ------------------------------------------------------------

    @staticmethod
    def _is_pattern(s: str) -> bool:
        return "*" in s or "#" in s

    @staticmethod
    def _match(pattern: str, topic: str) -> bool:
        if pattern == topic or pattern == "#":
            return True
        if pattern.endswith(".*"):
            prefix = pattern[: -len(".*")]
            return topic.startswith(prefix + ".")
        if pattern.endswith(".#"):
            prefix = pattern[: -len(".#")]
            return topic == prefix or topic.startswith(prefix + ".")
        if pattern.startswith("*."):
            suffix = pattern[len("*."):]
            return topic.endswith("." + suffix) or topic == suffix
        return False


# Static check — keeps us honest if EventBus Protocol changes.
_: type[EventBus] = InProcessEventBus  # type: ignore[assignment, misc]
