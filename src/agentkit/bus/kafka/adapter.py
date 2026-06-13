"""Kafka / Redpanda :class:`agentkit.bus.EventBus` adapter.

Implementation notes:

* **Producer** is a single :class:`aiokafka.AIOKafkaProducer` shared
  across the process — that's the recommended aiokafka pattern.
* **Consumers** are created on demand by :meth:`subscribe`; each
  returns its own :class:`KafkaSubscriber` handle.
* **Auto-commit is OFF** — the framework controls commits via
  :meth:`ack` so at-least-once semantics combine cleanly with
  Runtime's Dedup gating (Doc03 §6.1).
* **DLQ** is sent via the same producer to ``<topic>.dlq``. If the
  DLQ topic doesn't exist Redpanda auto-creates it under default
  config (matches our docker-compose setup).

Wildcard / pattern subscription support is intentionally limited to
``agentkit.bus.naming`` conventions in this v0.1 cut: a literal topic
or a ``...*`` suffix. Full AMQP-style ``*`` / ``#`` translation is
on the Phase 2 roadmap (Doc02 §3.2).
"""

from __future__ import annotations

import asyncio
import contextlib
import time

from aiokafka import AIOKafkaConsumer, AIOKafkaProducer
from aiokafka.errors import KafkaConnectionError, KafkaError, MessageSizeTooLargeError
from aiokafka.structs import TopicPartition

from agentkit.bus.errors import BusUnavailable, OversizedPayload, PublishFailed
from agentkit.bus.interface import (
    BrokerHealth,
    DeliveredMessage,
    PublishReceipt,
    SubscribeSpec,
)
from agentkit.bus.kafka.config import KafkaSettings
from agentkit.bus.kafka.serde import encode, partition_key
from agentkit.bus.kafka.subscriber import KafkaSubscriber
from agentkit.bus.naming import dlq_topic_for
from agentkit.common.logging import get_logger
from agentkit.models.envelope import Envelope

log = get_logger(__name__)

# Defaults for offset-commit batching (no matching fields on
# :class:`KafkaSettings` — kept module-local per Doc02 §9.1 conventions).
# A commit is flushed once ``COMMIT_BATCH_SIZE`` offsets are buffered, or
# ``COMMIT_INTERVAL_MS`` have elapsed since the oldest buffered offset —
# whichever comes first. This turns N broker round-trips into ~1.
COMMIT_BATCH_SIZE = 100
COMMIT_INTERVAL_MS = 100


class CommitBatcher:
    """Accumulates per-partition offsets and commits them in batches.

    Kafka commits are per ``(topic, partition)`` and represent "consumed
    through offset - 1" — i.e. committing offset ``O`` means the next
    fetch should start at ``O``. To ack message offset ``m`` we therefore
    record ``m`` and, on flush, commit ``max(seen) + 1`` per partition.

    Correctness invariants:

    * We only ever commit ``max(offset seen so far) + 1`` for a
      partition — out-of-order acks (e.g. concurrent handlers) can never
      regress the committed offset below a value already buffered.
    * A flush is triggered by whichever comes first: ``batch_size``
      acked offsets accumulating since the last flush, or
      ``interval_ms`` elapsing since the first one in the current
      window.
    * :meth:`flush` is safe to call concurrently with the background
      loop — both serialize through ``self._lock``.
    """

    def __init__(
        self,
        consumer: AIOKafkaConsumer,
        *,
        batch_size: int = COMMIT_BATCH_SIZE,
        interval_ms: int = COMMIT_INTERVAL_MS,
    ) -> None:
        self._consumer = consumer
        self._batch_size = max(1, batch_size)
        self._interval_s = max(0, interval_ms) / 1000.0
        self._lock = asyncio.Lock()
        # Highest offset *seen* per partition (not yet committed).
        self._pending: dict[TopicPartition, int] = {}
        # Highest offset *committed* per partition — guards against
        # ever sending a commit lower than one already issued.
        self._committed: dict[TopicPartition, int] = {}
        # Count of ``add()`` calls accumulated since the last flush —
        # this is the "N offsets buffered" the size trigger compares
        # against (independent of how many distinct partitions that
        # spans).
        self._pending_count = 0
        self._first_pending_at: float | None = None
        self._flush_task: asyncio.Task[None] | None = None

    # ------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------

    def start(self) -> None:
        """Launch the background periodic-flush loop. Idempotent."""
        if self._flush_task is None:
            self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Cancel the background loop and flush any remaining offsets.

        Always flushes — even if the loop was never started — so a
        batcher created and torn down quickly never silently drops acks.
        """
        task, self._flush_task = self._flush_task, None
        if task is not None:
            task.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await task
        await self.flush()

    async def add(self, tp: TopicPartition, offset: int) -> None:
        """Record that ``offset`` on ``tp`` has been processed.

        Triggers an immediate flush if the buffer has reached
        ``batch_size``; otherwise the offset waits for the periodic
        loop (or the next size-triggered flush) to be committed.
        """
        should_flush = False
        async with self._lock:
            current = self._pending.get(tp)
            if current is None or offset > current:
                self._pending[tp] = offset
            if self._first_pending_at is None:
                self._first_pending_at = time.monotonic()
            self._pending_count += 1
            if self._pending_count >= self._batch_size:
                should_flush = True
        if should_flush:
            await self.flush()

    async def flush(self) -> None:
        """Commit all buffered offsets now (no-op if nothing pending)."""
        async with self._lock:
            to_commit = self._build_commit_map()
            if not to_commit:
                return
            try:
                await self._consumer.commit(to_commit)
            except KafkaError:
                log.exception("bus.commit.batch_failed", partitions=len(to_commit))
                raise
            for tp, target in to_commit.items():
                self._committed[tp] = target
                # Drop only entries we actually committed for — concurrent
                # ``add`` calls may have queued newer offsets meanwhile.
                if self._pending.get(tp, -1) + 1 == target:
                    del self._pending[tp]
            if self._pending:
                self._first_pending_at = time.monotonic()
            else:
                self._first_pending_at = None
                self._pending_count = 0
            log.debug("bus.commit.batch", partitions=len(to_commit))

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    def _build_commit_map(self) -> dict[TopicPartition, int]:
        """Compute the next commit target per partition.

        Returns ``{tp: max(seen, committed-1) + 1}`` — i.e. never lower
        than what's already been committed — for every partition with a
        higher offset than its last commit.
        """
        commit_map: dict[TopicPartition, int] = {}
        for tp, seen in self._pending.items():
            target = seen + 1
            last = self._committed.get(tp)
            if last is not None and target <= last:
                # Already committed at or beyond this point; nothing to do.
                continue
            commit_map[tp] = target
        return commit_map

    async def _flush_loop(self) -> None:
        """Periodically flush whenever the time threshold has elapsed."""
        try:
            while True:
                await asyncio.sleep(self._interval_s)
                async with self._lock:
                    started_at = self._first_pending_at
                    due = (
                        started_at is not None
                        and (time.monotonic() - started_at) >= self._interval_s
                    )
                if due:
                    await self.flush()
        except asyncio.CancelledError:
            raise


class KafkaEventBus:
    """Concrete :class:`agentkit.bus.EventBus` backed by Kafka / Redpanda."""

    def __init__(self, settings: KafkaSettings | None = None) -> None:
        self._settings = settings or KafkaSettings()
        self._producer: AIOKafkaProducer | None = None
        # Track open subscribers so :meth:`stop` can close them all.
        self._subscribers: list[KafkaSubscriber] = []
        # One :class:`CommitBatcher` per live subscriber, keyed by the
        # subscriber instance — accumulates acked offsets and commits
        # them in batches instead of one broker round-trip per message.
        self._commit_batchers: dict[KafkaSubscriber, CommitBatcher] = {}
        self._started = False
        self._lock = asyncio.Lock()

    # ------------------------------------------------------------
    # Lifecycle
    # ------------------------------------------------------------

    async def start(self) -> None:
        """Open the producer connection. Idempotent."""
        async with self._lock:
            if self._started:
                return
            self._producer = AIOKafkaProducer(
                bootstrap_servers=self._settings.bootstrap_servers,
                client_id=f"{self._settings.client_id}.producer",
                acks=self._settings.producer_acks,
                max_batch_size=self._settings.producer_max_batch_size,
                linger_ms=self._settings.producer_linger_ms,
                compression_type=self._settings.producer_compression,
                request_timeout_ms=self._settings.producer_request_timeout_ms,
                max_request_size=self._settings.producer_max_message_bytes,
                # ``enable_idempotence`` requires acks=all (we already enforce that).
                enable_idempotence=self._settings.producer_acks == "all",
            )
            try:
                await self._producer.start()
            except KafkaConnectionError as e:
                self._producer = None
                raise BusUnavailable(f"failed to connect to Kafka: {e}") from e
            self._started = True
            log.info(
                "bus.start",
                brokers=self._settings.brokers,
                client_id=self._settings.client_id,
            )

    async def stop(self) -> None:
        """Close all subscribers and the producer."""
        async with self._lock:
            if not self._started:
                return
            for sub in list(self._subscribers):
                await self._teardown_subscriber(sub)
            self._subscribers.clear()
            self._commit_batchers.clear()
            if self._producer is not None:
                try:
                    await self._producer.stop()
                except Exception:
                    log.exception("bus.producer.close_failed")
            self._producer = None
            self._started = False
            log.info("bus.stop")

    # ------------------------------------------------------------
    # Publish
    # ------------------------------------------------------------

    async def publish(self, env: Envelope) -> PublishReceipt:
        producer = self._require_producer()
        key = partition_key(env)
        value = encode(env)
        try:
            metadata = await producer.send_and_wait(env.topic, value=value, key=key)
        except MessageSizeTooLargeError as e:
            raise OversizedPayload(
                f"envelope ({len(value)} bytes) exceeds broker max",
            ) from e
        except KafkaError as e:
            raise PublishFailed(f"kafka publish failed: {e}") from e

        receipt = PublishReceipt(
            event_id=env.event_id,
            topic=metadata.topic,
            partition=metadata.partition,
            offset=metadata.offset,
        )
        log.debug(
            "bus.publish",
            topic=receipt.topic,
            partition=receipt.partition,
            offset=receipt.offset,
            event_id=receipt.event_id,
            trace_id=env.trace_id,
            run_id=env.run_id,
            size_bytes=len(value),
        )
        return receipt

    async def publish_batch(self, envs: list[Envelope]) -> list[PublishReceipt]:
        """Publish concurrently, preserving result order."""
        if not envs:
            return []
        # ``asyncio.gather`` keeps result ordering matching the input.
        return list(await asyncio.gather(*(self.publish(e) for e in envs)))

    # ------------------------------------------------------------
    # Subscribe
    # ------------------------------------------------------------

    async def subscribe(self, spec: SubscribeSpec) -> KafkaSubscriber:
        """Create a Kafka consumer matching ``spec``.

        v0.1 supports literal topics and a single trailing ``*`` (treated
        as Kafka's regex ``<prefix>.*``). Full AMQP-style wildcards
        are tracked in Doc02 §17.
        """
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            client_id=f"{self._settings.client_id}.consumer.{spec.group}",
            group_id=spec.group,
            enable_auto_commit=False,
            auto_offset_reset=self._reset_strategy(spec),
            max_poll_records=min(
                self._settings.consumer_max_poll_records, spec.batch_size,
            ),
            session_timeout_ms=self._settings.consumer_session_timeout_ms,
        )

        # Subscribe to either a literal topic or a regex pattern.
        if "*" in spec.topic_pattern or "#" in spec.topic_pattern:
            pattern = self._to_kafka_pattern(spec.topic_pattern)
            consumer.subscribe(pattern=pattern)
        else:
            consumer.subscribe([spec.topic_pattern])

        try:
            await consumer.start()
        except KafkaConnectionError as e:
            await consumer.stop()
            raise BusUnavailable(f"failed to connect Kafka consumer: {e}") from e

        subscriber = KafkaSubscriber(consumer, spec)
        self._subscribers.append(subscriber)
        batcher = CommitBatcher(
            consumer,
            batch_size=getattr(self._settings, "commit_batch_size", COMMIT_BATCH_SIZE),
            interval_ms=getattr(self._settings, "commit_interval_ms", COMMIT_INTERVAL_MS),
        )
        batcher.start()
        self._commit_batchers[subscriber] = batcher
        log.info(
            "bus.subscribe",
            topic_pattern=spec.topic_pattern,
            group=spec.group,
            starting_position=spec.starting_position,
        )
        return subscriber

    # ------------------------------------------------------------
    # Delivery confirmation
    # ------------------------------------------------------------

    async def ack(self, sub: KafkaSubscriber, msg: DeliveredMessage) -> None:  # type: ignore[override]
        """Queue ``msg`` offset for commit via the subscriber's batcher.

        Offsets are accumulated and flushed in batches — either once
        ``COMMIT_BATCH_SIZE`` offsets are buffered or every
        ``COMMIT_INTERVAL_MS`` — instead of one broker round-trip per
        message, which is what made the ack path the bottleneck at
        high throughput (Doc02 §6.1 follow-up).
        """
        tp = TopicPartition(msg.envelope.topic, msg.partition)
        await self._batcher_for(sub).add(tp, msg.offset)

    async def nack(  # type: ignore[override]
        self,
        sub: KafkaSubscriber,
        msg: DeliveredMessage,
        *,
        requeue: bool = True,
        reason: str = "",
    ) -> None:
        """Reject ``msg``.

        * ``requeue=True``: do nothing — the consumer will receive
          the message again after rebalance / restart (since we did
          NOT commit). For per-message redelivery without restart,
          callers can ``seek`` the partition; we keep the simple
          path for v0.1.
        * ``requeue=False``: send to DLQ and queue the offset for
          commit (via the same batcher as :meth:`ack`), so we don't
          loop on the poison message.
        """
        if requeue:
            log.warning(
                "bus.nack.requeue",
                topic=msg.envelope.topic,
                partition=msg.partition,
                offset=msg.offset,
                reason=reason,
            )
            return

        await self.send_to_dlq(msg.envelope, reason=reason)
        await self.ack(sub, msg)

    # ------------------------------------------------------------
    # DLQ
    # ------------------------------------------------------------

    async def send_to_dlq(self, env: Envelope, *, reason: str) -> None:
        """Publish ``env`` to its DLQ topic with a reason header."""
        dlq_env = env.model_copy(deep=True)
        dlq_env.headers.user_headers = {
            **dlq_env.headers.user_headers,
            "dlq_reason": reason,
            "dlq_original_topic": env.topic,
        }
        dlq_env.topic = dlq_topic_for(env.topic)
        await self.publish(dlq_env)
        log.warning(
            "bus.dlq",
            original_topic=env.topic,
            dlq_topic=dlq_env.topic,
            event_id=env.event_id,
            reason=reason,
        )

    # ------------------------------------------------------------
    # Ops
    # ------------------------------------------------------------

    async def health(self) -> BrokerHealth:
        producer = self._producer
        if producer is None:
            return BrokerHealth(healthy=False, detail="producer not started")
        try:
            # Fetching cluster metadata is a cheap reachability probe.
            await producer.client.fetch_all_metadata()
        except KafkaError as e:
            return BrokerHealth(healthy=False, detail=str(e))
        return BrokerHealth(healthy=True)

    async def consumer_lag(self, group: str, topic: str) -> int:
        """Return total lag across all partitions of ``topic`` for ``group``.

        Implementation note: we use a transient consumer to compute
        ``end_offset - committed_offset`` per partition. For very
        high-frequency calls this would be expensive; Autoscaler is
        expected to call us at low frequency (~15s), which is fine.
        """
        consumer = AIOKafkaConsumer(
            bootstrap_servers=self._settings.bootstrap_servers,
            group_id=group,
            enable_auto_commit=False,
        )
        await consumer.start()
        try:
            partitions = consumer.partitions_for_topic(topic)
            if partitions is None:
                # Topic may not exist yet — lag is undefined; treat as 0.
                return 0

            tps = [TopicPartition(topic, p) for p in partitions]
            end_offsets = await consumer.end_offsets(tps)
            committed = {tp: await consumer.committed(tp) for tp in tps}

            total = 0
            for tp in tps:
                end = end_offsets[tp]
                cur = committed[tp] or 0
                total += max(0, end - cur)
            return total
        finally:
            await consumer.stop()

    # ------------------------------------------------------------
    # Internals
    # ------------------------------------------------------------

    def _require_producer(self) -> AIOKafkaProducer:
        if self._producer is None or not self._started:
            raise BusUnavailable("KafkaEventBus has not been started")
        return self._producer

    def _batcher_for(self, sub: KafkaSubscriber) -> CommitBatcher:
        """Return ``sub``'s :class:`CommitBatcher`, creating one if missing.

        Subscribers are normally registered in :meth:`subscribe`; the
        lazy fallback here only matters for subscribers created outside
        the adapter's own bookkeeping (e.g. tests), so ``ack``/``nack``
        never crash for lack of a batcher.
        """
        batcher = self._commit_batchers.get(sub)
        if batcher is None:
            batcher = CommitBatcher(sub.consumer)
            batcher.start()
            self._commit_batchers[sub] = batcher
        return batcher

    async def _teardown_subscriber(self, sub: KafkaSubscriber) -> None:
        """Flush + stop ``sub``'s batcher, then close the subscriber.

        Order matters: pending offsets must be committed *before* the
        consumer is stopped, otherwise acked-but-uncommitted messages
        would be redelivered on restart.
        """
        batcher = self._commit_batchers.pop(sub, None)
        if batcher is not None:
            try:
                await batcher.stop()
            except Exception:
                log.exception("bus.commit_batcher.stop_failed")
        try:
            await sub.close()
        except Exception:
            log.exception("bus.subscriber.close_failed")

    @staticmethod
    def _reset_strategy(spec: SubscribeSpec) -> str:
        """Map our ``starting_position`` to aiokafka's ``auto_offset_reset``."""
        if spec.starting_position == "earliest":
            return "earliest"
        if spec.starting_position == "latest":
            return "latest"
        # ``committed`` semantics: start from group's last commit, or
        # latest if no commit exists. aiokafka's ``latest`` is a fine
        # default here since manual commit makes the group track.
        return "latest"

    @staticmethod
    def _to_kafka_pattern(topic_pattern: str) -> str:
        """Translate the framework's ``*`` / ``#`` into a Kafka regex.

        Limited v0.1 mapping; full AMQP rules are TODO.
        """
        import re as _re

        # Anchor the regex so that ``foo.bar.*`` doesn't accidentally
        # match ``foo.barbaz``.
        escaped = _re.escape(topic_pattern)
        # Translate our wildcards back from their escaped forms.
        escaped = escaped.replace(r"\*", "[^.]+").replace(r"\#", ".*")
        return f"^{escaped}$"
