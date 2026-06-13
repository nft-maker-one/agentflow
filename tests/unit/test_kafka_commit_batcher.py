"""Unit tests for :class:`agentkit.bus.kafka.adapter.CommitBatcher`.

These exercise the in-adapter commit-batching mechanism added for B12
(high-throughput ack path). No live broker is required — we use a
minimal fake consumer that mimics the ``aiokafka`` interface surface
the batcher depends on (``commit(offsets_dict)``).
"""

from __future__ import annotations

import asyncio

from aiokafka.structs import TopicPartition

from agentkit.bus.kafka.adapter import CommitBatcher


class FakeConsumer:
    """Records every ``commit()`` call for later assertion."""

    def __init__(self) -> None:
        self.commit_calls: list[dict[TopicPartition, int]] = []

    async def commit(self, offsets: dict[TopicPartition, int]) -> None:
        # Defensive copy — mirrors the immutable-snapshot expectation;
        # callers must not be able to mutate history after the fact.
        self.commit_calls.append(dict(offsets))

    @property
    def all_committed(self) -> dict[TopicPartition, int]:
        """Merge all commit calls, keeping the latest value per partition."""
        merged: dict[TopicPartition, int] = {}
        for call in self.commit_calls:
            merged.update(call)
        return merged


TP_A = TopicPartition("agentkit.test.topic", 0)
TP_B = TopicPartition("agentkit.test.topic", 1)


# ----------------------------------------------------------------
# 1. Size-threshold flush
# ----------------------------------------------------------------


async def test_flush_triggers_at_size_threshold() -> None:
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=3, interval_ms=60_000)
    try:
        await batcher.add(TP_A, 0)
        await batcher.add(TP_A, 1)
        assert consumer.commit_calls == []  # not yet at threshold

        await batcher.add(TP_A, 2)
        # Buffer reached batch_size=3 distinct adds -> immediate flush.
        assert len(consumer.commit_calls) == 1
        assert consumer.commit_calls[0] == {TP_A: 3}
    finally:
        await batcher.stop()


# ----------------------------------------------------------------
# 2. Time-threshold flush
# ----------------------------------------------------------------


async def test_flush_triggers_at_time_threshold() -> None:
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=1_000, interval_ms=50)
    batcher.start()
    try:
        await batcher.add(TP_A, 5)
        assert consumer.commit_calls == []  # below size threshold

        # Wait comfortably longer than the interval for the background
        # loop to notice and flush.
        for _ in range(20):
            await asyncio.sleep(0.05)
            if consumer.commit_calls:
                break

        assert consumer.commit_calls, "expected a time-triggered flush"
        assert consumer.commit_calls[-1] == {TP_A: 6}
    finally:
        await batcher.stop()


# ----------------------------------------------------------------
# 3. Per-partition max-offset correctness
# ----------------------------------------------------------------


async def test_per_partition_max_offset_never_regresses() -> None:
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=1_000, interval_ms=60_000)
    try:
        # Out-of-order acks across two partitions.
        await batcher.add(TP_A, 5)
        await batcher.add(TP_A, 2)  # lower than current max -> ignored
        await batcher.add(TP_B, 10)
        await batcher.add(TP_A, 7)
        await batcher.add(TP_B, 9)  # lower than current max -> ignored

        await batcher.flush()

        assert consumer.commit_calls[-1] == {TP_A: 8, TP_B: 11}

        # A subsequent lower offset must never produce a regressive commit.
        await batcher.add(TP_A, 3)
        await batcher.flush()

        committed = consumer.all_committed
        assert committed[TP_A] == 8
        assert committed[TP_B] == 11
        # No commit call ever requested a value lower than one already sent.
        seen_a = [c[TP_A] for c in consumer.commit_calls if TP_A in c]
        seen_b = [c[TP_B] for c in consumer.commit_calls if TP_B in c]
        assert seen_a == sorted(seen_a)
        assert seen_b == sorted(seen_b)
    finally:
        await batcher.stop()


async def test_per_partition_higher_offset_after_flush_advances_commit() -> None:
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=1_000, interval_ms=60_000)
    try:
        await batcher.add(TP_A, 1)
        await batcher.flush()
        assert consumer.commit_calls[-1] == {TP_A: 2}

        await batcher.add(TP_A, 4)
        await batcher.flush()
        assert consumer.commit_calls[-1] == {TP_A: 5}

        assert consumer.all_committed[TP_A] == 5
    finally:
        await batcher.stop()


# ----------------------------------------------------------------
# 4. Explicit flush-on-close commits remaining offsets
# ----------------------------------------------------------------


async def test_stop_flushes_remaining_offsets() -> None:
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=1_000, interval_ms=60_000)
    batcher.start()

    await batcher.add(TP_A, 0)
    await batcher.add(TP_B, 3)
    assert consumer.commit_calls == []  # nothing flushed yet

    await batcher.stop()

    assert consumer.commit_calls, "stop() must flush pending offsets"
    assert consumer.all_committed == {TP_A: 1, TP_B: 4}

    # The background flush task must be fully cancelled/awaited —
    # calling stop() again should be a safe no-op (no pending work).
    await batcher.stop()
    assert consumer.all_committed == {TP_A: 1, TP_B: 4}


async def test_stop_without_start_still_flushes() -> None:
    """``stop()`` must flush even if the background loop never ran."""
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=1_000, interval_ms=60_000)

    await batcher.add(TP_A, 9)
    await batcher.stop()

    assert consumer.all_committed == {TP_A: 10}


# ----------------------------------------------------------------
# 5. DLQ path still commits via the batcher
# ----------------------------------------------------------------


async def test_dlq_offset_is_committed_through_batcher() -> None:
    """Simulates the adapter's nack(requeue=False) -> ack -> batcher path.

    The important property: a message routed to the DLQ must still have
    its offset queued and eventually committed, so the consumer group
    doesn't redeliver (and reprocess into the DLQ) forever.
    """
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=2, interval_ms=60_000)
    try:
        # First message acked normally.
        await batcher.add(TP_A, 0)
        # Second message failed -> sent to DLQ -> still must be acked/committed.
        await batcher.add(TP_A, 1)

        # batch_size=2 reached -> flush should have fired already.
        assert consumer.commit_calls
        assert consumer.commit_calls[-1] == {TP_A: 2}
    finally:
        await batcher.stop()


async def test_dlq_offset_committed_on_explicit_flush() -> None:
    consumer = FakeConsumer()
    batcher = CommitBatcher(consumer, batch_size=1_000, interval_ms=60_000)
    try:
        # Message goes straight to DLQ (requeue=False) — adapter still
        # routes the offset through the batcher so it's committed.
        await batcher.add(TP_A, 7)
        await batcher.flush()

        assert consumer.all_committed == {TP_A: 8}
    finally:
        await batcher.stop()
