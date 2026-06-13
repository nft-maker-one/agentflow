"""Tests for the tier-2 concurrency *correctness* fixes (docs/CONCURRENCY.md §10).

* B17 — the owning subscriber + guardrail reservation are threaded
  explicitly through the message-processing chain, so concurrent
  in-flight messages (max_concurrent>1) ack/nack on the *right*
  subscriber instead of clobbering a shared ``self._subscriber``.
* B18 — aggregator buffer mutations are lock-guarded; InMemoryRunStore
  operations are atomic (no dict-changed-size-during-iteration).
* B19 — aggregator buffer is bounded by TTL + max-buckets so an
  orphaned run (a required topic that never arrives) can't leak.
"""

from __future__ import annotations

import asyncio
from datetime import timedelta

import pytest

from agentkit.bus.builder import build_envelope
from agentkit.bus.interface import DeliveredMessage
from agentkit.common.time import utcnow
from agentkit.orchestrator.errors import RunNotFound
from agentkit.orchestrator.run import new_run
from agentkit.orchestrator.store import InMemoryRunStore
from agentkit.runtime.config import RuntimeSettings
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.runtime.instance import AgentInstance
from agentkit.testing.mock_llm import MockLLMGateway
from agentkit.workflow.plan import AgentPlan


# ============================================================
# Helpers
# ============================================================


class _RecordingBus:
    """Captures which subscriber each ack/nack/DLQ was routed to."""

    def __init__(self) -> None:
        self.acks: list[tuple[object, str]] = []
        self.nacks: list[tuple[object, str]] = []
        self.dlq: list[str] = []
        self.published: list[str] = []

    async def ack(self, sub: object, msg: DeliveredMessage) -> None:
        self.acks.append((sub, msg.envelope.event_id))

    async def nack(self, sub: object, msg: DeliveredMessage, *,
                   requeue: bool = True, reason: str = "") -> None:
        self.nacks.append((sub, msg.envelope.event_id))

    async def send_to_dlq(self, env, *, reason: str = "") -> None:
        self.dlq.append(env.event_id)

    async def publish(self, env):
        from agentkit.bus.interface import PublishReceipt
        self.published.append(env.event_id)
        return PublishReceipt(event_id=env.event_id, topic=env.topic,
                              partition=0, offset=0)


def _msg(topic: str, payload: dict, run_id: str = "r") -> DeliveredMessage:
    env = build_envelope(topic=topic, payload=payload, workflow_id="wf",
                         run_id=run_id, trace_id="trc")
    return DeliveredMessage(envelope=env, partition=0, offset=0)


def _instance(bus, *, handler=None, settings=None, aggregate=None,
              subscribe=("t.a", "t.b")) -> AgentInstance:
    async def _noop(ctx, ev):  # noqa: ANN001
        return []

    plan = AgentPlan(
        template_key="x", role="thinking", description="",
        subscribe_topics=list(subscribe), publish_topics=["t.out"],
        aggregate=aggregate,
    )
    return AgentInstance(
        plan=plan, workflow_id="wf", bus=bus, llm=MockLLMGateway(),
        executor=_FunctionExecutor(handler or _noop),
        settings=settings or RuntimeSettings(max_handler_retries=0),
    )


# ============================================================
# B17 — subscriber threaded, not shared
# ============================================================


class TestSubscriberThreading:
    async def test_concurrent_messages_ack_on_their_own_subscriber(self) -> None:
        # A slow handler forces the two messages to overlap in flight;
        # the fix guarantees each acks on the subscriber passed to
        # _process_message, not a shared self._subscriber.
        async def slow(ctx, ev):  # noqa: ANN001
            await asyncio.sleep(0.02)
            return []

        bus = _RecordingBus()
        inst = _instance(bus, handler=slow)

        sub_a, sub_b = object(), object()
        msg_a = _msg("t.a", {"n": 1}, run_id="ra")
        msg_b = _msg("t.b", {"n": 2}, run_id="rb")

        await asyncio.gather(
            inst._process_message(msg_a, sub_a),
            inst._process_message(msg_b, sub_b),
        )

        by_sub = {eid: sub for sub, eid in bus.acks}
        assert by_sub[msg_a.envelope.event_id] is sub_a
        assert by_sub[msg_b.envelope.event_id] is sub_b
        assert len(bus.acks) == 2

    async def test_failure_nacks_on_correct_subscriber(self) -> None:
        async def boom(ctx, ev):  # noqa: ANN001
            raise RuntimeError("permanent")

        bus = _RecordingBus()
        # max_handler_retries=0 → recoverable becomes terminal → nack to DLQ.
        inst = _instance(bus, handler=boom,
                         settings=RuntimeSettings(max_handler_retries=0))
        sub = object()
        msg = _msg("t.a", {}, run_id="rx")
        await inst._process_message(msg, sub)
        # The single failure path nacked on the subscriber we passed.
        assert bus.nacks and bus.nacks[0][0] is sub
        assert bus.nacks[0][1] == msg.envelope.event_id


# ============================================================
# B18/B19 — aggregator lock + bounds
# ============================================================


class TestAggregatorBounds:
    async def test_admit_is_async_and_merges_on_threshold(self) -> None:
        bus = _RecordingBus()
        inst = _instance(bus, aggregate={"threshold": 2, "required": []})

        out1 = await inst._aggregate_admit(_msg("t.a", {"a": 1}, "r1").envelope)
        assert out1 is None  # only 1 of 2
        out2 = await inst._aggregate_admit(_msg("t.b", {"b": 2}, "r1").envelope)
        assert out2 is not None  # threshold met → merged
        assert out2.payload["a"] == 1 and out2.payload["b"] == 2
        assert "_inputs" in out2.payload
        # Bucket cleared after completion.
        assert "r1" not in inst._aggregate_buffer

    async def test_ttl_evicts_orphaned_run(self) -> None:
        bus = _RecordingBus()
        inst = _instance(
            bus, aggregate={"threshold": 2, "required": []},
            settings=RuntimeSettings(aggregate_buffer_ttl_ms=3_600_000),
        )
        # r1 buffers one input but never completes.
        await inst._aggregate_admit(_msg("t.a", {"a": 1}, "r1").envelope)
        assert "r1" in inst._aggregate_buffer
        # Backdate it past the TTL.
        inst._aggregate_seen_at["r1"] = utcnow() - timedelta(hours=2)
        # A new admit for r2 triggers eviction of the stale r1.
        await inst._aggregate_admit(_msg("t.a", {"x": 1}, "r2").envelope)
        assert "r1" not in inst._aggregate_buffer
        assert "r2" in inst._aggregate_buffer

    async def test_max_buckets_cap_bounds_buffer(self) -> None:
        bus = _RecordingBus()
        inst = _instance(
            bus, aggregate={"threshold": 2, "required": []},
            settings=RuntimeSettings(
                aggregate_buffer_ttl_ms=0,  # disable TTL — isolate the cap
                aggregate_max_buckets=2,
            ),
        )
        for rid in ("r1", "r2", "r3", "r4"):
            await inst._aggregate_admit(_msg("t.a", {}, rid).envelope)
        # Never exceeds the cap; the most-recent run is always retained.
        assert len(inst._aggregate_buffer) <= 2
        assert "r4" in inst._aggregate_buffer

    async def test_concurrent_admit_same_run_is_consistent(self) -> None:
        bus = _RecordingBus()
        inst = _instance(bus, aggregate={"threshold": 5, "required": []})
        # Fire many concurrent admits for the same run on distinct topics;
        # the lock keeps the nested dict consistent (no lost/torn writes).
        envs = [_msg(f"t.{i}", {f"k{i}": i}, "rc").envelope for i in range(4)]
        results = await asyncio.gather(*(inst._aggregate_admit(e) for e in envs))
        assert all(r is None for r in results)  # threshold 5 not met
        assert len(inst._aggregate_buffer["rc"]) == 4  # all 4 topics retained


# ============================================================
# B18 — InMemoryRunStore atomic ops
# ============================================================


class TestInMemoryRunStoreLocking:
    async def test_basic_crud_still_works(self) -> None:
        store = InMemoryRunStore()
        run = new_run(workflow_id="wf", workflow_version=1, input={})
        await store.put(run)
        got = await store.get(run.run_id)
        assert got.run_id == run.run_id
        await store.update(run)
        assert run in await store.list_active()

    async def test_get_missing_raises(self) -> None:
        store = InMemoryRunStore()
        with pytest.raises(RunNotFound):
            await store.get("nope")

    async def test_concurrent_puts_and_list_active_no_dict_resize_error(self) -> None:
        # Before the lock, list_active() iterating while a concurrent put()
        # resized the dict could raise "dict changed size during iteration".
        store = InMemoryRunStore()

        async def writer() -> None:
            for _ in range(200):
                r = new_run(workflow_id="wf", workflow_version=1, input={})
                await store.put(r)
                await asyncio.sleep(0)

        async def lister() -> None:
            for _ in range(200):
                await store.list_active()
                await asyncio.sleep(0)

        # Should complete without RuntimeError.
        await asyncio.gather(writer(), writer(), lister(), lister())
        assert len(store) == 400
