"""Unit tests for B16 — batched branch-event flush in the Orchestrator.

These exercise :meth:`Orchestrator._record_branch_event` and the
background flush loop directly (no bus / router wiring needed) using
an in-memory store wrapped to count ``update()`` calls, so we can
assert that many buffered events collapse into a single
read-modify-write per run per flush window.
"""

from __future__ import annotations

import asyncio

import pytest

from agentkit.orchestrator.orchestrator import Orchestrator
from agentkit.orchestrator.run import BranchEvent, new_run
from agentkit.orchestrator.store import InMemoryRunStore
from tests.helpers.mock_bus import MockEventBus


class CountingRunStore:
    """Wraps :class:`InMemoryRunStore`, counting + recording ``update()``
    calls so tests can assert batching collapsed N writes into 1."""

    def __init__(self) -> None:
        self._inner = InMemoryRunStore()
        self.update_count = 0
        # Snapshot of branch_log edge_ids at each update() call, in order.
        self.update_branch_logs: list[list[str]] = []

    async def put(self, run):
        await self._inner.put(run)

    async def get(self, run_id):
        return await self._inner.get(run_id)

    async def update(self, run):
        self.update_count += 1
        self.update_branch_logs.append(
            [ev.edge_id for ev in run.cursor.branch_log],
        )
        await self._inner.update(run)

    async def list_active(self):
        return await self._inner.list_active()


def make_event(edge_id: str) -> BranchEvent:
    return BranchEvent(edge_id=edge_id, chosen=edge_id, by="system")


@pytest.fixture
async def bus() -> MockEventBus:
    b = MockEventBus()
    await b.start()
    return b


@pytest.fixture
def store() -> CountingRunStore:
    return CountingRunStore()


@pytest.fixture
async def orch(bus, store):
    o = Orchestrator(bus=bus, store=store)
    yield o
    if o._running:
        await o.stop()


async def seed_run(store: CountingRunStore, *, workflow_id: str = "wf_x"):
    run = new_run(workflow_id=workflow_id)
    await store.put(run)
    # Reset the counter — we only care about updates from branch flushes.
    store.update_count = 0
    store.update_branch_logs.clear()
    return run


class TestBatchingCollapsesWrites:
    async def test_multiple_events_collapse_into_single_update(
        self, orch, store,
    ) -> None:
        run = await seed_run(store)

        for i in range(5):
            await orch._record_branch_event(run.run_id, make_event(f"e{i}"))

        await orch._flush_branch_events_for_run(run.run_id)

        assert store.update_count == 1
        loaded = await store.get(run.run_id)
        assert [ev.edge_id for ev in loaded.cursor.branch_log] == [
            "e0", "e1", "e2", "e3", "e4",
        ]

    async def test_no_flush_when_buffer_empty(self, orch, store) -> None:
        run = await seed_run(store)
        await orch._flush_branch_events_for_run(run.run_id)
        assert store.update_count == 0


class TestTimeBasedFlush:
    async def test_background_loop_flushes_after_interval(
        self, bus, store,
    ) -> None:
        o = Orchestrator(
            bus=bus, store=store,
            branch_flush_interval_ms=20, branch_flush_max=1000,
        )
        run = await seed_run(store)
        await o.start()
        try:
            await o._record_branch_event(run.run_id, make_event("a"))
            await o._record_branch_event(run.run_id, make_event("b"))

            # Not flushed yet — give it less than the interval.
            await asyncio.sleep(0.001)
            assert store.update_count == 0

            # Wait past the flush interval.
            await asyncio.sleep(0.15)
            assert store.update_count == 1
            loaded = await store.get(run.run_id)
            assert [ev.edge_id for ev in loaded.cursor.branch_log] == ["a", "b"]
        finally:
            await o.stop()


class TestSizeBasedFlush:
    async def test_buffer_threshold_triggers_immediate_flush(
        self, bus, store,
    ) -> None:
        o = Orchestrator(
            bus=bus, store=store,
            branch_flush_interval_ms=10_000, branch_flush_max=3,
        )
        run = await seed_run(store)
        await o.start()
        try:
            await o._record_branch_event(run.run_id, make_event("a"))
            await o._record_branch_event(run.run_id, make_event("b"))
            assert store.update_count == 0  # below threshold

            await o._record_branch_event(run.run_id, make_event("c"))
            # Hits the size threshold — flush triggered inline.
            assert store.update_count == 1
            loaded = await store.get(run.run_id)
            assert [ev.edge_id for ev in loaded.cursor.branch_log] == [
                "a", "b", "c",
            ]
        finally:
            await o.stop()


class TestFlushOnStopAndTerminal:
    async def test_stop_flushes_remaining_buffered_events(
        self, bus, store,
    ) -> None:
        o = Orchestrator(
            bus=bus, store=store,
            branch_flush_interval_ms=10_000, branch_flush_max=1000,
        )
        run = await seed_run(store)
        await o.start()

        await o._record_branch_event(run.run_id, make_event("last1"))
        await o._record_branch_event(run.run_id, make_event("last2"))
        assert store.update_count == 0  # nothing flushed yet

        await o.stop()

        assert store.update_count == 1
        loaded = await store.get(run.run_id)
        assert [ev.edge_id for ev in loaded.cursor.branch_log] == [
            "last1", "last2",
        ]
        # Clean cancellation — no leftover task.
        assert o._branch_flush_task is None

    async def test_on_terminal_flushes_pending_events_first(
        self, bus, store,
    ) -> None:
        o = Orchestrator(
            bus=bus, store=store,
            branch_flush_interval_ms=10_000, branch_flush_max=1000,
        )
        run = await seed_run(store)
        run.status = run.status.__class__.RUNNING
        await store.update(run)
        store.update_count = 0
        store.update_branch_logs.clear()
        await o.start()
        try:
            await o._record_branch_event(run.run_id, make_event("final_branch"))
            assert store.update_count == 0

            await o._on_terminal(run.run_id, "__end__", "completed")

            loaded = await store.get(run.run_id)
            edge_ids = [ev.edge_id for ev in loaded.cursor.branch_log]
            assert "final_branch" in edge_ids
            # The buffered event must appear *before* the terminal entry
            # appended by _on_terminal — ordering preserved.
            assert edge_ids.index("final_branch") < edge_ids.index("__terminal__")
        finally:
            await o.stop()


class TestOrderingPreserved:
    async def test_ordering_preserved_across_flushes(self, orch, store) -> None:
        run = await seed_run(store)

        for i in range(7):
            await orch._record_branch_event(run.run_id, make_event(f"x{i}"))

        await orch._flush_branch_events_for_run(run.run_id)

        loaded = await store.get(run.run_id)
        assert [ev.edge_id for ev in loaded.cursor.branch_log] == [
            f"x{i}" for i in range(7)
        ]

    async def test_ordering_preserved_with_interleaved_flushes(
        self, orch, store,
    ) -> None:
        run = await seed_run(store)

        await orch._record_branch_event(run.run_id, make_event("a"))
        await orch._record_branch_event(run.run_id, make_event("b"))
        await orch._flush_branch_events_for_run(run.run_id)

        await orch._record_branch_event(run.run_id, make_event("c"))
        await orch._record_branch_event(run.run_id, make_event("d"))
        await orch._flush_branch_events_for_run(run.run_id)

        loaded = await store.get(run.run_id)
        assert [ev.edge_id for ev in loaded.cursor.branch_log] == [
            "a", "b", "c", "d",
        ]
