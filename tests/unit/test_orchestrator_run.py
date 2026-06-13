"""Unit tests for the Run / RunCursor / BranchEvent data models."""

from __future__ import annotations

import pytest

from agentkit.models.enums import RunStatus
from agentkit.orchestrator.errors import RunNotFound
from agentkit.orchestrator.run import (
    BranchEvent,
    Run,
    RunCursor,
    new_run,
)
from agentkit.orchestrator.store import InMemoryRunStore


class TestRun:
    def test_new_run_starts_pending(self) -> None:
        run = new_run(workflow_id="wf_x", input={"q": "hi"})
        assert run.status is RunStatus.PENDING
        assert run.workflow_id == "wf_x"
        assert run.input == {"q": "hi"}
        assert run.run_id.startswith("run_")
        assert run.trace_id.startswith("trc_")

    def test_terminal_property(self) -> None:
        run = new_run(workflow_id="wf_x")
        assert run.is_terminal is False
        run.status = RunStatus.SUCCEEDED
        assert run.is_terminal is True
        run.status = RunStatus.FAILED
        assert run.is_terminal is True
        run.status = RunStatus.CANCELLED
        assert run.is_terminal is True
        run.status = RunStatus.RUNNING
        assert run.is_terminal is False

    def test_add_branch_event_appends(self) -> None:
        run = new_run(workflow_id="wf_x")
        ev = BranchEvent(edge_id="e1", chosen="writer")
        run.add_branch_event(ev)
        assert len(run.cursor.branch_log) == 1
        assert run.cursor.branch_log[0].edge_id == "e1"


class TestBranchEvent:
    def test_branch_event_required_fields(self) -> None:
        ev = BranchEvent(edge_id="e1", chosen="writer", by="auto")
        assert ev.by == "auto"
        assert ev.at is not None

    def test_branch_event_extra_field_rejected(self) -> None:
        from pydantic import ValidationError

        with pytest.raises(ValidationError):
            BranchEvent(edge_id="e1", chosen="x", weird=1)  # type: ignore[call-arg]


class TestRunCursor:
    def test_default_empty_log(self) -> None:
        cursor = RunCursor()
        assert cursor.branch_log == []


class TestInMemoryRunStore:
    async def test_put_and_get_round_trip(self) -> None:
        store = InMemoryRunStore()
        run = new_run(workflow_id="wf_x")
        await store.put(run)
        loaded = await store.get(run.run_id)
        assert loaded.run_id == run.run_id

    async def test_get_unknown_raises(self) -> None:
        store = InMemoryRunStore()
        with pytest.raises(RunNotFound):
            await store.get("run_ghost")

    async def test_update_unknown_raises(self) -> None:
        store = InMemoryRunStore()
        run = new_run(workflow_id="wf_x")
        with pytest.raises(RunNotFound):
            await store.update(run)

    async def test_list_active_filters_terminal(self) -> None:
        store = InMemoryRunStore()
        active = new_run(workflow_id="wf_x")
        active.status = RunStatus.RUNNING
        done = new_run(workflow_id="wf_x")
        done.status = RunStatus.SUCCEEDED

        await store.put(active)
        await store.put(done)

        active_list = await store.list_active()
        assert len(active_list) == 1
        assert active_list[0].run_id == active.run_id
