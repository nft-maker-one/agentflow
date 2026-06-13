"""RunStore.list_recent — backend-agnostic run listing for the UI.

Regression for the bug where the runs API reached into
``InMemoryRunStore._runs`` directly, crashing under ``PostgresRunStore``
(no ``_runs`` attribute).
"""

from __future__ import annotations

from agentkit.orchestrator.run import new_run
from agentkit.orchestrator.store import InMemoryRunStore
from agentkit.models.enums import RunStatus


async def _seed(store, *, wf, status=RunStatus.SUCCEEDED):
    run = new_run(workflow_id=wf, workflow_version=1, input={})
    run.status = status
    await store.put(run)
    return run


class TestListRecent:
    async def test_includes_terminal_runs(self) -> None:
        store = InMemoryRunStore()
        await _seed(store, wf="wf1", status=RunStatus.SUCCEEDED)
        await _seed(store, wf="wf1", status=RunStatus.RUNNING)
        out = await store.list_recent()
        assert len(out) == 2  # terminal + active both returned

    async def test_workflow_filter(self) -> None:
        store = InMemoryRunStore()
        await _seed(store, wf="wf1")
        await _seed(store, wf="wf2")
        out = await store.list_recent(workflow_id="wf1")
        assert {r.workflow_id for r in out} == {"wf1"}

    async def test_status_filter(self) -> None:
        store = InMemoryRunStore()
        await _seed(store, wf="wf1", status=RunStatus.SUCCEEDED)
        await _seed(store, wf="wf1", status=RunStatus.FAILED)
        out = await store.list_recent(status="Failed")
        assert all(r.status is RunStatus.FAILED for r in out)
        assert len(out) == 1

    async def test_limit(self) -> None:
        store = InMemoryRunStore()
        for _ in range(5):
            await _seed(store, wf="wf1")
        out = await store.list_recent(limit=2)
        assert len(out) == 2

    def test_is_part_of_protocol(self) -> None:
        # The Protocol method exists so any backend (incl. Postgres) satisfies it.
        assert hasattr(InMemoryRunStore, "list_recent")
        from agentkit.orchestrator.store_postgres import PostgresRunStore
        assert hasattr(PostgresRunStore, "list_recent")
