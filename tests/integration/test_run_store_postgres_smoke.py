"""Smoke test for PostgresRunStore against a real Postgres instance.

Skipped automatically when Postgres is unreachable.  To run::

    docker-compose up -d postgres
    pytest -m integration tests/integration/test_run_store_postgres_smoke.py -v -s

Schema isolation: each test session creates a unique schema
``agentkit_test_<random8hex>`` and drops it on teardown, so concurrent
runs never collide.
"""

from __future__ import annotations

import secrets

import asyncpg
import pytest

from agentkit.models.enums import RunStatus
from agentkit.orchestrator.errors import RunNotFound
from agentkit.orchestrator.run import BranchEvent, new_run
from agentkit.orchestrator.store_postgres import PostgresRunStore, connect_from_env

pytestmark = pytest.mark.integration


# ── fixture ───────────────────────────────────────────────────────────────────


@pytest.fixture
async def pg_store():
    """Stand up an isolated PostgresRunStore and tear it down after the test."""
    dsn = connect_from_env()
    schema = f"agentkit_test_{secrets.token_hex(4)}"

    store = PostgresRunStore(dsn=dsn, schema=schema, min_size=1, max_size=3)
    try:
        await store.start()
    except (asyncpg.InvalidCatalogNameError, OSError, Exception) as exc:
        pytest.skip(f"Postgres not reachable: {exc}")

    yield store

    # Cleanup: drop the whole schema so nothing leaks between runs.
    try:
        async with store._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(f"DROP SCHEMA IF EXISTS {schema} CASCADE")
    finally:
        await store.stop()


# ── tests ─────────────────────────────────────────────────────────────────────


async def test_put_get_update_list_active(pg_store: PostgresRunStore) -> None:
    """Full round-trip: put → get (field check) → update → list_active → RunNotFound."""

    # ── 1. put and get back ───────────────────────────────────────────────
    run = new_run(
        workflow_id="wf_smoke",
        workflow_version=3,
        input={"x": 42, "tags": ["a", "b"]},
        trigger="api",
    )
    await pg_store.put(run)

    fetched = await pg_store.get(run.run_id)
    assert fetched.run_id == run.run_id
    assert fetched.workflow_id == "wf_smoke"
    assert fetched.workflow_version == 3
    assert fetched.trace_id == run.trace_id
    assert fetched.trigger == "api"
    assert fetched.status == RunStatus.PENDING
    assert fetched.input == {"x": 42, "tags": ["a", "b"]}
    assert fetched.output is None
    assert fetched.failure_reason is None
    assert fetched.ended_at is None
    assert fetched.cursor.branch_log == []

    # ── 2. put duplicate raises RuntimeError ─────────────────────────────
    with pytest.raises(RuntimeError, match="run already exists"):
        await pg_store.put(run)

    # ── 3. update: change status and add a branch event ──────────────────
    run.status = RunStatus.RUNNING
    run.add_branch_event(BranchEvent(edge_id="edge-1", chosen="nodeA", by="auto"))
    run.output = {"result": "ok"}
    await pg_store.update(run)

    updated = await pg_store.get(run.run_id)
    assert updated.status == RunStatus.RUNNING
    assert updated.output == {"result": "ok"}
    assert len(updated.cursor.branch_log) == 1
    assert updated.cursor.branch_log[0].edge_id == "edge-1"
    assert updated.cursor.branch_log[0].chosen == "nodeA"

    # ── 4. list_active contains our RUNNING run ───────────────────────────
    active = await pg_store.list_active()
    active_ids = {r.run_id for r in active}
    assert run.run_id in active_ids

    # ── 5. terminal run is excluded from list_active ─────────────────────
    run.status = RunStatus.SUCCEEDED
    await pg_store.update(run)

    active_after = await pg_store.list_active()
    assert run.run_id not in {r.run_id for r in active_after}

    # ── 6. get non-existent run raises RunNotFound ────────────────────────
    with pytest.raises(RunNotFound):
        await pg_store.get("run_does_not_exist_xyz")

    # ── 7. update non-existent run raises RunNotFound ─────────────────────
    ghost = new_run(workflow_id="wf_ghost")
    with pytest.raises(RunNotFound):
        await pg_store.update(ghost)
