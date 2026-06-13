"""Run persistence — Protocol + in-memory default implementation.

The :class:`RunStore` Protocol mirrors what :class:`Orchestrator`
needs at runtime. Phase 1 ships an in-memory default that is
sufficient for unit tests and single-process self-hosted demos; a
PostgreSQL-backed store plugs in here later (Doc10 §6.1).
"""

from __future__ import annotations

import asyncio
from typing import Protocol, runtime_checkable

from agentkit.orchestrator.errors import RunNotFound
from agentkit.orchestrator.run import Run


@runtime_checkable
class RunStore(Protocol):
    """The minimal persistence interface the Orchestrator depends on."""

    async def put(self, run: Run) -> None: ...
    async def get(self, run_id: str) -> Run: ...
    async def update(self, run: Run) -> None: ...
    async def list_active(self) -> list[Run]: ...

    async def list_recent(
        self,
        *,
        workflow_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[Run]:
        """Most-recent-first runs (terminal *and* active), with optional
        ``workflow_id`` / ``status`` filters and a ``limit``. Used by the
        UI's run list (which wants completed runs too, unlike
        :meth:`list_active`)."""
        ...


class InMemoryRunStore:
    """In-memory store for tests / single-process dev.

    Each operation is made atomic with an :class:`asyncio.Lock` (B18):
    without it, ``list_active`` could iterate ``self._runs`` while a
    concurrent ``put`` resizes the dict (``RuntimeError: dict changed
    size during iteration``) once ``max_concurrent>1`` or multiple
    routers mutate runs in parallel. The lock guards *structural*
    consistency; cross-operation read-modify-write serialization is the
    caller's responsibility (the Postgres store uses transactions).
    """

    def __init__(self) -> None:
        self._runs: dict[str, Run] = {}
        self._lock = asyncio.Lock()

    async def put(self, run: Run) -> None:
        async with self._lock:
            self._runs[run.run_id] = run

    async def get(self, run_id: str) -> Run:
        async with self._lock:
            run = self._runs.get(run_id)
        if run is None:
            raise RunNotFound(run_id)
        return run

    async def update(self, run: Run) -> None:
        async with self._lock:
            if run.run_id not in self._runs:
                raise RunNotFound(run.run_id)
            self._runs[run.run_id] = run

    async def list_active(self) -> list[Run]:
        async with self._lock:
            # Snapshot under the lock so iteration can't race a put/update.
            return [r for r in self._runs.values() if not r.is_terminal]

    async def list_recent(
        self,
        *,
        workflow_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[Run]:
        async with self._lock:
            runs = list(self._runs.values())
        if workflow_id:
            runs = [r for r in runs if r.workflow_id == workflow_id]
        if status:
            runs = [r for r in runs if r.status.value == status]
        runs.sort(
            key=lambda r: r.started_at.timestamp() if r.started_at else 0.0,
            reverse=True,
        )
        return runs[: max(0, limit)]

    # Test helpers — not part of the Protocol.

    def all_ids(self) -> list[str]:
        return list(self._runs)

    def __len__(self) -> int:
        return len(self._runs)


# Compile-time check — keeps drift between Protocol and impl visible.
_: type[RunStore] = InMemoryRunStore  # type: ignore[assignment, misc]
