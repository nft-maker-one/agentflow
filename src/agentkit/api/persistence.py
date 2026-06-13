"""Control-plane persistence — pluggable backend for the *metadata* that
lives on :class:`~agentkit.api.state.AppState` (projects, deployed
workflows, inbox) so it survives a ``serve`` restart.

This is the metadata sibling of :class:`RunStore` (which persists *runs*):

* :class:`ControlPlanePersistence` — the Protocol the API state depends on.
* :class:`NoOpControlPlane` — memory mode; nothing is durably stored
  (the in-memory dicts on AppState are the only copy). Returned by the
  factory when ``--store memory`` / no ``AGENTKIT_PG_DSN``.
* :class:`PostgresControlPlane` — Postgres-backed; mirrors writes to
  ``projects`` / ``workflows`` / ``inbox`` tables and loads them on boot.

The abstract-factory :func:`build_persistence` keeps the rest of the
codebase decoupled from the ``--store`` choice (Doc:point-5).
"""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any, Protocol, runtime_checkable

from agentkit.common.logging import get_logger

if TYPE_CHECKING:  # pragma: no cover
    import asyncpg

log = get_logger(__name__)


# ============================================================
# Protocol
# ============================================================


@runtime_checkable
class ControlPlanePersistence(Protocol):
    """Durable storage for control-plane metadata.

    All ``load_*`` methods return plain dicts (decoupled from the
    in-memory dataclasses). All ``upsert_*`` / ``delete_*`` are
    idempotent and best-effort from the caller's perspective.
    """

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def load_projects(self) -> list[dict[str, Any]]: ...
    async def load_workflows(self) -> list[dict[str, Any]]: ...
    async def load_inbox(self, *, limit: int = 500) -> list[dict[str, Any]]: ...

    async def upsert_project(self, project: dict[str, Any]) -> None: ...
    async def delete_project(self, project_id: str) -> None: ...

    async def upsert_workflow(self, workflow: dict[str, Any]) -> None: ...
    async def delete_workflow(self, workflow_id: str) -> None: ...

    async def upsert_inbox(self, item: dict[str, Any]) -> None: ...
    async def update_inbox(
        self, item_id: str, *, read: bool | None = None,
        archived: bool | None = None,
    ) -> None: ...
    async def mark_all_inbox_read(self, *, workflow_id: str | None = None) -> None: ...
    async def delete_inbox(self, item_id: str) -> None: ...
    async def delete_inbox_for_workflow(self, workflow_id: str) -> None: ...

    # Per-run event timeline (flushed on completion, read back on archival).
    async def persist_run_events(
        self, run_id: str, workflow_id: str, envelopes: list[dict[str, Any]],
    ) -> None: ...
    async def load_run_events(
        self, run_id: str, *, limit: int = 2000,
    ) -> list[dict[str, Any]]: ...

    @property
    def enabled(self) -> bool:
        """True for a durable backend (Postgres), False for memory/no-op."""
        ...


# ============================================================
# No-op (memory mode)
# ============================================================


class NoOpControlPlane:
    """Persistence used in memory mode — stores nothing.

    Every method is a no-op; ``load_*`` returns empty lists. AppState's
    in-memory dicts remain the single source of truth.
    """

    enabled = False

    async def start(self) -> None: ...
    async def stop(self) -> None: ...

    async def load_projects(self) -> list[dict[str, Any]]:
        return []

    async def load_workflows(self) -> list[dict[str, Any]]:
        return []

    async def load_inbox(self, *, limit: int = 500) -> list[dict[str, Any]]:
        return []

    async def upsert_project(self, project: dict[str, Any]) -> None: ...
    async def delete_project(self, project_id: str) -> None: ...
    async def upsert_workflow(self, workflow: dict[str, Any]) -> None: ...
    async def delete_workflow(self, workflow_id: str) -> None: ...
    async def upsert_inbox(self, item: dict[str, Any]) -> None: ...
    async def update_inbox(self, item_id, *, read=None, archived=None) -> None: ...
    async def mark_all_inbox_read(self, *, workflow_id=None) -> None: ...
    async def delete_inbox(self, item_id: str) -> None: ...
    async def delete_inbox_for_workflow(self, workflow_id: str) -> None: ...
    async def persist_run_events(self, run_id, workflow_id, envelopes) -> None: ...
    async def load_run_events(self, run_id, *, limit=2000):
        return []


# ============================================================
# Postgres
# ============================================================


_MIGRATION = """\
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.projects (
    project_id   TEXT PRIMARY KEY,
    name         TEXT NOT NULL,
    description  TEXT NOT NULL DEFAULT '',
    created_at   TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at   TIMESTAMPTZ NOT NULL DEFAULT now()
);

CREATE TABLE IF NOT EXISTS {schema}.workflows (
    workflow_id        TEXT PRIMARY KEY,
    project_id         TEXT NOT NULL DEFAULT 'default',
    description        TEXT NOT NULL DEFAULT '',
    raw_spec           JSONB NOT NULL,
    agent_kwargs       JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    mode               TEXT NOT NULL DEFAULT 'normal',
    start_input_fields JSONB NOT NULL DEFAULT '["q"]'::jsonb,
    created_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at         TIMESTAMPTZ NOT NULL DEFAULT now(),
    -- Soft-delete tombstone. NULL = live; non-NULL = logically deleted.
    -- Workflows are referenced by run history and carry audit value, so
    -- delete tombstones the row (recoverable, purgeable after retention)
    -- instead of physically removing it.
    deleted_at         TIMESTAMPTZ
);
-- Forward-migrate older `workflows` tables.
ALTER TABLE {schema}.workflows ADD COLUMN IF NOT EXISTS agent_kwargs JSONB NOT NULL DEFAULT '{{}}'::jsonb;
ALTER TABLE {schema}.workflows ADD COLUMN IF NOT EXISTS deleted_at TIMESTAMPTZ;
CREATE INDEX IF NOT EXISTS idx_workflows_project ON {schema}.workflows (project_id);
-- Partial index: only live rows are ever scanned on the boot/load path.
CREATE INDEX IF NOT EXISTS idx_workflows_live
    ON {schema}.workflows (workflow_id) WHERE deleted_at IS NULL;

CREATE TABLE IF NOT EXISTS {schema}.inbox (
    item_id      TEXT PRIMARY KEY,
    workflow_id  TEXT NOT NULL DEFAULT '',
    category     TEXT NOT NULL,
    title        TEXT NOT NULL,
    body         TEXT NOT NULL DEFAULT '',
    payload      JSONB,
    ts           TIMESTAMPTZ NOT NULL DEFAULT now(),
    read         BOOLEAN NOT NULL DEFAULT false,
    archived     BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_inbox_ts ON {schema}.inbox (ts DESC);

-- Per-run event timeline. Intermediate bus events are buffered in memory
-- while a run is in-flight, then async-flushed here on completion so an
-- archived run's full timeline survives buffer eviction + restart.
CREATE TABLE IF NOT EXISTS {schema}.run_events (
    run_id      TEXT NOT NULL,
    seq         INT  NOT NULL,
    workflow_id TEXT NOT NULL DEFAULT '',
    topic       TEXT NOT NULL DEFAULT '',
    envelope    JSONB NOT NULL,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (run_id, seq)
);
CREATE INDEX IF NOT EXISTS idx_run_events_run ON {schema}.run_events (run_id);
"""


def _dumps(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, default=str)


def _loads(val: Any) -> Any:
    if val is None:
        return None
    if isinstance(val, (dict, list)):
        return val
    return json.loads(val)


class PostgresControlPlane:
    """Postgres-backed control-plane metadata store."""

    enabled = True

    def __init__(
        self,
        *,
        dsn: str | None = None,
        pool: "asyncpg.Pool | None" = None,
        schema: str = "public",
        min_size: int = 1,
        max_size: int = 5,
    ) -> None:
        if dsn is None and pool is None:
            raise ValueError("Provide either dsn or pool")
        self._dsn = dsn
        self._pool = pool
        self._schema = schema
        self._min_size = min_size
        self._max_size = max_size
        self._own_pool = pool is None

    async def start(self) -> None:
        import asyncpg  # noqa: PLC0415
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn, min_size=self._min_size, max_size=self._max_size,
            )
        async with self._pool.acquire() as conn:
            await conn.execute(_MIGRATION.format(schema=self._schema))
        log.info("controlplane.pg.started", schema=self._schema)

    async def stop(self) -> None:
        if self._own_pool and self._pool is not None:
            await self._pool.close()
            self._pool = None

    # ---- load (boot) ----

    async def load_projects(self) -> list[dict[str, Any]]:
        sql = f"SELECT project_id, name, description, created_at FROM {self._schema}.projects"
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql)
        return [dict(r) for r in rows]

    async def load_workflows(self) -> list[dict[str, Any]]:
        sql = (
            f"SELECT workflow_id, project_id, description, raw_spec, agent_kwargs, "
            f"mode, start_input_fields FROM {self._schema}.workflows "
            f"WHERE deleted_at IS NULL"
        )
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql)
        out = []
        for r in rows:
            d = dict(r)
            d["raw_spec"] = _loads(d["raw_spec"])
            d["agent_kwargs"] = _loads(d.get("agent_kwargs")) or {}
            d["start_input_fields"] = _loads(d["start_input_fields"]) or ["q"]
            out.append(d)
        return out

    async def load_inbox(self, *, limit: int = 500) -> list[dict[str, Any]]:
        sql = (
            f"SELECT item_id, workflow_id, category, title, body, payload, ts, "
            f"read, archived FROM {self._schema}.inbox "
            f"ORDER BY ts DESC LIMIT $1"
        )
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql, max(0, limit))
        out = []
        for r in rows:
            d = dict(r)
            d["payload"] = _loads(d["payload"])
            out.append(d)
        return out

    # ---- projects ----

    async def upsert_project(self, project: dict[str, Any]) -> None:
        sql = f"""
            INSERT INTO {self._schema}.projects (project_id, name, description)
            VALUES ($1, $2, $3)
            ON CONFLICT (project_id) DO UPDATE
              SET name = EXCLUDED.name,
                  description = EXCLUDED.description,
                  updated_at = now()
        """
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                sql, project["id"], project["name"], project.get("description", ""),
            )

    async def delete_project(self, project_id: str) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                f"DELETE FROM {self._schema}.projects WHERE project_id = $1", project_id,
            )

    # ---- workflows ----

    async def upsert_workflow(self, workflow: dict[str, Any]) -> None:
        sql = f"""
            INSERT INTO {self._schema}.workflows
              (workflow_id, project_id, description, raw_spec, agent_kwargs,
               mode, start_input_fields)
            VALUES ($1, $2, $3, $4, $5, $6, $7)
            ON CONFLICT (workflow_id) DO UPDATE
              SET project_id = EXCLUDED.project_id,
                  description = EXCLUDED.description,
                  raw_spec = EXCLUDED.raw_spec,
                  agent_kwargs = EXCLUDED.agent_kwargs,
                  mode = EXCLUDED.mode,
                  start_input_fields = EXCLUDED.start_input_fields,
                  updated_at = now(),
                  -- Re-deploying a previously-deleted id resurrects it.
                  deleted_at = NULL
        """
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                sql,
                workflow["workflow_id"],
                workflow.get("project_id", "default"),
                workflow.get("description", ""),
                _dumps(workflow["raw_spec"]),
                _dumps(workflow.get("agent_kwargs", {})),
                workflow.get("mode", "normal"),
                _dumps(workflow.get("start_input_fields", ["q"])),
            )

    async def delete_workflow(self, workflow_id: str) -> None:
        """Soft delete: tombstone the row so run history keeps its FK target
        and the deletion stays auditable / recoverable. Idempotent — only
        stamps ``deleted_at`` on rows that are still live."""
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                f"UPDATE {self._schema}.workflows "
                f"SET deleted_at = now(), updated_at = now() "
                f"WHERE workflow_id = $1 AND deleted_at IS NULL",
                workflow_id,
            )

    # ---- inbox ----

    async def upsert_inbox(self, item: dict[str, Any]) -> None:
        sql = f"""
            INSERT INTO {self._schema}.inbox
              (item_id, workflow_id, category, title, body, payload, read, archived)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            ON CONFLICT (item_id) DO UPDATE
              SET read = EXCLUDED.read, archived = EXCLUDED.archived
        """
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                sql, item["id"], item.get("workflow_id", ""), item["category"],
                item["title"], item.get("body", ""),
                _dumps(item.get("payload")) if item.get("payload") is not None else None,
                bool(item.get("read", False)), bool(item.get("archived", False)),
            )

    async def update_inbox(
        self, item_id: str, *, read: bool | None = None, archived: bool | None = None,
    ) -> None:
        sets, args = [], []
        if read is not None:
            args.append(read); sets.append(f"read = ${len(args)}")
        if archived is not None:
            args.append(archived); sets.append(f"archived = ${len(args)}")
        if not sets:
            return
        args.append(item_id)
        sql = f"UPDATE {self._schema}.inbox SET {', '.join(sets)} WHERE item_id = ${len(args)}"
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(sql, *args)

    async def mark_all_inbox_read(self, *, workflow_id: str | None = None) -> None:
        if workflow_id:
            sql = f"UPDATE {self._schema}.inbox SET read = true WHERE workflow_id = $1"
            args = (workflow_id,)
        else:
            sql = f"UPDATE {self._schema}.inbox SET read = true"
            args = ()
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(sql, *args)

    async def delete_inbox(self, item_id: str) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                f"DELETE FROM {self._schema}.inbox WHERE item_id = $1", item_id,
            )

    async def delete_inbox_for_workflow(self, workflow_id: str) -> None:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.execute(
                f"DELETE FROM {self._schema}.inbox WHERE workflow_id = $1", workflow_id,
            )

    # ---- run events (timeline) ----
    async def persist_run_events(
        self, run_id: str, workflow_id: str, envelopes: list[dict[str, Any]],
    ) -> None:
        if not envelopes:
            return
        rows = [
            (run_id, i, workflow_id, str(e.get("topic", "")), _dumps(e))
            for i, e in enumerate(envelopes)
        ]
        sql = (
            f"INSERT INTO {self._schema}.run_events "
            f"(run_id, seq, workflow_id, topic, envelope) "
            f"VALUES ($1, $2, $3, $4, $5) "
            f"ON CONFLICT (run_id, seq) DO NOTHING"
        )
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            await conn.executemany(sql, rows)

    async def load_run_events(
        self, run_id: str, *, limit: int = 2000,
    ) -> list[dict[str, Any]]:
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(
                f"SELECT envelope FROM {self._schema}.run_events "
                f"WHERE run_id = $1 ORDER BY seq LIMIT $2",
                run_id, limit,
            )
        return [_loads(r["envelope"]) for r in rows]


# ============================================================
# Factory
# ============================================================


def build_persistence(store_kind: str) -> ControlPlanePersistence:
    """Return the control-plane persistence for the resolved ``--store``.

    ``pg`` / ``postgres`` → :class:`PostgresControlPlane` (same DSN as the
    run store); anything else → :class:`NoOpControlPlane`.
    """
    k = (store_kind or "memory").strip().lower()
    if k in ("pg", "postgres"):
        from agentkit.orchestrator.store_postgres import connect_from_env  # noqa: PLC0415
        return PostgresControlPlane(dsn=connect_from_env())
    return NoOpControlPlane()
