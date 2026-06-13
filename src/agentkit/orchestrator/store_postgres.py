"""PostgreSQL-backed RunStore using asyncpg connection pool.

Drop-in replacement for :class:`InMemoryRunStore`; satisfies the
:class:`RunStore` Protocol.  Auto-migrates the ``runs`` table on
:meth:`start`.
"""

from __future__ import annotations

import os
from typing import Any

import asyncpg
import orjson

from agentkit.orchestrator.errors import RunNotFound
from agentkit.orchestrator.run import TERMINAL_STATUSES, Run

# ── DSN helpers ──────────────────────────────────────────────────────────────

def connect_from_env() -> str:
    """Build a DSN from env vars, falling back to docker-compose defaults."""
    dsn = os.environ.get("AGENTKIT_PG_DSN")
    if dsn:
        return dsn
    host = os.environ.get("AGENTKIT_PG_HOST", "localhost")
    port = os.environ.get("AGENTKIT_PG_PORT", "5432")
    user = os.environ.get("AGENTKIT_PG_USER", "agentkit")
    password = os.environ.get("AGENTKIT_PG_PASSWORD", "agentkit")
    db = os.environ.get("AGENTKIT_PG_DB", "agentkit")
    return f"postgresql://{user}:{password}@{host}:{port}/{db}"


# ── Serialization helpers ─────────────────────────────────────────────────────

def _dumps(obj: Any) -> str:
    """Serialize a Python object to a JSON string for asyncpg JSONB params."""
    return orjson.dumps(obj).decode()


def _loads(raw: str | bytes | None) -> Any:
    """Deserialize a JSON string returned by asyncpg for JSONB columns."""
    if raw is None:
        return None
    if isinstance(raw, (str, bytes)):
        return orjson.loads(raw)
    return raw  # already a dict/list if a codec happened to be registered


# ── Migration DDL ─────────────────────────────────────────────────────────────

_MIGRATION_TEMPLATE = """\
CREATE SCHEMA IF NOT EXISTS {schema};

CREATE TABLE IF NOT EXISTS {schema}.runs (
    run_id            TEXT PRIMARY KEY,
    workflow_id       TEXT NOT NULL,
    workflow_version  INT  NOT NULL DEFAULT 1,
    trace_id          TEXT NOT NULL,
    status            TEXT NOT NULL,
    trigger           TEXT NOT NULL DEFAULT 'api',
    started_at        TIMESTAMPTZ NOT NULL,
    ended_at          TIMESTAMPTZ,
    failure_reason    TEXT,
    input             JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    output            JSONB,
    cursor            JSONB NOT NULL DEFAULT '{{}}'::jsonb,
    workflow_snapshot JSONB,
    created_at        TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at        TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Forward-migrate older `runs` tables created before the snapshot column.
ALTER TABLE {schema}.runs ADD COLUMN IF NOT EXISTS workflow_snapshot JSONB;

CREATE INDEX IF NOT EXISTS idx_runs_workflow_id
    ON {schema}.runs (workflow_id);

CREATE INDEX IF NOT EXISTS idx_runs_status
    ON {schema}.runs (status);

CREATE INDEX IF NOT EXISTS idx_runs_active
    ON {schema}.runs (run_id)
    WHERE status NOT IN ('Succeeded', 'Failed', 'Cancelled', 'Archived');
"""

# Pre-compute the tuple of terminal status string values once.
_TERMINAL_VALUES: tuple[str, ...] = tuple(s.value for s in TERMINAL_STATUSES)


# ── Row → Run ─────────────────────────────────────────────────────────────────

def _row_to_run(row: asyncpg.Record) -> Run:
    """Convert an asyncpg Record to a Run model instance."""
    data: dict[str, Any] = dict(row)

    # JSONB columns are returned as strings by asyncpg (no codec set).
    data["input"] = _loads(data["input"]) or {}
    data["output"] = _loads(data["output"])
    data["cursor"] = _loads(data["cursor"]) or {}
    data["workflow_snapshot"] = _loads(data.get("workflow_snapshot"))

    # Strip DB-only book-keeping columns not present in Run.
    data.pop("created_at", None)
    data.pop("updated_at", None)

    # Pydantic v2 handles StrEnum coercion and nested model construction.
    return Run.model_validate(data)


# ── PostgresRunStore ──────────────────────────────────────────────────────────

class PostgresRunStore:
    """Postgres-backed RunStore using asyncpg connection pool.

    Usage::

        store = PostgresRunStore(dsn=connect_from_env())
        await store.start()   # creates pool + migrates schema
        ...
        await store.stop()    # closes pool (if we created it)
    """

    def __init__(
        self,
        *,
        dsn: str | None = None,
        pool: asyncpg.Pool | None = None,
        schema: str = "public",
        min_size: int = 2,
        max_size: int = 10,
    ) -> None:
        if dsn is None and pool is None:
            raise ValueError("Provide either dsn or pool")
        self._dsn = dsn
        self._pool: asyncpg.Pool | None = pool
        self._schema = schema
        self._min_size = min_size
        self._max_size = max_size
        self._own_pool: bool = pool is None  # True when we create the pool

    # ── lifecycle ──

    async def start(self) -> None:
        """Create connection pool (if dsn-based) and run DDL migration."""
        if self._pool is None:
            self._pool = await asyncpg.create_pool(
                self._dsn,
                min_size=self._min_size,
                max_size=self._max_size,
            )
        ddl = _MIGRATION_TEMPLATE.format(schema=self._schema)
        async with self._pool.acquire() as conn:
            await conn.execute(ddl)

    async def stop(self) -> None:
        """Close pool if we own it."""
        if self._own_pool and self._pool is not None:
            await self._pool.close()
            self._pool = None

    # ── RunStore Protocol ──

    async def put(self, run: Run) -> None:
        """Insert a new Run row; raises RuntimeError if run_id already exists."""
        sql = f"""
            INSERT INTO {self._schema}.runs (
                run_id, workflow_id, workflow_version, trace_id,
                status, trigger, started_at, ended_at,
                failure_reason, input, output, cursor, workflow_snapshot
            ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13)
        """
        try:
            async with self._pool.acquire() as conn:  # type: ignore[union-attr]
                await conn.execute(
                    sql,
                    run.run_id,
                    run.workflow_id,
                    run.workflow_version,
                    run.trace_id,
                    run.status.value,
                    run.trigger,
                    run.started_at,
                    run.ended_at,
                    run.failure_reason,
                    _dumps(run.input),
                    _dumps(run.output) if run.output is not None else None,
                    _dumps(run.cursor.model_dump(mode="json")),
                    _dumps(run.workflow_snapshot) if run.workflow_snapshot is not None else None,
                )
        except asyncpg.UniqueViolationError:
            raise RuntimeError("run already exists")

    async def get(self, run_id: str) -> Run:
        """Fetch a Run by run_id; raises RunNotFound if absent."""
        sql = f"SELECT * FROM {self._schema}.runs WHERE run_id = $1"
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            row = await conn.fetchrow(sql, run_id)
        if row is None:
            raise RunNotFound(run_id)
        return _row_to_run(row)

    async def update(self, run: Run) -> None:
        """Update an existing Run row; raises RunNotFound if absent."""
        sql = f"""
            UPDATE {self._schema}.runs SET
                workflow_id      = $2,
                workflow_version = $3,
                trace_id         = $4,
                status           = $5,
                trigger          = $6,
                started_at       = $7,
                ended_at         = $8,
                failure_reason   = $9,
                input            = $10,
                output           = $11,
                cursor           = $12,
                updated_at       = now()
            WHERE run_id = $1
        """
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            result = await conn.execute(
                sql,
                run.run_id,
                run.workflow_id,
                run.workflow_version,
                run.trace_id,
                run.status.value,
                run.trigger,
                run.started_at,
                run.ended_at,
                run.failure_reason,
                _dumps(run.input),
                _dumps(run.output) if run.output is not None else None,
                _dumps(run.cursor.model_dump(mode="json")),
            )
        # asyncpg returns "UPDATE N" — zero rows means the run_id was absent.
        if result == "UPDATE 0":
            raise RunNotFound(run.run_id)

    async def list_active(self) -> list[Run]:
        """Return all Runs whose status is not terminal."""
        placeholders = ", ".join(f"${i + 1}" for i in range(len(_TERMINAL_VALUES)))
        sql = f"""
            SELECT * FROM {self._schema}.runs
            WHERE status NOT IN ({placeholders})
        """
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql, *_TERMINAL_VALUES)
        return [_row_to_run(row) for row in rows]

    async def list_recent(
        self,
        *,
        workflow_id: str | None = None,
        status: str | None = None,
        limit: int = 50,
    ) -> list[Run]:
        """Most-recent-first runs (terminal + active), filtered + limited.

        Pushes filtering/ordering/limit into SQL so the UI's run list
        works against Postgres without loading the whole table.
        """
        conds: list[str] = []
        args: list[object] = []
        if workflow_id:
            args.append(workflow_id)
            conds.append(f"workflow_id = ${len(args)}")
        if status:
            args.append(status)
            conds.append(f"status = ${len(args)}")
        where = f"WHERE {' AND '.join(conds)}" if conds else ""
        args.append(max(0, limit))
        sql = f"""
            SELECT * FROM {self._schema}.runs
            {where}
            ORDER BY started_at DESC NULLS LAST
            LIMIT ${len(args)}
        """
        async with self._pool.acquire() as conn:  # type: ignore[union-attr]
            rows = await conn.fetch(sql, *args)
        return [_row_to_run(row) for row in rows]
