"""Audit log façade — Doc10 §6.

Audit records flow from Notifier (delivery), Orchestrator (Run
finalization), Guardrail (quota touch), Compiler (IR deploys), and
Collab (ACL changes) into a single :class:`AuditWriter`.

Phase 1 ships the in-memory implementation that holds entries in a
deque (cap-bounded). The PG backend implementing the same Protocol
plugs in transparently in Phase 2 — no caller changes needed.

The audit shape is deliberately *generic* — not specific to any
table — so the same writer can serve every module's audit feed.
The producer fills ``kind`` (e.g. ``notifier.delivery``,
``run.terminate``, ``guardrail.consume``) and a free-form ``data``
dict; the writer (or downstream PG schema) is responsible for
projecting the dict into table columns.
"""

from __future__ import annotations

from collections import deque
from datetime import datetime
from typing import Any, Protocol, runtime_checkable

from pydantic import BaseModel, ConfigDict, Field

from agentkit.common.ids import prefixed_id
from agentkit.common.time import utcnow


# ============================================================
# Data model
# ============================================================


class AuditEntry(BaseModel):
    """One free-form audit record.

    Designed to land cleanly in any of the ``*_audit`` tables in
    Doc10 §6.1 by selecting a different ``kind`` and a
    correspondingly-shaped ``data``.
    """

    model_config = ConfigDict(extra="forbid")

    audit_id: str = Field(default_factory=lambda: prefixed_id("aud"))
    at: datetime = Field(default_factory=utcnow)

    # Coarse category — one per source module / event.
    kind: str

    # Correlation IDs (free-form — at least one usually populated).
    trace_id: str | None = None
    run_id: str | None = None
    workflow_id: str | None = None
    agent_id: str | None = None
    actor: str | None = None  # the user / system that initiated

    # Free-form per-kind detail.
    data: dict[str, Any] = Field(default_factory=dict)


# ============================================================
# Writer Protocol
# ============================================================


@runtime_checkable
class AuditWriter(Protocol):
    """Minimal contract for any audit backend (memory / PG / OS file)."""

    async def write(self, entry: AuditEntry) -> None: ...
    async def flush(self) -> None: ...
    async def close(self) -> None: ...


# ============================================================
# In-memory implementation
# ============================================================


class InMemoryAuditWriter:
    """Append-only deque, bounded so test runs / dev REPLs don't OOM.

    Useful for unit tests + dev REPLs. Production deployments swap
    in a Postgres-backed writer that satisfies the same Protocol.
    """

    def __init__(self, *, max_entries: int = 10_000) -> None:
        self._entries: deque[AuditEntry] = deque(maxlen=max_entries)

    async def write(self, entry: AuditEntry) -> None:
        self._entries.append(entry)

    async def flush(self) -> None:
        return None

    async def close(self) -> None:
        return None

    # ---- inspection helpers ----

    def __len__(self) -> int:
        return len(self._entries)

    def all(self) -> list[AuditEntry]:
        return list(self._entries)

    def filter(
        self,
        *,
        kind: str | None = None,
        run_id: str | None = None,
        actor: str | None = None,
    ) -> list[AuditEntry]:
        out = list(self._entries)
        if kind is not None:
            out = [e for e in out if e.kind == kind]
        if run_id is not None:
            out = [e for e in out if e.run_id == run_id]
        if actor is not None:
            out = [e for e in out if e.actor == actor]
        return out

    def clear(self) -> None:
        self._entries.clear()


# Static check — keep us honest if Protocol changes.
_: type[AuditWriter] = InMemoryAuditWriter  # type: ignore[assignment, misc]
