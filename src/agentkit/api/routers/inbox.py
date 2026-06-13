"""Inbox notification API.

* ``GET    /api/inbox``                       — list items (filterable)
* ``GET    /api/inbox/unread-count``           — total unread count + per-workflow
* ``POST   /api/inbox/{id}/read``              — mark one as read
* ``POST   /api/inbox/read-all``               — bulk mark-read
* ``POST   /api/inbox/{id}/archive``           — archive (kept but hidden from default)
* ``DELETE /api/inbox/{id}``                   — permanently remove
* ``POST   /api/inbox/clear``                  — bulk delete (filter optional)

Polled by the frontend every 3-5s (no SSE — keeps the bus uncluttered).
"""

from __future__ import annotations

from typing import Any

from fastapi import APIRouter, HTTPException, Query, Request

from agentkit.common.logging import get_logger

log = get_logger(__name__)

router = APIRouter(tags=["inbox"])


@router.get("/inbox")
async def list_inbox(
    req: Request,
    workflow_id: str | None = Query(default=None),
    include_archived: bool = Query(default=False),
    unread_only: bool = Query(default=False),
    limit: int = Query(default=200, ge=1, le=1000),
):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    items = state.inbox.list_items(
        workflow_id=workflow_id,
        include_archived=include_archived,
        unread_only=unread_only,
        limit=limit,
    )
    return {
        "items": [it.to_dict() for it in items],
        "total": state.inbox.total_count(),
        "unread": state.inbox.unread_count(),
    }


@router.get("/inbox/unread-count")
async def unread_count(
    req: Request,
    workflow_id: str | None = Query(default=None),
) -> dict[str, Any]:
    state = req.app.state.app_state
    return {"unread": state.inbox.unread_count(workflow_id=workflow_id)}


@router.post("/inbox/{item_id}/read", status_code=204)
async def mark_read(item_id: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    if not state.inbox.mark_read(item_id):
        raise HTTPException(status_code=404, detail="inbox item not found")
    if getattr(state.persistence, "enabled", False):
        await state.persistence.update_inbox(item_id, read=True)


@router.post("/inbox/read-all")
async def mark_all_read(
    req: Request,
    workflow_id: str | None = Query(default=None),
) -> dict[str, int]:
    state = req.app.state.app_state
    n = state.inbox.mark_all_read(workflow_id=workflow_id)
    if getattr(state.persistence, "enabled", False):
        await state.persistence.mark_all_inbox_read(workflow_id=workflow_id)
    return {"marked": n}


@router.post("/inbox/{item_id}/archive", status_code=204)
async def archive(item_id: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    if not state.inbox.archive(item_id):
        raise HTTPException(status_code=404, detail="inbox item not found")
    if getattr(state.persistence, "enabled", False):
        await state.persistence.update_inbox(item_id, archived=True)


@router.delete("/inbox/{item_id}", status_code=204)
async def delete(item_id: str, req: Request):  # type: ignore[no-untyped-def]
    state = req.app.state.app_state
    if not state.inbox.delete(item_id):
        raise HTTPException(status_code=404, detail="inbox item not found")
    if getattr(state.persistence, "enabled", False):
        await state.persistence.delete_inbox(item_id)


@router.post("/inbox/clear")
async def clear(
    req: Request,
    workflow_id: str | None = Query(default=None),
    archived_only: bool = Query(default=False),
) -> dict[str, int]:
    state = req.app.state.app_state
    n = state.inbox.clear(
        workflow_id=workflow_id, archived_only=archived_only,
    )
    return {"removed": n}
