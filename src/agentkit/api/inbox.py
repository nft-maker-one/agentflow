"""In-memory Inbox store — surfaces workflow lifecycle events to the UI.

The inbox is a small ring buffer of structured notification items derived
from three event sources:

* **Run termination** (Succeeded / Failed / Cancelled) — pushed by the
  Orchestrator's terminal callback hook.
* **External I/O** (source received / sink delivered / sink error) —
  pushed by the ``ExternalIOManager`` whenever an adapter emits or
  consumes an envelope.
* **Other failures** (compile, deploy, redeploy) — pushed by the
  workflow-edit router.

The frontend polls ``GET /api/inbox`` every few seconds (no SSE — keeps
the wiring simple and the bus uncluttered with another tap subscriber).

Capped at ``MAX_INBOX_ITEMS`` to bound memory; oldest entries are
silently evicted.  Entries can be marked read, archived, or deleted.
"""

from __future__ import annotations

import uuid
from collections import deque
from dataclasses import dataclass, field
from datetime import datetime, timezone
from typing import Any, Iterable, Literal

from agentkit.common.logging import get_logger

log = get_logger(__name__)

#: Hard cap on stored items. Older ones are evicted FIFO.
MAX_INBOX_ITEMS: int = 500

# Categories surfaced to the UI.  The order also drives default
# colour / icon picking on the frontend.
InboxCategory = Literal[
    "run_succeeded",
    "run_failed",
    "run_cancelled",
    "ext_source",
    "ext_sink_ok",
    "ext_sink_error",
    "error",
]


@dataclass
class InboxItem:
    """One notification entry."""

    id: str
    workflow_id: str
    category: InboxCategory
    title: str
    body: str
    payload: dict[str, Any] | None = None
    ts: datetime = field(default_factory=lambda: datetime.now(timezone.utc))
    read: bool = False
    archived: bool = False

    def to_dict(self) -> dict[str, Any]:
        return {
            "id": self.id,
            "workflow_id": self.workflow_id,
            "category": self.category,
            "title": self.title,
            "body": self.body,
            "payload": self.payload,
            "ts": self.ts.isoformat(),
            "read": self.read,
            "archived": self.archived,
        }


class Inbox:
    """In-memory store + simple query helpers."""

    def __init__(self, *, capacity: int = MAX_INBOX_ITEMS) -> None:
        # Newest at the LEFT (deque appendleft) so iteration is
        # newest-first by default. Capped to ``capacity``.
        self._items: deque[InboxItem] = deque(maxlen=capacity)
        self._index: dict[str, InboxItem] = {}

    # ------------------------------------------------------------------
    # Mutation
    # ------------------------------------------------------------------

    def push(
        self,
        *,
        workflow_id: str,
        category: InboxCategory,
        title: str,
        body: str = "",
        payload: dict[str, Any] | None = None,
    ) -> InboxItem:
        """Append a new item.  Returns it."""
        item = InboxItem(
            id=f"inb_{uuid.uuid4().hex[:12]}",
            workflow_id=workflow_id,
            category=category,
            title=title,
            body=body,
            payload=payload,
        )
        # Evict oldest from index when deque overflows.
        if len(self._items) >= self._items.maxlen:  # type: ignore[arg-type]
            evicted = self._items[-1]  # the right-most (oldest) entry
            self._index.pop(evicted.id, None)
        self._items.appendleft(item)
        self._index[item.id] = item
        log.debug(
            "inbox.push",
            workflow_id=workflow_id, category=category, title=title,
        )
        return item

    def restore_many(self, items: list[dict[str, Any]]) -> None:
        """Rebuild from persisted rows (newest-first) on boot, without
        re-persisting. Newest ends up leftmost (matching ``push``)."""
        for d in reversed(items):  # oldest first → appendleft → newest leftmost
            item = InboxItem(
                id=d.get("item_id") or d.get("id"),
                workflow_id=d.get("workflow_id", ""),
                category=d["category"],
                title=d["title"],
                body=d.get("body", ""),
                payload=d.get("payload"),
                ts=d.get("ts") or datetime.now(timezone.utc),
                read=bool(d.get("read", False)),
                archived=bool(d.get("archived", False)),
            )
            if len(self._items) >= self._items.maxlen:  # type: ignore[arg-type]
                self._index.pop(self._items[-1].id, None)
            self._items.appendleft(item)
            self._index[item.id] = item

    def remove_for_workflow(self, workflow_id: str) -> int:
        """Drop all items belonging to ``workflow_id`` (e.g. on workflow
        delete, so notifications don't dangle to a gone workflow)."""
        keep = [it for it in self._items if it.workflow_id != workflow_id]
        removed = len(self._items) - len(keep)
        if removed:
            self._items.clear()
            self._items.extend(keep)
            self._index = {it.id: it for it in self._items}
        return removed

    def mark_read(self, item_id: str) -> bool:
        item = self._index.get(item_id)
        if item is None or item.read:
            return False
        item.read = True
        return True

    def mark_all_read(self, *, workflow_id: str | None = None) -> int:
        n = 0
        for item in self._items:
            if item.read:
                continue
            if workflow_id and item.workflow_id != workflow_id:
                continue
            item.read = True
            n += 1
        return n

    def archive(self, item_id: str) -> bool:
        item = self._index.get(item_id)
        if item is None:
            return False
        item.archived = True
        item.read = True
        return True

    def delete(self, item_id: str) -> bool:
        item = self._index.pop(item_id, None)
        if item is None:
            return False
        try:
            self._items.remove(item)
        except ValueError:
            pass
        return True

    def clear(
        self,
        *,
        workflow_id: str | None = None,
        archived_only: bool = False,
    ) -> int:
        """Bulk delete.  Returns count removed."""
        survivors = []
        removed = 0
        for it in self._items:
            keep = True
            if archived_only and not it.archived:
                pass  # keep
            elif workflow_id and it.workflow_id != workflow_id:
                pass  # keep
            elif archived_only and it.archived:
                keep = False
            elif not archived_only and (
                not workflow_id or it.workflow_id == workflow_id
            ):
                keep = False
            if keep:
                survivors.append(it)
            else:
                removed += 1
                self._index.pop(it.id, None)
        self._items.clear()
        self._items.extend(survivors)
        return removed

    # ------------------------------------------------------------------
    # Query
    # ------------------------------------------------------------------

    def list_items(
        self,
        *,
        workflow_id: str | None = None,
        include_archived: bool = False,
        unread_only: bool = False,
        limit: int = 200,
    ) -> Iterable[InboxItem]:
        out: list[InboxItem] = []
        for it in self._items:
            if not include_archived and it.archived:
                continue
            if unread_only and it.read:
                continue
            if workflow_id and it.workflow_id != workflow_id:
                continue
            out.append(it)
            if len(out) >= limit:
                break
        return out

    def unread_count(self, *, workflow_id: str | None = None) -> int:
        return sum(
            1 for it in self._items
            if not it.read and not it.archived
            and (not workflow_id or it.workflow_id == workflow_id)
        )

    def total_count(self) -> int:
        return len(self._items)
