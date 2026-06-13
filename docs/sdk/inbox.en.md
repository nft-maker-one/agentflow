# Message Notifications — Inbox

Inbox is Agentflow's global notification system. When a workflow completes, an External I/O interaction occurs, or a run encounters an error, the system automatically pushes a notification item. Users can access these via the 📬 icon in the Web UI or programmatically via the SDK's `AgentKitClient`.

---

## Notification Categories

| Category | Trigger Condition | Icon |
| --- | --- | --- |
| `run_succeeded` | Run terminal state = Succeeded | ✓ Green |
| `run_failed` | Run terminal state = Failed | ✗ Red |
| `run_cancelled` | Run terminal state = Cancelled | ⊘ Grey |
| `ext_source` | External Source received a message | 🔌 Purple |
| `ext_sink_ok` | External Sink delivery successful | 📤 Green |
| `ext_sink_error` | External Sink delivery failed | 📤 Red |
| `error` | Agent handler execution failure | ⚠ Amber |

---

## Architecture

```text
EventBus (tap loop)
    │
    ├── system.run.<id>.completed  →  run_succeeded / run_failed / run_cancelled
    ├── system.ext.source.<name>.received  →  ext_source
    ├── system.ext.sink.<name>.delivered   →  ext_sink_ok / ext_sink_error
    └── system.run.<id>.error             →  error
    │
    ▼
AppState.inbox (ring-buffer, cap=500)
    │
    ▼
GET /api/inbox  ←  UI polls every 3s / SDK client.list_inbox()

```

---

## SDK Usage

```python
from agentkit import AgentKitClient

async with AgentKitClient("http://localhost:8080") as c:
    # ── Querying ──
    inbox = await c.list_inbox()
    print(f"Total: {inbox['total']}, Unread: {inbox['unread']}")
    for item in inbox["items"]:
        print(f"  [{item['category']}] {item['title']} — {item['body']}")

    # ── Filter by workflow ──
    filtered = await c.list_inbox(workflow_id="wf_chatbot")

    # ── Show unread only ──
    unread = await c.list_inbox(unread_only=True)

    # ── Include archived ──
    all_items = await c.list_inbox(include_archived=True)

    # ── Mark as read ──
    await c.inbox_mark_read(item["id"])

    # ── Mark all read ──
    result = await c.inbox_mark_all_read(workflow_id="wf_chatbot")
    print(f"Marked {result['marked']} items")

    # ── Archive ──
    await c.inbox_archive(item["id"])

    # ── Permanently delete ──
    await c.inbox_delete(item["id"])

    # ── Batch clear ──
    result = await c.inbox_clear(workflow_id="wf_chatbot", archived_only=True)
    print(f"Cleared {result['removed']} items")

```

---

## InboxItem Structure

```json
{
  "id": "inb_a3f8c1d2e4b5",
  "workflow_id": "wf_chatbot",
  "category": "ext_source",
  "title": "🔌 tg_in received",
  "body": "q=hello",
  "payload": {
    "source_name": "tg_in",
    "publish_topic": "ext.tg.in",
    "kind": "telegram",
    "preview": "q=hello",
    "fields": ["q"]
  },
  "ts": "2026-06-03T08:30:00.123456+00:00",
  "read": false,
  "archived": false
}

```

---

## REST API Endpoints

| Method | Path | Description |
| --- | --- | --- |
| GET | `/api/inbox` | List notifications (supports `?workflow_id` / `?unread_only` / `?include_archived` / `?limit`) |
| GET | `/api/inbox/unread-count` | Count of unread items |
| POST | `/api/inbox/{id}/read` | Mark a single item as read |
| POST | `/api/inbox/read-all` | Batch mark as read |
| POST | `/api/inbox/{id}/archive` | Archive an item |
| DELETE | `/api/inbox/{id}` | Delete an item |
| POST | `/api/inbox/clear` | Batch clear |

---

## Web UI Behavior

1. **Header 📬 Icon** — Always visible; a red badge displays the unread count (maxes out at `99+`).
2. **Notification Chime** — Plays a synthesized Web Audio chime (E5 $\rightarrow$ A5 dual-tone ~150ms) when the unread count increases.
3. **Right Drawer** — Clicking 📬 slides out a drawer:
* Top: Workflow filter dropdown + archived toggle switch.
* Per item: Hovering reveals 📁 Archive / ✕ Delete actions.
* Click item: Navigates to the corresponding workflow details page + marks as read.
* Bottom: "Mark all read" / "Clear archived" buttons.



---

## Capacity and Eviction Strategy

* Default capacity is `MAX_INBOX_ITEMS = 500`.
* When exceeded, the oldest items are evicted via FIFO (`deque(maxlen=500)` synchronized with an index dict).
* Stores data purely in-memory (lost on restart) — intended for development / demonstration environments.
* For production environments, it is recommended to connect a persistent store (by extending the `Inbox` base class).

---

## Relationship with External I/O

Inbox items categorized as `ext_source` / `ext_sink_ok` / `ext_sink_error` **carry the real `workflow_id**` (instead of just `"external"`). Consequently, filtering by workflow allows you to accurately view all external events belonging to that specific workflow.

```python
# View only ext events for wf_chatbot
inbox = await c.list_inbox(workflow_id="wf_chatbot")
ext_events = [i for i in inbox["items"] if i["category"].startswith("ext_")]

```

---

## Custom Push (Advanced)

If you need to push to the Inbox from custom code:

```python
# Inside server-side code (e.g., custom middleware / handler)
state = request.app.state.app_state
state.inbox.push(
    workflow_id="my_workflow",
    category="error",
    title="Custom Alert",
    body="A metric exceeded the threshold",
    payload={"metric": "cpu", "value": 95},
)

```