# 消息通知 — Inbox

Inbox 是 Agentflow 的全局通知系统。当 workflow 完成、External I/O 交互、或运行出错时，系统自动 push 通知条目。用户通过 Web UI 的 📬 图标或 SDK `AgentKitClient` 访问。

---

## 通知类别

| Category | 触发条件 | 图标 |
|----------|---------|------|
| `run_succeeded` | Run 终态 = Succeeded | ✓ 绿色 |
| `run_failed` | Run 终态 = Failed | ✗ 红色 |
| `run_cancelled` | Run 终态 = Cancelled | ⊘ 灰色 |
| `ext_source` | External Source 收到消息 | 🔌 紫色 |
| `ext_sink_ok` | External Sink 发送成功 | 📤 绿色 |
| `ext_sink_error` | External Sink 发送失败 | 📤 红色 |
| `error` | Agent handler 执行失败 | ⚠ 琥珀色 |

---

## 架构

```
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
GET /api/inbox  ←  UI 每 3s 轮询 / SDK client.list_inbox()
```

---

## SDK 用法

```python
from agentkit import AgentKitClient

async with AgentKitClient("http://localhost:8080") as c:
    # ── 查询 ──
    inbox = await c.list_inbox()
    print(f"总数: {inbox['total']}, 未读: {inbox['unread']}")
    for item in inbox["items"]:
        print(f"  [{item['category']}] {item['title']} — {item['body']}")

    # ── 按 workflow 过滤 ──
    filtered = await c.list_inbox(workflow_id="wf_chatbot")

    # ── 只看未读 ──
    unread = await c.list_inbox(unread_only=True)

    # ── 包含已归档 ──
    all_items = await c.list_inbox(include_archived=True)

    # ── 标为已读 ──
    await c.inbox_mark_read(item["id"])

    # ── 全部已读 ──
    result = await c.inbox_mark_all_read(workflow_id="wf_chatbot")
    print(f"已标记 {result['marked']} 条")

    # ── 归档 ──
    await c.inbox_archive(item["id"])

    # ── 永久删除 ──
    await c.inbox_delete(item["id"])

    # ── 批量清理 ──
    result = await c.inbox_clear(workflow_id="wf_chatbot", archived_only=True)
    print(f"已清理 {result['removed']} 条")
```

---

## InboxItem 结构

```json
{
  "id": "inb_a3f8c1d2e4b5",
  "workflow_id": "wf_chatbot",
  "category": "ext_source",
  "title": "🔌 tg_in received",
  "body": "q=你好",
  "payload": {
    "source_name": "tg_in",
    "publish_topic": "ext.tg.in",
    "kind": "telegram",
    "preview": "q=你好",
    "fields": ["q"]
  },
  "ts": "2026-06-03T08:30:00.123456+00:00",
  "read": false,
  "archived": false
}
```

---

## REST API 端点

| Method | Path | 说明 |
|--------|------|------|
| GET | `/api/inbox` | 列出通知（支持 `?workflow_id` / `?unread_only` / `?include_archived` / `?limit`） |
| GET | `/api/inbox/unread-count` | 未读计数 |
| POST | `/api/inbox/{id}/read` | 标单条已读 |
| POST | `/api/inbox/read-all` | 批量已读 |
| POST | `/api/inbox/{id}/archive` | 归档 |
| DELETE | `/api/inbox/{id}` | 删除 |
| POST | `/api/inbox/clear` | 批量清理 |

---

## Web UI 行为

1. **Header 📬 图标** — 始终显示，红色 badge 展示未读数（上限 `99+`）
2. **提示音** — 未读数增加时播放 Web Audio 合成声（E5→A5 双音阶 ~150ms）
3. **右侧抽屉** — 点击 📬 滑出：
   - 顶部 workflow 筛选 + archived 开关
   - 每条目 hover 显示 📁 归档 / ✕ 删除
   - 点击整条 → 跳转对应 workflow 详情页 + 标已读
   - 底部 "Mark all read" / "Clear archived"

---

## 容量与淘汰策略

- 默认容量 `MAX_INBOX_ITEMS = 500`
- 超出时 FIFO 淘汰最旧条目（`deque(maxlen=500)` + index dict 同步）
- 纯内存存储（重启丢失）——面向开发 / 演示环境
- 生产环境建议接入持久化存储（扩展 `Inbox` 基类即可）

---

## 与 External I/O 的关联

Inbox 的 `ext_source` / `ext_sink_ok` / `ext_sink_error` 条目**携带真实 workflow_id**（非 `"external"`），因此按 workflow 过滤时能准确看到属于该 workflow 的所有外部事件。

```python
# 只看 wf_chatbot 的 ext 事件
inbox = await c.list_inbox(workflow_id="wf_chatbot")
ext_events = [i for i in inbox["items"] if i["category"].startswith("ext_")]
```

---

## 自定义 Push（进阶）

如果需要从自定义代码 push 到 Inbox：

```python
# 在 server 端代码中（如自定义 middleware / handler）
state = request.app.state.app_state
state.inbox.push(
    workflow_id="my_workflow",
    category="error",
    title="自定义告警",
    body="某个指标超过阈值",
    payload={"metric": "cpu", "value": 95},
)
```
