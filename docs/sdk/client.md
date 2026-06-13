# 控制平面客户端 — AgentKitClient

`AgentKitClient` 是 SDK 与运行中的 Agentflow 服务器交互的 async httpx wrapper。它覆盖了 Web UI 的全部能力。

---

## 基本用法

```python
import asyncio
from agentkit import AgentKitClient

async def main():
    async with AgentKitClient("http://localhost:8080") as c:
        health = await c.health()
        print(health)  # {"ok": True, "version": "0.1.0", ...}

asyncio.run(main())
```

---

## 构造参数

```python
AgentKitClient(
    base_url: str = "http://localhost:8080",
    timeout: float = 30.0,
)
```

支持 `async with` 上下文管理器，退出时自动关闭连接。

---

## Workflow 生命周期

### 部署 SDK WorkflowDef

```python
from agentkit import workflow, Agent, AgentKitClient

class MyAgent(Agent):
    subscribe = ["in"]; publish = ["out"]
    python_script = 'def handle(p): return {"echo": p.get("q", "")}'

wf = workflow("demo", event_driven=True, start_input_fields=["q"])
a = MyAgent()
wf.add(a).connect("__start__", a).connect(a, "__end__")
wf.add_source(name="src", kind="python_script", topic="in",
              config={"script": "async def stream(ctx):\n yield {'q':'hi'}"})

async with AgentKitClient("http://localhost:8080") as c:
    detail = await c.deploy(wf)
    print(detail["id"])  # "demo"
```

`deploy()` 内部执行 6 步：

1. `POST /api/workflows` — bootstrap
2. 对每个 Agent: `POST /api/workflows/{id}/agents` — 传递全部字段含 python_script
3. 删除 bootstrap echo
4. `PUT /workflows/{id}/start-input` — 配置输入字段
5. `PUT /workflows/{id}/mode` — 切换 event_driven
6. `POST /external/{sources|sinks}` — 注册 External I/O

### 其他操作

```python
# 列出所有 workflow
wfs = await c.list_workflows()

# 获取详情
detail = await c.get_workflow("demo")

# 删除
await c.delete_workflow("demo")

# 撤销上一步编辑
await c.undo_workflow("demo")

# 切换模式
await c.set_mode("demo", "normal")
await c.set_mode("demo", "event_driven")

# 设置 guardrail
await c.set_workflow_guardrail(
    "demo",
    max_total_tokens=100_000,
    max_cycles_per_run=50,
)
```

---

## 触发与管理 Run

```python
# 创建 run
run = await c.create_run("demo", input={"q": "hello"})
print(run["run_id"], run["status"])

# 列出 runs
runs = await c.list_runs(workflow_id="demo", limit=10)

# 获取单个 run 详情
detail = await c.get_run(run["run_id"])

# 取消运行中的 run
await c.cancel_run(run["run_id"])
```

---

## 流式事件（SSE）

```python
async for envelope in c.stream_run_events(run["run_id"]):
    print(f"[{envelope['topic']}] {envelope['payload']}")
    # 会一直 yield 直到 run 终态 + 断开

# 或者一次性获取所有已有事件
events = await c.run_events_snapshot(run["run_id"])
```

---

## External I/O 管理

```python
# 列出支持的 kind
kinds = await c.list_external_kinds()
# [{"kind": "telegram", "direction": "source", ...}, ...]

# 查看某 workflow 的 ext IO
ext = await c.list_external("demo")
print(ext["sources"], ext["sinks"])

# 添加
await c.add_external(
    "demo",
    direction="sink", name="tg_out",
    kind="telegram", topic="agent.reply.out",
    config={"token": "...", "chat_id": "-123456"},
)

# 删除
await c.remove_external("demo", direction="sink", name="tg_out")
```

---

## Inbox 通知

```python
# 获取通知列表
inbox = await c.list_inbox(workflow_id="demo", unread_only=True)
for item in inbox["items"]:
    print(f"[{item['category']}] {item['title']}")

# 标为已读
await c.inbox_mark_read(item["id"])
await c.inbox_mark_all_read(workflow_id="demo")

# 归档 / 删除
await c.inbox_archive(item["id"])
await c.inbox_delete(item["id"])

# 批量清理
await c.inbox_clear(workflow_id="demo", archived_only=True)
```

---

## 完整示例：部署 + 触发 + 等待 + 读取结果

```python
import asyncio
from agentkit import Agent, workflow, AgentKitClient, START, END

class Summarizer(Agent):
    subscribe = ["agent.sum.in"]
    publish = ["agent.sum.out"]
    output_field = "summary"
    python_script = """
def handle(payload):
    text = payload.get("text", "")
    return {"summary": text[:50] + "..."}
"""

async def main():
    wf = workflow("wf_summary")
    s = Summarizer()
    wf.add(s).connect(START, s).connect(s, END)

    async with AgentKitClient("http://localhost:8080") as c:
        await c.deploy(wf)

        run = await c.create_run("wf_summary", input={"text": "很长的文本" * 100})

        # 轮询等待终态
        while True:
            r = await c.get_run(run["run_id"])
            if r["status"] != "Running":
                break
            await asyncio.sleep(0.5)

        print(f"Status: {r['status']}")
        events = await c.run_events_snapshot(run["run_id"])
        final = [e for e in events if e["topic"] == "agent.sum.out"]
        if final:
            print(f"Summary: {final[-1]['payload']['summary']}")

asyncio.run(main())
```

---

## 错误处理

所有方法在 HTTP 4xx/5xx 时抛出 `httpx.HTTPStatusError`：

```python
try:
    await c.create_run("nonexistent", input={})
except httpx.HTTPStatusError as e:
    print(e.response.status_code)  # 404
    print(e.response.json())       # {"detail": "workflow not found"}
```
