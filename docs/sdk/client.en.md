# Control Plane Client — AgentKitClient

`AgentKitClient` is an async `httpx` wrapper in the SDK used to interact with a running Agentflow server. It provides all the capabilities available in the Web UI.

---

## Basic Usage

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

## Constructor Arguments

```python
AgentKitClient(
    base_url: str = "http://localhost:8080",
    timeout: float = 30.0,
)

```

Supports the `async with` context manager, which automatically closes the connection upon exit.

---

## Workflow Lifecycle

### Deploying an SDK WorkflowDef

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

The `deploy()` method executes 6 steps internally:

1. `POST /api/workflows` — Bootstrap the workflow
2. For each Agent: `POST /api/workflows/{id}/agents` — Transmit all fields, including the `python_script`
3. Delete the bootstrap echo
4. `PUT /workflows/{id}/start-input` — Configure input fields
5. `PUT /workflows/{id}/mode` — Switch to `event_driven` mode
6. `POST /external/{sources|sinks}` — Register External I/O

### Other Operations

```python
# List all workflows
wfs = await c.list_workflows()

# Get workflow details
detail = await c.get_workflow("demo")

# Delete a workflow
await c.delete_workflow("demo")

# Undo the previous edit
await c.undo_workflow("demo")

# Switch modes
await c.set_mode("demo", "normal")
await c.set_mode("demo", "event_driven")

# Set workflow guardrails
await c.set_workflow_guardrail(
    "demo",
    max_total_tokens=100_000,
    max_cycles_per_run=50,
)

```

---

## Triggering and Managing Runs

```python
# Create a run
run = await c.create_run("demo", input={"q": "hello"})
print(run["run_id"], run["status"])

# List runs
runs = await c.list_runs(workflow_id="demo", limit=10)

# Get details of a single run
detail = await c.get_run(run["run_id"])

# Cancel an active run
await c.cancel_run(run["run_id"])

```

---

## Streaming Events (SSE)

```python
async for envelope in c.stream_run_events(run["run_id"]):
    print(f"[{envelope['topic']}] {envelope['payload']}")
    # Continues to yield until the run reaches a terminal state and disconnects

# Or fetch a snapshot of all existing events at once
events = await c.run_events_snapshot(run["run_id"])

```

---

## External I/O Management

```python
# List supported kinds
kinds = await c.list_external_kinds()
# [{"kind": "telegram", "direction": "source", ...}, ...]

# View external I/O for a specific workflow
ext = await c.list_external("demo")
print(ext["sources"], ext["sinks"])

# Add external I/O
await c.add_external(
    "demo",
    direction="sink", name="tg_out",
    kind="telegram", topic="agent.reply.out",
    config={"token": "...", "chat_id": "-123456"},
)

# Delete external I/O
await c.remove_external("demo", direction="sink", name="tg_out")

```

---

## Inbox Notifications

```python
# Get notification list
inbox = await c.list_inbox(workflow_id="demo", unread_only=True)
for item in inbox["items"]:
    print(f"[{item['category']}] {item['title']}")

# Mark as read
await c.inbox_mark_read(item["id"])
await c.inbox_mark_all_read(workflow_id="demo")

# Archive / Delete
await c.inbox_archive(item["id"])
await c.inbox_delete(item["id"])

# Batch clear
await c.inbox_clear(workflow_id="demo", archived_only=True)

```

---

## Full Example: Deploy + Trigger + Wait + Read Results

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

        run = await c.create_run("wf_summary", input={"text": "A very long text " * 100})

        # Poll and wait for terminal state
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

## Error Handling

All methods raise an `httpx.HTTPStatusError` on HTTP 4xx/5xx responses:

```python
try:
    await c.create_run("nonexistent", input={})
except httpx.HTTPStatusError as e:
    print(e.response.status_code)  # 404
    print(e.response.json())       # {"detail": "workflow not found"}

```