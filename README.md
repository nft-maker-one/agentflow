# Agentflow

Agentflow is a distributed, event-driven Agent orchestration framework which is dedicated to make agent system creating and editing as efficient as docx editing in office.  
Every builder who is interested in of event driven agent workflow is warmly welcomed, please directly message me at github or email wjluo57@gmail.com

📖 **Full documentation:** https://nft-maker-one.github.io/agentflow/ — guides, SDK reference, deployment, and CLI (中文 / English).

---

## Why Agentflow?

Most agent frameworks orchestrate agents as a **graph you invoke**: nodes and edges over a shared state object, traversed once per request. That's a great fit for a structured, one-shot request → response.

Agentflow has a different center of gravity: **agents on an event bus.** Each agent simply *subscribes* to topics and *publishes* results — fully decoupled. External events (a Telegram message, an email, a webhook) are injected onto the bus as **first-class sources**, and an event-driven mode keeps the workflow **continuously listening**.

> **In one line:** graph frameworks *invoke a graph*; Agentflow lets *events flow on a bus*.

### Agentflow vs. graph-based orchestrators (e.g. LangGraph)

| | Graph-based (e.g. LangGraph) | **Agentflow** |
|---|---|---|
| **Orchestration model** | Graph traversal over shared state | Topic **pub/sub on an event bus** |
| **Agent ↔ agent** | State passed along edges | Decoupled envelopes on the bus (fan-in / fan-out) |
| **Add an agent** | Rewire the graph / edit edges | **Subscribe to a topic** — nothing else changes |
| **Inbound trigger** | You call the API to start a run (+ cron) | **External sources inject events** onto the bus (hot-pluggable) |
| **Webhooks** | Outbound — notify *after* a run completes | Inbound events drive agents directly |
| **Running shape** | One run per invocation | Single run **or** always-on **event-driven** mode |
| **Transport** | In-process (+ hosted platform) | **Pluggable broker**: Redis Streams (default) / Kafka / in-process — one env var, auto-degrades if offline |
| **Editing** | Code-first | **Live in-browser** topology + prompt editing, hot redeploy (zero downtime) |

> Graph frameworks are excellent — this is a difference in **design center**, not a "more features" claim. Persistence, durable execution, and visualization exist on both sides.

### Highlights

- 🔌 **Decoupled by design** — add or replace an agent = one subscription; the rest of the workflow is untouched.
- 📨 **External events are first-class** — Telegram / email / webhook **sources** inject onto the bus without editing the workflow.
- 🖱️ **Edit it like a document** — drag the topology, edit prompts, hot-redeploy live in the browser.
- ⚡ **Millisecond deploys** — Redis Streams consumer groups are `O(1)` (no rebalance); creating / redeploying a workflow is milliseconds, not seconds.
- 🧱 **Pluggable backends + graceful degrade** — Redis Streams / Kafka / in-process bus, Postgres / in-memory store, Redis / in-memory guardrail; selected by one env var, auto-degrade when the middleware is offline.
- 🧾 **Every run is replayable** — a per-run **topology snapshot + full event timeline** is persisted; an archived run renders exactly as it ran.
- 🔭 **Operable out of the box** — per-run / per-agent token & cycle guardrails, OpenTelemetry traces, Prometheus + Grafana.

### Why event-driven is where agent systems are heading

Real agent systems aren't one-shot Q&A — they are **always-on and reactive**, responding to streams of events: messages, emails, webhooks, schedules, and each other's outputs. Microservices made exactly this move — from RPC spaghetti to event streaming — for the same reasons: loose coupling, fan-in / fan-out, backpressure, durability, replay. **Agents are the next microservices, and they need an event backbone — not a bigger graph.**

---

## Quick start

```bash
# 1) Install (Python 3.11+, uv: https://docs.astral.sh/uv/)
uv sync --all-extras

# 2) Run the in-process demo (no docker needed)
.venv/bin/python -m agentkit.cli.main version
.venv/bin/python -m agentkit.cli.main init /tmp/my-bot
cd /tmp/my-bot
agentkit run workflows/wf_hello.yaml --input '{"q":"hi"}' --handlers handlers
```

Output:

```
✓ run.Succeeded  run_id=run_01KST...  (2 branch event(s))
last event on agent.echo.out.reply:
{ "text": "hi" }
```

---

## Two ways to define a Workflow

### A. Python SDK

```python
from agentkit import Event, agent, workflow
from agentkit.testing import LocalRuntime, MockLLMGateway

@agent(role="thinking", subscribe=["q"], publish=["reply"])
async def echo(ctx, event):
    text = await ctx.llm.chat(f"Echo: {event.payload['q']}")
    return [Event(topic="reply", payload={"text": text})]

wf = workflow("wf_demo")
wf.add(echo)
wf.connect("__start__", "echo", via="q")
wf.connect("echo", "__end__", via="reply")

async with LocalRuntime(wf, llm=MockLLMGateway(reply="hi from mock")) as rt:
    run = await rt.run(input={"q": "hello"})
    assert run.status.value == "Succeeded"
```

### B. YAML

```yaml
id: wf_demo
agents:
  echo:
    role: thinking
    subscribe: [{ topic: q }]
    publish:   [{ topic: reply }]
edges:
  e_in:  { from: __start__, to: echo, via: q }
  e_out: { from: echo, to: __end__, via: reply }
```

```bash
agentkit run workflows/wf_demo.yaml --input '{"q":"hi"}' --handlers handlers
```

Both compile to the **same `WorkflowIR`** through the same 6-step Compiler.

---

## Full development stack with Docker

```bash
make up          # Redpanda + Redis + PG + Prometheus + Grafana + OTel
make ps          # check service status
make down        # tear down
```

| Service | URL | Notes |
|---|---|---|
| Redpanda (Kafka) | `localhost:9092` | EventBus broker |
| Redis | `localhost:6379` | Guardrail quotas, dedup |
| Postgres | `localhost:5432` | `agentkit/agentkit` |
| **Prometheus** | http://localhost:9090 | Scrapes AgentKit `/metrics` |
| **Grafana** | http://localhost:3000 | `admin/admin`, AgentKit Overview dashboard |
| OTel Collector | `grpc://localhost:4317` | Receives OTLP traces |
| AgentKit probe | http://localhost:9100/metrics | Started by your `agentkit` process |

---

## Tests

```bash
make test-unit          # 475 fast tests, no docker (~7s)
make test-integration   # real Kafka + Redis (requires `make up-core`)
make test-e2e           # full stack: Kafka + Prometheus query (requires `make up`)
make test-perf          # perf benchmarks
```

Test layout:

| Directory | Marker | What it tests |
|---|---|---|
| `tests/unit/` | (none) | All logic with InProcessEventBus + mocks |
| `tests/integration/` | `integration` | Real Kafka / Redis / OpenAI compat endpoint |
| `tests/e2e/` | `e2e` | Real Kafka + Prometheus scrape verification |
| `tests/perf/` | `perf` | Throughput / latency benchmarks |

---

## Architecture

```
┌──────────────────────────────────────────────────────────────────┐
│ cli           agentkit init / run / validate / compile / schema  │  L6
├──────────────────────────────────────────────────────────────────┤
│ sdk + testing @agent / @judge / IRBuilder / LocalRuntime         │  L5
│ orchestrator  Run + Switch + Terminal + Branch                   │  L5
├──────────────────────────────────────────────────────────────────┤
│ runtime       FSM + 4-Gate + Worker + AgentInstance              │  L4
│ notifier      Rule DSL + Channel + Template (Jinja2)             │  L4
├──────────────────────────────────────────────────────────────────┤
│ workflow      IR + 6-step Compiler                               │  L3
│ llm           9-step Gateway + Provider + Tokenizer              │  L3
│ guardrail     Lua atomic precheck/consume/release on Redis       │  L3
├──────────────────────────────────────────────────────────────────┤
│ bus           EventBus Protocol + Kafka + InProcess adapters     │  L2
├──────────────────────────────────────────────────────────────────┤
│ models        Envelope + enums (Role / AgentState / RunStatus)   │  L1
│ observability Metrics + Tracing + Audit + ProbeServer            │  L1
├──────────────────────────────────────────────────────────────────┤
│ common        config / ids (ULID) / time / logging (structlog)   │  L0
└──────────────────────────────────────────────────────────────────┘
```

**Strictly layered:** lower layers MUST NOT import upper layers. Verified by an AST-based `make` check in CI.

---

## CLI commands

```bash
agentkit version
agentkit init <project>         # scaffold a project (handlers.py + workflows/wf_hello.yaml)
agentkit validate <yaml>        # static IR validation
agentkit compile <yaml>         # produce canonical IR JSON
agentkit schema export          # dump the WorkflowIR JSON Schema
agentkit run <yaml> --input <json> --handlers <module>
```

---

## Development

```bash
make lint        # ruff check
make format      # ruff format + auto-fix
make typecheck   # mypy strict
```

### Adding a custom LLM provider

```python
from agentkit.llm.provider import LLMProvider
class MyProvider:                    # satisfy the Protocol structurally
    name = "myco"; capabilities = ...
    async def complete(self, req): ...
    async def stream(self, req): ...
    def count_tokens(self, content, model): ...
```

Plug it into the Gateway via `LLMGatewayClient(providers={"myco": MyProvider()})`.

---

## Module layout

```
src/agentkit/
  common/         # L0 — config, ids, time, logging, errors
  models/         # L1 — Pydantic data models
  observability/  # L1 — metrics, tracing, audit, probe server
  bus/            # L2 — EventBus Protocol + Kafka + InProcess adapters
  llm/            # L3 — Gateway pipeline + tokenizer + ratelimit
  workflow/       # L3 — IR + Compiler (parse/expand/resolve/inject/validate/lower/plan)
  guardrail/      # L3 — Resolver + Redis Lua backend
  runtime/        # L4 — FSM + 4-Gate + AgentInstance + Worker
  notifier/       # L4 — Rule DSL + matcher + channels + templates
  orchestrator/   # L5 — Run + Switch + Terminal routing
  sdk/            # L5 — @agent / @judge / IRBuilder / WorkflowDef
  testing/        # L5 — LocalRuntime + MockLLMGateway + run_agent_locally
  cli/            # L6 — Typer app
```

---

## License

MIT
