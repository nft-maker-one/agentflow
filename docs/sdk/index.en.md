# Agentflow SDK Usage Tutorial

Agentflow is a distributed, event-driven Agent orchestration framework. The SDK provides a Python-first, declarative interface that takes you all the way from definition to compilation, local testing, and remote deployment.

---

## Quick Start

```python
from agentkit import Agent, workflow, START, END

class Summarizer(Agent):
    role = "thinking"
    subscribe = ["agent.summarizer.in"]
    publish = ["agent.summarizer.out"]
    llm = "deepseek/deepseek-chat"
    prompt = "Summarize in one sentence: {{ payload.text }}"
    output_field = "summary"

wf = workflow("my_first_wf")
wf.add(Summarizer())
wf.connect(START, Summarizer())
wf.connect(Summarizer(), END)

# Local Testing
from agentkit.testing import LocalRuntime
async with LocalRuntime(wf) as rt:
    run = await rt.run(input={"text": "Agentflow is very easy to use..."})
    print(run.status)  # Succeeded

```

---

## Documentation Directory

| Module | Description |
| --- | --- |
| [Quick Start](getting-started.md) | Installation, Hello World, local execution |
| [Agent Definition](agent.md) | Agent classes, ClassVar fields, handle mechanics, python_script |
| [Workflow Construction](https://www.google.com/search?q=workflow.md) | WorkflowDef, connect, switch routing, guardrails |
| [External I/O](https://www.google.com/search?q=external-io.md) | External sources/sinks: Telegram, Email, Python Script |
| [Control Plane Client](https://www.google.com/search?q=client.md) | AgentKitClient: Deployment, runs, streaming events |
| [Message Notifications (Inbox)](https://www.google.com/search?q=inbox.md) | Event notifications, filtering, archiving |
| [Testing Tools](testing.md) | LocalRuntime, MockLLM, single handler testing |
| [CLI Commands](https://www.google.com/search?q=cli.md) | init / validate / compile / run / serve |
| [API Reference](https://www.google.com/search?q=api-reference.md) | All public types, enumerations, data models |

---

## Core Concepts

```text
┌──────────────┐      ┌──────────┐      ┌──────────┐
│ External     │      │  Agent   │      │ External │
│  Source      │─────▶│  Graph   │─────▶│   Sink   │
│ (Telegram…)  │      │ (Bus)    │      │ (SMTP…)  │
└──────────────┘      └──────────┘      └──────────┘
       ▲                    │
       │                    ▼
  POST /api/runs       Inbox Notifications

```

* **Agent** — The smallest processing unit; subscribes to topics, processes data, and publishes to topics.
* **Workflow** — A directed graph of Agents, compiled into immutable IR.
* **EventBus** — Topic-based publish/subscribe, decoupling communication between agents.
* **External I/O** — The bridge between the outside world and the bus (does not enter IR, hot-pluggable).
* **Orchestrator** — Manages the Run lifecycle + detects terminal states.
* **Inbox** — A user-facing notification ring-buffer (run completions, ext events, errors).

---

## Installation

```bash
# From source
cd agentflow
pip install -e ".[dev]"

# Or using uv (Recommended)
uv pip install -e ".[dev]"

```

## Environment Variables Reference (Configure as Needed)

Below is the **complete** list of environment variables read by the framework. Unless otherwise specified, they follow the rule: "If not set, use defaults / disable the corresponding capability." Configure them as needed.

> The framework automatically loads missing variables from a `.env` file (searching upwards directory by directory). For local development, simply write them into a `.env` file at the root of your project.

### 1) LLM Provider API Keys (Configure based on your choice)

When `agentkit serve` starts, it scans the environment and **enables any provider it detects a key for** (see `api/server.py::_build_gateway`). Each provider accepts a "common name" and a normalized `AGENTKIT_LLM_*` name; some also accept official vendor aliases.

```bash
OPENAI_API_KEY=sk-...        # OpenAI (Compatible endpoints) Alias: AGENTKIT_LLM_OPENAI_API_KEY
DEEPSEEK_API_KEY=sk-...      # DeepSeek                      Alias: AGENTKIT_LLM_DEEPSEEK_API_KEY
DASHSCOPE_API_KEY=sk-...     # Alibaba Qwen/DashScope        Alias: QWEN_API_KEY / AGENTKIT_LLM_QWEN_API_KEY
GEMINI_API_KEY=...           # Google Gemini                 Alias: GOOGLE_API_KEY / AGENTKIT_LLM_GEMINI_API_KEY
ANTHROPIC_API_KEY=sk-...     # Claude                        Alias: AGENTKIT_LLM_ANTHROPIC_API_KEY

```

* **If none are configured** $\rightarrow$ Automatically falls back to **MockLLM** (no keys required, convenient for local test runs).
* **Default provider selection order:** deepseek > openai > first detected. Use `agentkit serve --deepseek` to force deepseek.
* **Overrides:** An individual agent can override the global default during instantiation: `Agent(llm="qwen/qwen-plus")` takes precedence.

### 2) External I/O Secrets (Telegram / Email source & sink)

Parsing is centralized in `src/agentkit/external_io/env.py`. When source/sink `config`s **omit** these fields, they fall back to environment variables. **If explicitly written in the config, they override** the environment variables.

```bash
# Telegram (telegram source / sink)
TELEGRAM_BOT_TOKEN=123:ABC   # Bot Token from @BotFather     Alias: AGENTKIT_TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID=-100123     # sink fixed target chat (opt)  Alias: AGENTKIT_TELEGRAM_CHAT_ID

# Sending Emails (email_smtp sink)
SMTP_HOST=smtp.qq.com        # SMTP Server                   Alias: EMAIL_SMTP_HOST
SMTP_PORT=465                # SSL Port (Default: 465)
SMTP_USER=you@qq.com         # Login Email                   Alias: EMAIL_USER
SMTP_PASSWORD=auth_code      # Email auth code/password      Alias: EMAIL_PASSWORD
SMTP_TO=to@example.com       # Default Recipient             Alias: EMAIL_TO
SMTP_SUBJECT=Subject         # Default subject (optional)

# Receiving Emails (email_imap source)
IMAP_HOST=imap.qq.com        # IMAP Server                   Alias: EMAIL_IMAP_HOST
IMAP_PORT=993                # SSL Port (Default: 993)
IMAP_USER=you@qq.com         # Email                         Alias: EMAIL_USER
IMAP_PASSWORD=auth_code      # Auth code/password            Alias: EMAIL_PASSWORD

```

### 3) Persistence / Messaging Backends (Only when using `--store pg / --mq kafka / --guardrail redis`)

By default, everything uses zero-dependency, in-memory implementations, meaning these are not required. Configure them only when enabling the respective backend:

```bash
# Postgres (--store pg): Provide the full DSN, or break it into parts
AGENTKIT_PG_DSN=postgresql://agentkit:agentkit@localhost:5432/agentkit
# Or: AGENTKIT_PG_HOST / AGENTKIT_PG_PORT / AGENTKIT_PG_USER / AGENTKIT_PG_PASSWORD / AGENTKIT_PG_DB
AGENTKIT_PG_POOL_MIN=2       # Min connection pool size (Default: 2)
AGENTKIT_PG_POOL_MAX=10      # Max connection pool size (Default: 10)

# Kafka / Redpanda (--mq kafka)
AGENTKIT_BUS_BROKERS=localhost:9092   # Broker addresses (comma-separated)
# Other tuning parameters use the AGENTKIT_BUS_* prefix: producer_acks / consumer_max_poll_records /
# visibility_timeout_ms / max_redelivery / default_partitions … (See bus/kafka/config.py)

# Redis Rate Limiting / Quotas (--guardrail redis)
AGENTKIT_GUARDRAIL_REDIS_URL=redis://localhost:6379/0
# Other settings use the AGENTKIT_GUARDRAIL_* prefix: fail_mode / default_reservation_ttl_ms / namespace_prefix …

```

### 4) Runtime and Observability Tuning (Optional, all have defaults)

```bash
# Logging / Environment (common/config.py, prefix AGENTKIT_)
AGENTKIT_LOG_LEVEL=INFO      # DEBUG | INFO | WARNING | ERROR
AGENTKIT_LOG_FORMAT=json     # json | console
AGENTKIT_PROFILE=local       # local | staging | prod

# Observability (observability/config.py, prefix AGENTKIT_)
AGENTKIT_OTEL_ENDPOINT=http://localhost:4317   # OTLP export address (If not set, no export)
AGENTKIT_TRACE_SAMPLE_RATE=1.0                 # Sample rate 0.0–1.0
AGENTKIT_PROM_PORT=9000                        # Prometheus metrics port
AGENTKIT_SERVICE_NAME=agentkit

# Single-Instance Concurrency / Retry Tuning (runtime/config.py, prefix AGENTKIT_RUNTIME_)
AGENTKIT_RUNTIME_DEFAULT_MAX_CONCURRENT=4      # Concurrent processes per instance (Default: 4, set to 1 for strict serial)
AGENTKIT_RUNTIME_MAX_HANDLER_RETRIES=3         # Number of handler failure retries
AGENTKIT_RUNTIME_HANDLER_TIMEOUT_MS=60000      # Handler timeout in ms
# Other fields can similarly be overridden using the AGENTKIT_RUNTIME_<FIELD_UPPERCASE> format (See runtime/config.py)

```

> **Pattern:** All `AGENTKIT_<MODULE>_*` variables are automatically bound by `pydantic-settings`, where the environment variable name = Prefix + UPPERCASE field name. Refer to the respective `config.py` files for the definitive list of fields.