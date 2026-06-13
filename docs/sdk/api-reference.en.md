# API Reference

This page lists all public types, enumerations, and data models in the Agentflow SDK.

---

## Top-Level Exports

```python
from agentkit import (
    # Core SDK
    workflow,             # Factory function for WorkflowDef
    Agent,               # Base class for Agent
    IRBuilder,           # Low-level IR builder
    WorkflowDef,         # Workflow definition object
    agent,               # @agent decorator
    judge,               # @judge decorator (syntactic sugar for role=judge)

    # Runtime Types
    Event,               # Event for Handler input / output
    AgentContext,        # Capability context for Handler
    Envelope,            # Complete wire format transmitted over the Bus

    # Enumerations
    Role,                # Agent roles
    AgentState,          # Agent FSM states
    RunStatus,           # Run lifecycle states

    # Constants
    START, END, ERROR,   # Special node names

    # Client
    AgentKitClient,      # Control plane HTTP client
)

```

---

## Enumerations

### `Role`

```python
class Role(StrEnum):
    FETCH = "fetch"
    THINKING = "thinking"
    JUDGE = "judge"
    TOOL = "tool"
    MEMORY = "memory"
    GUARD = "guard"
    HUMAN = "human"
    AGGREGATOR = "aggregator"

```

### `AgentState`

```python
class AgentState(StrEnum):
    INIT = "Init"
    ACTIVE = "Active"
    PROCESSING = "Processing"
    RETRY = "Retry"
    WAITING = "Waiting"
    FAILURE = "Failure"
    DOWN = "Down"
    COMPLETE = "Complete"

```

### `RunStatus`

```python
class RunStatus(StrEnum):
    PENDING = "Pending"
    RUNNING = "Running"
    SUSPENDED = "Suspended"
    AWAITING_HUMAN = "AwaitingHuman"
    DEGRADED = "Degraded"
    SUCCEEDED = "Succeeded"
    FAILED = "Failed"
    CANCELLED = "Cancelled"
    ARCHIVED = "Archived"

```

---

## Event

```python
class Event(BaseModel):
    topic: str
    payload: dict[str, Any] = {}
    user_headers: dict[str, str] = {}
    to_tags: dict[str, str] = {}

    # Supports positional arguments
    Event("topic.name", {"key": "value"})
    Event(topic="topic.name", payload={"key": "value"})

```

---

## Envelope (Wire Format)

```python
class Envelope(BaseModel):
    event_id: str
    schema_ver: str = "1.0"
    topic: str
    to_filter: ToFilter
    trace_id: str
    workflow_id: str = ""
    run_id: str = ""
    causation_id: str | None = None
    from_: AgentRef          # JSON alias: "from"
    payload: dict[str, Any] = {}
    schema_ref: str | None = None
    headers: EnvelopeHeaders
    ts: datetime

class ToFilter(BaseModel):
    role: Role | None = None
    tags: dict[str, str] = {}

class AgentRef(BaseModel):
    role: Role | None = None
    agent_id: str | None = None
    runtime_node: str | None = None

class EnvelopeHeaders(BaseModel):
    priority: int = 5           # 0-9
    deadline_ms: int | None = None
    retry_attempt: int = 0
    runtime_token: str | None = None
    replayed_from: str | None = None
    user_headers: dict[str, str] = {}

```

---

## AgentContext

```python
class AgentContext:
    agent_id: str
    template_key: str
    workflow_id: str
    run_id: str
    trace_id: str
    causation_id: str | None
    agent_tags: dict[str, str]
    llm: LLMHandle
    logger: BoundLogger

    async def publish(self, event: Event) -> PublishReceipt: ...

```

### LLMHandle

```python
class LLMHandle:
    async def chat(
        self, prompt: str,
        *,
        provider: str | None = None,
        model: str | None = None,
        system: str | None = None,
        **kwargs,
    ) -> str: ...

    async def complete(self, req: LLMRequest) -> LLMResponse: ...

    @property
    def gateway(self) -> LLMGatewayClient: ...
    @property
    def binding(self) -> LLMBinding | None: ...

```

---

## InboxItem

```python
@dataclass
class InboxItem:
    id: str                   # "inb_xxxxxxxxxxxx"
    workflow_id: str
    category: InboxCategory   # See below
    title: str
    body: str
    payload: dict | None
    ts: datetime
    read: bool
    archived: bool

InboxCategory = Literal[
    "run_succeeded", "run_failed", "run_cancelled",
    "ext_source", "ext_sink_ok", "ext_sink_error", "error",
]

```

---

## WorkflowIR (Compilation Artifact)

```python
class WorkflowIR(BaseModel):
    id: str
    version: int
    description: str = ""
    owner: str | None = None
    project: str | None = None
    meta: WorkflowMeta
    agents: dict[str, AgentTemplate]
    edges: dict[str, EdgeSpec]
    topics: dict[str, TopicSpec]
    guardrails: WorkflowGuardrail | None = None

class AgentTemplate(BaseModel):
    role: Role
    description: str = ""
    subscribe: list[SubscribeSpec]
    publish: list[PublishSpec]
    llm: LLMBinding | None = None
    tags: dict[str, str] = {}
    replicas_min: int = 1
    replicas_max: int = 1
    aggregate: AggregateSpec | None = None
    guardrail: AgentGuardrailSpec | None = None

```

---

## Reserved Header Keys

The following keys in `Event.user_headers` will be silently stripped by the runtime:

```
trace_id, run_id, event_id, from, from_, ts,
schema_ver, runtime_token, retry_attempt, deadline_ms

```

---

## Topic Naming Conventions

```
[a-z0-9][a-z0-9._-]*

```

Conventions:

| Pattern | Purpose |
| --- | --- |
| `agent.<key>.in` | Agent input topic |
| `agent.<key>.out` | Agent output topic |
| `agent.<key>.out.<field>` | Sub-output topic |
| `ext.<kind>.<name>.in` | Default topic for External Source |
| `ext.<kind>.<name>.out` | Default topic for External Sink |
| `system.*` | Internal system topics (users should not subscribe) |