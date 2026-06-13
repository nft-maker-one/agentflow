# API 参考

本页列出 Agentflow SDK 全部公开类型、枚举、数据模型。

---

## 顶层导出

```python
from agentkit import (
    # SDK 核心
    workflow,             # WorkflowDef 工厂函数
    Agent,               # Agent 基类
    IRBuilder,           # 底层 IR 构建器
    WorkflowDef,         # Workflow 定义对象
    agent,               # @agent 装饰器
    judge,               # @judge 装饰器（role=judge 糖衣）

    # 运行时类型
    Event,               # Handler 输入 / 输出的事件
    AgentContext,        # Handler 的能力上下文
    Envelope,            # Bus 传输的完整 wire 格式

    # 枚举
    Role,                # Agent 角色
    AgentState,          # Agent FSM 状态
    RunStatus,           # Run 生命周期状态

    # 常量
    START, END, ERROR,   # 特殊节点名

    # Client
    AgentKitClient,      # 控制平面 HTTP 客户端
)
```

---

## 枚举

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

    # 支持位置参数
    Event("topic.name", {"key": "value"})
    Event(topic="topic.name", payload={"key": "value"})
```

---

## Envelope（wire 格式）

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
    category: InboxCategory   # 见下方
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

## WorkflowIR（编译产物）

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

## 保留 Header Keys

以下 key 在 `Event.user_headers` 中会被 runtime 静默剥离：

```
trace_id, run_id, event_id, from, from_, ts,
schema_ver, runtime_token, retry_attempt, deadline_ms
```

---

## Topic 命名规范

```
[a-z0-9][a-z0-9._-]*
```

惯例：

| 模式 | 用途 |
|------|------|
| `agent.<key>.in` | Agent 入口 topic |
| `agent.<key>.out` | Agent 出口 topic |
| `agent.<key>.out.<field>` | 细分出口 |
| `ext.<kind>.<name>.in` | External Source 默认 topic |
| `ext.<kind>.<name>.out` | External Sink 默认 topic |
| `system.*` | 系统内部（用户不应 subscribe） |
