# 测试工具

Agentflow 提供 `agentkit.testing` 模块，让你无需真实 Kafka / Redis / LLM API 就能完整测试 workflow。

---

## 导入

```python
from agentkit.testing import (
    LocalRuntime,        # 完整 workflow in-memory 运行
    MockLLMGateway,      # 固定回复的 LLM mock
    MockLLMProvider,     # 可编程的 LLM mock（队列模式）
    run_agent_locally,   # 单 handler 单元测试
)
```

---

## LocalRuntime — 完整集成测试

```python
import pytest
from agentkit import workflow, Agent, Event
from agentkit.testing import LocalRuntime, MockLLMGateway

class Tagger(Agent):
    subscribe = ["in"]; publish = ["out"]
    llm = "mock/mock"
    prompt = "Tag: {{ payload.text }}"
    output_field = "tag"

@pytest.mark.asyncio
async def test_full_pipeline():
    wf = workflow("test_wf")
    t = Tagger()
    wf.add(t).connect("__start__", t).connect(t, "__end__")

    async with LocalRuntime(wf, llm=MockLLMGateway(reply="positive")) as rt:
        run = await rt.run(input={"text": "great news"}, timeout=5.0)

        assert run.status.value == "Succeeded"
        # 检查 bus 上的 envelope
        out = [e for e in rt.bus.published if e.topic == "out"]
        assert out[0].payload["tag"] == "positive"
```

### 构造参数

```python
LocalRuntime(
    wf: WorkflowDef | None = None,      # SDK 定义的 workflow
    *,
    ir: WorkflowIR | None = None,       # 或直接传 IR
    plan: RuntimePlan | None = None,
    handlers: dict[str, HandlerFn] | None = None,
    llm: LLMGatewayClient | None = None,     # 默认 MockLLMGateway()
    guardrail: GuardrailHandle | None = None,
    runtime_settings: RuntimeSettings | None = None,
)
```

### 从 YAML 加载

```python
async with LocalRuntime.from_yaml(
    "workflows/wf_hello.yaml",
    handlers={"echo": my_echo_handler},
    llm=MockLLMGateway(reply="done"),
) as rt:
    run = await rt.run(input={"q": "test"})
```

### 检查内部状态

```python
rt.bus          # InProcessEventBus — 查 published / subscribers
rt.orchestrator # Orchestrator — 查 run store
rt.ir           # WorkflowIR — 查编译结果
rt.llm          # LLMGatewayClient — 查调用记录
```

---

## run_agent_locally — 单 Handler 单元测试

最轻量的测试方式——直接调用一个 handler 函数，不经过 Bus / Orchestrator / Worker：

```python
from agentkit.testing import run_agent_locally
from agentkit import Event

@agent(subscribe=["in"], publish=["out"])
async def my_handler(ctx, event):
    return [Event("out", {"echo": event.payload["q"]})]

@pytest.mark.asyncio
async def test_handler_unit():
    events = await run_agent_locally(
        my_handler,
        input_payload={"q": "hello"},
        input_topic="in",
    )
    assert len(events) == 1
    assert events[0].payload["echo"] == "hello"
```

### 参数

```python
await run_agent_locally(
    handler: HandlerFn,
    *,
    event: Envelope | Event | None = None,      # 自定义输入 event
    input_payload: dict | None = None,           # 快捷：只传 payload
    input_topic: str | None = None,              # 快捷：只传 topic
    llm: LLMGatewayClient | None = None,         # 注入 LLM mock
    template_key: str | None = None,
    workflow_id: str = "wf_local_test",
    run_id: str = "run_local_test",
    trace_id: str = "trc_local_test",
) -> list[Event]
```

---

## MockLLMGateway — 固定回复

```python
from agentkit.testing import MockLLMGateway

# 总是回复 "ok"
llm = MockLLMGateway()

# 自定义固定回复
llm = MockLLMGateway(reply="这是AI的回复")
```

适合只关心 pipeline 逻辑、不关心 LLM 内容的测试。

---

## MockLLMProvider — 可编程队列

更精细控制 — 支持多次调用返回不同结果：

```python
from agentkit.testing import MockLLMProvider

provider = MockLLMProvider()
provider.queue_response("第一次调用的回复")
provider.queue_response("第二次调用的回复")
provider.set_default_reply("后续调用的默认回复")
```

### 模拟错误

```python
from agentkit.llm_gateway.errors import LLMError

provider.queue_error(LLMError("rate_limit", "too many requests"))
provider.queue_response("恢复后的正常回复")
```

### 使用方式

```python
# 嵌入 LocalRuntime
from agentkit.llm_gateway import LLMGatewayClient

gateway = LLMGatewayClient()
gateway.register_provider(provider)

async with LocalRuntime(wf, llm=gateway) as rt:
    run = await rt.run(input={"q": "test"})
```

---

## 测试模式最佳实践

### 1. 分层测试

```
┌─────────────────────┐
│  run_agent_locally  │  ← 单 handler 逻辑
├─────────────────────┤
│    LocalRuntime     │  ← 完整 pipeline 集成
├─────────────────────┤
│  AgentKitClient +   │  ← SDK → API 端到端
│  ASGITransport      │
└─────────────────────┘
```

### 2. 隔离 Registry

```python
from agentkit.runtime import HandlerRegistry

@pytest.fixture(autouse=True)
def clean_registry():
    """每个测试用自己的 registry 避免全局污染。"""
    reg = HandlerRegistry()
    yield reg
    reg.clear()
```

### 3. ASGITransport 端到端（无真实网络）

```python
import httpx
from agentkit import AgentKitClient
from agentkit.api import AppState, create_app

@pytest.fixture
async def client():
    state = AppState()
    await state.start()
    state.set_llm_gateway(MockLLMGateway())
    app = create_app(state)
    transport = httpx.ASGITransport(app=app)
    c = AgentKitClient("http://test")
    await c._client.aclose()
    c._client = httpx.AsyncClient(transport=transport, base_url="http://test")
    try:
        yield c
    finally:
        await c.close()
        await state.stop()
```

### 4. 时序注意

InProcessEventBus 的 subscription 是即时生效的，但 redeploy 后 agent instance 的 subscription 注册有微小延迟。在端到端测试中：

```python
await c.deploy(wf)
await asyncio.sleep(0.3)    # 让 subscription settle
run = await c.create_run(...)
```
