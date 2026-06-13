# Testing Tools

Agentflow provides the `agentkit.testing` module, allowing you to fully test workflows without needing real Kafka, Redis, or LLM APIs.

---

## Imports

```python
from agentkit.testing import (
    LocalRuntime,        # Full workflow in-memory execution
    MockLLMGateway,      # LLM mock with fixed responses
    MockLLMProvider,     # Programmable LLM mock (queue mode)
    run_agent_locally,   # Single handler unit testing
)

```

---

## LocalRuntime — Full Integration Testing

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
        # Check the envelopes on the bus
        out = [e for e in rt.bus.published if e.topic == "out"]
        assert out[0].payload["tag"] == "positive"

```

### Constructor Parameters

```python
LocalRuntime(
    wf: WorkflowDef | None = None,      # Workflow defined via SDK
    *,
    ir: WorkflowIR | None = None,       # Or pass IR directly
    plan: RuntimePlan | None = None,
    handlers: dict[str, HandlerFn] | None = None,
    llm: LLMGatewayClient | None = None,     # Defaults to MockLLMGateway()
    guardrail: GuardrailHandle | None = None,
    runtime_settings: RuntimeSettings | None = None,
)

```

### Loading from YAML

```python
async with LocalRuntime.from_yaml(
    "workflows/wf_hello.yaml",
    handlers={"echo": my_echo_handler},
    llm=MockLLMGateway(reply="done"),
) as rt:
    run = await rt.run(input={"q": "test"})

```

### Inspecting Internal State

```python
rt.bus          # InProcessEventBus — Check published / subscribers
rt.orchestrator # Orchestrator — Check run store
rt.ir           # WorkflowIR — Check compilation results
rt.llm          # LLMGatewayClient — Check call history

```

---

## run_agent_locally — Single Handler Unit Testing

The most lightweight testing method — directly invoking a handler function, bypassing the Bus, Orchestrator, and Worker:

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

### Parameters

```python
await run_agent_locally(
    handler: HandlerFn,
    *,
    event: Envelope | Event | None = None,       # Custom input event
    input_payload: dict | None = None,           # Shortcut: Only pass payload
    input_topic: str | None = None,              # Shortcut: Only pass topic
    llm: LLMGatewayClient | None = None,         # Inject LLM mock
    template_key: str | None = None,
    workflow_id: str = "wf_local_test",
    run_id: str = "run_local_test",
    trace_id: str = "trc_local_test",
) -> list[Event]

```

---

## MockLLMGateway — Fixed Responses

```python
from agentkit.testing import MockLLMGateway

# Always replies with "ok"
llm = MockLLMGateway()

# Custom fixed response
llm = MockLLMGateway(reply="This is the AI's reply")

```

Ideal for tests where you only care about the pipeline logic and not the actual LLM content.

---

## MockLLMProvider — Programmable Queue

Finer control — supports returning different results across multiple calls:

```python
from agentkit.testing import MockLLMProvider

provider = MockLLMProvider()
provider.queue_response("Response for the first call")
provider.queue_response("Response for the second call")
provider.set_default_reply("Default response for subsequent calls")

```

### Simulating Errors

```python
from agentkit.llm_gateway.errors import LLMError

provider.queue_error(LLMError("rate_limit", "too many requests"))
provider.queue_response("Normal response after recovery")

```

### Usage

```python
# Embed into LocalRuntime
from agentkit.llm_gateway import LLMGatewayClient

gateway = LLMGatewayClient()
gateway.register_provider(provider)

async with LocalRuntime(wf, llm=gateway) as rt:
    run = await rt.run(input={"q": "test"})

```

---

## Testing Mode Best Practices

### 1. Tiered Testing

```text
┌─────────────────────┐
│  run_agent_locally  │  ← Single handler logic
├─────────────────────┤
│    LocalRuntime     │  ← Full pipeline integration
├─────────────────────┤
│  AgentKitClient +   │  ← SDK → API End-to-End
│  ASGITransport      │
└─────────────────────┘

```

### 2. Isolated Registry

```python
from agentkit.runtime import HandlerRegistry

@pytest.fixture(autouse=True)
def clean_registry():
    """Each test uses its own registry to avoid global pollution."""
    reg = HandlerRegistry()
    yield reg
    reg.clear()

```

### 3. ASGITransport End-to-End (No Real Network)

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

### 4. Timing Considerations

`InProcessEventBus` subscriptions take effect immediately, but after a redeploy, there is a slight delay in registering the agent instance's subscriptions. In end-to-end tests:

```python
await c.deploy(wf)
await asyncio.sleep(0.3)    # Allow subscriptions to settle
run = await c.create_run(...)

```