# Workflow Construction

A Workflow is a directed graph of Agents. You define it declaratively using the SDK's `WorkflowDef`, compile it into an immutable IR (Intermediate Representation), and hand it over to the runtime for execution.

---

## Basic Usage

```python
from agentkit import Agent, workflow, START, END

class Researcher(Agent):
    subscribe = ["agent.researcher.in"]
    publish = ["agent.researcher.out"]
    prompt = "Research: {{ payload.q }}"
    llm = "deepseek/deepseek-chat"

class Writer(Agent):
    subscribe = ["agent.writer.in"]
    publish = ["agent.writer.out"]
    prompt = "Write a report based on the research results: {{ payload.research }}"
    llm = "deepseek/deepseek-chat"

wf = workflow("wf_pipeline", description="Research → Write pipeline")
r, w = Researcher(), Writer()
wf.add(r).add(w)
wf.connect(START, r)
wf.connect(r, w)       # Automatically infers via = topic intersection
wf.connect(w, END)

```

---

## `workflow()` Factory Parameters

```python
wf = workflow(
    "my_wf",
    version=2,
    description="...",
    owner="team-ai",
    project="news-bot",
    guardrails={
        "per_agent": {"max_tokens_per_call": 4000},
        "per_run":   {"max_total_tokens": 100_000, "max_cycles_per_run": 50},
    },
    event_driven=True,                  # event_driven mode
    start_input_fields=["msg", "uid"],  # Custom start payload field names
    end_join=True,                       # end node fan-in: wait for all end signals to arrive before terminating
)

```

| Parameter | Default | Description |
| --- | --- | --- |
| `id` | — | Workflow unique identifier (globally unique) |
| `version` | `1` | Version number |
| `description` | `""` | Description |
| `owner` | `None` | Owner |
| `project` | `None` | Associated project |
| `guardrails` | `None` | Global guardrail configuration |
| `event_driven` | `False` | Enable event-driven mode (takes effect upon deployment) |
| `start_input_fields` | `["q"]` | List of field names accepted by the Run input |
| `end_join` | `False` | End node fan-in aggregation: When multiple paths converge into `__end__`, waits for all signals to be collected before judging completion (see [FlowControl](https://www.google.com/search?q=%23flowcontrol--fan-in-aggregation)). |

---

## Connecting Edges — `connect()`

```python
wf.connect(from_node, to_node)
wf.connect(from_node, to_node, via="custom.topic")
wf.connect(from_node, to_node, edge_id="e_my_edge")

```

**Automatic `via` Inference Rules**:

If `via` is omitted, the SDK checks `from_.publish ∩ to_.subscribe`. As long as the intersection is exactly 1 topic, it is automatically filled in. Otherwise, an error is thrown prompting for manual specification.

Special nodes:

| Node | Usage |
| --- | --- |
| `START` / `"__start__"` | Entry point — the run's input payload is dispatched from here. |
| `END` / `"__end__"` | Successful terminal state — when an envelope reaches here, the run becomes Succeeded. |
| `ERROR` / `"__error__"` | Failed terminal state — automatically injected by the compiler. |

---

## Switch Routing — `connect_switch()`

Conditional routing based on payload field values:

```python
wf.connect_switch(
    critic,
    expr="$.decision",      # JSONPath expression to extract the field
    cases={
        "approve": {"to": "publisher", "via": "agent.publisher.in"},
        "reject":  {"to": "rewriter",  "via": "agent.rewriter.in"},
    },
    default={"to": "rewriter", "via": "agent.rewriter.in"},
)

```

---

## FlowControl — Fan-in Aggregation

When multiple signals converge, the framework provides two "wait for all before proceeding" coordination mechanisms: **Agent-level aggregators** (making one agent wait for multiple upstreams) and **End-level fan-in** (making the workflow wait for multiple terminal states). Both are isomorphic: they accumulate signals into a bucket by `run_id` and release them once only when the conditions are met.

### 1. Agent Aggregator (`aggregate`)

Makes an agent trigger its handler only once after receiving **multiple upstream signals** — a classic fan-in / join.

```python
combiner = Agent(
    template_key="combiner",
    role="aggregator",
    subscribe=["agent.a.out", "agent.b.out", "agent.c.out"],
    publish=["agent.combiner.out"],
    aggregate={"threshold": 0, "required": []},   # ← FlowControl
    llm="qwen/qwen-plus",
    prompt="Synthesize the following three results: {{ payload._inputs }}",
)

```

| Field | Meaning |
| --- | --- |
| `threshold` | The **number of signals** required to be collected. `0` = Takes the total number of topics in `subscribe` (i.e., "all present"). |
| `required` | A list of topics that must be present (even if the threshold is met, it will not trigger without these). |

**Underlying Principle** (see `src/agentkit/runtime/instance.py::_aggregate_admit`):

1. Buffers incoming envelopes by `run_id` and deduplicates them by topic (later arrivals for the same topic overwrite earlier ones): `buffer[run_id][topic] = env`;
2. Protected by `asyncio.Lock`, concurrency-safe under `max_concurrent>1`;
3. Gating: Releases only if `required ⊆ buffer.keys()` **and** `len(buffer) ≥ threshold` — otherwise, it acks the message, **the handler does not execute**, and the FSM reverts to Active (continues waiting);
4. Upon release, it **merges** all buffered payloads into a single envelope (union + `_inputs` mapping indexed by topic), clears the bucket for that run, and the handler executes **only once** with all inputs;
5. TTL + maximum bucket limit eviction, preventing memory leaks if a required topic for a run never arrives.

### 2. End Fan-in (`end_join`)

`__end__` is not an agent. Its default behavior is: **Any single path's** end signal arrives $\rightarrow$ the run is immediately judged as Succeeded (`_on_terminal` is idempotent, first one wins). When multiple agents feed directly into `__end__`, this causes "the first one to arrive terminates the entire workflow", discarding the outputs of the rest.

Enable `end_join=True` to add fan-in semantics to the end node as well:

```python
wf = workflow("fanout_to_end", end_join=True)   # ← Adds FlowControl to the end node
wf.connect(a, END, via="a.out")
wf.connect(b, END, via="b.out")
wf.connect(c, END, via="c.out")
# Now: the run only evaluates the workflow as finished once all three end signals from a, b, and c have arrived.

```

**Underlying Principle** (see `src/agentkit/orchestrator/routing.py::TerminalDetector`): Accumulates the arrived end-topic set by `run_id`. It only callbacks `_on_terminal(END)` once after gathering all `via` topics directly connected to the end edge; includes maximum bucket limit eviction.

**Note**:

* It is **opt-in** (defaults to `False`, keeping the original behavior). If end edges are conditionally triggered (some runs only produce partial agent outputs), forcefully enabling this will cause the run to hang indefinitely — for such scenarios, please use switch routing instead of direct end connections.
* `__error__` and the synthetic marker `system.run.<id>.end` emitted when a switch resolves to end will **bypass the join** and terminate immediately (exceptions/explicit single-point terminations should take effect instantly).
* When there is only 1 end edge, this marker automatically idles (no join needed).

> Summary comparison: `aggregate` means "**a specific agent** waits for multiple upstreams before running once"; `end_join` means "**the entire workflow** waits for multiple terminal states before ending once".

---

## Compilation and Exporting

```python
# Compile into IR + RuntimePlan
ir, plan = wf.compile()

# Export as a dict (round-trips to YAML)
spec = wf.to_dict()

# Write directly to YAML
wf.dump_yaml("workflows/wf_pipeline.yaml")

```

Automatically executes 7 IR validation rules during compilation:

1. All agents must have at least one incoming edge.
2. `__start__` must have at least one outgoing edge.
3. `__end__` must have at least one incoming edge.
4. Self-loops are not allowed.
5. Syntax validation for Switch expressions.
6. Topic naming conventions (`[a-z0-9._-]`).
7. Agent subscribe/publish lists cannot be empty.

---

## Two Execution Modes

### Normal Mode (Default)

Each `POST /api/runs` creates an independent run, which terminates once it traverses the graph.

### Event-Driven Mode

```python
wf = workflow("bot", event_driven=True)

```

* External Source continuously listens for external events.
* Every message shares the same session run (long lifecycle).
* Ideal for chatbot / message bridge scenarios.
* When switching back to normal mode, ext sources/sinks are paused (not deleted).

---

## Guardrails — Global Safety Nets

```python
wf = workflow(
    "expensive_pipeline",
    guardrails={
        "per_agent": {
            "max_tokens_per_call": 8000,    # Max LLM tokens per call
            "max_cycles": 5,                 # Max cycles per agent
        },
        "per_run": {
            "max_total_tokens": 200_000,    # Token budget for the entire run
            "max_cycles_per_run": 100,      # Max event loop iterations for the entire run
        },
    },
)

```

At runtime, the `InProcessGuardrail` singleton is hooked into both the LLM Gateway and the Agent Worker, performing real-time deductions and rejecting excess usage.

---

## Multi-Agent Graph Example

```python
from agentkit import Agent, workflow, START, END

class Fetcher(Agent):
    subscribe = ["fetch.in"]; publish = ["fetch.out"]
    python_script = 'def handle(p): return {"data": "..."}'

class Analyzer(Agent):
    subscribe = ["analyze.in"]; publish = ["analyze.out"]
    llm = "deepseek/deepseek-chat"
    prompt = "Analyze the data: {{ payload.data }}"

class Reporter(Agent):
    subscribe = ["report.in"]; publish = ["report.out"]
    llm = "deepseek/deepseek-chat"
    prompt = "Generate a report: {{ payload.result }}"

wf = workflow("wf_report")
f, a, r = Fetcher(), Analyzer(), Reporter()
wf.add(f).add(a).add(r)

wf.connect(START, f, via="fetch.in")
wf.connect(f, a, via="analyze.in")
wf.connect(a, r, via="report.in")
wf.connect(r, END)

```

Topology:

```
__start__ → Fetcher → Analyzer → Reporter → __end__

```