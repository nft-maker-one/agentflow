# Workflow 构建

Workflow 是 Agent 的有向图。你通过 SDK 的 `WorkflowDef` 声明式定义它，编译成不可变 IR 后交给运行时执行。

---

## 基本用法

```python
from agentkit import Agent, workflow, START, END

class Researcher(Agent):
    subscribe = ["agent.researcher.in"]
    publish = ["agent.researcher.out"]
    prompt = "研究：{{ payload.q }}"
    llm = "deepseek/deepseek-chat"

class Writer(Agent):
    subscribe = ["agent.writer.in"]
    publish = ["agent.writer.out"]
    prompt = "基于研究结果撰写报告：{{ payload.research }}"
    llm = "deepseek/deepseek-chat"

wf = workflow("wf_pipeline", description="Research → Write pipeline")
r, w = Researcher(), Writer()
wf.add(r).add(w)
wf.connect(START, r)
wf.connect(r, w)       # 自动推断 via = topic 交集
wf.connect(w, END)
```

---

## `workflow()` 工厂参数

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
    event_driven=True,                  # event_driven 模式
    start_input_fields=["msg", "uid"],  # 自定义 start payload 字段名
    end_join=True,                       # end 节点扇入：收齐所有 end 信号才结束
)
```

| 参数 | 默认 | 说明 |
|------|------|------|
| `id` | — | Workflow 唯一标识（全局唯一） |
| `version` | `1` | 版本号 |
| `description` | `""` | 描述 |
| `owner` | `None` | 所有者 |
| `project` | `None` | 所属项目 |
| `guardrails` | `None` | 全局 guardrail 配置 |
| `event_driven` | `False` | 启用事件驱动模式（部署时生效） |
| `start_input_fields` | `["q"]` | Run input 接受的字段名列表 |
| `end_join` | `False` | end 节点扇入聚合：多路汇入 `__end__` 时收齐全部信号才判定结束（见 [FlowControl](#flowcontrol-扇入聚合-fan-in)） |

---

## 连边 — `connect()`

```python
wf.connect(from_node, to_node)
wf.connect(from_node, to_node, via="custom.topic")
wf.connect(from_node, to_node, edge_id="e_my_edge")
```

**自动 `via` 推断规则**：

如果省略 `via`，SDK 检查 `from_.publish ∩ to_.subscribe`。只要交集恰好 1 个 topic，就自动填入。否则报错提示手动指定。

特殊节点：

| 节点 | 用法 |
|------|------|
| `START` / `"__start__"` | 入口——run 的 input payload 从这里 dispatch |
| `END` / `"__end__"` | 成功终态——envelope 到达此处 run 变为 Succeeded |
| `ERROR` / `"__error__"` | 失败终态——编译器自动注入 |

---

## Switch 路由 — `connect_switch()`

根据 payload 字段值做条件路由：

```python
wf.connect_switch(
    critic,
    expr="$.decision",      # JSONPath 表达式提取字段
    cases={
        "approve": {"to": "publisher", "via": "agent.publisher.in"},
        "reject":  {"to": "rewriter",  "via": "agent.rewriter.in"},
    },
    default={"to": "rewriter", "via": "agent.rewriter.in"},
)
```

---

## FlowControl — 扇入聚合（fan-in）

多路信号汇聚时，框架提供两处"等齐再继续"的协调机制：**agent 级聚合器**（让一个 agent 等多个上游）和 **end 级扇入**（让 workflow 等多路终态）。两者同构：都按 `run_id` 把信号攒进一个桶，凑齐条件才放行一次。

### 1. Agent 聚合器（`aggregate`）

让一个 agent 在收到**多个上游信号**后才触发一次 handler——经典的 fan-in / join。

```python
combiner = Agent(
    template_key="combiner",
    role="aggregator",
    subscribe=["agent.a.out", "agent.b.out", "agent.c.out"],
    publish=["agent.combiner.out"],
    aggregate={"threshold": 0, "required": []},   # ← FlowControl
    llm="qwen/qwen-plus",
    prompt="综合以下三方结果：{{ payload._inputs }}",
)
```

| 字段 | 含义 |
|------|------|
| `threshold` | 需要收齐的**信号数**。`0` = 取 `subscribe` 的 topic 总数（即"全部到齐"） |
| `required` | 必须出现的 topic 列表（即使 threshold 已满，缺这些也不触发） |

**底层原理**（[instance.py `_aggregate_admit`](../../src/agentkit/runtime/instance.py)）：

1. 按 `run_id` 缓冲进来的 envelope，按 topic 去重（同 topic 后到覆盖先到）：`buffer[run_id][topic] = env`；
2. `asyncio.Lock` 保护，`max_concurrent>1` 下并发安全；
3. 门控：`required ⊆ buffer.keys()` **且** `len(buffer) ≥ threshold` 才放行——否则 ack 该消息、**handler 不执行**、FSM 退回 Active（继续等）；
4. 放行时把所有缓冲 payload **合并**成一个 envelope（union + 按 topic 索引的 `_inputs` 映射），清空该 run 的桶，handler 拿全部输入**只跑一次**；
5. TTL + 桶数上限淘汰，防止某 run 必需 topic 永不到来导致内存泄漏。

### 2. End 扇入（`end_join`）

`__end__` 不是 agent，默认行为是：**任意一路** end 信号到达 → run 立即判定 Succeeded（`_on_terminal` 幂等，第一个赢）。当多个 agent 直接汇入 `__end__` 时，这会导致"先到的那个就结束了整个 workflow"，其余产出被丢弃。

开启 `end_join=True` 给 end 节点也加上扇入语义：

```python
wf = workflow("fanout_to_end", end_join=True)   # ← 给 end 节点加 FlowControl
wf.connect(a, END, via="a.out")
wf.connect(b, END, via="b.out")
wf.connect(c, END, via="c.out")
# 现在：run 只有在 a/b/c 三路 end 信号都到齐后，才判定一次 workflow 结束
```

**底层原理**（[routing.py `TerminalDetector`](../../src/agentkit/orchestrator/routing.py)）：按 `run_id` 累积已到的 end-topic 集合，收齐全部直连 end 边的 `via` topic 才回调 `_on_terminal(END)` 一次；带桶数上限淘汰。

**注意**：

- 是 **opt-in**（默认 `False`，行为不变）。若 end 边是条件触发（某些 run 只有部分 agent 产出），强开会让 run 永久挂起——这类场景请用 switch 路由而非直连 end。
- `__error__` 与 switch 解析到 end 时发的合成标记 `system.run.<id>.end` **绕过 join**、立即结束（异常/显式单点终止应当即时生效）。
- 只有 1 条 end 边时该标记自动空转（无需 join）。

> 一句话对照：`aggregate` 是"**某个 agent** 等齐多个上游再跑一次"；`end_join` 是"**整个 workflow** 等齐多路终态再结束一次"。

---

## 编译与导出

```python
# 编译为 IR + RuntimePlan
ir, plan = wf.compile()

# 导出为 dict（round-trips to YAML）
spec = wf.to_dict()

# 直接写 YAML
wf.dump_yaml("workflows/wf_pipeline.yaml")
```

编译时自动执行 7 条 IR 校验规则：

1. 所有 agent 必须至少有一条入边
2. `__start__` 必须有至少一条出边
3. `__end__` 必须有至少一条入边
4. 不允许自环
5. Switch 表达式语法校验
6. Topic 命名规范（`[a-z0-9._-]`）
7. Agent subscribe/publish 非空

---

## 两种执行模式

### Normal 模式（默认）

每个 `POST /api/runs` 创建一个独立 run，走完图即结束。

### Event-Driven 模式

```python
wf = workflow("bot", event_driven=True)
```

- External Source 持续监听外部事件
- 每条消息共享同一 session run（长生命周期）
- 适合 chatbot / 消息桥 场景
- 切回 normal 时 ext sources/sinks 暂停（不删除）

---

## Guardrails — 全局安全栏

```python
wf = workflow(
    "expensive_pipeline",
    guardrails={
        "per_agent": {
            "max_tokens_per_call": 8000,    # 单次 LLM 调用上限
            "max_cycles": 5,                 # 单 agent 最大循环次数
        },
        "per_run": {
            "max_total_tokens": 200_000,    # 整个 run 的 token 预算
            "max_cycles_per_run": 100,      # 整个 run 的最大事件循环
        },
    },
)
```

运行时 `InProcessGuardrail` 单例同时挂在 LLM Gateway 和 Agent Worker，实时扣减 + 拒绝超额。

---

## 多 Agent 图示例

```python
from agentkit import Agent, workflow, START, END

class Fetcher(Agent):
    subscribe = ["fetch.in"]; publish = ["fetch.out"]
    python_script = 'def handle(p): return {"data": "..."}'

class Analyzer(Agent):
    subscribe = ["analyze.in"]; publish = ["analyze.out"]
    llm = "deepseek/deepseek-chat"
    prompt = "分析数据：{{ payload.data }}"

class Reporter(Agent):
    subscribe = ["report.in"]; publish = ["report.out"]
    llm = "deepseek/deepseek-chat"
    prompt = "生成报告：{{ payload.result }}"

wf = workflow("wf_report")
f, a, r = Fetcher(), Analyzer(), Reporter()
wf.add(f).add(a).add(r)

wf.connect(START, f, via="fetch.in")
wf.connect(f, a, via="analyze.in")
wf.connect(a, r, via="report.in")
wf.connect(r, END)
```

拓扑：
```
__start__ → Fetcher → Analyzer → Reporter → __end__
```
