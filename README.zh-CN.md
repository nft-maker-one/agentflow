# Agentflow

[English](README.md) | **中文**

Agentflow 是一个分布式、事件驱动的 Agent 编排框架,目标是让 agent 系统的搭建与编辑像在 Office 里编辑 docx 一样高效。
欢迎每一位对事件驱动 agent workflow 感兴趣的开发者,直接在 GitHub 上联系我,或邮件 wjluo57@gmail.com。

📖 **完整文档:** https://nft-maker-one.github.io/agentflow/ —— 指南、SDK 参考、部署、CLI(中文 / English)。

---

## 为什么是 Agentflow?

大多数 agent 框架把 agent 编排成**一张"你去调用"的图**:节点 + 边,跑在一个共享 state 对象上,每个请求遍历一次。这很适合结构化的、一问一答式的 request → response。

Agentflow 的重心不同:**agent 挂在一条事件总线上。** 每个 agent 只管*订阅* topic、*发布*结果 —— 彼此完全解耦。外部事件(一条 Telegram 消息、一封邮件、一个 webhook)作为**一等公民事件源**注入总线,event-driven 模式让工作流**持续监听**。

> **一句话:** 图式框架是*"调用一张图"*;Agentflow 是*"让事件在总线上流动"*。

### Agentflow vs. 图式编排框架(如 LangGraph)

| | 图式(如 LangGraph) | **Agentflow** |
|---|---|---|
| **编排模型** | 图遍历 + 共享 state | topic **发布/订阅,事件总线** |
| **agent ↔ agent** | 沿边传递共享 state | 总线上解耦的 envelope(扇入 / 扇出) |
| **新增一个 agent** | 重连图 / 改边 | **订阅一个 topic** —— 其余一律不动 |
| **入站触发** | 你调用 API 发起一次 run(+ cron) | **外部源把事件注入**总线(热插拔) |
| **Webhook** | 出站 —— run 完成*后*通知外部 | 入站事件直接驱动 agent |
| **运行形态** | 一次调用 = 一次 run | 单次 run **或**常驻 **event-driven** 模式 |
| **传输后端** | 进程内(+ 托管平台) | **可插拔 broker**:Redis Streams(默认)/ Kafka / 内存 —— 一个环境变量切换,不在线自动降级 |
| **编辑方式** | 代码优先 | **浏览器内实时**编辑拓扑 + prompt,热部署(零停机) |

> 图式框架很优秀 —— 这是**设计取向**的不同,不是"我功能更多"。持久化、可恢复执行、可视化两边都有。

### 亮点

- 🔌 **天生解耦** —— 增/换一个 agent = 一次订阅;工作流其余部分零改动。
- 📨 **外部事件是一等公民** —— Telegram / 邮件 / webhook **源**直接往总线注入,不动工作流定义。
- 🖱️ **像编辑文档一样改它** —— 浏览器里拖拽拓扑、改 prompt、热部署。
- ⚡ **毫秒级部署** —— Redis Streams 消费者组是 `O(1)`(无 rebalance);创建 / 重部署一个 workflow 是毫秒级,而非数秒。
- 🧱 **可插拔后端 + 优雅降级** —— Redis Streams / Kafka / 内存 总线,Postgres / 内存 存储,Redis / 内存 限流;一个环境变量选择,中间件不在线时自动降级。
- 🧾 **每次 run 可回放** —— 持久化**每次运行的拓扑快照 + 完整事件时间线**;归档的 run 能原样回看。
- 🔭 **开箱即用的可运维性** —— 按 run / 按 agent 的 token & cycle 限流、OpenTelemetry trace、Prometheus + Grafana。

### 为什么事件驱动是 agent 系统的大势所趋

真实的 agent 系统不是一问一答 —— 它们是**常驻、反应式**的,持续响应事件流:消息、邮件、webhook、定时、以及彼此的输出。微服务当年正是这样从 RPC 意大利面走向事件流的,理由完全相同:松耦合、扇入 / 扇出、背压、持久化、重放。**Agent 是下一代微服务,它们需要的是一条事件骨干,而不是一张更大的图。**

---

## 快速开始

```bash
# 1) 安装(Python 3.11+,uv: https://docs.astral.sh/uv/)
uv sync --all-extras

# 2) 跑进程内 demo(无需 docker)
.venv/bin/python -m agentkit.cli.main version
.venv/bin/python -m agentkit.cli.main init /tmp/my-bot
cd /tmp/my-bot
agentkit run workflows/wf_hello.yaml --input '{"q":"hi"}' --handlers handlers
```

输出:

```
✓ run.Succeeded  run_id=run_01KST...  (2 branch event(s))
last event on agent.echo.out.reply:
{ "text": "hi" }
```

---

## 两种定义 Workflow 的方式

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

两者都通过同一个 6 步编译器编译成**同一份 `WorkflowIR`**。

---

## 用 Docker 起完整开发栈

```bash
make up          # Redpanda + Redis + PG + Prometheus + Grafana + OTel
make ps          # 查看服务状态
make down        # 拆除
```

| 服务 | 地址 | 说明 |
|---|---|---|
| Redpanda (Kafka) | `localhost:9092` | EventBus broker |
| Redis | `localhost:6379` | 限流配额、去重 |
| Postgres | `localhost:5432` | `agentkit/agentkit` |
| **Prometheus** | http://localhost:9090 | 抓取 AgentKit `/metrics` |
| **Grafana** | http://localhost:3000 | `admin/admin`,AgentKit Overview 看板 |
| OTel Collector | `grpc://localhost:4317` | 接收 OTLP trace |
| AgentKit probe | http://localhost:9100/metrics | 由你的 `agentkit` 进程启动 |

---

## 测试

```bash
make test-unit          # 快速单测,无需 docker(~7s)
make test-integration   # 真实 Kafka + Redis(需先 `make up-core`)
make test-e2e           # 全栈:Kafka + Prometheus 查询(需先 `make up`)
make test-perf          # 性能基准
```

测试布局:

| 目录 | 标记 | 测什么 |
|---|---|---|
| `tests/unit/` | (无) | 全部逻辑,用 InProcessEventBus + mock |
| `tests/integration/` | `integration` | 真实 Kafka / Redis / OpenAI 兼容端点 |
| `tests/e2e/` | `e2e` | 真实 Kafka + Prometheus 抓取校验 |
| `tests/perf/` | `perf` | 吞吐 / 延迟基准 |

---

## 架构

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

**严格分层:** 下层**绝不**导入上层。CI 里用基于 AST 的 `make` 检查强制保证。

---

## CLI 命令

```bash
agentkit version
agentkit init <project>         # 脚手架一个项目(handlers.py + workflows/wf_hello.yaml)
agentkit validate <yaml>        # 静态 IR 校验
agentkit compile <yaml>         # 产出规范化 IR JSON
agentkit schema export          # 导出 WorkflowIR JSON Schema
agentkit run <yaml> --input <json> --handlers <module>
```

---

## 开发

```bash
make lint        # ruff check
make format      # ruff format + 自动修复
make typecheck   # mypy strict
```

### 接入自定义 LLM provider

```python
from agentkit.llm.provider import LLMProvider
class MyProvider:                    # 结构化满足 Protocol 即可
    name = "myco"; capabilities = ...
    async def complete(self, req): ...
    async def stream(self, req): ...
    def count_tokens(self, content, model): ...
```

通过 `LLMGatewayClient(providers={"myco": MyProvider()})` 插入 Gateway。

---

## 模块布局

```
src/agentkit/
  common/         # L0 — config、ids、time、logging、errors
  models/         # L1 — Pydantic 数据模型
  observability/  # L1 — metrics、tracing、audit、probe server
  bus/            # L2 — EventBus Protocol + Kafka + InProcess 适配器
  llm/            # L3 — Gateway 管线 + tokenizer + 限速
  workflow/       # L3 — IR + 编译器(parse/expand/resolve/inject/validate/lower/plan)
  guardrail/      # L3 — Resolver + Redis Lua 后端
  runtime/        # L4 — FSM + 4-Gate + AgentInstance + Worker
  notifier/       # L4 — Rule DSL + matcher + channels + templates
  orchestrator/   # L5 — Run + Switch + Terminal 路由
  sdk/            # L5 — @agent / @judge / IRBuilder / WorkflowDef
  testing/        # L5 — LocalRuntime + MockLLMGateway + run_agent_locally
  cli/            # L6 — Typer app
```

---

## 许可证

MIT
