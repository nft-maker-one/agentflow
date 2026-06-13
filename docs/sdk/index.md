# Agentflow SDK 使用教程

Agentflow 是一个分布式事件驱动 Agent 编排框架。SDK 提供 Python-first 的声明式接口，让你从定义 → 编译 → 本地测试 → 远程部署一路贯通。

---

## 快速入门

```python
from agentkit import Agent, workflow, START, END

class Summarizer(Agent):
    role = "thinking"
    subscribe = ["agent.summarizer.in"]
    publish = ["agent.summarizer.out"]
    llm = "deepseek/deepseek-chat"
    prompt = "用一句话总结：{{ payload.text }}"
    output_field = "summary"

wf = workflow("my_first_wf")
wf.add(Summarizer())
wf.connect(START, Summarizer())
wf.connect(Summarizer(), END)

# 本地测试
from agentkit.testing import LocalRuntime
async with LocalRuntime(wf) as rt:
    run = await rt.run(input={"text": "Agentflow 很好用..."})
    print(run.status)  # Succeeded
```

---

## 文档目录

| 模块 | 说明 |
|------|------|
| [快速入门](getting-started.md) | 安装、Hello World、本地运行 |
| [Agent 定义](agent.md) | Agent 类、ClassVar 字段、handle 机制、python_script |
| [Workflow 构建](workflow.md) | WorkflowDef、connect、switch 路由、guardrail |
| [External I/O](external-io.md) | 外部源/汇：Telegram、Email、Python Script |
| [控制平面客户端](client.md) | AgentKitClient：部署、运行、流式事件 |
| [消息通知（Inbox）](inbox.md) | 事件通知、过滤、归档 |
| [测试工具](testing.md) | LocalRuntime、MockLLM、单 handler 测试 |
| [CLI 命令](cli.md) | init / validate / compile / run / serve |
| [API 参考](api-reference.md) | 全部公开类型、枚举、数据模型 |

---

## 核心概念

```
┌──────────────┐      ┌──────────┐      ┌──────────┐
│ External     │      │  Agent   │      │ External │
│  Source      │─────▶│  Graph   │─────▶│   Sink   │
│ (Telegram…)  │      │ (Bus)    │      │ (SMTP…)  │
└──────────────┘      └──────────┘      └──────────┘
       ▲                    │
       │                    ▼
  POST /api/runs       Inbox 通知
```

- **Agent** — 最小处理单元，订阅 topic、处理、发布 topic
- **Workflow** — Agent 的有向图，编译为不可变 IR
- **EventBus** — topic-based publish/subscribe，解耦 agent 间通信
- **External I/O** — 外部世界与 bus 的桥梁（不进 IR，热插拔）
- **Orchestrator** — Run 生命周期管理 + 终态检测
- **Inbox** — 面向用户的通知 ring-buffer（run 完成、ext 事件、错误）

---

## 安装

```bash
# 从源码
cd agentflow
pip install -e ".[dev]"

# 或用 uv (推荐)
uv pip install -e ".[dev]"
```

## 环境变量参考（按需配置）

下面列出框架会读取的**全部**环境变量。除非特别说明，都是「不设则用默认 / 关闭对应能力」，按需配置即可。

> 框架会自动从 `.env`（当前目录向上逐级查找）加载缺失的变量，所以本地开发把它们写进项目根的 `.env` 即可。

### 1) LLM Provider API Key（用哪家配哪家）

`agentkit serve` 启动时扫描环境，**检测到哪家的 key 就启用哪家**（[server.py `_build_gateway`](../../src/agentkit/api/server.py)）。每家都接受一个「通用名」和一个 `AGENTKIT_LLM_*` 规范名，部分还接受厂商官方别名。

```bash
OPENAI_API_KEY=sk-...        # OpenAI（兼容端点）       别名: AGENTKIT_LLM_OPENAI_API_KEY
DEEPSEEK_API_KEY=sk-...      # DeepSeek                别名: AGENTKIT_LLM_DEEPSEEK_API_KEY
DASHSCOPE_API_KEY=sk-...     # 阿里云 Qwen/DashScope    别名: QWEN_API_KEY / AGENTKIT_LLM_QWEN_API_KEY
GEMINI_API_KEY=...           # Google Gemini           别名: GOOGLE_API_KEY / AGENTKIT_LLM_GEMINI_API_KEY
ANTHROPIC_API_KEY=sk-...     # Claude                  别名: AGENTKIT_LLM_ANTHROPIC_API_KEY
```

- 一条都没配 → 自动退回 **MockLLM**（无需 key，便于本地试跑）。
- 默认 provider 选取顺序：deepseek > openai > 第一个检测到的；`agentkit serve --deepseek` 可强制 deepseek。
- 单个 agent 可在实例化时覆盖：`Agent(llm="qwen/qwen-plus")` 优先于全局默认。

### 2) 外部 I/O 密钥（Telegram / 邮件 source & sink）

集中在 [external_io/env.py](../../src/agentkit/external_io/env.py) 解析：source/sink 的 `config` **省略**这些字段时从环境变量兜底，**显式写在 config 里则覆盖**环境变量。

```bash
# Telegram（telegram source / sink）
TELEGRAM_BOT_TOKEN=123:ABC   # @BotFather 的 Bot Token   别名: AGENTKIT_TELEGRAM_BOT_TOKEN
TELEGRAM_CHAT_ID=-100123     # sink 固定目标 chat（可选） 别名: AGENTKIT_TELEGRAM_CHAT_ID

# 发邮件（email_smtp sink）
SMTP_HOST=smtp.qq.com        # SMTP 服务器              别名: EMAIL_SMTP_HOST
SMTP_PORT=465                # SSL 端口（默认 465）
SMTP_USER=you@qq.com         # 登录邮箱                 别名: EMAIL_USER
SMTP_PASSWORD=授权码          # 邮箱授权码/密码           别名: EMAIL_PASSWORD
SMTP_TO=to@example.com       # 默认收件人               别名: EMAIL_TO
SMTP_SUBJECT=主题             # 默认邮件主题（可选）

# 收邮件（email_imap source）
IMAP_HOST=imap.qq.com        # IMAP 服务器              别名: EMAIL_IMAP_HOST
IMAP_PORT=993                # SSL 端口（默认 993）
IMAP_USER=you@qq.com         # 邮箱                     别名: EMAIL_USER
IMAP_PASSWORD=授权码          # 授权码/密码              别名: EMAIL_PASSWORD
```

### 3) 持久化 / 消息后端

事件总线默认走 **Redis Streams**（检测到 Redis 即用,否则降级内存）；存储 / 限流默认零依赖内存。按需配：

```bash
# Postgres（--store pg）：给整条 DSN，或拆成几段
AGENTKIT_PG_DSN=postgresql://agentkit:agentkit@localhost:5432/agentkit
# 或：AGENTKIT_PG_HOST / AGENTKIT_PG_PORT / AGENTKIT_PG_USER / AGENTKIT_PG_PASSWORD / AGENTKIT_PG_DB
AGENTKIT_PG_POOL_MIN=2       # 连接池下限（默认 2）
AGENTKIT_PG_POOL_MAX=10      # 连接池上限（默认 10）

# 事件总线后端：单一变量选择，使用前 TCP 探活，不在线自动降级为内存总线
AGENTKIT_BUS_BACKEND=redis            # redis(默认) | kafka | memory
#  - redis(默认/推荐)：Redis Streams，XGROUP CREATE O(1) 无 rebalance，
#    部署/创建 workflow 从 Kafka 的 ~6s 降到 ~5ms。连接：
AGENTKIT_BUS_REDIS_URL=redis://localhost:6379/0   # 回退 AGENTKIT_REDIS_URL / 上面的 guardrail URL
#    其余 AGENTKIT_BUS_* 前缀：key_prefix / maxlen / block_ms / scan_interval_ms（见 bus/redis_stream/config.py）
#  - kafka：显式开启 Kafka/Redpanda
AGENTKIT_BUS_BROKERS=localhost:9092   # broker 地址（逗号分隔）
#    其余 AGENTKIT_BUS_* 前缀：producer_acks / max_redelivery / default_partitions …（见 bus/kafka/config.py）

# Redis 限流/配额（--guardrail redis）
AGENTKIT_GUARDRAIL_REDIS_URL=redis://localhost:6379/0
# 其余走 AGENTKIT_GUARDRAIL_* 前缀：fail_mode / default_reservation_ttl_ms / namespace_prefix …
```

### 4) 运行时与可观测性调优（选填，全有默认值）

```bash
# 日志 / 环境（common/config.py，前缀 AGENTKIT_）
AGENTKIT_LOG_LEVEL=INFO      # DEBUG | INFO | WARNING | ERROR
AGENTKIT_LOG_FORMAT=json     # json | console
AGENTKIT_PROFILE=local       # local | staging | prod

# 可观测性（observability/config.py，前缀 AGENTKIT_）
AGENTKIT_OTEL_ENDPOINT=http://localhost:4317   # OTLP 上报地址（不设则不上报）
AGENTKIT_TRACE_SAMPLE_RATE=1.0                 # 采样率 0.0–1.0
AGENTKIT_PROM_PORT=9000                        # Prometheus 指标端口
AGENTKIT_SERVICE_NAME=agentkit

# 单实例并发/重试调优（runtime/config.py，前缀 AGENTKIT_RUNTIME_）
AGENTKIT_RUNTIME_DEFAULT_MAX_CONCURRENT=4      # 每实例并发处理数（默认 4，设 1 退回严格串行）
AGENTKIT_RUNTIME_MAX_HANDLER_RETRIES=3         # handler 失败重试次数
AGENTKIT_RUNTIME_HANDLER_TIMEOUT_MS=60000      # handler 超时
# 其余字段同样走 AGENTKIT_RUNTIME_<字段大写> 形式覆盖（见 runtime/config.py）
```

> 规律：所有 `AGENTKIT_<MODULE>_*` 都是 pydantic-settings 自动绑定的，环境变量名 = 前缀 + 字段名大写。完整字段以各 `config.py` 为准。
