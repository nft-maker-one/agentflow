# Agentflow 部署与配置指南

> 本文档面向**首次部署 / 运维 / 接手项目**的工程师，覆盖：
> 1. 本地开发环境一键起服
> 2. 完整 Docker 基础设施栈
> 3. 所有环境变量分类清单（特别是 LLM API Key）
> 4. 控制平面（HTTP API + Web UI）启动方式
> 5. 持久化数据查看
> 6. 生产部署 checklist

---

## 1. 系统要求

| 组件 | 版本 | 用途 |
|---|---|---|
| Python | **3.11+** | 运行时（asyncio、TaskGroup、`Self` 类型） |
| `uv` | latest | 包管理（替代 `pip` + `venv`） |
| Docker + Compose | 任意近期版本 | 跑 Postgres / Redis / Redpanda / 观测栈 |
| Node.js | **v20+**（推荐 v26） | 仅在需要构建 Web UI 时 |
| `make` | GNU Make 3.81+ | Makefile 快捷命令（可选） |

> **macOS / Linux 直接支持。Windows 建议用 WSL2。**

---

## 2. 项目结构概览

```
agentflow/
├── src/agentkit/         # 核心代码
│   ├── api/              # FastAPI 控制平面
│   ├── bus/              # EventBus（InProcess / Redis Streams / Kafka）
│   ├── orchestrator/     # Run 调度 + RunStore（内存 / Postgres）
│   ├── runtime/          # Agent 实例 + 4-gate 流水线
│   ├── llm/              # LLM Gateway + 5 个 provider
│   ├── workflow/         # IR + 编译器
│   ├── notifier/         # 规则引擎 + 通道
│   ├── observability/    # Metrics / Tracing / Audit
│   └── cli/              # `agentkit` 命令行
├── web/                  # Vite + React + Tailwind 控制台
├── tests/                # unit / integration / perf / e2e
├── docs/                 # 本文档所在目录
├── deploy/               # Prometheus / Grafana / OTel 配置
├── docker-compose.yml    # 开发栈
├── Makefile              # 一键命令
└── pyproject.toml
```

---

## 3. 快速启动（5 分钟跑通）

### 3.1 安装依赖

```bash
cd agentflow

# 首次：装 uv（一次性）
curl -LsSf https://astral.sh/uv/install.sh | sh

# 装 Python 依赖（自动建 .venv）
uv sync --all-extras
# 等价: make install
```

### 3.2 配置 API Key

在 **项目根目录** 放一个 `.env` 并且将其加载到当前终端：

```dotenv
# 至少配一个就能跑
DEEPSEEK_API_KEY=sk-xxx
OPENAI_API_KEY=sk-xxx
ANTHROPIC_API_KEY=sk-ant-xxx
GEMINI_API_KEY=AIza-xxx
QWEN_API_KEY=sk-xxx
```

> **加载机制**：`agentkit serve` 启动时自动从 CWD 向上**最多 4 层**寻找 `.env`，遇到的第一个生效。
> 已在 `os.environ` 里的 key 不会被覆盖（`os.environ.setdefault`）。

### 3.3 起基础设施

```bash
# 默认：完整栈（数据面 + 观测面）
make up
# 等价: docker compose --profile default up -d

# 只起核心三件套（快）
make up-core
# 等价: docker compose --profile core up -d
```

`make up` 启动后会打印：

```
Services:
  Redpanda    : localhost:9092
  Redis       : localhost:6379
  Postgres    : localhost:5432  (agentkit/agentkit)
  Prometheus  : http://localhost:9090
  Grafana     : http://localhost:3000  (admin/admin)
```

### 3.4 起控制平面

```bash
# 自动检测 .env 中所有 provider，无 key 时回退到 MockLLM
uv run agentkit serve --workflows examples/workflows --handlers examples.handlers

# 强制 mock（不需要 key，纯本地）
make serve-mock

# 强制 DeepSeek（最便宜，推荐开发用）
uv run agentkit serve --deepseek
```

打开浏览器：

- Web UI： <http://localhost:8080/>
- Swagger： <http://localhost:8080/docs>
- Metrics： <http://localhost:8080/metrics>
- 健康检查：<http://localhost:8080/health>

---

## 4. 环境变量完整清单

> **优先级总规则**：CLI 显式参数 > `.env` > `os.environ`（已有不覆盖）> 框架默认值。

### 4.1 LLM Provider Key（最常用）

| Provider | 简短名 | 推荐 env name | 兼容别名 |
|---|---|---|---|
| OpenAI | `openai` | `OPENAI_API_KEY` | `AGENTKIT_LLM_OPENAI_API_KEY` |
| DeepSeek | `deepseek` | `DEEPSEEK_API_KEY` | `AGENTKIT_LLM_DEEPSEEK_API_KEY` |
| Qwen / DashScope | `qwen` | `QWEN_API_KEY` | `AGENTKIT_LLM_QWEN_API_KEY` / `DASHSCOPE_API_KEY` |
| Gemini | `gemini` | `GEMINI_API_KEY` | `AGENTKIT_LLM_GEMINI_API_KEY` / `GOOGLE_API_KEY` |
| Anthropic | `anthropic` | `ANTHROPIC_API_KEY` | `AGENTKIT_LLM_ANTHROPIC_API_KEY` |

**自动检测规则**（`api/server.py::_build_gateway`）：

> `serve` 启动时会扫描所有 provider 的 env，**每个找到 key 的 provider 都注册成一个独立实例**。默认 provider 选择优先级是：`--deepseek` flag → `deepseek` → `openai` → 第一个找到的。

> 若全部为空（且未传 `--mock`），自动 fallback 到 `MockLLMGateway`，**不会因缺 key 启动失败**。

### 4.2 Postgres（持久化 RunStore）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `AGENTKIT_PG_DSN` | （无） | 完整 DSN，**优先级最高**。例：`postgresql://user:pwd@host:5432/db` |
| `AGENTKIT_PG_HOST` | `localhost` | 主机 |
| `AGENTKIT_PG_PORT` | `5432` | 端口 |
| `AGENTKIT_PG_USER` | `agentkit` | 用户名 |
| `AGENTKIT_PG_PASSWORD` | `agentkit` | 密码 |
| `AGENTKIT_PG_DB` | `agentkit` | 数据库名 |

> 默认值与 `docker-compose.yml` 中 `postgres` 服务一致，**纯本地无需任何配置**。
>
> 控制平面 `serve` 默认仍用 `InMemoryRunStore`（重启丢失）。要启用持久化：
>
> ```python
> from agentkit.orchestrator import PostgresRunStore, Orchestrator
> store = PostgresRunStore(dsn="postgresql://agentkit:agentkit@localhost:5432/agentkit")
> await store.start()
> orch = Orchestrator(bus=bus, store=store)
> ```

### 4.3 Redis（去重 / Guardrail / 跨进程通知）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `REDIS_URL` | `redis://localhost:6379/0` | dedup / pubsub 用（`RedisDedupStore` / `RedisCompletionNotifier`） |
| `AGENTKIT_GUARDRAIL_REDIS_URL` | `redis://localhost:6379/0` | Guardrail Lua 后端（如不同 db 可分库） |

### 4.4 EventBus（前缀 `AGENTKIT_BUS_`）

AgentKit 有三种总线后端,通过 **`AGENTKIT_BUS_BACKEND`** 单一变量选择,**使用前会 TCP 探活,中间件不在线则自动降级为内存总线**(server 照常启动):

| `AGENTKIT_BUS_BACKEND` | 后端 | 适用 |
|---|---|---|
| `redis`（**默认**) | **Redis Streams** | 推荐。`XGROUP CREATE` 是 O(1),没有消费者组 rebalance,完美契合 AgentKit"海量低扇出 topic"的模式 —— deploy/创建 workflow 从 Kafka 的 ~6s 降到 ~5ms |
| `kafka` | Kafka / Redpanda | 需要 Kafka 生态(多分区、高扇出、跨集群)时显式开启 |
| `memory` | 进程内总线 | 纯本地 / 测试,零依赖 |

> 选择逻辑:`AGENTKIT_BUS_BACKEND=redis`(默认)→ 探 Redis(6379)→ 通则用 Redis Streams,不通则内存;`=kafka` → 探 9092,同理;`=memory` 直接内存。

**Redis Streams 配置**(`redis` 后端):

| 变量 | 默认值 | 说明 |
|---|---|---|
| `AGENTKIT_BUS_REDIS_URL` | 回退 `AGENTKIT_REDIS_URL` / `AGENTKIT_GUARDRAIL_REDIS_URL` / `redis://localhost:6379/0` | 连接 URL |
| `AGENTKIT_BUS_KEY_PREFIX` | `ak` | 键命名空间(流 `ak:s:{topic}`、注册集合 `ak:topics`) |
| `AGENTKIT_BUS_MAXLEN` | `10000` | 每流 `XADD MAXLEN`(近似),限内存 |
| `AGENTKIT_BUS_BLOCK_MS` | `400` | `XREADGROUP` 阻塞窗口 |
| `AGENTKIT_BUS_SCAN_INTERVAL_MS` | `500` | 通配订阅跨进程发现新流的轮询间隔 |

**Kafka 配置**(`kafka` 后端):

| 变量 | 默认值 | 说明 |
|---|---|---|
| `AGENTKIT_BUS_BROKERS` | `localhost:9092` | 多 broker 用逗号分隔 |
| `AGENTKIT_BUS_CLIENT_ID` | `agentkit` | producer / consumer client id |
| `AGENTKIT_BUS_PRODUCER_ACKS` | `all` | 生产 ack 级别（`0` / `1` / `all`） |
| `AGENTKIT_BUS_PRODUCER_COMPRESSION` | `gzip` | `gzip` / `snappy` / `lz4` / `zstd` |
| `AGENTKIT_BUS_DEFAULT_PARTITIONS` | `6` | 自动建 topic 时的默认分区数 |
| `AGENTKIT_BUS_MAX_REDELIVERY` | `6` | 进 DLQ 前最大重投次数 |

> 完整列表见 `bus/redis_stream/config.py::RedisStreamSettings` 与 `bus/kafka/config.py::KafkaSettings`。

### 4.5 观测栈（OpenTelemetry / Prometheus）

| 变量 | 默认值 | 说明 |
|---|---|---|
| `OTEL_EXPORTER_OTLP_ENDPOINT` | `http://localhost:4318` | OTLP HTTP collector 地址 |
| `OTEL_SERVICE_NAME` | `agentkit` | service.name resource attr |
| `OTEL_RESOURCE_ATTRIBUTES` | （空） | `key=value,...` 额外标签 |
| `AGENTKIT_TRACING_ENABLED` | `false` | 设为 `true` 才会启用 OTel exporter |
| `AGENTKIT_PROBE_PORT` | `9000` | `/metrics` `/health` `/ready` 探针端口 |

> 在 `agentkit serve` 模式下，metrics 直接挂在主端口 `8080/metrics`，**无需** ProbeServer。

### 4.6 杂项

| 变量 | 默认值 | 说明 |
|---|---|---|
| `LOG_LEVEL` | `info` | `debug` / `info` / `warning` / `error` |
| `AGENTKIT_LOG_FORMAT` | `json` | `json`（structlog 生产）/ `console`（开发彩色） |

---

## 5. CLI 命令一览

```bash
agentkit version              # 打印版本
agentkit init my-proj         # 在 my-proj/ 生成 workflows + handlers 模板
agentkit validate workflow.yaml  # 静态校验 YAML
agentkit compile  workflow.yaml  # 输出 IR JSON（diff 友好）
agentkit run      workflow.yaml --input '{"q":"hi"}'  # 一次性跑
agentkit serve --workflows ./workflows --handlers myapp.handlers
                              # 起控制平面 + Web UI
```

`serve` 选项：

| 选项 | 默认值 | 说明 |
|---|---|---|
| `--workflows` | `workflows` | YAML 目录，启动时全部 deploy |
| `--handlers` | `handlers` | Python 模块，扫描 `@agent` / `Agent` 子类 |
| `--host` | `0.0.0.0` | 监听 host |
| `--port` | `8080` | 监听端口 |
| `--mock` | `false` | 强制 MockLLM |
| `--deepseek` | `false` | 强制以 DeepSeek 为默认 provider |

---

## 6. Web UI 构建（可选）

### 6.1 本地开发模式

```bash
make ui-dev
# 等价:
#   cd web && npm install && npm run dev
```



Vite 起在 `http://localhost:5173`，请求会代理到 `http://localhost:8080/api/*`（即另一终端的 `agentkit serve`）。

![alt text](image.png)
<div align="center"> <i>Agentflow 入口界面 </i> </div>

### 6.2 生产构建

```bash
make ui-build
# cd web && npm run build  → web/dist/

# 之后 agentkit serve 会自动从 web/dist/ serve UI
uv run agentkit serve
```

`api/app.py` 检测到 `web/dist/` 存在就把它挂到根路径 `/`。

---

## 7. 数据持久化与查看

### 7.1 表结构（Postgres `runs` 表）

`PostgresRunStore.start()` 自动建表 + 索引：

| 列 | 类型 | 说明 |
|---|---|---|
| `run_id` | `TEXT PK` | ULID |
| `workflow_id` / `workflow_version` | `TEXT / INT` | 编译产物指纹 |
| `trace_id` | `TEXT` | OTel 关联 |
| `status` | `TEXT` | `Running` / `Succeeded` / `Failed` / `Cancelled` |
| `started_at` / `ended_at` | `TIMESTAMPTZ` | UTC |
| `failure_reason` | `TEXT` | 失败原因 |
| `input` / `output` / `cursor` | `JSONB` | 全量 JSON |
| `created_at` / `updated_at` | `TIMESTAMPTZ` | DDL 默认 `now()` |

### 7.2 查询数据

```bash
# 进 psql
docker exec -it agentkit-postgres psql -U agentkit -d agentkit

# 列出所有 schema 与表
\dn        -- schema 列表
\dt *.*    -- 全表

# 最近 10 条 run
SELECT run_id, workflow_id, status,
       ROUND(EXTRACT(EPOCH FROM (ended_at - started_at))*1000) AS ms
FROM public.runs ORDER BY started_at DESC LIMIT 10;

# p50 / p95 / p99 延迟
SELECT
  percentile_cont(0.50) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ended_at - started_at))*1000) AS p50,
  percentile_cont(0.95) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ended_at - started_at))*1000) AS p95,
  percentile_cont(0.99) WITHIN GROUP (ORDER BY EXTRACT(EPOCH FROM (ended_at - started_at))*1000) AS p99
FROM public.runs WHERE ended_at IS NOT NULL;
```

### 7.3 Redis 查看

```bash
docker exec -it agentkit-redis redis-cli

# 看 dedup key
KEYS dedup:*
TTL  dedup:<event_id>

# 监听完成事件
SUBSCRIBE 'run:terminal:*'
```

### 7.4 观测台

| 端点 | 用途 |
|---|---|
| <http://localhost:9090> | Prometheus（PromQL 查询，scrape `/metrics`） |
| <http://localhost:3000> | Grafana（admin/admin，预置 dashboard） |
| <http://localhost:8080/metrics> | 应用 Prometheus 指标原始输出 |
| <http://localhost:4318> | OTLP HTTP collector |

---

## 8. 测试

```bash
make test-unit            # 527 unit tests，零外部依赖
make test-integration     # 12 integration tests，需 make up-core
make test-e2e             # 全栈端到端（含 metrics scrape 链路验证）
make test-perf            # 13 perf benchmark
```

或：

```bash
uv run pytest tests/unit -q
uv run pytest tests/integration -m integration
uv run pytest tests/perf -m perf -s --benchmark-disable
```

---

## 9. 常见问题

### Q1：`agentkit serve` 一直回退到 MockLLM

→ 检查 `.env` **不在 CWD 也不在向上 4 层目录里**，或 key 名拼错。

```bash
uv run python -c "import os; print({k: v[:8]+'...' for k,v in os.environ.items() if 'KEY' in k})"
```

### Q2：Postgres 连接失败 `connection refused`

→ 确认容器在跑 `docker ps | grep postgres`；端口被占？`lsof -i :5432`。

### Q3：Redpanda / Kafka 消息没消费

→ 默认 `AGENTKIT_BUS_BROKERS=localhost:9092`，跨容器需改为 `redpanda:9092` + 加 `--network agentkit_default`。

### Q4：UI 加载白屏

→ 没构建。先 `make ui-build`，或用 dev 模式 `make ui-dev`（端口 `5173`）。

### Q5：测试中 fakeredis HGETALL 报错

→ `fakeredis 2.35` 在 `decode_responses=True` 时不可靠 — 生产代码已自带 `_to_str` / `_decode_hash` 兜底。

---

## 10. 生产部署 Checklist

> 默认配置面向**本地开发**，生产前请逐项确认：

- [ ] **Postgres**：换成托管 RDS / Aurora，`AGENTKIT_PG_DSN` 走 SSL：`?sslmode=require`
- [ ] **Redis**：换托管 ElastiCache，加 password：`redis://:pwd@host:6379/0`
- [ ] **Kafka**：`AGENTKIT_BUS_BROKERS` 至少 3 broker，`producer_acks=all`
- [ ] **API Key**：用 K8s Secret / Vault，**严禁 commit `.env`**
- [ ] **TLS**：`agentkit serve` 前面挂 nginx / envoy，做 TLS 终结
- [ ] **资源**：JVM 没用到，但 Redpanda / Postgres / Prometheus 都要给足内存
- [ ] **观测**：`AGENTKIT_TRACING_ENABLED=true` + 真实 OTLP endpoint
- [ ] **告警**：Notifier 配 `webhook` 通道指向 PagerDuty / 飞书机器人
- [ ] **备份**：Postgres `pg_dump` 定时；Run 数据是审计源
- [ ] **限流**：`run.max_total_tokens` / `agent.max_tokens_per_call` 在 workflow YAML 里配

---

## 附：一键复盘命令

```bash
# 从零跑通
git clone <repo> && cd agentflow
uv sync --all-extras
make up                         # 起基础设施
echo "DEEPSEEK_API_KEY=sk-..." > ../.env
uv run agentkit serve --deepseek
# → 浏览器打开 http://localhost:8080
```

最小本地 demo（不需要任何外部服务）：

```bash
uv sync
make serve-mock
# → 纯内存跑，立即可用
```
