# CLI 命令

Agentflow 提供一组 CLI 工具，通过 `agentkit` 命令或 `python -m agentkit.cli.main` 调用。

---

## 安装后可用

```bash
pip install -e ".[dev]"
agentkit --help
```

---

## 命令总览

| 命令 | 说明 |
|------|------|
| `agentkit version` | 打印版本号 |
| `agentkit init <name>` | 脚手架新项目 |
| `agentkit validate <yaml>` | 校验 workflow YAML |
| `agentkit compile <yaml>` | 编译为 IR JSON |
| `agentkit schema export` | 导出 IR JSON Schema |
| `agentkit run <yaml>` | 本地执行 workflow |
| `agentkit serve` | 启动 API server + Web UI |

---

## `agentkit init`

```bash
agentkit init my_project
agentkit init my_project --force  # 覆盖已有目录
```

生成结构：

```
my_project/
├── workflows/
│   └── wf_hello.yaml      # 示例 workflow
├── handlers.py             # 示例 handler 模块
└── README.md
```

---

## `agentkit validate`

```bash
agentkit validate workflows/wf_pipeline.yaml
```

执行 7 条 IR 校验规则。通过时打印 `✓ valid`，失败时打印具体错误。

---

## `agentkit compile`

```bash
# 输出到 stdout
agentkit compile workflows/wf_pipeline.yaml

# 写入文件
agentkit compile workflows/wf_pipeline.yaml -o ir.json
```

输出标准化 IR JSON（等价于 `wf.to_dict()`）。

---

## `agentkit schema export`

```bash
agentkit schema export -o workflow_schema.json
```

导出 `WorkflowIR` 的完整 JSON Schema，用于 IDE 校验 YAML 文件。

---

## `agentkit run`

```bash
agentkit run workflows/wf_hello.yaml \
  --input '{"q": "hello world"}' \
  --handlers handlers \
  --timeout 10 \
  --json
```

### 参数

| 参数 | 说明 |
|------|------|
| `<yaml>` | Workflow YAML 文件路径 |
| `--input` | JSON 字符串或 `@file.json` |
| `--handlers` | Python 模块名（包含 `@agent` 装饰的函数） |
| `--timeout` | 超时秒数（默认 30） |
| `--json` | 以 JSON 格式输出结果 |

### 行为

1. 加载 YAML → 编译 IR
2. 导入 `--handlers` 模块，按 `template_key` 匹配
3. 创建 `LocalRuntime`（InProcessBus + MockLLM 除非配了真实 API key）
4. 执行 run，等待终态
5. 打印最终状态 + 最后一个 user event 的 payload

---

## `agentkit serve`

```bash
agentkit serve \
  --workflows workflows/ \
  --handlers handlers \
  --host 0.0.0.0 \
  --port 8080 \
  --mock          # 使用 MockLLM（不需要 API key）
```

### 参数

| 参数 | 默认 | 说明 |
|------|------|------|
| `--workflows` | `./workflows` | Workflow YAML 目录 |
| `--handlers` | — | Python handler 模块 |
| `--host` | `0.0.0.0` | 绑定地址 |
| `--port` | `8080` | 端口 |
| `--mock` | — | 启用 MockLLM（无需 API key） |
| `--deepseek` | — | 自动读 `DEEPSEEK_API_KEY` 环境变量 |

### 行为

1. 扫描 `--workflows` 目录下所有 `.yaml` 文件
2. 逐个编译 + 部署
3. 启动 FastAPI server：
   - `/api/*` — REST API
   - `/` — React Web UI（静态文件）
4. 支持实时 live-edit（UI 修改立即热重载）

### LLM Provider 自动探测

server 启动时扫描环境变量：

| 环境变量 | Provider |
|----------|----------|
| `DEEPSEEK_API_KEY` | deepseek |
| `OPENAI_API_KEY` | openai |
| `ANTHROPIC_API_KEY` | anthropic |
| `GOOGLE_API_KEY` | gemini |
| `DASHSCOPE_API_KEY` | qwen |

---

## 开发模式（前后端热重载）

```bash
# 终端 1：后端
agentkit serve --port 8080 --deepseek

# 终端 2：前端 dev server（Vite HMR）
cd web && npm run dev
# 访问 http://localhost:5173 (自动代理 /api → 8080)
```

或使用 Makefile：

```bash
make ui-dev    # 同时启动后端 + Vite dev
```
