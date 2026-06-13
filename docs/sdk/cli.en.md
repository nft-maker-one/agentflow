# CLI Commands

Agentflow provides a suite of CLI tools, which can be invoked via the `agentkit` command or `python -m agentkit.cli.main`.

---

## Available After Installation

```bash
pip install -e ".[dev]"
agentkit --help

```

---

## Command Overview

| Command | Description |
| --- | --- |
| `agentkit version` | Print the version number |
| `agentkit init <name>` | Scaffold a new project |
| `agentkit validate <yaml>` | Validate workflow YAML |
| `agentkit compile <yaml>` | Compile to IR JSON |
| `agentkit schema export` | Export IR JSON Schema |
| `agentkit run <yaml>` | Execute workflow locally |
| `agentkit serve` | Start API server + Web UI |

---

## `agentkit init`

```bash
agentkit init my_project
agentkit init my_project --force  # Overwrite existing directory

```

Generated structure:

```text
my_project/
├── workflows/
│   └── wf_hello.yaml       # Example workflow
├── handlers.py             # Example handler module
└── README.md

```

---

## `agentkit validate`

```bash
agentkit validate workflows/wf_pipeline.yaml

```

Executes 7 IR validation rules. Prints `✓ valid` on success, or specific errors on failure.

---

## `agentkit compile`

```bash
# Output to stdout
agentkit compile workflows/wf_pipeline.yaml

# Write to file
agentkit compile workflows/wf_pipeline.yaml -o ir.json

```

Outputs standardized IR JSON (equivalent to `wf.to_dict()`).

---

## `agentkit schema export`

```bash
agentkit schema export -o workflow_schema.json

```

Exports the complete JSON Schema for `WorkflowIR`, useful for YAML validation in IDEs.

---

## `agentkit run`

```bash
agentkit run workflows/wf_hello.yaml \
  --input '{"q": "hello world"}' \
  --handlers handlers \
  --timeout 10 \
  --json

```

### Parameters

| Parameter | Description |
| --- | --- |
| `<yaml>` | Path to the Workflow YAML file |
| `--input` | JSON string or `@file.json` |
| `--handlers` | Python module name (containing `@agent` decorated functions) |
| `--timeout` | Timeout in seconds (default 30) |
| `--json` | Output results in JSON format |

### Behavior

1. Load YAML → Compile IR
2. Import `--handlers` module, match by `template_key`
3. Create `LocalRuntime` (InProcessBus + MockLLM unless a real API key is configured)
4. Execute the run, wait for the terminal state
5. Print the final state + the payload of the last user event

---

## `agentkit serve`

```bash
agentkit serve \
  --workflows workflows/ \
  --handlers handlers \
  --host 0.0.0.0 \
  --port 8080 \
  --mock          # Use MockLLM (no API key required)

```

### Parameters

| Parameter | Default | Description |
| --- | --- | --- |
| `--workflows` | `./workflows` | Directory containing Workflow YAML files |
| `--handlers` | — | Python handler module |
| `--host` | `0.0.0.0` | Bind address |
| `--port` | `8080` | Port |
| `--mock` | — | Enable MockLLM (no API key required) |
| `--deepseek` | — | Automatically read `DEEPSEEK_API_KEY` environment variable |

### Behavior

1. Scan for all `.yaml` files in the `--workflows` directory
2. Compile + deploy each sequentially
3. Start the FastAPI server:
* `/api/*` — REST API
* `/` — React Web UI (static files)


4. Supports real-time live-edit (UI changes trigger instant hot reloading)

### LLM Provider Auto-Detection

Scans environment variables on server startup:

| Environment Variable | Provider |
| --- | --- |
| `DEEPSEEK_API_KEY` | deepseek |
| `OPENAI_API_KEY` | openai |
| `ANTHROPIC_API_KEY` | anthropic |
| `GOOGLE_API_KEY` | gemini |
| `DASHSCOPE_API_KEY` | qwen |

---

## Development Mode (Frontend/Backend Hot Reloading)

```bash
# Terminal 1: Backend
agentkit serve --port 8080 --deepseek

# Terminal 2: Frontend dev server (Vite HMR)
cd web && npm run dev
# Access http://localhost:5173 (auto-proxies /api → 8080)

```

Or using the Makefile:

```bash
make ui-dev    # Start backend + Vite dev simultaneously

```