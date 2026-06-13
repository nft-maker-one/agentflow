.PHONY: help install lint format typecheck \
        test test-unit test-integration test-e2e test-perf \
        up up-core up-observability down logs ps clean \
        ui-install ui-dev ui-build serve serve-mock

help:  ## Show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-20s\033[0m %s\n", $$1, $$2}'

# ============================================================
# Python — install / lint / typecheck
# ============================================================

install:  ## Install dependencies via uv
	uv sync --all-extras

lint:  ## Run ruff lint
	uv run ruff check src tests

format:  ## Run ruff format (auto-fix)
	uv run ruff format src tests
	uv run ruff check --fix src tests

typecheck:  ## Run mypy
	uv run mypy src

# ============================================================
# Tests
# ============================================================

test-unit:  ## Run unit tests (no external deps)
	uv run pytest tests/unit -v

test-integration:  ## Real Kafka+Redis integration tests (requires `make up-core`)
	uv run pytest tests/integration -v -m integration

test-e2e:  ## Full-stack end-to-end test (requires `make up`)
	uv run pytest tests/e2e -v -m e2e -s

test-perf:  ## Performance benchmarks — micro + throughput (requires `make up-core`)
	@echo "=== Micro-benchmarks (pytest-benchmark) ==="
	uv run pytest tests/perf -v -m perf --benchmark-only
	@echo
	@echo "=== Throughput tests (Kafka roundtrip — needs `make up-core`) ==="
	uv run pytest tests/perf -v -m perf --benchmark-disable

test:  test-unit  ## Default: run unit tests

# ============================================================
# Docker stack
# ============================================================

up:  ## Start FULL dev stack (core + observability)
	docker compose --profile default up -d
	@echo
	@echo "  Services:"
	@echo "    Redpanda    : localhost:9092"
	@echo "    Redis       : localhost:6379"
	@echo "    Postgres    : localhost:5432  (agentkit/agentkit)"
	@echo "    Prometheus  : http://localhost:9090"
	@echo "    Grafana     : http://localhost:3000  (admin/admin)"
	@echo "    OTel OTLP   : grpc://localhost:4317  http://localhost:4318"
	@echo
	@echo "  AgentKit probe (start your agentkit process to expose):"
	@echo "    /metrics    : http://localhost:9100/metrics"

up-core:  ## Start only the data plane (redpanda + redis + postgres)
	docker compose --profile core up -d redpanda redis postgres

up-observability:  ## Start only the observability stack (prometheus + grafana + otel)
	docker compose --profile observability up -d otel-collector prometheus grafana

down:  ## Stop and remove all dev infrastructure
	docker compose down -v

logs:  ## Tail logs from all services
	docker compose logs -f --tail=100

ps:  ## Show service status
	docker compose ps

clean:  ## Remove caches and build artifacts
	rm -rf .pytest_cache .ruff_cache .mypy_cache .benchmarks build dist *.egg-info
	rm -rf web/dist web/node_modules
	find . -type d -name __pycache__ -exec rm -rf {} + 2>/dev/null || true

# ============================================================
# Web UI
# ============================================================

ui-install:  ## Install web UI npm dependencies
	cd web && npm install

ui-dev:  ## Run Vite dev server on :5173 (proxies /api to :8080)
	cd web && npm run dev

ui-build:  ## Build production UI bundle into web/dist/
	cd web && npm run build

# ============================================================
# Run server
# ============================================================

serve:  ## Start the FastAPI control plane (auto-detects DEEPSEEK_API_KEY)
	uv run agentkit serve

serve-mock:  ## Start the control plane with mock LLM (no API key needed)
	uv run agentkit serve --mock

ruff:
	ruff check src/agentkit/api/ --select F821
