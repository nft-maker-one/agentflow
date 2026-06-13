"""``agentkit serve`` runtime — wires AppState + FastAPI + uvicorn.

Boot sequence:

1. ``AppState()`` allocates Bus / Orchestrator / HandlerRegistry.
2. ``await state.start()`` opens Bus + tap subscriber + Orchestrator.
3. Optionally import a handlers module so user ``@agent`` /
   :class:`Agent` instances register themselves.
4. Compile + deploy every YAML in ``workflows_dir``.
5. ``create_app(state)`` builds the FastAPI app.
6. ``uvicorn.Server.serve()`` blocks on the HTTP loop until SIGTERM.
"""

from __future__ import annotations

import importlib
import os
import sys
from pathlib import Path
from types import ModuleType

import uvicorn

from agentkit.api.app import create_app
from agentkit.api.state import AppState
from agentkit.common.logging import get_logger
from agentkit.llm import LLMInstanceConfig, build_llm_gateway
from agentkit.runtime.executor import _FunctionExecutor
from agentkit.sdk.agent_class import Agent
from agentkit.sdk.decorators import get_agent_meta
from agentkit.testing import MockLLMGateway
from agentkit.workflow import compile_from_dict, load_workflow_yaml

log = get_logger(__name__)


async def serve(
    *,
    workflows_dir: str | Path = "workflows",
    handlers_module: str | None = "handlers",
    host: str = "0.0.0.0",
    port: int = 8080,
    use_mock_llm: bool = False,
    deepseek: bool = False,
    mq: str = "auto",
    store: str = "auto",
    guardrail: str = "auto",
) -> None:
    """Boot the API server (blocks until SIGTERM).

    ``mq`` selects the EventBus backend (``memory`` | ``kafka`` |
    ``redpanda``); ``store`` selects the RunStore backend (``memory``
    | ``pg``); ``guardrail`` selects the quota backend (``memory`` |
    ``redis``). All default to in-process so no external infra is
    required. Connection details are read from ``AGENTKIT_BUS_*`` /
    ``AGENTKIT_PG_*`` / ``AGENTKIT_GUARDRAIL_*`` env (loaded from
    ``.env``).
    """
    from agentkit.api.backends import (  # noqa: PLC0415
        build_bus,
        build_guardrail,
        build_store,
        resolve_backend_selection,
    )

    # Load .env from CWD upward — so DEEPSEEK_API_KEY *and* the backend
    # connection vars (AGENTKIT_BUS_BROKERS / AGENTKIT_PG_DSN …) are
    # available before we construct the adapters below.
    _autoload_dotenv()

    # Resolve ``auto`` selectors against the now-loaded env: if
    # AGENTKIT_PG_DSN / AGENTKIT_BUS_BROKERS / AGENTKIT_GUARDRAIL_REDIS_URL
    # is present, the matching persistent backend turns on automatically.
    mq, store, guardrail = resolve_backend_selection(
        mq=mq, store=store, guardrail=guardrail,
    )

    from agentkit.api.persistence import build_persistence  # noqa: PLC0415

    bus = build_bus(mq)
    run_store = build_store(store)
    guardrail_backend = build_guardrail(guardrail)
    persistence = build_persistence(store)   # projects/workflows/inbox metadata
    log.info("api.backends.selected", mq=mq, store=store, guardrail=guardrail)

    state = AppState(
        bus=bus, store=run_store, guardrail=guardrail_backend,
        persistence=persistence,
    )
    await state.start()

    mod = _import_handlers(handlers_module) if handlers_module else None
    gateway = _build_gateway(use_mock=use_mock_llm, deepseek=deepseek)
    state.set_llm_gateway(gateway)
    # Wire the shared guardrail into the LLM gateway so token consumption
    # is tracked against per-run quotas (InProcessGuardrail).
    if hasattr(gateway, "_guardrail"):
        gateway._guardrail = state.guardrail

    # Reload persisted projects / workflows / inbox and re-deploy the
    # workflows (needs the gateway, hence after set_llm_gateway).
    await state.restore_from_persistence()

    wf_dir = Path(workflows_dir)
    if wf_dir.exists():
        for yaml_path in sorted(wf_dir.glob("*.yaml")):
            try:
                raw = load_workflow_yaml(yaml_path)
                ir, plan = compile_from_dict(raw)
            except Exception:
                log.exception("api.workflow.compile_failed", path=str(yaml_path))
                continue
            if mod is not None:
                _register_handlers_from_module(state, mod, ir.id, ir.agents)
            await state.deploy_workflow(
                ir, plan,
                raw_spec=raw,
                llm_gateway=gateway,
            )
    else:
        # Not an error: workflows can be created/deployed through the Web
        # UI. Only worth an info line so a bare `agentkit serve` (just the
        # control plane + UI) doesn't look broken.
        log.info(
            "api.workflows_dir.absent",
            path=str(wf_dir),
            hint="no workflows directory — create workflows in the Web UI "
                 "or pass --workflows <dir>",
        )

    app = create_app(state)
    config = uvicorn.Config(
        app=app, host=host, port=port,
        log_config=None, log_level="info",
    )
    server = uvicorn.Server(config)
    log.info(
        "api.server.start", host=host, port=port,
        workflows=sorted(state.ir_by_id),
    )
    await server.serve()


# ============================================================
# Internals
# ============================================================


def _import_handlers(module_name: str) -> ModuleType | None:
    sys.path.insert(0, str(Path.cwd()))
    try:
        mod = importlib.import_module(module_name)
        log.info("api.handlers_module.loaded", module=module_name)
        return mod
    except ModuleNotFoundError as e:
        # Two very different cases hide behind ModuleNotFoundError:
        #   1. the handlers module ITSELF is absent (e.name == module_name)
        #      — totally fine: agents can be defined via the Web UI / the
        #      global handler registry. Not an error, nothing to load.
        #   2. the module exists but one of ITS OWN imports is missing
        #      (e.name != module_name) — that's a real bug worth a warning.
        missing = e.name or ""
        module_itself_absent = (
            missing == module_name or module_name.startswith(missing + ".")
        )
        if module_itself_absent:
            log.info(
                "api.handlers_module.absent",
                module=module_name,
                hint="no local handlers module — define agents in the Web UI "
                     "or pass --handlers <module>",
            )
        else:
            log.warning(
                "api.handlers_module.import_failed",
                module=module_name, error=str(e),
            )
        return None
    except ImportError as e:
        # Module exists but failed to import (syntax / bad import) — real bug.
        log.warning(
            "api.handlers_module.import_failed",
            module=module_name, error=str(e),
        )
        return None


def _parse_dotenv_value(val: str) -> str:
    """Parse the right-hand side of a ``KEY=value`` .env line.

    Handles the common cases a hand-written ``.env`` throws at us:

    * quoted values (``"..."`` / ``'...'``) — quotes stripped, inner
      content (incl. ``#`` and spaces) preserved;
    * **inline comments** on unquoted values — a ``#`` *preceded by
      whitespace* starts a comment and is dropped (so
      ``TOKEN=abc   # note`` → ``abc``);
    * surrounding whitespace — trimmed.

    A ``#`` that is *not* preceded by whitespace stays in the value
    (e.g. ``PASS=a#b`` → ``a#b``), matching standard dotenv behavior;
    quote the value if it legitimately contains `` #``.
    """
    lstripped = val.lstrip()
    if lstripped[:1] in ('"', "'"):
        quote = lstripped[0]
        end = lstripped.find(quote, 1)
        if end != -1:
            return lstripped[1:end]    # content between quotes, verbatim
        return lstripped[1:]           # unterminated quote — best effort
    # Unquoted: a '#' preceded by whitespace starts an inline comment.
    # Scan the RAW value (not lstripped) so a value that is *only* a
    # leading-space comment (``KEY=   # note``) resolves to "" — while a
    # '#' with no preceding space (``KEY=#foo``) stays in the value.
    cut = len(val)
    for i in range(len(val)):
        if val[i] == "#" and i > 0 and val[i - 1] in " \t":
            cut = i
            break
    return val[:cut].strip()


def _autoload_dotenv(max_levels: int = 4) -> None:
    """Walk up from CWD looking for ``.env`` and inject missing keys.

    We don't ship python-dotenv as a dep — this small loader covers
    ``KEY=value`` with blank lines, full-line ``#`` comments, quoted
    values, and **inline comments** (see :func:`_parse_dotenv_value`).
    """
    cwd = Path.cwd()
    for parent in [cwd, *cwd.parents][:max_levels]:
        env_path = parent / ".env"
        if not env_path.is_file():
            continue
        try:
            for raw_line in env_path.read_text().splitlines():
                line = raw_line.strip()
                if not line or line.startswith("#") or "=" not in line:
                    continue
                key, _, val = line.partition("=")
                key = key.strip()
                # Strip an optional ``export `` prefix (common in .env).
                if key.startswith("export "):
                    key = key[len("export "):].strip()
                val = _parse_dotenv_value(val)
                # Don't override env vars the user explicitly set.
                os.environ.setdefault(key, val)
            log.info("api.dotenv.loaded", path=str(env_path))
        except OSError:
            pass
        return  # only the first .env wins


def _build_gateway(*, use_mock: bool, deepseek: bool):  # type: ignore[no-untyped-def]
    if use_mock:
        log.info("api.llm.mock")
        return MockLLMGateway()

    # Auto-detect every provider whose API key is in the environment.
    # Each provider gets registered as a separate instance so the user
    # can select among them per-agent in the UI.
    detected: list[LLMInstanceConfig] = []

    def _key(*envs: str) -> str | None:
        for e in envs:
            v = os.getenv(e)
            if v:
                return v
        return None

    # OpenAI
    if k := _key("OPENAI_API_KEY", "AGENTKIT_LLM_OPENAI_API_KEY"):
        detected.append(LLMInstanceConfig(
            name="openai", adapter="openai", compat="openai", api_key=k,
        ))
    # DeepSeek
    if k := _key("DEEPSEEK_API_KEY", "AGENTKIT_LLM_DEEPSEEK_API_KEY"):
        detected.append(LLMInstanceConfig(
            name="deepseek", adapter="openai", compat="deepseek", api_key=k,
        ))
    # Qwen / DashScope
    if k := _key("QWEN_API_KEY", "AGENTKIT_LLM_QWEN_API_KEY", "DASHSCOPE_API_KEY"):
        detected.append(LLMInstanceConfig(
            name="qwen", adapter="openai", compat="qwen", api_key=k,
        ))
    # Gemini (via Google's OpenAI-compat shim)
    if k := _key("GEMINI_API_KEY", "AGENTKIT_LLM_GEMINI_API_KEY", "GOOGLE_API_KEY"):
        detected.append(LLMInstanceConfig(
            name="gemini", adapter="openai", compat="gemini", api_key=k,
        ))
    # Anthropic (native adapter)
    if k := _key("ANTHROPIC_API_KEY", "AGENTKIT_LLM_ANTHROPIC_API_KEY"):
        detected.append(LLMInstanceConfig(
            name="anthropic", adapter="anthropic", api_key=k,
        ))

    if not detected:
        log.info("api.llm.fallback_mock", reason="no provider keys detected")
        return MockLLMGateway()

    # Pick default: --deepseek flag wins; else prefer deepseek (cheap)
    # if present, then openai, then first detected.
    names = [c.name for c in detected]
    if deepseek and "deepseek" in names:
        default_name = "deepseek"
    elif "deepseek" in names:
        default_name = "deepseek"
    elif "openai" in names:
        default_name = "openai"
    else:
        default_name = names[0]

    log.info(
        "api.llm.providers_detected",
        providers=names, default=default_name,
    )
    return build_llm_gateway(
        instances=detected,
        default_provider=default_name,
        default_model=_DEFAULT_MODEL_BY_PROVIDER.get(default_name),
    )


# Default model per provider — used when the gateway is built with
# auto-detected providers and the user calls ``ctx.llm.chat()`` without
# an explicit model. Per-agent overrides in the UI's LLM dropdown
# always win over this.
_DEFAULT_MODEL_BY_PROVIDER: dict[str, str] = {
    "openai":    "gpt-5",
    "deepseek":  "deepseek-chat",     # cheapest
    "qwen":      "qwen3.6-plus",
    "gemini":    "gemini-3.5-flash",
    "anthropic": "claude-haiku-4-5",
}


def _register_handlers_from_module(
    state: AppState,
    module: ModuleType,
    workflow_id: str,
    agent_keys,
) -> None:
    """Find @agent functions / Agent instances in ``module`` and register
    them on ``state.handler_registry`` under (workflow_id, template_key).
    """
    for name in dir(module):
        obj = getattr(module, name)
        if isinstance(obj, Agent):
            if obj.key not in agent_keys:
                continue
            state.handler_registry.register(
                workflow_id=workflow_id,
                template_key=obj.key,
                executor=_FunctionExecutor(obj.handler),
                replace=True,
            )
            state.agents_by_key[(workflow_id, obj.key)] = obj
            continue
        meta = get_agent_meta(obj)
        if meta is None:
            continue
        if meta.template_key not in agent_keys:
            continue
        state.handler_registry.register(
            workflow_id=workflow_id,
            template_key=meta.template_key,
            executor=_FunctionExecutor(meta.handler),
            replace=True,
        )
